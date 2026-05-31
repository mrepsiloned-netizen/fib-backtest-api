# ============================================================
# FIB BACKTEST API — FastAPI Backend v3
# With Supabase persistent candle caching
# ============================================================

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import ccxt
import numpy as np
from datetime import datetime, timezone
from typing import Optional, List
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
import httpx

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
executor = ThreadPoolExecutor(max_workers=8)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=ignore-duplicates",
}

# ── MODELS ────────────────────────────────────────────────
class BacktestRequest(BaseModel):
    symbol: str = "INJ/USDT"
    timeframe: str = "4h"
    start_date: str = "2025-01-01"
    end_date: str = "2026-01-01"
    pivot_n: int = 5
    risk_method: str = "fixed"
    risk_pct: float = 0.02
    rr: float = 2.0
    fib_level: float = 0.618
    max_bars: int = 200

class BatchRequest(BaseModel):
    configs: List[BacktestRequest]

# ── SUPABASE CACHE ─────────────────────────────────────────
def get_cached_candles(symbol, timeframe, start_ms, end_ms):
    """Pull candles from Supabase if available"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/candles"
        params = {
            "symbol": f"eq.{symbol}",
            "timeframe": f"eq.{timeframe}",
            "ts": f"gte.{start_ms}",
            "ts": f"lte.{end_ms}",
            "order": "ts.asc",
            "limit": "10000",
            "select": "ts,open,high,low,close,volume"
        }
        # Build proper query string
        query = f"symbol=eq.{symbol}&timeframe=eq.{timeframe}&ts=gte.{start_ms}&ts=lte.{end_ms}&order=ts.asc&limit=10000&select=ts,open,high,low,close,volume"
        res = httpx.get(f"{url}?{query}", headers=HEADERS, timeout=10)
        if res.status_code == 200:
            rows = res.json()
            if len(rows) > 50:
                candles = [[r["ts"],r["open"],r["high"],r["low"],r["close"],r["volume"]] for r in rows]
                return candles
    except Exception as e:
        print(f"Cache read error: {e}")
    return None

def save_candles_to_cache(symbol, timeframe, candles):
    """Save candles to Supabase in batches"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/candles"
        # Save in batches of 500
        batch_size = 500
        for i in range(0, len(candles), batch_size):
            batch = candles[i:i+batch_size]
            rows = [{"symbol":symbol,"timeframe":timeframe,"ts":c[0],"open":c[1],"high":c[2],"low":c[3],"close":c[4],"volume":c[5]} for c in batch]
            httpx.post(url, json=rows, headers=HEADERS, timeout=15)
        print(f"Saved {len(candles)} candles to Supabase cache")
    except Exception as e:
        print(f"Cache write error: {e}")

# ── FETCH ─────────────────────────────────────────────────
_mem_cache = {}

def fetch_candles(symbol, timeframe, start_date, end_date):
    start_ms = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ms   = int(datetime.now(timezone.utc).timestamp() * 1000) if end_date == "now" else \
               int(datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)

    cache_key = f"{symbol}_{timeframe}_{start_date}_{end_date}"

    # 1. Check in-memory cache first (fastest)
    if cache_key in _mem_cache:
        print(f"Memory cache hit: {cache_key}")
        return _mem_cache[cache_key], "Cache (memory)"

    # 2. Check Supabase cache
    if SUPABASE_URL:
        cached = get_cached_candles(symbol, timeframe, start_ms, end_ms)
        if cached:
            print(f"Supabase cache hit: {cache_key} ({len(cached)} candles)")
            _mem_cache[cache_key] = cached
            return cached, "Cache (Supabase)"

    # 3. Fetch from exchange
    exchanges = [("KuCoin", ccxt.kucoin()), ("OKX", ccxt.okx()), ("Bybit", ccxt.bybit())]
    for name, ex in exchanges:
        try:
            all_candles, since = [], start_ms
            while since < end_ms:
                batch = ex.fetch_ohlcv(symbol, timeframe, since=since, limit=1000)
                if not batch: break
                all_candles += [c for c in batch if c[0] < end_ms]
                if len(batch) < 1000: break
                since = batch[-1][0] + 1
            if len(all_candles) > 50:
                print(f"Fetched {len(all_candles)} candles from {name}")
                # Save to both caches
                _mem_cache[cache_key] = all_candles
                if SUPABASE_URL:
                    save_candles_to_cache(symbol, timeframe, all_candles)
                return all_candles, name
        except Exception as e:
            print(f"{name} failed: {e}")
            continue

    raise Exception(f"All exchanges failed for {symbol} {timeframe}")

# ── PIVOTS ────────────────────────────────────────────────
def find_pivots(highs, lows, N):
    pivots = []
    for i in range(N, len(highs) - N):
        if highs[i] == max(highs[i-N:i+N+1]):
            pivots.append({"idx":i,"type":"H","price":float(highs[i])})
        elif lows[i] == min(lows[i-N:i+N+1]):
            pivots.append({"idx":i,"type":"L","price":float(lows[i])})
    deduped = []
    for p in pivots:
        if not deduped: deduped.append(p); continue
        last = deduped[-1]
        if last["type"] == p["type"]:
            if p["type"]=="H" and p["price"] > last["price"]: deduped[-1] = p
            elif p["type"]=="L" and p["price"] < last["price"]: deduped[-1] = p
        else:
            deduped.append(p)
    return deduped

# ── BACKTEST ──────────────────────────────────────────────
def run_backtest_core(candles, pivots, risk_pct, rr, fib_level, max_bars):
    highs = np.array([c[2] for c in candles])
    lows  = np.array([c[3] for c in candles])
    n     = len(candles)
    timestamps = [c[0] for c in candles]
    trades, equity, bias, used = [], 100.0, None, -1

    for pi in range(2, len(pivots)):
        p1,p2,p3 = pivots[pi-2],pivots[pi-1],pivots[pi]
        st = None
        if p1["type"]=="H" and p2["type"]=="L" and p3["type"]=="H" and p3["price"]<p1["price"]: st="bear"
        elif p1["type"]=="L" and p2["type"]=="H" and p3["type"]=="L" and p3["price"]>p1["price"]: st="bull"
        if not st or p3["idx"]<=used: continue
        if bias and bias!=st: continue

        fh  = p1["price"] if st=="bear" else p2["price"]
        fl  = p2["price"] if st=="bear" else p1["price"]
        rng = fh - fl
        if rng<=0: continue
        f618  = fl+rng*fib_level if st=="bear" else fh-rng*fib_level
        sl    = fh+rng*0.02      if st=="bear" else fl-rng*0.02
        rpp   = abs(f618-sl)
        if rpp<=0: continue
        tp    = f618-rpp*rr      if st=="bear" else f618+rpp*rr
        pos   = (equity*risk_pct)/rpp

        ec=None
        for ci in range(p3["idx"]+1, min(p3["idx"]+max_bars,n)):
            if st=="bear":
                if highs[ci]>fh: break
                if highs[ci]>=f618: ec=ci; break
            else:
                if lows[ci]<fl: break
                if lows[ci]<=f618: ec=ci; break
        if ec is None: continue

        xc=xp=xr=None
        for ci in range(ec+1, min(ec+max_bars,n)):
            if st=="bear":
                if highs[ci]>=sl: xp=sl;xr="SL";xc=ci;break
                if lows[ci]<=tp:  xp=tp;xr="TP";xc=ci;break
            else:
                if lows[ci]<=sl:  xp=sl;xr="SL";xc=ci;break
                if highs[ci]>=tp: xp=tp;xr="TP";xc=ci;break
        if xc is None: continue

        pnl    = (f618-xp)*pos if st=="bear" else (xp-f618)*pos
        equity += pnl
        won    = xr=="TP"
        bias   = None if won else ("bull" if st=="bear" else "bear")
        trades.append({
            "id":len(trades)+1,"direction":"LONG" if st=="bull" else "SHORT",
            "entry_time":timestamps[ec],"exit_time":timestamps[xc],
            "entry":round(f618,4),"sl":round(sl,4),"tp":round(tp,4),
            "exit_price":round(xp,4),"result":xr,
            "pnl":round(pnl,4),"equity":round(equity,4),"won":won
        })
        used = p3["idx"]
    return trades

# ── STATS ─────────────────────────────────────────────────
def calc_stats(trades, days):
    if not trades: return None
    wins   = [t for t in trades if t["won"]]
    losses = [t for t in trades if not t["won"]]
    final  = trades[-1]["equity"]
    tr     = (final-100)/100*100
    wr     = len(wins)/len(trades)*100
    peak=100; mdd=0
    for t in trades:
        if t["equity"]>peak: peak=t["equity"]
        mdd=max(mdd,(peak-t["equity"])/peak*100)
    max_cw=max_cl=cw=cl=0
    for t in trades:
        if t["won"]: cw+=1;cl=0;max_cw=max(max_cw,cw)
        else: cl+=1;cw=0;max_cl=max(max_cl,cl)
    rets  = [t["pnl"]/(t["equity"]-t["pnl"])*100 for t in trades]
    mean  = sum(rets)/len(rets)
    std   = (sum((r-mean)**2 for r in rets)/len(rets))**0.5
    sharpe= (mean/std*(365**0.5)) if std>0 else 0
    cagr  = ((final/100)**(365/max(days,1))-1)*100
    gw    = sum(t["pnl"] for t in wins)
    gl    = abs(sum(t["pnl"] for t in losses))
    pf    = gw/gl if gl>0 else 999
    kf    = wr/100-(1-wr/100)/(gw/len(wins)/(gl/len(losses))) if wins and losses else 0
    return {
        "total_trades":len(trades),"wins":len(wins),"losses":len(losses),
        "win_rate":round(wr,2),"final_equity":round(final,2),
        "total_return":round(tr,2),"cagr":round(cagr,2),
        "daily_return":round(tr/max(days,1),3),"max_drawdown":round(mdd,2),
        "sharpe":round(sharpe,2),"profit_factor":round(pf,2),
        "avg_win":round(gw/len(wins),3) if wins else 0,
        "avg_loss":round(gl/len(losses),3) if losses else 0,
        "max_consec_wins":max_cw,"max_consec_losses":max_cl,
        "kelly_full":round(kf,3),"kelly_half":round(kf/2,3),
    }

# ── PROCESS ONE REQUEST ───────────────────────────────────
def process_request(req: BacktestRequest):
    try:
        candles, source = fetch_candles(req.symbol, req.timeframe, req.start_date, req.end_date)
        highs = np.array([c[2] for c in candles])
        lows  = np.array([c[3] for c in candles])
        pivots = find_pivots(highs, lows, req.pivot_n)
        days   = (candles[-1][0]-candles[0][0])//(86400000)

        trades_fixed = run_backtest_core(candles, pivots, 0.02, req.rr, req.fib_level, req.max_bars)
        stats_fixed  = calc_stats(trades_fixed, days)

        risk_pct = req.risk_pct
        if req.risk_method == "half_kelly" and stats_fixed:
            risk_pct = max(stats_fixed["kelly_half"], 0.01)
        elif req.risk_method == "full_kelly" and stats_fixed:
            risk_pct = max(stats_fixed["kelly_full"], 0.01)

        trades = run_backtest_core(candles, pivots, risk_pct, req.rr, req.fib_level, req.max_bars)
        stats  = calc_stats(trades, days)

        eq_curve = []
        eq = 100.0; ti = 0
        for i, c in enumerate(candles):
            if ti < len(trades) and trades[ti]["exit_time"] <= c[0]:
                eq = trades[ti]["equity"]; ti += 1
            if i % 10 == 0:
                eq_curve.append({"t":c[0],"eq":round(eq,2)})

        return {
            "success":True,"source":source,
            "symbol":req.symbol,"timeframe":req.timeframe,
            "period":f"{req.start_date} → {req.end_date}",
            "risk_method":req.risk_method,"risk_pct":round(risk_pct*100,2),
            "pivot_n":req.pivot_n,"stats":stats,
            "equity_curve":eq_curve,"trades":trades[-50:],
        }
    except Exception as e:
        return {
            "success":False,"error":str(e),
            "symbol":req.symbol,"timeframe":req.timeframe,
            "risk_method":req.risk_method,"pivot_n":req.pivot_n
        }

# ── ROUTES ────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status":"Fib Backtest API v3 running","cache":"Supabase" if SUPABASE_URL else "memory only"}

@app.get("/pairs")
def get_pairs():
    return {"pairs":["INJ/USDT","BTC/USDT","ETH/USDT","SOL/USDT","BNB/USDT","XRP/USDT"]}

@app.get("/cache-status")
def cache_status():
    return {"memory_cache_keys": list(_mem_cache.keys()), "supabase_connected": bool(SUPABASE_URL)}

@app.post("/backtest")
def backtest(req: BacktestRequest):
    return process_request(req)

@app.post("/batch")
async def batch(req: BatchRequest):
    loop = asyncio.get_event_loop()
    tasks = [loop.run_in_executor(executor, process_request, cfg) for cfg in req.configs]
    results = await asyncio.gather(*tasks)
    return {"success":True,"results":list(results),"total":len(results)}

@app.delete("/cache")
def clear_cache():
    _mem_cache.clear()
    return {"status":"Memory cache cleared"}
