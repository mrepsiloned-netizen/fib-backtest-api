# ============================================================
# FIB BACKTEST API — FastAPI Backend
# Paste this into main.py on Replit
# ============================================================

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import ccxt
import numpy as np
from datetime import datetime, timezone
from typing import Optional

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── MODELS ────────────────────────────────────────────────
class BacktestRequest(BaseModel):
    symbol: str = "INJ/USDT"
    timeframe: str = "4h"
    start_date: str = "2025-01-01"
    end_date: str = "2026-01-01"
    pivot_n: int = 5
    risk_method: str = "fixed"   # fixed | half_kelly | full_kelly
    risk_pct: float = 0.02
    rr: float = 2.0
    fib_level: float = 0.618
    max_bars: int = 200

# ── FETCH ─────────────────────────────────────────────────
def fetch_candles(symbol, timeframe, start_date, end_date):
    start_ms = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ms   = int(datetime.now(timezone.utc).timestamp() * 1000) if end_date == "now" else \
               int(datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)

    exchanges = [
        ("KuCoin", ccxt.kucoin()),
        ("OKX",    ccxt.okx()),
        ("Bybit",  ccxt.bybit()),
    ]
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
                return all_candles, name
        except:
            continue
    raise Exception("All exchanges failed")

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
def run_backtest(candles, timestamps, pivots, risk_pct, rr, fib_level, max_bars):
    highs = np.array([c[2] for c in candles])
    lows  = np.array([c[3] for c in candles])
    n     = len(candles)

    trades  = []
    equity  = 100.0
    bias    = None
    used    = -1

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
        if rng <= 0: continue
        f618  = fl+rng*fib_level if st=="bear" else fh-rng*fib_level
        sl    = fh+rng*0.02      if st=="bear" else fl-rng*0.02
        rpp   = abs(f618-sl)
        if rpp <= 0: continue
        tp    = f618-rpp*rr      if st=="bear" else f618+rpp*rr
        pos   = (equity*risk_pct)/rpp

        ec = None
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
            "id":         len(trades)+1,
            "direction":  "LONG" if st=="bull" else "SHORT",
            "entry_time": timestamps[ec],
            "exit_time":  timestamps[xc],
            "entry":      round(f618,4),
            "sl":         round(sl,4),
            "tp":         round(tp,4),
            "exit_price": round(xp,4),
            "result":     xr,
            "pnl":        round(pnl,4),
            "equity":     round(equity,4),
            "won":        won,
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

    return {
        "total_trades": len(trades),
        "wins":         len(wins),
        "losses":       len(losses),
        "win_rate":     round(wr,2),
        "final_equity": round(final,2),
        "total_return": round(tr,2),
        "cagr":         round(cagr,2),
        "daily_return": round(tr/max(days,1),3),
        "max_drawdown": round(mdd,2),
        "sharpe":       round(sharpe,2),
        "profit_factor":round(pf,2),
        "avg_win":      round(gw/len(wins),3) if wins else 0,
        "avg_loss":     round(gl/len(losses),3) if losses else 0,
        "max_consec_wins":   max_cw,
        "max_consec_losses": max_cl,
        "kelly_full":   round(wr/100-(1-wr/100)/(gw/len(wins)/(gl/len(losses)) if losses and wins else 1),3) if wins and losses else 0,
        "kelly_half":   round((wr/100-(1-wr/100)/(gw/len(wins)/(gl/len(losses)) if losses and wins else 1))/2,3) if wins and losses else 0,
    }

# ── ROUTES ────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "Fib Backtest API running"}

@app.get("/pairs")
def get_pairs():
    return {"pairs": ["INJ/USDT","BTC/USDT","ETH/USDT","SOL/USDT","BNB/USDT","XRP/USDT"]}

@app.post("/backtest")
def backtest(req: BacktestRequest):
    try:
        candles, source = fetch_candles(req.symbol, req.timeframe, req.start_date, req.end_date)
        timestamps = [c[0] for c in candles]
        highs = np.array([c[2] for c in candles])
        lows  = np.array([c[3] for c in candles])
        pivots = find_pivots(highs, lows, req.pivot_n)

        # Determine risk pct based on method
        risk_pct = req.risk_pct
        if req.risk_method == "half_kelly":
            risk_pct = None  # will be calculated after first pass
        elif req.risk_method == "full_kelly":
            risk_pct = None

        # First pass with fixed 2% to get kelly values
        trades_fixed = run_backtest(candles, timestamps, pivots, 0.02, req.rr, req.fib_level, req.max_bars)
        stats_fixed  = calc_stats(trades_fixed, (candles[-1][0]-candles[0][0])//(86400000))

        # Set risk based on method
        if req.risk_method == "half_kelly" and stats_fixed:
            risk_pct = stats_fixed["kelly_half"]
        elif req.risk_method == "full_kelly" and stats_fixed:
            risk_pct = stats_fixed["kelly_full"]
        else:
            risk_pct = req.risk_pct

        # Final backtest with correct risk
        trades = run_backtest(candles, timestamps, pivots, risk_pct, req.rr, req.fib_level, req.max_bars)
        days   = (candles[-1][0]-candles[0][0])//(86400000)
        stats  = calc_stats(trades, days)

        # Equity curve (sampled every 10 candles for performance)
        eq_curve = []
        eq = 100.0
        ti = 0
        for i, c in enumerate(candles):
            if ti < len(trades) and trades[ti]["exit_time"] <= c[0]:
                eq = trades[ti]["equity"]
                ti += 1
            if i % 10 == 0:
                eq_curve.append({"t": c[0], "eq": round(eq,2)})

        return {
            "success":    True,
            "source":     source,
            "symbol":     req.symbol,
            "timeframe":  req.timeframe,
            "period":     f"{req.start_date} → {req.end_date}",
            "risk_method":req.risk_method,
            "risk_pct":   round(risk_pct*100,2),
            "stats":      stats,
            "equity_curve": eq_curve,
            "trades":     trades[-50:],  # last 50 trades
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/kelly")
def kelly_calc(req: BacktestRequest):
    try:
        candles, source = fetch_candles(req.symbol, req.timeframe, req.start_date, req.end_date)
        timestamps = [c[0] for c in candles]
        highs = np.array([c[2] for c in candles])
        lows  = np.array([c[3] for c in candles])
        pivots = find_pivots(highs, lows, req.pivot_n)
        trades = run_backtest(candles, timestamps, pivots, 0.02, req.rr, req.fib_level, req.max_bars)
        days   = (candles[-1][0]-candles[0][0])//(86400000)
        stats  = calc_stats(trades, days)
        return {"success":True, "kelly_full": stats["kelly_full"], "kelly_half": stats["kelly_half"], "stats": stats}
    except Exception as e:
        return {"success":False, "error":str(e)}
