# ============================================================
# FIB BACKTEST API — FastAPI Backend v6
# Added rules: time expiry, structure invalidation,
# one trade per pair, candle recency filter
# ============================================================

from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import ccxt
import numpy as np
from datetime import datetime, timezone
from typing import List
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
import httpx
import time

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
executor = ThreadPoolExecutor(max_workers=2)

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
    max_bars: int = 200          # time expiry — max candles to wait for entry
    max_hold: int = 200          # max candles to hold trade
    recency_bars: int = 50       # p3 must be within last N candles
    one_per_pair: bool = True    # only one trade per pair at a time

class BatchRequest(BaseModel):
    configs: List[BacktestRequest]

class CompareRequest(BaseModel):
    symbols: List[str] = ["INJ/USDT","BTC/USDT","ETH/USDT"]
    timeframes: List[str] = ["4h"]
    pivot_ns: List[int] = [5]
    risk_methods: List[str] = ["fixed"]
    rr_ratios: List[float] = [2.0, 3.0]
    fib_levels: List[float] = [0.618]
    period_a_start: str = "2025-01-01"
    period_a_end: str = "2026-01-01"
    period_b_start: str = "2026-01-01"
    period_b_end: str = "now"

# ── SUPABASE CANDLE CACHE ─────────────────────────────────
def get_cached_candles(symbol, timeframe, start_ms, end_ms):
    try:
        all_rows = []
        offset = 0
        page_size = 10000
        while True:
            query = (f"symbol=eq.{symbol}&timeframe=eq.{timeframe}"
                     f"&ts=gte.{start_ms}&ts=lte.{end_ms}"
                     f"&order=ts.asc&limit={page_size}&offset={offset}"
                     f"&select=ts,open,high,low,close,volume")
            res = httpx.get(f"{SUPABASE_URL}/rest/v1/candles?{query}", headers=HEADERS, timeout=30)
            if res.status_code == 200:
                rows = res.json()
                if not rows: break
                all_rows += rows
                if len(rows) < page_size: break
                offset += page_size
            else:
                break
        if len(all_rows) > 50:
            return [[r["ts"],r["open"],r["high"],r["low"],r["close"],r["volume"]] for r in all_rows]
    except Exception as e:
        print(f"Candle cache read error: {e}")
    return None

def save_candles(symbol, timeframe, candles):
    try:
        url = f"{SUPABASE_URL}/rest/v1/candles"
        save_headers = {**HEADERS, "Prefer": "resolution=merge-duplicates"}
        for i in range(0, len(candles), 500):
            batch = candles[i:i+500]
            rows = [{"symbol":symbol,"timeframe":timeframe,"ts":c[0],"open":float(c[1]),"high":float(c[2]),"low":float(c[3]),"close":float(c[4]),"volume":float(c[5])} for c in batch]
            res = httpx.post(url, json=rows, headers=save_headers, timeout=30)
            if res.status_code not in [200, 201, 204]:
                print(f"Candle save error {res.status_code}: {res.text[:200]}")
            else:
                print(f"Saved {len(batch)} candles for {symbol} {timeframe}")
    except Exception as e:
        print(f"Candle save error: {e}")

# ── RESULT CACHE ──────────────────────────────────────────
def get_cached_result(symbol, timeframe, pivot_n, risk_method, rr, start_date, end_date, fib_level=0.618):
    try:
        if end_date == "now": return None
        query = (f"symbol=eq.{symbol}&timeframe=eq.{timeframe}"
                 f"&pivot_n=eq.{pivot_n}&risk_method=eq.{risk_method}"
                 f"&rr=eq.{rr}&period_start=eq.{start_date}&period_end=eq.{end_date}"
                 f"&fib_level=eq.{fib_level}"
                 f"&select=*&limit=1")
        res = httpx.get(f"{SUPABASE_URL}/rest/v1/results?{query}", headers=HEADERS, timeout=10)
        if res.status_code == 200:
            rows = res.json()
            if rows:
                r = rows[0]
                return {k:r[k] for k in ["total_trades","wins","losses","win_rate","final_equity",
                    "total_return","cagr","daily_return","max_drawdown","sharpe","profit_factor",
                    "avg_win","avg_loss","max_consec_wins","max_consec_losses","kelly_full","kelly_half"]}
    except Exception as e:
        print(f"Result cache read error: {e}")
    return None

def save_result(symbol, timeframe, pivot_n, risk_method, rr, start_date, end_date, stats, fib_level=0.618):
    try:
        if end_date == "now" or not stats: return
        url = f"{SUPABASE_URL}/rest/v1/results"
        save_headers = {**HEADERS, "Prefer": "return=minimal,resolution=ignore-duplicates"}
        row = {
            "symbol":symbol,"timeframe":timeframe,"pivot_n":pivot_n,
            "risk_method":risk_method,"rr":rr,"fib_level":fib_level,
            "period_start":start_date,"period_end":end_date,
            **{k:stats[k] for k in ["total_trades","wins","losses","win_rate","final_equity",
                "total_return","cagr","daily_return","max_drawdown","sharpe","profit_factor",
                "avg_win","avg_loss","max_consec_wins","max_consec_losses","kelly_full","kelly_half"]},
            "computed_at":int(datetime.now(timezone.utc).timestamp()*1000),
        }
        httpx.post(url, json=row, headers=save_headers, timeout=10)
    except Exception as e:
        print(f"Result save error: {e}")

# ── FETCH CANDLES ─────────────────────────────────────────
_mem_cache = {}

def fetch_candles(symbol, timeframe, start_date, end_date):
    start_ms = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
    end_ms   = int(datetime.now(timezone.utc).timestamp()*1000) if end_date=="now" else \
               int(datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
    cache_key = f"{symbol}_{timeframe}_{start_date}_{end_date}"

    if cache_key in _mem_cache:
        return _mem_cache[cache_key], "Cache (memory)"

    if SUPABASE_URL:
        cached = get_cached_candles(symbol, timeframe, start_ms, end_ms)
        if cached:
            # Accept cache if it has data — crypto has gaps, don't refetch unnecessarily
            print(f"Cache hit: {symbol} {timeframe} {len(cached)} candles")
            _mem_cache[cache_key] = cached
            return cached, "Cache (Supabase)"

    exchanges = [("KuCoin",ccxt.kucoin()),("OKX",ccxt.okx()),("Bybit",ccxt.bybit())]
    exchange_errors = []
    for name, ex in exchanges:
        try:
            time.sleep(1.0)  # rate limit buffer between exchange attempts
            all_candles, since = [], start_ms
            empty_count = 0
            while since < end_ms:
                batch = ex.fetch_ohlcv(symbol, timeframe, since=since, limit=1000)
                if not batch:
                    empty_count += 1
                    if empty_count >= 3: break
                    time.sleep(1)
                    continue
                empty_count = 0
                filtered = [c for c in batch if c[0] < end_ms]
                all_candles += filtered
                last_ts = batch[-1][0]
                if last_ts >= end_ms: break       # reached end of requested range
                if last_ts <= since: break        # no progress — exchange returned same candles
                since = last_ts + 1
                time.sleep(0.2)                   # rate limit protection between batches
            if len(all_candles) > 50:
                _mem_cache[cache_key] = all_candles
                if SUPABASE_URL: save_candles(symbol, timeframe, all_candles)
                return all_candles, name
        except Exception as e:
            print(f"{name} failed: {e}")
            exchange_errors.append(f"{name}: {str(e)[:60]}")
    raise Exception(f"No data for {symbol} {timeframe}. Use Prefetch button first. ({'; '.join(exchange_errors)})")

# ── PIVOTS ────────────────────────────────────────────────

# ── PIVOT DETECTION ───────────────────────────────────────
def find_pivots(highs, lows, N):
    """
    Find pivot highs and lows using N candles left and right.
    A pivot high: highest among 2N+1 candles centered on i.
    A pivot low:  lowest among 2N+1 candles centered on i.
    Deduplicates consecutive same-type pivots.
    """
    pivots = []
    for i in range(N, len(highs) - N):
        if highs[i] == max(highs[i-N:i+N+1]):
            pivots.append({"idx": i, "type": "H", "price": float(highs[i])})
        elif lows[i] == min(lows[i-N:i+N+1]):
            pivots.append({"idx": i, "type": "L", "price": float(lows[i])})
    # Deduplicate consecutive same-type pivots — keep most extreme
    deduped = []
    for p in pivots:
        if not deduped:
            deduped.append(p)
            continue
        last = deduped[-1]
        if last["type"] == p["type"]:
            if p["type"] == "H" and p["price"] > last["price"]: deduped[-1] = p
            elif p["type"] == "L" and p["price"] < last["price"]: deduped[-1] = p
        else:
            deduped.append(p)
    return deduped


# ── BACKTEST CORE ─────────────────────────────────────────
def run_backtest_core(candles, pivots, risk_pct, rr, fib_level, max_bars, max_hold, recency_bars, one_per_pair):
    """
    BOS-based Fibonacci pullback backtest.

    LONG SETUP:
      1. P1 = confirmed pivot HIGH
      2. P3 = first candle that CLOSES above P1 (Break of Structure)
         - Must be at least N_MIN candles after P1
         - If rejected P3 - P1 < N_MIN, P3 becomes new P1
      3. P2 = min(close) between P1 and P3
      4. Range filter: (P3 - P2) / P2 > 0.3%
      5. Draw fib P2 → P3
      6. Entry: candle LOW <= fib618 AND close > fib618 (wick touch + rejection)
      7. Enter at NEXT candle open
      8. SL = P2, TP = entry + (entry - SL) * RR
      9. TP hit → N=1 HH detection for next setup
         SL hit → flip bias to SHORT

    SHORT: mirror logic
    """
    highs      = np.array([c[2] for c in candles])
    lows       = np.array([c[3] for c in candles])
    closes     = np.array([c[4] for c in candles])
    opens      = np.array([c[1] for c in candles])
    n          = len(candles)
    timestamps = [c[0] for c in candles]
    trades     = []
    equity     = 100.0
    bias       = None      # None, "bull", "bear"
    in_trade   = False
    MIN_RANGE  = 0.003     # 0.3% minimum fib range

    if n < 50 or len(pivots) < 2:
        return trades

    # ── Step 1: Precompute all BOS setups in chronological order ──
    # This avoids repeated scanning — one pass to find all valid setups
    def get_all_setups(pivots, bias_filter=None):
        """Find all valid BOS setups in chronological order."""
        setups = []
        ph = [p for p in pivots if p["type"] == "H"]
        pl = [p for p in pivots if p["type"] == "L"]
        N_min = 3

        # LONG setups: pivot HIGH → BOS up
        if bias_filter != "bear":
            for p1 in ph:
                p1_idx   = p1["idx"]
                p1_price = p1["price"]
                # Scan forward for BOS candle
                for ci in range(p1_idx + 1, n - 1):
                    if closes[ci] > p1_price:
                        # Check duration
                        if ci - p1_idx < N_min:
                            # Too close — this P3 becomes new P1, skip
                            break
                        p3_idx   = ci
                        p3_close = closes[ci]
                        # P2 = min close between P1 and P3
                        p2 = float(min(closes[p1_idx:p3_idx + 1]))
                        rng = p3_close - p2
                        if rng <= 0 or rng / max(p2, 1) < MIN_RANGE:
                            break
                        setups.append({
                            "st": "bull",
                            "p1_idx": p1_idx, "p1_price": p1_price,
                            "p2": p2, "p3_idx": p3_idx, "p3_close": p3_close,
                            "rng": rng, "fib618": p2 + rng * fib_level,
                            "sl": p2
                        })
                        break
                    # If price clearly breaks below P1 level by more than range,
                    # P1 is no longer relevant
                    if lows[ci] < p1_price * 0.90:
                        break

        # SHORT setups: pivot LOW → BOS down
        if bias_filter != "bull":
            for p1 in pl:
                p1_idx   = p1["idx"]
                p1_price = p1["price"]
                for ci in range(p1_idx + 1, n - 1):
                    if closes[ci] < p1_price:
                        if ci - p1_idx < N_min:
                            break
                        p3_idx   = ci
                        p3_close = closes[ci]
                        p2 = float(max(closes[p1_idx:p3_idx + 1]))
                        rng = p2 - p3_close
                        if rng <= 0 or rng / max(p2, 1) < MIN_RANGE:
                            break
                        setups.append({
                            "st": "bear",
                            "p1_idx": p1_idx, "p1_price": p1_price,
                            "p2": p2, "p3_idx": p3_idx, "p3_close": p3_close,
                            "rng": rng, "fib618": p2 - rng * fib_level,
                            "sl": p2
                        })
                        break
                    if highs[ci] > p1_price * 1.10:
                        break

        # Sort by P3 index (chronological order)
        setups.sort(key=lambda x: x["p3_idx"])
        return setups

    # ── Step 2: N=1 HH/LL detection after TP ──────────────
    def find_next_anchor(from_idx, direction):
        candidate_idx = from_idx
        max_candles   = 50
        if direction == "bull":
            candidate = highs[from_idx]
            for xi in range(from_idx + 1, min(from_idx + max_candles, n)):
                if highs[xi] > candidate:
                    candidate     = highs[xi]
                    candidate_idx = xi
                else:
                    # Confirmed — new HH
                    p3_close = closes[candidate_idx]
                    p2       = float(min(closes[from_idx:candidate_idx + 1]))
                    rng      = p3_close - p2
                    if rng > 0 and rng / max(p2, 1) >= MIN_RANGE:
                        return {"p2": p2, "p3_close": p3_close,
                                "p3_idx": candidate_idx, "rng": rng,
                                "fib618": p2 + rng * fib_level, "sl": p2}
                    return None
            # Guard: use candidate after max_candles
            p3_close = closes[candidate_idx]
            p2       = float(min(closes[from_idx:candidate_idx + 1]))
            rng      = p3_close - p2
            if rng > 0 and rng / max(p2, 1) >= MIN_RANGE:
                return {"p2": p2, "p3_close": p3_close,
                        "p3_idx": candidate_idx, "rng": rng,
                        "fib618": p2 + rng * fib_level, "sl": p2}
        else:  # bear
            candidate = lows[from_idx]
            for xi in range(from_idx + 1, min(from_idx + max_candles, n)):
                if lows[xi] < candidate:
                    candidate     = lows[xi]
                    candidate_idx = xi
                else:
                    p3_close = closes[candidate_idx]
                    p2       = float(max(closes[from_idx:candidate_idx + 1]))
                    rng      = p2 - p3_close
                    if rng > 0 and rng / max(p2, 1) >= MIN_RANGE:
                        return {"p2": p2, "p3_close": p3_close,
                                "p3_idx": candidate_idx, "rng": rng,
                                "fib618": p2 - rng * fib_level, "sl": p2}
                    return None
            p3_close = closes[candidate_idx]
            p2       = float(max(closes[from_idx:candidate_idx + 1]))
            rng      = p2 - p3_close
            if rng > 0 and rng / max(p2, 1) >= MIN_RANGE:
                return {"p2": p2, "p3_close": p3_close,
                        "p3_idx": candidate_idx, "rng": rng,
                        "fib618": p2 - rng * fib_level, "sl": p2}
        return None

    # ── Step 3: Main trade simulation ─────────────────────
    # Get all setups upfront in chronological order
    all_setups = get_all_setups(pivots)
    setup_idx  = 0   # pointer into all_setups
    active_setup = None
    last_p3_idx  = -1

    ci = 1
    while ci < n - 1:
        if in_trade:
            ci += 1
            continue

        # Get next active setup if none
        if active_setup is None:
            # Find next setup after last_p3_idx that matches bias
            while setup_idx < len(all_setups):
                s = all_setups[setup_idx]
                setup_idx += 1
                if s["p3_idx"] <= last_p3_idx: continue
                if bias == "bear" and s["st"] == "bull": continue
                if bias == "bull" and s["st"] == "bear": continue
                active_setup = s
                ci = s["p3_idx"] + 1  # start scanning from candle after P3
                break
            if active_setup is None:
                break  # no more setups

        st     = active_setup["st"]
        fib618 = active_setup["fib618"]
        sl_lvl = active_setup["sl"]
        p2     = active_setup["p2"]
        p3_idx = active_setup["p3_idx"]

        if ci >= n - 1:
            break

        c_low   = lows[ci]
        c_high  = highs[ci]
        c_close = closes[ci]

        # Invalidation: price broke through P2 (structure invalid)
        if st == "bull" and c_low < p2:
            active_setup = None
            ci += 1
            continue
        if st == "bear" and c_high > p2:
            active_setup = None
            ci += 1
            continue

        # Entry trigger: wick touches fib618, close confirms
        triggered = False
        if st == "bull" and c_low <= fib618 and c_close > fib618:
            triggered = True
        elif st == "bear" and c_high >= fib618 and c_close < fib618:
            triggered = True

        if not triggered:
            ci += 1
            continue

        # Enter at next candle open
        if ci + 1 >= n:
            break
        entry = float(opens[ci + 1])
        rpp   = abs(entry - sl_lvl)
        if rpp <= 0:
            active_setup = None
            ci += 1
            continue
        tp  = entry + rpp * rr if st == "bull" else entry - rpp * rr
        pos = (equity * risk_pct) / rpp

        # Find exit
        in_trade = True
        xc = xp = xr = None
        for xi in range(ci + 2, min(ci + max_hold, n)):
            if st == "bull":
                if lows[xi]  <= sl_lvl: xp = sl_lvl; xr = "SL"; xc = xi; break
                if highs[xi] >= tp:     xp = tp;      xr = "TP"; xc = xi; break
            else:
                if highs[xi] >= sl_lvl: xp = sl_lvl; xr = "SL"; xc = xi; break
                if lows[xi]  <= tp:     xp = tp;      xr = "TP"; xc = xi; break

        if xc is None:
            in_trade     = False
            active_setup = None
            ci += 1
            continue

        pnl    = (entry - xp) * pos if st == "bear" else (xp - entry) * pos
        equity += pnl
        won    = xr == "TP"

        last_p3_idx = p3_idx

        if won:
            # After TP: N=1 detection for next anchor
            anchor = find_next_anchor(xc, st)
            if anchor:
                active_setup = anchor
                active_setup["st"] = st
                ci = anchor["p3_idx"] + 1
            else:
                active_setup = None
                ci = xc + 1
            bias = None
        else:
            # SL hit: flip bias
            bias         = "bull" if st == "bear" else "bear"
            active_setup = None
            ci = xc + 1

        in_trade = False

        trades.append({
            "id": len(trades) + 1,
            "direction": "LONG" if st == "bull" else "SHORT",
            "entry_time": timestamps[ci - 1] if ci > 0 else timestamps[0],
            "exit_time":  timestamps[xc],
            "entry": round(entry, 4),
            "sl":    round(sl_lvl, 4),
            "tp":    round(tp, 4),
            "exit_price": round(xp, 4),
            "result": xr,
            "pnl":    round(pnl, 4),
            "equity": round(equity, 4),
            "won":    won
        })

    return trades

# ── STATS ─────────────────────────────────────────────────
def calc_stats(trades, days):
    if not trades: return None
    wins=[t for t in trades if t["won"]]
    losses=[t for t in trades if not t["won"]]
    final=trades[-1]["equity"]
    tr=(final-100)/100*100
    wr=len(wins)/len(trades)*100
    peak=100; mdd=0
    for t in trades:
        if t["equity"]>peak: peak=t["equity"]
        mdd=max(mdd,(peak-t["equity"])/peak*100)
    max_cw=max_cl=cw=cl=0
    for t in trades:
        if t["won"]: cw+=1;cl=0;max_cw=max(max_cw,cw)
        else: cl+=1;cw=0;max_cl=max(max_cl,cl)
    rets=[t["pnl"]/(t["equity"]-t["pnl"])*100 for t in trades]
    mean=sum(rets)/len(rets)
    std=(sum((r-mean)**2 for r in rets)/len(rets))**0.5
    sharpe=(mean/std*(365**0.5)) if std>0 else 0
    cagr=((final/100)**(365/max(days,1))-1)*100
    gw=sum(t["pnl"] for t in wins)
    gl=abs(sum(t["pnl"] for t in losses))
    pf=gw/gl if gl>0 else 999
    kf=wr/100-(1-wr/100)/(gw/len(wins)/(gl/len(losses))) if wins and losses else 0
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
        if SUPABASE_URL:
            cached_stats = get_cached_result(
                req.symbol, req.timeframe, req.pivot_n,
                req.risk_method, req.rr, req.start_date, req.end_date, req.fib_level
            )
            if cached_stats:
                return {
                    "success":True,"source":"Cache (results DB)",
                    "symbol":req.symbol,"timeframe":req.timeframe,
                    "period":f"{req.start_date} → {req.end_date}",
                    "risk_method":req.risk_method,"risk_pct":req.risk_pct*100,
                    "pivot_n":req.pivot_n,"rr":req.rr,"fib_level":req.fib_level,
                    "stats":cached_stats,"equity_curve":[],"trades":[],
                }

        candles, source = fetch_candles(req.symbol, req.timeframe, req.start_date, req.end_date)
        highs=np.array([c[2] for c in candles])
        lows=np.array([c[3] for c in candles])
        pivots=find_pivots(highs, lows, req.pivot_n)
        days=(candles[-1][0]-candles[0][0])//(86400000)

        trades_fixed=run_backtest_core(candles, pivots, 0.02, req.rr, req.fib_level,
                                        req.max_bars, req.max_hold, req.recency_bars, req.one_per_pair)
        stats_fixed=calc_stats(trades_fixed, days)

        risk_pct=req.risk_pct
        if req.risk_method=="half_kelly" and stats_fixed:
            risk_pct=max(stats_fixed["kelly_half"], 0.01)
        elif req.risk_method=="full_kelly" and stats_fixed:
            risk_pct=max(stats_fixed["kelly_full"], 0.01)

        trades=run_backtest_core(candles, pivots, risk_pct, req.rr, req.fib_level,
                                  req.max_bars, req.max_hold, req.recency_bars, req.one_per_pair)
        stats=calc_stats(trades, days)

        if SUPABASE_URL and stats:
            save_result(req.symbol, req.timeframe, req.pivot_n,
                       req.risk_method, req.rr, req.start_date, req.end_date, stats, req.fib_level)

        eq_curve=[]
        eq=100.0; ti=0
        for i,c in enumerate(candles):
            if ti<len(trades) and trades[ti]["exit_time"]<=c[0]:
                eq=trades[ti]["equity"]; ti+=1
            if i%10==0:
                eq_curve.append({"t":c[0],"eq":round(eq,2)})

        return {
            "success":True,"source":source,
            "symbol":req.symbol,"timeframe":req.timeframe,
            "period":f"{req.start_date} → {req.end_date}",
            "risk_method":req.risk_method,"risk_pct":round(risk_pct*100,2),
            "pivot_n":req.pivot_n,"rr":req.rr,"fib_level":req.fib_level,"stats":stats,
            "equity_curve":eq_curve,"trades":trades[-50:],
        }
    except Exception as e:
        return {
            "success":False,"error":str(e),
            "symbol":req.symbol,"timeframe":req.timeframe,
            "risk_method":req.risk_method,"pivot_n":req.pivot_n,"rr":req.rr
        }

# ── ROUTES ────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status":"Fib Backtest API v6","rules":"time_expiry+invalidation+one_per_pair+recency"}

@app.get("/pairs")
def get_pairs():
    return {"pairs":["INJ/USDT","BTC/USDT","ETH/USDT","SOL/USDT","BNB/USDT","XRP/USDT"]}

@app.get("/cache-status")
def cache_status():
    result_count = 0
    try:
        r1 = httpx.get(f"{SUPABASE_URL}/rest/v1/results?select=id&limit=1000", headers=HEADERS, timeout=5)
        if r1.status_code==200: result_count=len(r1.json())
    except: pass
    return {
        "memory_cache_keys":list(_mem_cache.keys()),
        "supabase_connected":bool(SUPABASE_URL),
        "cached_results":result_count,
    }

# ── PREFETCH SYSTEM ───────────────────────────────────────
_prefetch_status = {}  # key: "symbol_timeframe_start_end" → status dict

def _do_prefetch(symbol, timeframe, start_date, end_date):
    key = f"{symbol}_{timeframe}_{start_date}_{end_date}"
    _prefetch_status[key] = {"status":"running","candles":0,"error":None}
    try:
        candles, source = fetch_candles(symbol, timeframe, start_date, end_date)
        _prefetch_status[key] = {"status":"done","candles":len(candles),"source":source,"error":None}
        print(f"Prefetch done: {symbol} {timeframe} — {len(candles)} candles from {source}")
    except Exception as e:
        _prefetch_status[key] = {"status":"error","candles":0,"error":str(e)}
        print(f"Prefetch error: {symbol} {timeframe} — {e}")

class PrefetchRequest(BaseModel):
    symbol: str = "BTC/USDT"
    timeframe: str = "15m"
    start_date: str = "2025-01-01"
    end_date: str = "2026-01-01"

@app.post("/prefetch")
def prefetch(req: PrefetchRequest, background_tasks: BackgroundTasks):
    key = f"{req.symbol}_{req.timeframe}_{req.start_date}_{req.end_date}"
    if _prefetch_status.get(key, {}).get("status") == "running":
        return {"status":"already_running","message":f"Already fetching {req.symbol} {req.timeframe}"}
    background_tasks.add_task(_do_prefetch, req.symbol, req.timeframe, req.start_date, req.end_date)
    _prefetch_status[key] = {"status":"queued","candles":0,"error":None}
    return {"status":"queued","message":f"Fetching {req.symbol} {req.timeframe} {req.start_date}→{req.end_date} in background"}

@app.get("/prefetch-status")
def prefetch_status_check(symbol: str, timeframe: str, start_date: str, end_date: str):
    key = f"{symbol}_{timeframe}_{start_date}_{end_date}"
    job  = _prefetch_status.get(key, {"status":"not_started","candles":0,"error":None})

    db_count, expected, pct = 0, 0, 0
    try:
        now_ms   = int(datetime.now(timezone.utc).timestamp()*1000)
        end_ms   = now_ms if end_date=="now" else int(datetime.strptime(end_date[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
        start_ms = int(datetime.strptime(start_date[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
        tf_ms    = {"1m":60000,"5m":300000,"15m":900000,"1h":3600000,"4h":14400000,"1d":86400000}
        expected = (end_ms - start_ms) / tf_ms.get(timeframe, 900000)
        print(f"Prefetch status: {symbol} {timeframe} {start_date}→{end_date} expected={int(expected)}")

        # Paginated count — handles large candle sets (e.g. 525k for 1m)
        q_base = f"symbol=eq.{symbol}&timeframe=eq.{timeframe}&ts=gte.{start_ms}&ts=lte.{end_ms}&select=ts"
        offset = 0
        page_size = 10000
        while True:
            res = httpx.get(f"{SUPABASE_URL}/rest/v1/candles?{q_base}&limit={page_size}&offset={offset}",
                           headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}, timeout=30)
            if res.status_code == 200:
                rows = res.json()
                db_count += len(rows)
                if len(rows) < page_size: break
                offset += page_size
            else:
                break
        print(f"Supabase count: {db_count} candles for {symbol} {timeframe}")
        pct = round(db_count / expected * 100, 1) if expected > 0 else 0
    except Exception as e:
        print(f"Prefetch status error: {e}")

    return {
        "symbol":symbol,"timeframe":timeframe,
        "job_status":job["status"],
        "db_candles":db_count,
        "expected":int(expected),
        "completeness_pct":pct,
        "ready": pct >= 75,
        "error":job.get("error"),
    }

@app.get("/results-history")
def results_history():
    try:
        res = httpx.get(f"{SUPABASE_URL}/rest/v1/results?select=*&order=computed_at.desc&limit=500",
                       headers=HEADERS, timeout=10)
        if res.status_code==200:
            return {"success":True,"results":res.json(),"total":len(res.json())}
    except Exception as e:
        return {"success":False,"error":str(e)}

@app.post("/backtest")
def backtest(req: BacktestRequest):
    return process_request(req)

@app.post("/batch")
async def batch(req: BatchRequest):
    loop=asyncio.get_event_loop()
    tasks=[loop.run_in_executor(executor, process_request, cfg) for cfg in req.configs]
    results=await asyncio.gather(*tasks)
    return {"success":True,"results":list(results),"total":len(results)}

@app.post("/compare")
async def compare(req: CompareRequest):
    configs_a, configs_b = [], []
    for sym in req.symbols:
        for tf in req.timeframes:
            for pn in req.pivot_ns:
                for risk in req.risk_methods:
                    for rr in req.rr_ratios:
                        for fib in req.fib_levels:
                            base={"symbol":sym,"timeframe":tf,"pivot_n":pn,
                                  "risk_method":risk,"risk_pct":0.02,"rr":rr,
                                  "fib_level":fib,"max_bars":200,"max_hold":200,
                                  "recency_bars":50,"one_per_pair":True}
                            configs_a.append(BacktestRequest(**{**base,"start_date":req.period_a_start,"end_date":req.period_a_end}))
                            configs_b.append(BacktestRequest(**{**base,"start_date":req.period_b_start,"end_date":req.period_b_end}))

    loop=asyncio.get_event_loop()
    results_a=list(await asyncio.gather(*[loop.run_in_executor(executor, process_request, cfg) for cfg in configs_a]))
    results_b=list(await asyncio.gather(*[loop.run_in_executor(executor, process_request, cfg) for cfg in configs_b]))

    combined=[]
    for a,b in zip(results_a, results_b):
        if not a.get("success") or not a.get("stats"): continue
        if not b.get("success") or not b.get("stats"): continue
        sa,sb=a["stats"],b["stats"]
        avg_return=(sa["total_return"]+sb["total_return"])/2
        avg_sharpe=(sa["sharpe"]+sb["sharpe"])/2
        avg_dd=(sa["max_drawdown"]+sb["max_drawdown"])/2
        consistency=avg_return*(avg_sharpe/max(avg_dd,1)) if avg_dd>0 else 0
        combined.append({
            "symbol":a["symbol"],"timeframe":a["timeframe"],
            "risk":a["risk_method"],"pivot_n":a["pivot_n"],"rr":a["rr"],
            "fib_level":a.get("fib_level", 0.618),
            "period_a_return":round(sa["total_return"],2),
            "period_b_return":round(sb["total_return"],2),
            "period_a_dd":round(sa["max_drawdown"],2),
            "period_b_dd":round(sb["max_drawdown"],2),
            "period_a_sharpe":round(sa["sharpe"],2),
            "period_b_sharpe":round(sb["sharpe"],2),
            "period_a_trades":sa["total_trades"],
            "period_b_trades":sb["total_trades"],
            "avg_return":round(avg_return,2),
            "avg_sharpe":round(avg_sharpe,2),
            "avg_dd":round(avg_dd,2),
            "consistency_score":round(consistency,2),
            "both_positive":sa["total_return"]>0 and sb["total_return"]>0,
        })
    combined.sort(key=lambda x:x["consistency_score"],reverse=True)
    return {"success":True,"results":combined,"total":len(combined),
            "period_a":f"{req.period_a_start} → {req.period_a_end}",
            "period_b":f"{req.period_b_start} → {req.period_b_end}"}

@app.delete("/cache")
def clear_cache():
    _mem_cache.clear()
    return {"status":"Memory cache cleared"}

@app.get("/journal")
def journal():
    try:
        acc_res = httpx.get(
            f"{SUPABASE_URL}/rest/v1/paper_account?id=eq.1&select=*",
            headers=HEADERS, timeout=10
        )
        trades_res = httpx.get(
            f"{SUPABASE_URL}/rest/v1/paper_trades?select=*&order=created_at.desc&limit=500",
            headers=HEADERS, timeout=10
        )
        account = acc_res.json()[0] if acc_res.status_code==200 and acc_res.json() else None
        trades  = trades_res.json() if trades_res.status_code==200 else []
        return {"success":True,"account":account,"trades":trades}
    except Exception as e:
        return {"success":False,"error":str(e)}
