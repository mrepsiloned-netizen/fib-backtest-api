# ============================================================
# FIB BACKTEST API — FastAPI Backend v8
# All engines now consistent with live trading:
#   - Pairs: XRP, DOGE, TRX, XLM, ADA, ARB (low price, high volume)
#   - Touch mode: fills at exact fib618 (limit order on Bybit)
#   - Rejection/Reclaim: fills at next candle open
#   - Fees on notional: 0.02% entry, 0.02% TP exit, 0.055% SL exit
#   - All engines (original/classic/classic_v2/structure) fee-adjusted
#   - Pivot confirmation lag on original engine
# ============================================================

from fastapi import FastAPI, BackgroundTasks, Request
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
    symbol: str = "XRP/USDT"
    timeframe: str = "4h"
    start_date: str = "2025-01-01"
    end_date: str = "2026-01-01"
    pivot_n: int = 5
    risk_method: str = "fixed"
    risk_pct: float = 0.02
    rr: float = 2.0
    fib_level: float = 0.618
    max_bars: int = 200
    max_hold: int = 1000
    recency_bars: int = 50
    one_per_pair: bool = True
    min_swing_pct: float = 0.002
    stop_buffer_pct: float = 0.001
    k_stale: int = 0
    entry_mode: str = "rejection"  # touch | rejection | reclaim
    engine: str = "original"      # original | classic | classic_v2 | structure
    use_ema_filter: bool = False   # True = only trade in EMA trend direction
    ema_fast: int = 34             # fast EMA period
    ema_slow: int = 55             # slow EMA period
    adx_period: int = 14           # ADX period for trend strength filter
    adx_threshold: float = 25.0    # ADX minimum value to allow trades

class BatchRequest(BaseModel):
    configs: List[BacktestRequest]

class CompareRequest(BaseModel):
    symbols: List[str] = ["XRP/USDT","DOGE/USDT","TRX/USDT","XLM/USDT","ADA/USDT","ARB/USDT"]
    timeframes: List[str] = ["4h"]
    pivot_ns: List[int] = [5]
    risk_methods: List[str] = ["fixed"]
    rr_ratios: List[float] = [2.0, 3.0]
    fib_levels: List[float] = [0.618]
    engines: List[str] = ["original"]
    entry_modes: List[str] = ["touch"]
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
        save_headers = {**HEADERS, "Prefer": "return=minimal,resolution=ignore-duplicates"}
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
            interval_ms = {"1m":60000,"5m":300000,"15m":900000,"1h":3600000,"4h":14400000,"1d":86400000}.get(timeframe,60000)
            expected    = (end_ms - start_ms) / interval_ms
            coverage    = len(cached) / expected
            print(f"Cache hit: {symbol} {timeframe} {len(cached)} candles ({coverage*100:.0f}% of expected)")
            if coverage >= 0.70:
                _mem_cache[cache_key] = cached
                return cached, "Cache (Supabase)"
            else:
                print(f"Cache incomplete ({coverage*100:.0f}%), fetching fresh data...")

    exchanges = [("KuCoin",ccxt.kucoin()),("OKX",ccxt.okx()),("Bybit",ccxt.bybit())]
    exchange_errors = []
    for name, ex in exchanges:
        try:
            time.sleep(1.0)
            all_candles, since = [], start_ms
            empty_count = 0
            retry_count = 0
            while since < end_ms:
                try:
                    batch = ex.fetch_ohlcv(symbol, timeframe, since=since, limit=1000)
                except Exception as re:
                    if "429" in str(re) or "rate" in str(re).lower() or "too many" in str(re).lower():
                        retry_count += 1
                        if retry_count > 5: break
                        wait = retry_count * 3
                        print(f"Rate limited on {name}, waiting {wait}s...")
                        time.sleep(wait)
                        continue
                    raise
                if not batch:
                    empty_count += 1
                    if empty_count >= 3: break
                    time.sleep(1)
                    continue
                empty_count = 0
                retry_count = 0
                filtered = [c for c in batch if c[0] < end_ms]
                all_candles += filtered
                last_ts = batch[-1][0]
                if last_ts >= end_ms: break
                if last_ts <= since: break
                since = last_ts + 1
                time.sleep(0.5)
            if len(all_candles) > 50:
                _mem_cache[cache_key] = all_candles
                if SUPABASE_URL: save_candles(symbol, timeframe, all_candles)
                return all_candles, name
        except Exception as e:
            print(f"{name} failed: {e}")
            exchange_errors.append(f"{name}: {str(e)[:60]}")
    raise Exception(f"No data for {symbol} {timeframe}. Use Prefetch button first. ({'; '.join(exchange_errors)})")

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
            pivots.append({"idx": i, "type": "H", "price": float(highs[i]), "n": N})
        elif lows[i] == min(lows[i-N:i+N+1]):
            pivots.append({"idx": i, "type": "L", "price": float(lows[i]), "n": N})
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
def run_backtest_core(candles, pivots, risk_pct, rr, fib_level, max_bars, max_hold, recency_bars, one_per_pair, min_swing_pct=0.002, stop_buffer_pct=0.001, k_stale=0, entry_mode="rejection", engine="structure", use_ema_filter=False, ema_fast=34, ema_slow=55, adx_period=14, adx_threshold=25.0):
    """
    Dispatches to the appropriate engine.
    Original engine has live-realism fixes applied (v7).
    Classic/structure engines unchanged.
    """
    highs      = np.array([c[2] for c in candles])
    lows       = np.array([c[3] for c in candles])
    closes     = np.array([c[4] for c in candles])
    opens      = np.array([c[1] for c in candles])
    n          = len(candles)
    timestamps = [c[0] for c in candles]
    trades     = []
    equity     = 100.0
    N          = pivots[0].get("n", 3) if pivots else 3

    if n < 50 or len(pivots) < 2:
        return trades

    # ── EMA FILTER ────────────────────────────────────────────
    # Compute EMA fast and slow on close prices
    # ema_trend[i] = "bull" if ema_fast > ema_slow, "bear" if ema_fast < ema_slow
    def calc_ema(arr, period):
        ema = np.zeros(len(arr))
        k   = 2 / (period + 1)
        ema[0] = arr[0]
        for i in range(1, len(arr)):
            ema[i] = arr[i] * k + ema[i-1] * (1 - k)
        return ema

    ema_f = calc_ema(closes, ema_fast) if use_ema_filter else None
    ema_s = calc_ema(closes, ema_slow) if use_ema_filter else None

    def ema_allows(idx, direction):
        """
        Returns True if trade is allowed at candle idx.
        Checks two independent filters — either can block a trade:
          1. EMA trend filter (if use_ema_filter=True)
          2. ADX trending filter (if adx_threshold > 0)
        """
        if idx >= n: return False
        # EMA trend direction filter
        if use_ema_filter:
            if direction == "bull" and ema_f[idx] <= ema_s[idx]: return False
            if direction == "bear" and ema_f[idx] >= ema_s[idx]: return False
        # ADX trending market filter — blocks all trades in ranging markets
        if adx_threshold > 0 and adx_vals[idx] < adx_threshold: return False
        return True

    # ── ADX CALCULATION ───────────────────────────────────────
    def calc_adx(period):
        """Calculate ADX, +DI, -DI arrays."""
        adx_arr  = np.zeros(n)
        plus_di  = np.zeros(n)
        minus_di = np.zeros(n)
        tr_arr   = np.zeros(n)
        plus_dm  = np.zeros(n)
        minus_dm = np.zeros(n)

        for i in range(1, n):
            h_diff = highs[i] - highs[i-1]
            l_diff = lows[i-1] - lows[i]
            plus_dm[i]  = h_diff if h_diff > l_diff and h_diff > 0 else 0
            minus_dm[i] = l_diff if l_diff > h_diff and l_diff > 0 else 0
            tr_arr[i]   = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))

        # Smooth with Wilder's method
        sm_tr = sm_pdm = sm_mdm = 0.0
        for i in range(1, period+1):
            sm_tr  += tr_arr[i]
            sm_pdm += plus_dm[i]
            sm_mdm += minus_dm[i]

        dx_arr = np.zeros(n)
        for i in range(period+1, n):
            sm_tr  = sm_tr  - sm_tr/period  + tr_arr[i]
            sm_pdm = sm_pdm - sm_pdm/period + plus_dm[i]
            sm_mdm = sm_mdm - sm_mdm/period + minus_dm[i]
            pdi = (sm_pdm/sm_tr*100) if sm_tr > 0 else 0
            mdi = (sm_mdm/sm_tr*100) if sm_tr > 0 else 0
            plus_di[i]  = pdi
            minus_di[i] = mdi
            s = pdi + mdi
            dx_arr[i]   = (abs(pdi-mdi)/s*100) if s > 0 else 0

        # Smooth DX to get ADX
        adx_sum = 0.0
        start   = period * 2
        for i in range(period+1, start+1):
            adx_sum += dx_arr[i]
        if start < n:
            adx_arr[start] = adx_sum / period
        for i in range(start+1, n):
            adx_arr[i] = (adx_arr[i-1] * (period-1) + dx_arr[i]) / period

        return adx_arr, plus_di, minus_di

    adx_vals, plus_di_vals, minus_di_vals = calc_adx(adx_period)

    def adx_trending(idx):
        """Return True if ADX indicates trending market at candle idx."""
        if idx >= n: return False
        return adx_vals[idx] >= adx_threshold

    def is_pivot_high(idx):
        return any(p["idx"] == idx and p["type"] == "H" for p in pivots)

    def is_pivot_low(idx):
        return any(p["idx"] == idx and p["type"] == "L" for p in pivots)

    pivot_highs = {p["idx"]: p["price"] for p in pivots if p["type"] == "H"}
    pivot_lows  = {p["idx"]: p["price"] for p in pivots if p["type"] == "L"}

    # ── ORIGINAL ENGINE (v7 — live-realistic) ─────────────
    def run_original():
        """
        Original v6 logic with live-realism fixes:

        KEY FIX 1 — Pivot confirmation lag:
          In live trading, a pivot at index i can only be confirmed after
          N candles have closed to its right (that's how find_pivots works).
          So when P3 is at index p3_idx, the earliest we could know it's
          a valid pivot is candle p3_idx + N. Entry search therefore starts
          at p3_idx + N + 1 (one more for the actual entry candle open).
          Without this fix, backtest enters on setups that wouldn't be
          recognisable yet in live — inflating WR and trade count.

        KEY FIX 2 — Realistic entry price:
          Backtest previously entered at exact fib618 price.
          Live bot places a market order, so it fills at the OPEN of the
          candle after the fib touch. Entry is now opens[ec+1].
          fib_entry field still shows the planned fib level for reference.

        KEY FIX 3 — PnL uses actual fill price:
          Both the position sizing and PnL calculation now use actual_entry
          instead of fib618, matching the live bot's accounting.

        SL/TP behaviour (unchanged, confirmed correct):
          Checked on each candle's high/low — no close required.
          SL checked before TP on same candle (conservative, standard).
        """
        nonlocal equity
        orig_trades = []
        bias_o      = None
        used        = -1

        highs_ = np.array([c[2] for c in candles])
        lows_  = np.array([c[3] for c in candles])

        for pi in range(2, len(pivots)):
            p1, p2, p3 = pivots[pi-2], pivots[pi-1], pivots[pi]

            # Identify structure
            st = None
            if (p1["type"]=="H" and p2["type"]=="L" and
                p3["type"]=="H" and p3["price"] < p1["price"]):
                st = "bear"
            elif (p1["type"]=="L" and p2["type"]=="H" and
                  p3["type"]=="L" and p3["price"] > p1["price"]):
                st = "bull"
            if not st: continue
            if p3["idx"] <= used: continue
            if bias_o and bias_o != st: continue
            # EMA filter — only trade in trend direction
            if not ema_allows(p3["idx"], st): continue

            # Fib calculation
            fh  = p1["price"] if st=="bear" else p2["price"]
            fl  = p2["price"] if st=="bear" else p1["price"]
            rng = fh - fl
            if rng <= 0: continue

            fib618 = fl + rng * fib_level if st=="bear" else fh - rng * fib_level
            sl_lvl = fh + rng * 0.02      if st=="bear" else fl - rng * 0.02

            # Pivot confirmation lag — can only know P3 is a pivot
            # after N candles have closed to its right
            search_start = p3["idx"] + N + 1

            # Find candle where price touches fib618
            ec = None
            for ci in range(search_start, min(p3["idx"] + max_bars, n - 1)):
                if st == "bear":
                    if highs_[ci] > fh: break        # structure invalidated — P1 broken
                    if highs_[ci] >= fib618: ec = ci; break
                else:
                    if lows_[ci] < fl: break         # structure invalidated — P1 broken
                    if lows_[ci] <= fib618: ec = ci; break
            if ec is None: continue

            # Entry price:
            #   touch     — limit order sitting at fib618, fills exactly there
            #   rejection — needs candle close confirmation, enter next open
            #   reclaim   — needs candle close confirmation, enter next open
            if entry_mode == "touch":
                actual_entry = fib618
                entry_candle_idx = ec
            else:
                if ec + 1 >= n: continue
                actual_entry = float(opens[ec + 1])
                entry_candle_idx = ec + 1

            # Position sizing and PnL use actual_entry
            rpp = abs(actual_entry - sl_lvl)
            if rpp <= 0: continue

            tp  = actual_entry + rpp * rr if st == "bull" else actual_entry - rpp * rr
            pos = (equity * risk_pct) / rpp

            # Notional position size = pos * actual_entry (used for fee calc)
            notional = pos * actual_entry

            # Find exit — SL/TP checked on wick, SL first (conservative)
            # Exit starts one candle after entry candle
            xc = xp = xr = None
            exit_start = entry_candle_idx + 1
            for ci in range(exit_start, min(exit_start + max_hold, n)):
                if st == "bear":
                    if highs_[ci] >= sl_lvl: xp = sl_lvl; xr = "SL"; xc = ci; break
                    if lows_[ci]  <= tp:     xp = tp;     xr = "TP"; xc = ci; break
                else:
                    if lows_[ci]  <= sl_lvl: xp = sl_lvl; xr = "SL"; xc = ci; break
                    if highs_[ci] >= tp:     xp = tp;     xr = "TP"; xc = ci; break
            if xc is None: continue

            gross_pnl = (actual_entry - xp) * pos if st == "bear" else (xp - actual_entry) * pos
            won       = xr == "TP"

            # Bybit fees on notional:
            #   Entry: limit order  = 0.02% maker
            #   TP exit: limit/maker = 0.02%
            #   SL exit: stop-market = 0.055% taker
            fee_entry = notional * 0.0002
            fee_exit  = notional * 0.0002 if won else notional * 0.00055
            total_fee = fee_entry + fee_exit
            pnl       = gross_pnl - total_fee

            equity += pnl
            bias_o  = None if won else ("bull" if st=="bear" else "bear")
            used    = p3["idx"]

            orig_trades.append({
                "id":         0,
                "direction":  "LONG" if st=="bull" else "SHORT",
                "p1_time":    timestamps[p1["idx"]],
                "entry_time": timestamps[entry_candle_idx],
                "exit_time":  timestamps[xc],
                "entry":      round(actual_entry, 6),
                "sl":         round(sl_lvl, 6),
                "tp":         round(tp, 6),
                "exit_price": round(xp, 6),
                "result":     xr,
                "gross_pnl":  round(gross_pnl, 4),
                "fee":        round(total_fee, 4),
                "pnl":        round(pnl, 4),
                "equity":     round(equity, 4),
                "won":        won,
                "p1":         round(p1["price"], 6),
                "p2":         round(p2["price"], 6),
                "p3":         round(p3["price"], 6),
                "fib_entry":  round(fib618, 6),
            })

        return orig_trades

    # ── STRUCTURE ENGINE ───────────────────────────────────
    def run_direction(st):
        """BOS-based state machine for bull or bear direction."""
        nonlocal equity
        direction_trades = []

        p1_idx   = None
        p1_price = None

        for p in pivots:
            if st == "bull" and p["type"] == "H":
                p1_idx = p["idx"]; p1_price = p["price"]; break
            if st == "bear" and p["type"] == "L":
                p1_idx = p["idx"]; p1_price = p["price"]; break
        if p1_idx is None:
            return direction_trades

        state        = 2
        p2           = None
        p3_float     = None
        fib_entry    = None
        bos_idx      = None
        armed_since  = None
        pending_p1   = None
        c_watch      = None
        p2_time      = None   # timestamp when P2 high/low formed
        p3_time      = None   # timestamp of candle that set final P3

        if st == "bull":
            p2_candidate      = float(min(lows[p1_idx:p1_idx+2]))
            p2_candidate_time = timestamps[p1_idx]
        else:
            p2_candidate      = float(max(highs[p1_idx:p1_idx+2]))
            p2_candidate_time = timestamps[p1_idx]

        ci = p1_idx + 1

        while ci < n - 1:
            c_high  = highs[ci]
            c_low   = lows[ci]
            c_close = closes[ci]
            c_open  = opens[ci]

            if state == 2:
                if st == "bull":
                    if float(lows[ci]) < p2_candidate:
                        p2_candidate      = float(lows[ci])
                        p2_candidate_time = timestamps[ci]
                else:
                    if float(highs[ci]) > p2_candidate:
                        p2_candidate      = float(highs[ci])
                        p2_candidate_time = timestamps[ci]

                is_ph = ci in pivot_highs
                is_pl = ci in pivot_lows

                if st == "bull" and is_ph:
                    new_high = pivot_highs[ci]
                    if new_high > p1_price:
                        p2        = p2_candidate
                        bos_idx   = ci
                        p3_float  = float(highs[ci])
                        p2_time   = p2_candidate_time  # exact candle where lowest low formed
                        p3_time   = timestamps[ci]     # P3 starts at BOS candle
                        rng       = p3_float - p2
                        if rng > 0 and rng / max(p2, 1) >= min_swing_pct:
                            fib_entry   = p3_float - rng * fib_level
                            armed_since = ci
                            pending_p1  = None
                            state       = 3
                        else:
                            p1_idx = ci; p1_price = new_high
                            p2_candidate      = float(min(lows[ci:ci+2]))
                            p2_candidate_time = timestamps[ci]

                elif st == "bear" and is_pl:
                    new_low = pivot_lows[ci]
                    if new_low < p1_price:
                        p2        = p2_candidate
                        bos_idx   = ci
                        p3_float  = float(lows[ci])
                        p2_time   = p2_candidate_time  # exact candle where highest high formed
                        p3_time   = timestamps[ci]     # P3 starts at BOS candle
                        rng       = p2 - p3_float
                        if rng > 0 and rng / max(p2, 1) >= min_swing_pct:
                            fib_entry   = p3_float + rng * fib_level
                            armed_since = ci
                            pending_p1  = None
                            state       = 3
                        else:
                            p1_idx = ci; p1_price = new_low
                            p2_candidate      = float(max(highs[ci:ci+2]))
                            p2_candidate_time = timestamps[ci]
                    else:
                        p1_idx = ci; p1_price = new_low
                        p2_candidate      = float(max(highs[ci:ci+2]))
                        p2_candidate_time = timestamps[ci]

                ci += 1
                continue

            if state == 3:
                if k_stale > 0 and (ci - armed_since) > k_stale:
                    state = 2
                    for p in pivots:
                        if p["idx"] > ci:
                            if st == "bull" and p["type"] == "H":
                                p1_idx = p["idx"]; p1_price = p["price"]
                                p2_candidate      = float(min(lows[p1_idx:p1_idx+2]))
                                p2_candidate_time = timestamps[p1_idx]
                                ci = p1_idx + 1; break
                            elif st == "bear" and p["type"] == "L":
                                p1_idx = p["idx"]; p1_price = p["price"]
                                p2_candidate      = float(max(highs[p1_idx:p1_idx+2]))
                                p2_candidate_time = timestamps[p1_idx]
                                ci = p1_idx + 1; break
                    continue

                if st == "bull" and c_high > p3_float:
                    p3_float  = float(c_high)
                    p3_time   = timestamps[ci]   # track which candle set new P3 high
                    rng       = p3_float - p2
                    if rng > 0:
                        fib_entry = p3_float - rng * fib_level

                elif st == "bear" and c_low < p3_float:
                    p3_float  = float(c_low)
                    p3_time   = timestamps[ci]   # track which candle set new P3 low
                    rng       = p2 - p3_float
                    if rng > 0:
                        fib_entry = p3_float + rng * fib_level

                if st == "bull" and c_close < p1_price:
                    state = 2
                    p1_idx = None
                    for p in pivots:
                        if p["idx"] > ci and p["type"] == "H":
                            p1_idx = p["idx"]; p1_price = p["price"]
                            p2_candidate      = float(min(lows[p1_idx:p1_idx+2]))
                            p2_candidate_time = timestamps[p1_idx]
                            break
                    if p1_idx is None:
                        break
                    ci = p1_idx + 1
                    continue

                if st == "bear" and c_close > p1_price:
                    state = 2
                    p1_idx = None
                    for p in pivots:
                        if p["idx"] > ci and p["type"] == "L":
                            p1_idx = p["idx"]; p1_price = p["price"]
                            p2_candidate      = float(max(highs[p1_idx:p1_idx+2]))
                            p2_candidate_time = timestamps[p1_idx]
                            break
                    if p1_idx is None:
                        break
                    ci = p1_idx + 1
                    continue

                is_ph = ci in pivot_highs
                is_pl = ci in pivot_lows

                if st == "bull" and is_ph:
                    new_ph = pivot_highs[ci]
                    if new_ph > p1_price:
                        pending_p1 = {"idx": ci, "price": new_ph}

                if st == "bear" and is_pl:
                    new_pl = pivot_lows[ci]
                    if new_pl < p1_price:
                        pending_p1 = {"idx": ci, "price": new_pl}

                if pending_p1 is not None:
                    if st == "bull" and c_close > pending_p1["price"]:
                        p1_idx   = pending_p1["idx"]
                        p1_price = pending_p1["price"]
                        new_p2   = float(min(lows[p1_idx:ci+1]))
                        new_p3   = float(max(highs[p1_idx:ci+1]))
                        rng      = new_p3 - new_p2
                        if rng > 0 and rng / max(new_p2, 1) >= min_swing_pct:
                            p2        = new_p2
                            p3_float  = new_p3
                            p2_time   = timestamps[p1_idx]
                            p3_time   = timestamps[ci]
                            fib_entry = p3_float - rng * fib_level
                            bos_idx   = ci
                            armed_since = ci
                            pending_p1  = None
                        else:
                            state = 2
                            p2_candidate      = float(min(lows[ci:ci+2]))
                            p2_candidate_time = timestamps[ci]
                            pending_p1 = None
                        p1_idx   = pending_p1["idx"]
                        p1_price = pending_p1["price"]
                        new_p2   = float(max(highs[p1_idx:ci+1]))
                        new_p3   = float(min(lows[p1_idx:ci+1]))
                        rng      = new_p2 - new_p3
                        if rng > 0 and rng / max(new_p2, 1) >= min_swing_pct:
                            p2        = new_p2
                            p3_float  = new_p3
                            p2_time   = timestamps[p1_idx]
                            p3_time   = timestamps[ci]
                            fib_entry = p3_float + rng * fib_level
                            bos_idx   = ci
                            armed_since = ci
                            pending_p1  = None
                        else:
                            state = 2
                            p2_candidate      = float(max(highs[ci:ci+2]))
                            p2_candidate_time = timestamps[ci]
                            pending_p1 = None
                        ci += 1
                        continue

                triggered   = False
                entry_price = None
                if fib_entry is None:
                    ci += 1; continue

                if entry_mode == "touch":
                    if st == "bull" and c_low <= fib_entry:
                        triggered = True
                        entry_price = fib_entry  # limit order fills exactly
                    elif st == "bear" and c_high >= fib_entry:
                        triggered = True
                        entry_price = fib_entry

                elif entry_mode == "rejection":
                    if st == "bull" and c_low <= fib_entry and c_close > fib_entry:
                        triggered = True
                        entry_price = float(opens[ci + 1]) if ci + 1 < n else None
                    elif st == "bear" and c_high >= fib_entry and c_close < fib_entry:
                        triggered = True
                        entry_price = float(opens[ci + 1]) if ci + 1 < n else None

                elif entry_mode == "reclaim":
                    if c_watch is None:
                        if st == "bull" and c_close < fib_entry:
                            c_watch = ci
                        elif st == "bear" and c_close > fib_entry:
                            c_watch = ci
                    else:
                        if (ci - c_watch) <= 2:
                            if st == "bull" and c_close > fib_entry:
                                triggered = True
                                entry_price = c_close
                                c_watch = None
                            elif st == "bear" and c_close < fib_entry:
                                triggered = True
                                entry_price = c_close
                                c_watch = None
                        else:
                            c_watch = None

                if not triggered or entry_price is None:
                    ci += 1
                    continue

                if entry_mode != "touch" and ci + 1 >= n:
                    break

                entry_candle = ci if entry_mode == "touch" else ci + 1
                entry   = entry_price
                sl_lvl  = p2 - (p2 * stop_buffer_pct) if st == "bull" else p2 + (p2 * stop_buffer_pct)
                rpp     = abs(entry - sl_lvl)
                if rpp <= 0:
                    ci += 1; continue

                tp       = entry + rpp * rr if st == "bull" else entry - rpp * rr
                pos      = (equity * risk_pct) / rpp
                notional = pos * entry

                xc = xp = xr = None
                for xi in range(entry_candle + 1, n):
                    if st == "bull":
                        if lows[xi]  <= sl_lvl: xp = sl_lvl; xr = "SL"; xc = xi; break
                        if highs[xi] >= tp:      xp = tp;     xr = "TP"; xc = xi; break
                        if xi >= entry_candle + max_hold:
                            if closes[xi] > entry: xp = float(closes[xi]); xr = "TIMEOUT"; xc = xi; break
                    else:
                        if highs[xi] >= sl_lvl: xp = sl_lvl; xr = "SL"; xc = xi; break
                        if lows[xi]  <= tp:      xp = tp;     xr = "TP"; xc = xi; break
                        if xi >= entry_candle + max_hold:
                            if closes[xi] < entry: xp = float(closes[xi]); xr = "TIMEOUT"; xc = xi; break

                if xc is None:
                    xc = n - 1
                    xp = float(closes[xc])
                    xr = "TIMEOUT"

                gross_pnl = (xp - entry) * pos if st == "bull" else (entry - xp) * pos
                won       = xr == "TP"
                fee_entry = notional * 0.0002
                fee_exit  = notional * 0.0002 if won else notional * 0.00055
                total_fee = fee_entry + fee_exit
                pnl       = gross_pnl - total_fee
                equity   += pnl

                direction_trades.append({
                    "id":         0,
                    "direction":  "LONG" if st == "bull" else "SHORT",
                    "p1_time":    timestamps[p1_idx] if p1_idx < n else None,
                    "p2_time":    p2_time,
                    "p3_time":    p3_time,
                    "entry_time": timestamps[entry_candle],
                    "exit_time":  timestamps[xc],
                    "entry":      round(entry, 6),
                    "sl":         round(sl_lvl, 6),
                    "tp":         round(tp, 6),
                    "exit_price": round(xp, 6),
                    "result":     xr,
                    "gross_pnl":  round(gross_pnl, 4),
                    "fee":        round(total_fee, 4),
                    "pnl":        round(pnl, 4),
                    "equity":     round(equity, 4),
                    "won":        won,
                    "p1":         round(p1_price, 6),
                    "p2":         round(p2, 6),
                    "p3":         round(p3_float, 6),
                    "fib_entry":  round(fib_entry, 6),
                })

                state = 2
                pending_p1 = None
                c_watch    = None
                for p in pivots:
                    if p["idx"] > xc:
                        if st == "bull" and p["type"] == "H":
                            p1_idx = p["idx"]; p1_price = p["price"]
                            p2_candidate      = float(min(lows[p1_idx:p1_idx+2]))
                            p2_candidate_time = timestamps[p1_idx]
                            ci = p1_idx + 1; break
                        elif st == "bear" and p["type"] == "L":
                            p1_idx = p["idx"]; p1_price = p["price"]
                            p2_candidate      = float(max(highs[p1_idx:p1_idx+2]))
                            p2_candidate_time = timestamps[p1_idx]
                            ci = p1_idx + 1; break
                else:
                    break
                continue

            ci += 1

        return direction_trades

    # ── CLASSIC ENGINE ─────────────────────────────────────
    def run_classic(v2=False):
        nonlocal equity
        trades_out = []
        MIN_RANGE  = min_swing_pct

        highs_  = np.array([c[2] for c in candles])
        lows_   = np.array([c[3] for c in candles])
        closes_ = np.array([c[4] for c in candles])
        opens_  = np.array([c[1] for c in candles])

        pivot_highs_ = {p["idx"]: p["price"] for p in pivots if p["type"] == "H"}
        pivot_lows_  = {p["idx"]: p["price"] for p in pivots if p["type"] == "L"}

        def build_setups():
            setups = []
            N_min = 3
            # BULL: P1=pivot high, BOS when high breaks above P1, P2=lowest low between P1 and BOS, P3=BOS high
            for p1 in [p for p in pivots if p["type"] == "H"]:
                p1_idx = p1["idx"]; p1_price = p1["price"]
                for ci in range(p1_idx + 1, n - 1):
                    if highs_[ci] > p1_price:              # BOS: high breaks above P1
                        if ci - p1_idx < N_min: continue   # too close, keep looking
                        p3_idx = ci
                        p3_price = float(highs_[ci])        # P3 = BOS high
                        p2 = float(min(lows_[p1_idx:p3_idx + 1]))   # P2 = lowest low
                        p2_ci = p1_idx + int(np.argmin(lows_[p1_idx:p3_idx + 1]))
                        rng = p3_price - p2
                        if rng <= 0 or rng / max(p2, 1) < MIN_RANGE: break
                        setups.append({"st":"bull","p1_idx":p1_idx,"p1_price":p1_price,
                            "p1_time":timestamps[p1_idx],
                            "p2":p2,"p2_time":timestamps[p2_ci],
                            "p3_idx":p3_idx,"p3_close":p3_price,"p3_time":timestamps[ci],
                            "rng":rng,"fib618":p2 + rng * fib_level,"sl":p2})
                        break
                    if lows_[ci] < p1_price * 0.90: break
            # BEAR: P1=pivot low, BOS when low breaks below P1, P2=highest high between P1 and BOS, P3=BOS low
            for p1 in [p for p in pivots if p["type"] == "L"]:
                p1_idx = p1["idx"]; p1_price = p1["price"]
                for ci in range(p1_idx + 1, n - 1):
                    if lows_[ci] < p1_price:               # BOS: low breaks below P1
                        if ci - p1_idx < N_min: continue   # too close, keep looking
                        p3_idx = ci
                        p3_price = float(lows_[ci])         # P3 = BOS low
                        p2 = float(max(highs_[p1_idx:p3_idx + 1]))  # P2 = highest high
                        p2_ci = p1_idx + int(np.argmax(highs_[p1_idx:p3_idx + 1]))
                        rng = p2 - p3_price
                        if rng <= 0 or rng / max(p2, 1) < MIN_RANGE: break
                        setups.append({"st":"bear","p1_idx":p1_idx,"p1_price":p1_price,
                            "p1_time":timestamps[p1_idx],
                            "p2":p2,"p2_time":timestamps[p2_ci],
                            "p3_idx":p3_idx,"p3_close":p3_price,"p3_time":timestamps[ci],
                            "rng":rng,"fib618":p2 - rng * fib_level,"sl":p2})
                        break
                    if highs_[ci] > p1_price * 1.10: break
            setups.sort(key=lambda x: x["p3_idx"])
            return setups

        all_setups   = build_setups()
        setup_idx    = 0
        active       = None
        last_p3_idx  = -1
        ci           = 1

        while ci < n - 1:
            if active is None:
                while setup_idx < len(all_setups):
                    s = all_setups[setup_idx]; setup_idx += 1
                    if s["p3_idx"] <= last_p3_idx: continue
                    active = s
                    ci = s["p3_idx"] + 1
                    break
                if active is None: break

            st     = active["st"]
            fib618 = active["fib618"]
            sl_lvl = active["sl"]
            p2     = active["p2"]
            p3_idx = active["p3_idx"]
            p1_idx = active["p1_idx"]
            p1_price = active["p1_price"]

            if ci >= n - 1: break

            c_low   = lows_[ci]
            c_high  = highs_[ci]
            c_close = closes_[ci]

            if v2:
                if k_stale > 0 and (ci - p3_idx) > k_stale:
                    active = None; ci += 1; continue

                if st == "bull" and ci in pivot_highs_:
                    new_ph = pivot_highs_[ci]
                    if new_ph > p1_price:
                        active["p1_idx"]   = ci
                        active["p1_price"] = new_ph
                        active["p3_idx"]   = ci
                        active["p3_close"] = new_ph
                        active["p3_time"]  = timestamps[ci]
                        new_p2 = float(min(lows_[p1_idx:ci + 1]))
                        p2_ci  = p1_idx + int(np.argmin(lows_[p1_idx:ci + 1]))
                        new_rng = new_ph - new_p2
                        if new_rng > 0 and new_rng / max(new_p2, 1) >= MIN_RANGE:
                            active["p2"]      = new_p2
                            active["p2_time"] = timestamps[p2_ci]
                            active["rng"]     = new_rng
                            active["fib618"]  = new_p2 + new_rng * fib_level
                            active["sl"]      = new_p2
                            fib618 = active["fib618"]
                            sl_lvl = active["sl"]
                            p2     = active["p2"]
                            p1_idx = ci
                            p1_price = new_ph
                            p3_idx = ci
                        ci += 1; continue
                    elif new_ph > active["p3_close"]:
                        new_p2  = float(min(lows_[p3_idx:ci + 1]))
                        p2_ci   = p3_idx + int(np.argmin(lows_[p3_idx:ci + 1]))
                        new_rng = new_ph - new_p2
                        if new_rng > 0 and new_rng / max(new_p2, 1) >= MIN_RANGE:
                            active["p2"]      = new_p2
                            active["p2_time"] = timestamps[p2_ci]
                            active["p3_idx"]  = ci
                            active["p3_close"]= new_ph
                            active["p3_time"] = timestamps[ci]
                            active["rng"]     = new_rng
                            active["fib618"]  = new_p2 + new_rng * fib_level
                            active["sl"]      = new_p2
                            fib618 = active["fib618"]
                            sl_lvl = active["sl"]
                            p2     = active["p2"]
                            p3_idx = ci
                        ci += 1; continue

                if st == "bear" and ci in pivot_lows_:
                    new_pl = pivot_lows_[ci]
                    if new_pl < p1_price:
                        active["p1_idx"]   = ci
                        active["p1_price"] = new_pl
                        active["p3_idx"]   = ci
                        active["p3_close"] = new_pl
                        active["p3_time"]  = timestamps[ci]
                        new_p2 = float(max(highs_[p1_idx:ci + 1]))
                        p2_ci  = p1_idx + int(np.argmax(highs_[p1_idx:ci + 1]))
                        new_rng = new_p2 - new_pl
                        if new_rng > 0 and new_rng / max(new_p2, 1) >= MIN_RANGE:
                            active["p2"]      = new_p2
                            active["p2_time"] = timestamps[p2_ci]
                            active["rng"]     = new_rng
                            active["fib618"]  = new_p2 - new_rng * fib_level
                            active["sl"]      = new_p2
                            fib618 = active["fib618"]
                            sl_lvl = active["sl"]
                            p2     = active["p2"]
                            p1_idx = ci
                            p1_price = new_pl
                            p3_idx = ci
                        ci += 1; continue
                    elif new_pl < active["p3_close"]:
                        new_p2  = float(max(highs_[p3_idx:ci + 1]))
                        p2_ci   = p3_idx + int(np.argmax(highs_[p3_idx:ci + 1]))
                        new_rng = new_p2 - new_pl
                        if new_rng > 0 and new_rng / max(new_p2, 1) >= MIN_RANGE:
                            active["p2"]      = new_p2
                            active["p2_time"] = timestamps[p2_ci]
                            active["p3_idx"]  = ci
                            active["p3_close"]= new_pl
                            active["p3_time"] = timestamps[ci]
                            active["rng"]     = new_rng
                            active["fib618"]  = new_p2 - new_rng * fib_level
                            active["sl"]      = new_p2
                            fib618 = active["fib618"]
                            sl_lvl = active["sl"]
                            p2     = active["p2"]
                            p3_idx = ci
                        ci += 1; continue

            if st == "bull" and c_low < p2:
                active = None; ci += 1; continue
            if st == "bear" and c_high > p2:
                active = None; ci += 1; continue

            triggered   = False
            entry_price = None
            if entry_mode == "touch":
                if st == "bull" and c_low <= fib618:
                    triggered = True; entry_price = fib618  # limit order fills exactly
                elif st == "bear" and c_high >= fib618:
                    triggered = True; entry_price = fib618
            elif entry_mode == "rejection":
                if st == "bull" and c_low <= fib618 and c_close > fib618:
                    triggered = True; entry_price = float(opens_[ci+1]) if ci+1 < n else None
                elif st == "bear" and c_high >= fib618 and c_close < fib618:
                    triggered = True; entry_price = float(opens_[ci+1]) if ci+1 < n else None
            elif entry_mode == "reclaim":
                if not active.get("c_watch"):
                    if st == "bull" and c_close < fib618: active["c_watch"] = ci
                    elif st == "bear" and c_close > fib618: active["c_watch"] = ci
                else:
                    if (ci - active["c_watch"]) <= 2:
                        if st == "bull" and c_close > fib618:
                            triggered = True; entry_price = c_close; active["c_watch"] = None
                        elif st == "bear" and c_close < fib618:
                            triggered = True; entry_price = c_close; active["c_watch"] = None
                    else:
                        active["c_watch"] = None

            if not triggered or entry_price is None:
                ci += 1; continue

            if entry_mode != "touch" and ci + 1 >= n: break
            entry_candle = ci if entry_mode == "touch" else ci + 1
            entry  = entry_price
            sl_lvl = p2 - (p2 * stop_buffer_pct) if st == "bull" else p2 + (p2 * stop_buffer_pct)
            rpp    = abs(entry - sl_lvl)
            if rpp <= 0: active = None; ci += 1; continue
            tp       = entry + rpp * rr if st == "bull" else entry - rpp * rr
            pos      = (equity * risk_pct) / rpp
            notional = pos * entry

            xc = xp = xr = None
            for xi in range(entry_candle + 1, n):
                if st == "bull":
                    if lows_[xi]  <= sl_lvl: xp = sl_lvl; xr = "SL"; xc = xi; break
                    if highs_[xi] >= tp:     xp = tp;     xr = "TP"; xc = xi; break
                    # After max_hold, only close if in profit
                    if xi >= entry_candle + max_hold:
                        if closes_[xi] > entry: xp = float(closes_[xi]); xr = "TIMEOUT"; xc = xi; break
                else:
                    if highs_[xi] >= sl_lvl: xp = sl_lvl; xr = "SL"; xc = xi; break
                    if lows_[xi]  <= tp:     xp = tp;     xr = "TP"; xc = xi; break
                    # After max_hold, only close if in profit
                    if xi >= entry_candle + max_hold:
                        if closes_[xi] < entry: xp = float(closes_[xi]); xr = "TIMEOUT"; xc = xi; break

            # Fallback — if we reach end of data without closing
            if xc is None:
                xc = n - 1
                xp = float(closes_[xc]); xr = "TIMEOUT"

            gross_pnl = (xp - entry) * pos if st == "bull" else (entry - xp) * pos
            won       = xr == "TP"
            fee_entry = notional * 0.0002
            fee_exit  = notional * 0.0002 if won else notional * 0.00055
            total_fee = fee_entry + fee_exit
            pnl       = gross_pnl - total_fee
            equity   += pnl
            last_p3_idx = p3_idx

            trades_out.append({
                "id":         0,
                "direction":  "LONG" if st == "bull" else "SHORT",
                "p1_time":    active.get("p1_time") or (timestamps[p1_idx] if p1_idx < n else None),
                "p2_time":    active.get("p2_time"),
                "p3_time":    active.get("p3_time"),
                "entry_time": timestamps[entry_candle],
                "exit_time":  timestamps[xc],
                "entry":      round(entry, 6),
                "sl":         round(sl_lvl, 6),
                "tp":         round(tp, 6),
                "exit_price": round(xp, 6),
                "result":     xr,
                "gross_pnl":  round(gross_pnl, 4),
                "fee":        round(total_fee, 4),
                "pnl":        round(pnl, 4),
                "equity":     round(equity, 4),
                "won":        won,
                "p1":         round(p1_price, 6),
                "p2":         round(p2, 6),
                "p3":         round(active["p3_close"], 6),
                "fib_entry":  round(fib618, 6),
            })

            active = None
            ci = xc + 1

        return trades_out

    # ── PULLBACK ENGINE ───────────────────────────────────
    def run_pullback():
        """
        Method 1 — Pullback (Fib Retracement)

        LONG:
          - Find P1 = pivot HIGH (highest HIGH, N candles each side)
          - When any candle HIGH > P1 → P3 = that candle HIGH (breakout confirmed)
          - P2 = lowest LOW between P1 and P3 (updates as P3 moves)
          - P3 updates each candle CLOSE if new HIGH > current P3
          - Fib entry = P2 + (P3 - P2) * fib_level
          - SL = P1 high (original pivot high that was broken)
          - TP = entry + RR * (entry - SL)
          - Reset if candle LOW < P1 (structure broken) or SL/TP hit

        SHORT = exact mirror using pivot LOWs and candle lows.
        """
        nonlocal equity
        pt = []

        def run_direction_pb(st):
            nonlocal equity
            dir_trades = []
            p1_idx = None; p1_price = None
            p3_price = None; p2_price = None
            fib_e = None; sl_lvl = None
            armed = False
            in_trade = False
            entry_price = tp = None
            entry_idx = None

            ci = N
            while ci < n - 1:
                c_high  = highs[ci]
                c_low   = lows[ci]
                c_close = closes[ci]

                # ── RESET CHECK ─────────────────────────────
                if armed and not in_trade:
                    if st == "bull" and c_low < p1_price:
                        p1_idx = None; p1_price = None
                        p3_price = None; p2_price = None
                        fib_e = None; sl_lvl = None
                        armed = False
                        ci += 1; continue
                    if st == "bear" and c_high > p1_price:
                        p1_idx = None; p1_price = None
                        p3_price = None; p2_price = None
                        fib_e = None; sl_lvl = None
                        armed = False
                        ci += 1; continue

                # ── FIND P1 ─────────────────────────────────
                if p1_idx is None:
                    if st == "bull":
                        check = ci - N
                        if check >= N:
                            window_highs = highs[check-N:check+N+1]
                            if len(window_highs) == 2*N+1 and highs[check] == max(window_highs):
                                p1_idx = check
                                p1_price = float(highs[check])
                    else:
                        check = ci - N
                        if check >= N:
                            window_lows = lows[check-N:check+N+1]
                            if len(window_lows) == 2*N+1 and lows[check] == min(window_lows):
                                p1_idx = check
                                p1_price = float(lows[check])
                    ci += 1; continue

                # ── SNAPSHOT FIB BEFORE P3 UPDATE ────────────
                # Critical: snapshot the current fib level BEFORE updating P3
                # Entry trigger checks against this snapshot so:
                # - Touch: checks if wick reached the fib that was active THIS candle
                # - Rejection: checks close against same fib level the wick touched
                # This ensures Touch and Rejection produce genuinely different results
                fib_snapshot = fib_e
                sl_snapshot  = sl_lvl

                # ── P3 DETECTION AND UPDATE ──────────────────
                # Each candle close is one opportunity to update P3
                if not armed:
                    if st == "bull" and c_high > p1_price:
                        if p3_price is None or c_high > p3_price:
                            p3_price = float(c_high)
                            p2_price = float(min(lows[p1_idx:ci+1]))
                            rng = p3_price - p2_price
                            if rng > 0:
                                fib_e  = p2_price + rng * fib_level
                                sl_lvl = p1_price
                                armed  = True
                    elif st == "bear" and c_low < p1_price:
                        if p3_price is None or c_low < p3_price:
                            p3_price = float(c_low)
                            p2_price = float(max(highs[p1_idx:ci+1]))
                            rng = p2_price - p3_price
                            if rng > 0:
                                fib_e  = p2_price - rng * fib_level
                                sl_lvl = p1_price
                                armed  = True
                else:
                    # Update P3 on new extreme — fib shifts for NEXT candle
                    if st == "bull" and c_high > p3_price:
                        p3_price = float(c_high)
                        p2_price = float(min(lows[p1_idx:ci+1]))
                        rng = p3_price - p2_price
                        if rng > 0:
                            fib_e  = p2_price + rng * fib_level
                            sl_lvl = p1_price
                    elif st == "bear" and c_low < p3_price:
                        p3_price = float(c_low)
                        p2_price = float(max(highs[p1_idx:ci+1]))
                        rng = p2_price - p3_price
                        if rng > 0:
                            fib_e  = p2_price - rng * fib_level
                            sl_lvl = p1_price

                # ── EMA FILTER ───────────────────────────────
                if armed and not in_trade:
                    if not ema_allows(ci, st):
                        ci += 1; continue

                # ── ENTRY TRIGGER — uses fib_snapshot ────────
                # fib_snapshot = fib level BEFORE this candle's P3 update
                # Ensures entry checks the level that was active when price touched it
                # Touch:     wick reached snapshot fib → fill at that level
                # Rejection: wick reached snapshot fib AND close back above it → fill
                if armed and not in_trade and fib_snapshot is not None:
                    triggered = False
                    if entry_mode == "touch":
                        if st == "bull" and c_low  <= fib_snapshot: triggered = True
                        if st == "bear" and c_high >= fib_snapshot: triggered = True
                    else:  # rejection
                        if st == "bull" and c_low  <= fib_snapshot and c_close > fib_snapshot: triggered = True
                        if st == "bear" and c_high >= fib_snapshot and c_close < fib_snapshot: triggered = True

                    if triggered:
                        entry_price = fib_snapshot  # fill at the fib level that was active
                        sl_price    = sl_snapshot if sl_snapshot else p1_price
                        entry_idx   = ci
                        rpp         = abs(entry_price - sl_price)
                        sl_lvl      = sl_price  # lock SL at snapshot value
                        if rpp <= 0: ci += 1; continue
                        tp      = entry_price + rpp * rr if st == "bull" else entry_price - rpp * rr
                        pos     = (equity * risk_pct) / rpp
                        notional = pos * entry_price
                        in_trade = True

                # ── TRADE MONITOR ────────────────────────────
                if in_trade:
                    xp = xr = xc = None
                    if st == "bull":
                        if c_low  <= sl_lvl: xp = sl_lvl; xr = "SL"; xc = ci
                        elif c_high >= tp:   xp = tp;     xr = "TP"; xc = ci
                    else:
                        if c_high >= sl_lvl: xp = sl_lvl; xr = "SL"; xc = ci
                        elif c_low  <= tp:   xp = tp;     xr = "TP"; xc = ci

                    if xr:
                        gross_pnl = (xp - entry_price) * pos if st == "bull" else (entry_price - xp) * pos
                        won       = xr == "TP"
                        fee_entry = notional * 0.0002
                        fee_exit  = notional * 0.0002 if won else notional * 0.00055
                        total_fee = fee_entry + fee_exit
                        pnl       = gross_pnl - total_fee
                        equity   += pnl

                        dir_trades.append({
                            "id":         0,
                            "direction":  "LONG" if st=="bull" else "SHORT",
                            "p1_time":    timestamps[p1_idx],
                            "entry_time": timestamps[entry_idx],
                            "exit_time":  timestamps[xc],
                            "entry":      round(entry_price, 6),
                            "sl":         round(sl_lvl, 6),
                            "tp":         round(tp, 6),
                            "exit_price": round(xp, 6),
                            "result":     xr,
                            "gross_pnl":  round(gross_pnl, 4),
                            "fee":        round(total_fee, 4),
                            "pnl":        round(pnl, 4),
                            "equity":     round(equity, 4),
                            "won":        won,
                            "p1":         round(p1_price, 6),
                            "p2":         round(p2_price, 6),
                            "p3":         round(p3_price, 6),
                            "fib_entry":  round(fib_e, 6),
                        })

                        # Reset after trade
                        p1_idx = None; p1_price = None
                        p3_price = None; p2_price = None
                        fib_e = None; sl_lvl = None
                        armed = False; in_trade = False
                        entry_price = tp = None; entry_idx = None

                ci += 1
            return dir_trades

        bull_t = run_direction_pb("bull")
        bear_t = run_direction_pb("bear")
        return sorted(bull_t + bear_t, key=lambda t: t["entry_time"])

    # ── EMA CROSS ENGINE ──────────────────────────────────
    def run_ema_cross():
        """
        EMA Cross Pullback Strategy:

        LONG setup:
          - Uptrend: close > EMA fast AND close > EMA slow AND EMA fast > EMA slow
          - ADX >= threshold (trending, not ranging)
          - Entry: candle LOW touches or dips below either EMA (fast or slow)
                   AND candle closes back above the EMA it touched (rejection)
          - SL: ATR(N) × 1.5 below entry
          - TP: entry + RR × (entry - SL)

        SHORT = exact mirror.
        """
        nonlocal equity
        ec_trades = []

        def run_direction_ec(st):
            nonlocal equity
            dir_trades = []
            in_trade   = False
            entry_p = sl_l = tp_l = pos_l = notional_l = entry_idx = None

            # ATR for SL
            atr_arr = np.zeros(n)
            for i in range(1, n):
                tr = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
                atr_arr[i] = (atr_arr[i-1] * (N-1) + tr) / N if i >= N else tr

            for ci in range(max(ema_slow, adx_period*2) + 1, n - 1):
                c_high  = highs[ci]
                c_low   = lows[ci]
                c_close = closes[ci]
                ef      = ema_f[ci] if ema_f is not None else None
                es      = ema_s[ci] if ema_s is not None else None
                if ef is None or es is None: continue

                # Monitor open trade
                if in_trade:
                    xp = xr = xc = None
                    if st == "bull":
                        if c_low  <= sl_l: xp = sl_l; xr = "SL"; xc = ci
                        elif c_high >= tp_l: xp = tp_l; xr = "TP"; xc = ci
                    else:
                        if c_high >= sl_l: xp = sl_l; xr = "SL"; xc = ci
                        elif c_low  <= tp_l: xp = tp_l; xr = "TP"; xc = ci

                    if xr:
                        gross_pnl = (xp - entry_p) * pos_l if st=="bull" else (entry_p - xp) * pos_l
                        won       = xr == "TP"
                        fee_e     = notional_l * 0.0002
                        fee_x     = notional_l * 0.0002 if won else notional_l * 0.00055
                        pnl       = gross_pnl - fee_e - fee_x
                        equity   += pnl
                        dir_trades.append({
                            "id":0, "direction":"LONG" if st=="bull" else "SHORT",
                            "p1_time":timestamps[entry_idx],
                            "entry_time":timestamps[entry_idx],
                            "exit_time":timestamps[xc],
                            "entry":round(entry_p,6), "sl":round(sl_l,6), "tp":round(tp_l,6),
                            "exit_price":round(xp,6), "result":xr,
                            "gross_pnl":round(gross_pnl,4), "fee":round(fee_e+fee_x,4),
                            "pnl":round(pnl,4), "equity":round(equity,4), "won":won,
                            "p1":round(ef,6), "p2":round(es,6), "p3":round(c_close,6),
                            "fib_entry":round(entry_p,6),
                        })
                        in_trade = False
                        entry_p = sl_l = tp_l = pos_l = notional_l = entry_idx = None
                    continue

                # Check trend + ADX conditions
                if st == "bull":
                    uptrend = c_close > ef and c_close > es and ef > es
                    if not uptrend: continue
                else:
                    downtrend = c_close < ef and c_close < es and ef < es
                    if not downtrend: continue

                if not adx_trending(ci): continue

                # Entry trigger — candle LOW touches either EMA from above, closes back above
                if st == "bull":
                    touched_fast = c_low <= ef and c_close > ef
                    touched_slow = c_low <= es and c_close > es
                    if not (touched_fast or touched_slow): continue
                    ep = ef if touched_fast else es
                else:
                    touched_fast = c_high >= ef and c_close < ef
                    touched_slow = c_high >= es and c_close < es
                    if not (touched_fast or touched_slow): continue
                    ep = ef if touched_fast else es

                atr    = atr_arr[ci]
                if atr <= 0: continue
                sl     = ep - atr * 1.5 if st=="bull" else ep + atr * 1.5
                rpp    = abs(ep - sl)
                if rpp <= 0: continue
                tp     = ep + rpp * rr if st=="bull" else ep - rpp * rr
                pos    = (equity * risk_pct) / rpp
                notional = pos * ep

                entry_p    = ep
                sl_l       = sl
                tp_l       = tp
                pos_l      = pos
                notional_l = notional
                entry_idx  = ci
                in_trade   = True

            return dir_trades

        # EMA arrays must be computed — force them on
        if ema_f is None or ema_s is None:
            return []

        bull_t = run_direction_ec("bull")
        bear_t = run_direction_ec("bear")
        return sorted(bull_t + bear_t, key=lambda t: t["entry_time"])

    # ── ROUTE BY ENGINE ────────────────────────────────────
    if engine == "original":
        all_trades = run_original()
    elif engine == "classic":
        all_trades = run_classic(v2=False)
    elif engine == "classic_v2":
        all_trades = run_classic(v2=True)
    elif engine == "pullback":
        all_trades = run_classic(v2=True)
    elif engine == "bos_pivot":
        all_trades = run_classic(v2=False)
    elif engine == "ema_cross":
        all_trades = run_ema_cross()
    else:
        # structure engine — run both directions, merge chronologically
        bull_trades = run_direction("bull")
        bear_trades = run_direction("bear")
        all_trades  = sorted(bull_trades + bear_trades, key=lambda t: t["entry_time"])

    # Re-number sequentially
    for i, t in enumerate(all_trades):
        t["id"] = i + 1

    return all_trades


# ── STATS ─────────────────────────────────────────────────
def calc_stats(trades, days):
    if not trades: return None
    wins   = [t for t in trades if t["won"]]
    losses = [t for t in trades if not t["won"]]
    final  = trades[-1]["equity"]
    tr     = (final - 100) / 100 * 100
    wr     = len(wins) / len(trades) * 100
    total_fees = sum(t.get("fee", 0) for t in trades)
    peak   = 100; mdd = 0
    for t in trades:
        if t["equity"] > peak: peak = t["equity"]
        mdd = max(mdd, (peak - t["equity"]) / peak * 100)
    max_cw = max_cl = cw = cl = 0
    for t in trades:
        if t["won"]: cw += 1; cl = 0; max_cw = max(max_cw, cw)
        else:        cl += 1; cw = 0; max_cl = max(max_cl, cl)
    rets = [t["pnl"] / (t["equity"] - t["pnl"]) * 100 for t in trades]
    mean = sum(rets) / len(rets)
    std  = (sum((r - mean) ** 2 for r in rets) / len(rets)) ** 0.5
    sharpe = (mean / std * (365 ** 0.5)) if std > 0 else 0
    cagr   = ((final / 100) ** (365 / max(days, 1)) - 1) * 100
    gw = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in losses))
    pf = gw / gl if gl > 0 else 999
    kf = wr/100 - (1 - wr/100) / (gw/len(wins) / (gl/len(losses))) if wins and losses else 0
    return {
        "total_trades":     len(trades),
        "wins":             len(wins),
        "losses":           len(losses),
        "win_rate":         round(wr, 2),
        "final_equity":     round(final, 2),
        "total_return":     round(tr, 2),
        "cagr":             round(cagr, 2),
        "daily_return":     round(tr / max(days, 1), 3),
        "max_drawdown":     round(mdd, 2),
        "sharpe":           round(sharpe, 2),
        "profit_factor":    round(pf, 2),
        "avg_win":          round(gw / len(wins), 3) if wins else 0,
        "avg_loss":         round(gl / len(losses), 3) if losses else 0,
        "max_consec_wins":  max_cw,
        "max_consec_losses":max_cl,
        "kelly_full":       round(kf, 3),
        "kelly_half":       round(kf / 2, 3),
        "total_fees":       round(total_fees, 4),
    }


# ── PROCESS ONE REQUEST ───────────────────────────────────
def process_request(req: BacktestRequest):
    try:
        # Determine if this is a rolling/recent period
        # Rolling = end_date is "now" OR start_date is within last 90 days
        now_ts = datetime.now(timezone.utc)
        try:
            start_dt = datetime.strptime(req.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            days_ago = (now_ts - start_dt).days
            is_rolling = req.end_date == "now" or days_ago <= 90
        except Exception:
            is_rolling = req.end_date == "now"

        # Result cache disabled — EMA/ADX/engine params not in key, causes stale results

        candles, source = fetch_candles(req.symbol, req.timeframe, req.start_date, req.end_date)
        highs  = np.array([c[2] for c in candles])
        lows   = np.array([c[3] for c in candles])
        pivots = find_pivots(highs, lows, req.pivot_n)
        days   = (candles[-1][0] - candles[0][0]) // 86400000

        # Run fixed 2% first (needed for Kelly calculation)
        # EMA cross engine always needs EMA computed — force use_ema_filter on
        ema_kwargs = dict(
            use_ema_filter=req.use_ema_filter or req.engine == "ema_cross",
            ema_fast=req.ema_fast,
            ema_slow=req.ema_slow,
            adx_period=req.adx_period,
            adx_threshold=req.adx_threshold,
        )

        trades_fixed = run_backtest_core(
            candles, pivots, 0.02, req.rr, req.fib_level,
            req.max_bars, req.max_hold, req.recency_bars, req.one_per_pair,
            req.min_swing_pct, req.stop_buffer_pct, req.k_stale, req.entry_mode, req.engine,
            **ema_kwargs
        )
        stats_fixed = calc_stats(trades_fixed, days)

        risk_pct = req.risk_pct
        if req.risk_method == "half_kelly" and stats_fixed:
            risk_pct = max(stats_fixed["kelly_half"], 0.01)
        elif req.risk_method == "full_kelly" and stats_fixed:
            risk_pct = max(stats_fixed["kelly_full"], 0.01)

        trades = run_backtest_core(
            candles, pivots, risk_pct, req.rr, req.fib_level,
            req.max_bars, req.max_hold, req.recency_bars, req.one_per_pair,
            req.min_swing_pct, req.stop_buffer_pct, req.k_stale, req.entry_mode, req.engine,
            **ema_kwargs
        )
        stats = calc_stats(trades, days)

        # Result saving disabled — see cache note above

        # Build equity curve — sample rate varies by timeframe
        eq_curve = []
        eq = 100.0; ti = 0
        sample = {"1m":30,"5m":12,"15m":4,"1h":1,"4h":1,"1d":1}.get(req.timeframe, 10)
        for i, c in enumerate(candles):
            if ti < len(trades) and trades[ti]["exit_time"] <= c[0]:
                eq = trades[ti]["equity"]; ti += 1
            if i % sample == 0:
                eq_curve.append({"t": c[0], "eq": round(eq, 2)})

        return {
            "success":     True,
            "source":      source,
            "symbol":      req.symbol,
            "timeframe":   req.timeframe,
            "period":      f"{req.start_date} → {req.end_date}",
            "risk_method": req.risk_method,
            "risk_pct":    round(risk_pct * 100, 2),
            "pivot_n":     req.pivot_n,
            "rr":          req.rr,
            "fib_level":   req.fib_level,
            "engine":      req.engine,
            "entry_mode":  req.entry_mode,
            "use_ema_filter": req.use_ema_filter,
            "ema_fast":    req.ema_fast,
            "ema_slow":    req.ema_slow,
            "adx_period":  req.adx_period,
            "adx_threshold": req.adx_threshold,
            "stats":       stats,
            "equity_curve":eq_curve,
            "trades":      trades[-50:],
        }
    except Exception as e:
        return {
            "success":    False,
            "error":      str(e),
            "symbol":     req.symbol,
            "timeframe":  req.timeframe,
            "risk_method":req.risk_method,
            "pivot_n":    req.pivot_n,
            "rr":         req.rr,
            "engine":     req.engine,
            "entry_mode": req.entry_mode,
        }


# ── ROUTES ────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "Fib Backtest API v10", "rules": "ema_cross+adx+pullback_p1reset"}

@app.get("/pairs")
def get_pairs():
    return {"pairs": ["XRP/USDT","DOGE/USDT","TRX/USDT","XLM/USDT","ADA/USDT","ARB/USDT"]}

@app.get("/cache-status")
def cache_status():
    result_count = 0
    try:
        r1 = httpx.get(f"{SUPABASE_URL}/rest/v1/results?select=id&limit=1000", headers=HEADERS, timeout=5)
        if r1.status_code == 200: result_count = len(r1.json())
    except: pass
    return {
        "memory_cache_keys": list(_mem_cache.keys()),
        "supabase_connected": bool(SUPABASE_URL),
        "cached_results": result_count,
    }

# ── PREFETCH SYSTEM ───────────────────────────────────────
_prefetch_status = {}

class PrefetchRequest(BaseModel):
    symbol: str = "BTC/USDT"
    timeframe: str = "15m"
    start_date: str = "2025-01-01"
    end_date: str = "2026-01-01"

def _do_prefetch(symbol, timeframe, start_date, end_date):
    key = f"{symbol}_{timeframe}_{start_date}_{end_date}"
    _prefetch_status[key] = {"status": "running", "candles": 0, "error": None}
    try:
        candles, source = fetch_candles(symbol, timeframe, start_date, end_date)
        _prefetch_status[key] = {"status": "done", "candles": len(candles), "source": source, "error": None}
        print(f"Prefetch done: {symbol} {timeframe} — {len(candles)} candles from {source}")
    except Exception as e:
        _prefetch_status[key] = {"status": "error", "candles": 0, "error": str(e)}
        print(f"Prefetch error: {symbol} {timeframe} — {e}")

@app.post("/prefetch")
def prefetch(req: PrefetchRequest, background_tasks: BackgroundTasks):
    key = f"{req.symbol}_{req.timeframe}_{req.start_date}_{req.end_date}"
    if _prefetch_status.get(key, {}).get("status") == "running":
        return {"status": "already_running", "message": f"Already fetching {req.symbol} {req.timeframe}"}
    background_tasks.add_task(_do_prefetch, req.symbol, req.timeframe, req.start_date, req.end_date)
    _prefetch_status[key] = {"status": "queued", "candles": 0, "error": None}
    return {"status": "queued", "message": f"Fetching {req.symbol} {req.timeframe} {req.start_date}→{req.end_date} in background"}

@app.get("/prefetch-status")
def prefetch_status_check(symbol: str, timeframe: str, start_date: str, end_date: str):
    key  = f"{symbol}_{timeframe}_{start_date}_{end_date}"
    job  = _prefetch_status.get(key, {"status": "not_started", "candles": 0, "error": None})

    db_count = expected = pct = 0
    try:
        now_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
        end_ms   = now_ms if end_date == "now" else int(datetime.strptime(end_date[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        start_ms = int(datetime.strptime(start_date[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        tf_ms    = {"1m":60000,"5m":300000,"15m":900000,"1h":3600000,"4h":14400000,"1d":86400000}
        expected = (end_ms - start_ms) / tf_ms.get(timeframe, 900000)

        q_base = f"symbol=eq.{symbol}&timeframe=eq.{timeframe}&ts=gte.{start_ms}&ts=lte.{end_ms}"
        res = httpx.get(
            f"{SUPABASE_URL}/rest/v1/candles?{q_base}&select=ts",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Prefer": "count=exact",
                "Range-Unit": "items",
                "Range": "0-0"
            }, timeout=15
        )
        if res.status_code in [200, 206]:
            cr = res.headers.get("content-range", "0/0")
            if "/" in cr:
                total_part = cr.split("/")[-1]
                if total_part.isdigit():
                    db_count = int(total_part)
        pct = round(db_count / expected * 100, 1) if expected > 0 else 0
    except Exception as e:
        print(f"Prefetch status error: {e}")

    return {
        "symbol":          symbol,
        "timeframe":       timeframe,
        "job_status":      job["status"],
        "db_candles":      db_count,
        "expected":        int(expected),
        "completeness_pct":pct,
        "ready":           pct >= 75,
        "error":           job.get("error"),
    }

@app.get("/results-history")
def results_history():
    try:
        res = httpx.get(f"{SUPABASE_URL}/rest/v1/results?select=*&order=computed_at.desc&limit=500",
                       headers=HEADERS, timeout=10)
        if res.status_code == 200:
            return {"success": True, "results": res.json(), "total": len(res.json())}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/backtest")
def backtest(req: BacktestRequest):
    return process_request(req)

@app.post("/batch")
async def batch(req: BatchRequest):
    loop    = asyncio.get_event_loop()
    tasks   = [loop.run_in_executor(executor, process_request, cfg) for cfg in req.configs]
    results = await asyncio.gather(*tasks)
    return {"success": True, "results": list(results), "total": len(results)}

@app.post("/compare")
async def compare(req: CompareRequest):
    configs_a, configs_b = [], []
    for sym in req.symbols:
        for tf in req.timeframes:
            for pn in req.pivot_ns:
                for risk in req.risk_methods:
                    for rr in req.rr_ratios:
                        for fib in req.fib_levels:
                            for eng in req.engines:
                                for em in req.entry_modes:
                                    base = {
                                        "symbol":sym,"timeframe":tf,"pivot_n":pn,
                                        "risk_method":risk,"risk_pct":0.02,"rr":rr,
                                        "fib_level":fib,"max_bars":200,"max_hold":1000,
                                        "recency_bars":50,"one_per_pair":True,
                                        "engine":eng,"entry_mode":em
                                    }
                                    configs_a.append(BacktestRequest(**{**base,"start_date":req.period_a_start,"end_date":req.period_a_end}))
                                    configs_b.append(BacktestRequest(**{**base,"start_date":req.period_b_start,"end_date":req.period_b_end}))

    loop      = asyncio.get_event_loop()
    results_a = list(await asyncio.gather(*[loop.run_in_executor(executor, process_request, cfg) for cfg in configs_a]))
    results_b = list(await asyncio.gather(*[loop.run_in_executor(executor, process_request, cfg) for cfg in configs_b]))

    combined = []
    for a, b in zip(results_a, results_b):
        if not a.get("success") or not a.get("stats"): continue
        if not b.get("success") or not b.get("stats"): continue
        sa, sb = a["stats"], b["stats"]
        avg_return  = (sa["total_return"] + sb["total_return"]) / 2
        avg_sharpe  = (sa["sharpe"] + sb["sharpe"]) / 2
        avg_dd      = (sa["max_drawdown"] + sb["max_drawdown"]) / 2
        consistency = avg_return * (avg_sharpe / max(avg_dd, 1)) if avg_dd > 0 else 0
        combined.append({
            "symbol":          a["symbol"],
            "timeframe":       a["timeframe"],
            "risk":            a["risk_method"],
            "pivot_n":         a["pivot_n"],
            "rr":              a["rr"],
            "fib_level":       a.get("fib_level", 0.618),
            "engine":          a.get("engine", "—"),
            "entry_mode":      a.get("entry_mode", "—"),
            "period_a_return": round(sa["total_return"], 2),
            "period_b_return": round(sb["total_return"], 2),
            "period_a_dd":     round(sa["max_drawdown"], 2),
            "period_b_dd":     round(sb["max_drawdown"], 2),
            "period_a_sharpe": round(sa["sharpe"], 2),
            "period_b_sharpe": round(sb["sharpe"], 2),
            "period_a_trades": sa["total_trades"],
            "period_b_trades": sb["total_trades"],
            "avg_return":      round(avg_return, 2),
            "avg_sharpe":      round(avg_sharpe, 2),
            "avg_dd":          round(avg_dd, 2),
            "consistency_score":round(consistency, 2),
            "both_positive":   sa["total_return"] > 0 and sb["total_return"] > 0,
        })
    combined.sort(key=lambda x: x["consistency_score"], reverse=True)
    return {
        "success":  True,
        "results":  combined,
        "total":    len(combined),
        "period_a": f"{req.period_a_start} → {req.period_a_end}",
        "period_b": f"{req.period_b_start} → {req.period_b_end}",
    }

@app.delete("/cache")
def clear_cache():
    _mem_cache.clear()
    return {"status": "Memory cache cleared"}

class DeleteCandlesRequest(BaseModel):
    symbol: str
    timeframe: str
    start_date: str = "2025-01-01"
    end_date: str = "now"

@app.delete("/delete-candles")
def delete_candles(req: DeleteCandlesRequest):
    try:
        now_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
        start_ms = int(datetime.strptime(req.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_ms   = now_ms if req.end_date == "now" else int(datetime.strptime(req.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)

        # Delete from Supabase
        query = (f"symbol=eq.{req.symbol}&timeframe=eq.{req.timeframe}"
                 f"&ts=gte.{start_ms}&ts=lte.{end_ms}")
        res = httpx.delete(
            f"{SUPABASE_URL}/rest/v1/candles?{query}",
            headers={**HEADERS, "Prefer": "return=representation"},
            timeout=30
        )

        # Also clear memory cache keys matching this symbol+timeframe
        keys_to_clear = [k for k in _mem_cache if k.startswith(f"{req.symbol}_{req.timeframe}")]
        for k in keys_to_clear:
            del _mem_cache[k]

        deleted = len(res.json()) if res.status_code == 200 else 0
        print(f"Deleted {deleted} candles for {req.symbol} {req.timeframe} — cleared {len(keys_to_clear)} memory cache keys")
        return {
            "success":  True,
            "symbol":   req.symbol,
            "timeframe":req.timeframe,
            "deleted":  deleted,
            "cache_cleared": len(keys_to_clear)
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# ── MATRIX RUNNER BACKGROUND THREAD ──────────────────────
import threading

_matrix_thread = None
_matrix_running = False

def _run_matrix_thread():
    global _matrix_running
    _matrix_running = True
    try:
        import importlib.util, sys, os
        # Try to import matrix_runner from same directory
        spec = importlib.util.spec_from_file_location(
            "matrix_runner",
            os.path.join(os.path.dirname(__file__), "matrix_runner.py")
        )
        if spec:
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            mod.main()
        else:
            print("matrix_runner.py not found")
    except Exception as e:
        print(f"Matrix runner error: {e}")
    finally:
        _matrix_running = False

@app.post("/batch")
async def batch_backtest(request: Request):
    """Run multiple backtest configs in one call. Returns list of results."""
    body    = await request.json()
    configs = body.get("configs", [])
    results = []
    for cfg in configs:
        try:
            req = BacktestRequest(**cfg)
            res = process_request(req)
            results.append(res)
        except Exception as e:
            results.append({"success": False, "error": str(e)})
    return {"results": results}

@app.post("/run-matrix")
def run_matrix():
    global _matrix_thread, _matrix_running
    if _matrix_running:
        return {"success": False, "message": "Already running"}
    def _run_compute():
        global _matrix_running
        _matrix_running = True
        try:
            import importlib.util, os
            spec = importlib.util.spec_from_file_location(
                "matrix_runner",
                os.path.join(os.path.dirname(__file__), "matrix_runner.py")
            )
            if spec:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                mod.main_compute()
        except Exception as e:
            print(f"Compute error: {e}")
        finally:
            _matrix_running = False
    _matrix_thread = threading.Thread(target=_run_compute, daemon=True)
    _matrix_thread.start()
    return {"success": True, "message": "Computation started — check Runner tab"}

@app.post("/prefetch-candles")
def prefetch_candles_endpoint():
    global _matrix_thread, _matrix_running
    if _matrix_running:
        return {"success": False, "message": "Already running"}
    def _run_prefetch():
        global _matrix_running
        _matrix_running = True
        try:
            import importlib.util, os
            spec = importlib.util.spec_from_file_location(
                "matrix_runner",
                os.path.join(os.path.dirname(__file__), "matrix_runner.py")
            )
            if spec:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                mod.main_prefetch()
        except Exception as e:
            print(f"Prefetch error: {e}")
        finally:
            _matrix_running = False
    _matrix_thread = threading.Thread(target=_run_prefetch, daemon=True)
    _matrix_thread.start()
    return {"success": True, "message": "Prefetch started — check Runner tab"}

@app.get("/matrix-status")
def matrix_status():
    try:
        res = httpx.get(
            f"{SUPABASE_URL}/rest/v1/matrix_status?id=eq.1&select=*",
            headers=HEADERS, timeout=10
        )
        if res.status_code == 200 and res.json():
            data = res.json()[0]
            # Add live running flag
            data["is_running"] = _matrix_running
            return {"success": True, **data}
        return {"success": False, "status": "not_started", "completed": 0,
                "total": 0, "phase": "", "is_running": _matrix_running}
    except Exception as e:
        return {"success": False, "error": str(e), "is_running": _matrix_running}

@app.get("/matrix-results/all")
def matrix_results_all():
    """Export ALL results — fetches all pages from Supabase and returns complete CSV."""
    from fastapi.responses import Response
    import io, csv as csv_mod

    fields = ["pair","timeframe","engine","entry_mode","pivot_n","rr","fib_level",
              "ema_pair","adx_min","return_pct","cagr","max_dd","sharpe","profit_factor",
              "win_rate","trades","wins","losses","avg_win","avg_loss","kelly_full",
              "total_fees","period_start","period_end"]

    # Fetch all rows by paginating with offset
    all_rows = []
    offset   = 0
    batch    = 1000
    while True:
        try:
            # Use offset pagination — most reliable with Supabase
            q = (f"select={','.join(fields)}&order=sharpe.desc.nullslast"
                 f"&limit={batch}&offset={offset}")
            hdrs = {
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Prefer": "count=none",
            }
            res = httpx.get(
                f"{SUPABASE_URL}/rest/v1/matrix_results?{q}",
                headers=hdrs, timeout=60
            )
            if res.status_code != 200:
                print(f"Supabase error {res.status_code}: {res.text[:200]}")
                break
            rows = res.json()
            if not rows: break
            all_rows.extend(rows)
            print(f"Fetched {len(all_rows)} rows so far...")
            if len(rows) < batch: break
            offset += batch
        except Exception as e:
            print(f"Export page error at offset {offset}: {e}")
            break

    # Build CSV in memory
    buf = io.StringIO()
    w   = csv_mod.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    w.writeheader()
    w.writerows(all_rows)
    csv_str = buf.getvalue()

    print(f"Returning {len(all_rows)} rows as CSV ({len(csv_str):,} bytes)")
    return Response(
        content=csv_str,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=waddle_matrix_all.csv"}
    )

@app.get("/matrix-results/export")
def matrix_results_export(
    min_return: float = 0,
    min_sharpe: float = 0,
    min_trades: int   = 0,
    max_dd:     float = 100,
    pair:       str   = "",
    engine:     str   = "",
):
    from fastapi.responses import StreamingResponse
    import io, csv as csv_mod

    fields = ["pair","timeframe","engine","entry_mode","pivot_n","rr","fib_level",
              "ema_pair","adx_min",
              "return_pct","cagr","max_dd","sharpe","profit_factor",
              "win_rate","trades","wins","losses",
              "avg_win","avg_loss","kelly_full","total_fees",
              "period_start","period_end"]

    def generate():
        # Header row
        buf = io.StringIO()
        w   = csv_mod.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        yield buf.getvalue()

        # Paginate through Supabase in chunks of 5000
        filters = ["success=eq.true"]
        if min_return > 0: filters.append(f"return_pct=gte.{min_return}")
        if min_sharpe > 0: filters.append(f"sharpe=gte.{min_sharpe}")
        if min_trades > 0: filters.append(f"trades=gte.{min_trades}")
        if max_dd < 100:   filters.append(f"max_dd=lte.{max_dd}")
        if pair:            filters.append(f"pair=eq.{pair}")
        if engine:          filters.append(f"engine=eq.{engine}")
        base_q = "&".join(filters) + "&order=sharpe.desc"

        offset = 0
        batch  = 5000
        while True:
            try:
                q   = base_q + f"&limit={batch}&offset={offset}&select={','.join(fields)}"
                res = httpx.get(
                    f"{SUPABASE_URL}/rest/v1/matrix_results?{q}",
                    headers=HEADERS, timeout=60
                )
                if res.status_code != 200: break
                rows = res.json()
                if not rows: break
                buf = io.StringIO()
                w   = csv_mod.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
                w.writerows(rows)
                yield buf.getvalue()
                if len(rows) < batch: break
                offset += batch
            except Exception as e:
                print(f"Export page error: {e}")
                break

    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=waddle_matrix.csv"}
    )

@app.get("/journal")
def journal():
    try:
        acc_res    = httpx.get(f"{SUPABASE_URL}/rest/v1/paper_account?id=eq.1&select=*", headers=HEADERS, timeout=10)
        trades_res = httpx.get(f"{SUPABASE_URL}/rest/v1/paper_trades?select=*&order=created_at.desc&limit=500", headers=HEADERS, timeout=10)
        account    = acc_res.json()[0] if acc_res.status_code == 200 and acc_res.json() else None
        trades     = trades_res.json() if trades_res.status_code == 200 else []
        return {"success": True, "account": account, "trades": trades}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/live-journal")
def live_journal():
    try:
        acc_res    = httpx.get(f"{SUPABASE_URL}/rest/v1/live_account?id=eq.1&select=*", headers=HEADERS, timeout=10)
        trades_res = httpx.get(f"{SUPABASE_URL}/rest/v1/live_trades?select=*&order=created_at.desc&limit=500", headers=HEADERS, timeout=10)
        account    = acc_res.json()[0] if acc_res.status_code == 200 and acc_res.json() else None
        trades     = trades_res.json() if trades_res.status_code == 200 else []
        return {"success": True, "account": account, "trades": trades}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/candles")
def get_candles(
    symbol:    str = "XRP/USDT",
    timeframe: str = "15m",
    start_ms:  int = 0,
    end_ms:    int = 0,
    limit:     int = 5000,
):
    """
    Serve candle data from Supabase cache to the frontend chart.
    Paginates through all candles in the range up to limit.
    """
    try:
        if not SUPABASE_URL:
            return {"success": False, "error": "Supabase not configured", "candles": []}
        limit   = min(limit, 10000)
        all_rows = []
        offset   = 0
        page     = 1000
        while len(all_rows) < limit:
            filters = f"symbol=eq.{symbol}&timeframe=eq.{timeframe}&order=ts.asc&limit={page}&offset={offset}&select=ts,open,high,low,close"
            if start_ms > 0: filters += f"&ts=gte.{start_ms}"
            if end_ms   > 0: filters += f"&ts=lte.{end_ms}"
            res = httpx.get(
                f"{SUPABASE_URL}/rest/v1/candles?{filters}",
                headers=HEADERS, timeout=30
            )
            if res.status_code != 200:
                break
            rows = res.json()
            if not rows: break
            all_rows += rows
            if len(rows) < page: break
            offset += page
        return {"success": True, "candles": all_rows[:limit], "count": len(all_rows[:limit])}
    except Exception as e:
        return {"success": False, "error": str(e), "candles": []}
