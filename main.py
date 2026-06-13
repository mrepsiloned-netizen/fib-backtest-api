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
    entry_mode: str = "rejection"  # touch | rejection
    bos_break: str = "close"       # close | wick — BOS confirmation method
    engine: str = "structure"      # structure only
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
def run_backtest_core(candles, pivots, risk_pct, rr, fib_level, max_bars, max_hold, recency_bars, one_per_pair, min_swing_pct=0.002, stop_buffer_pct=0.001, k_stale=0, entry_mode="rejection", engine="structure", use_ema_filter=False, ema_fast=34, ema_slow=55, adx_period=14, adx_threshold=25.0, bos_break="close"):
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

    def get_sl(entry_price, direction, p2_price, candle_idx):
        """Return SL level — P2 structure SL with small buffer."""
        buf = p2_price * stop_buffer_pct
        return p2_price + buf if direction == "bear" else p2_price - buf

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

    # ── P1-P2-P3 ENGINE — faithful port of Pine "Master Algorithm v6.5" ──
    #
    # Both state machines run INTERLEAVED in one bar loop (Pine source order:
    # macro tracker → pivot fire → bull blocks → bear blocks), sharing
    # macro_trend / macro_extreme exactly like the indicator.
    #
    # Per side:
    #   STATE 0  idle. A pivot CONFIRMS at pivot_bar + N (no lookahead) → P1.
    #   STATE 1  hunting BOS. P2 floats (lowest low / highest high since P1,
    #            seeded with the confirmation window like ta.lowest(N+1)).
    #            Anchor FROZEN at P1 creation: prev_p2 if macro agrees,
    #            else macro_extreme. INVALID if price breaks the anchor.
    #            Any NEW same-side pivot while hunting replaces P1 (Pine).
    #            BOS = close beyond P1 (bos_break="close") or wick ("wick").
    #            On BOS: macro_trend set HERE, macro_extreme reset HERE,
    #            prev_p2 saved HERE, TTL = (BOS_bar − P1_bar) × 2 → STATE 2.
    #   STATE 2  LOCKED (allowStale=false): new pivots are ignored.
    #            KILLED if macro flips. P3 floats (>=) and the fib is
    #            re-drawn every bar. FAILED if P2 breaks. EXPIRED when
    #            bars since last P3 update > TTL.
    #            Trigger (Pine BUY/SELL): wick into fib + close back beyond
    #            it + candle in trade direction (green for longs).
    #            Trigger consumes the setup (machine resets) whether or not
    #            a position slot was free.
    #
    # Pine fall-through preserved: P1 block, state-1 block and state-2 block
    # are sequential ifs, so BOS bar also runs state-2 logic (same-bar
    # P3/fib/trigger), exactly like the indicator.
    #
    # Execution (backtest layer on top of the indicator):
    #   rejection → fill next open; the FILL CANDLE ITSELF is monitored
    #   touch     → fill at fib intrabar; monitored from next candle
    #   reclaim   → fill at signal close; monitored from next candle
    #   SL = P2 (buffered via get_sl), TP = RR × risk, SL checked before TP.
    #   One position per direction; equity compounds chronologically.
    #   Open positions at data end are flushed at last close as TIMEOUT.

    # Raw strict fractals fired at confirmation (ta.pivothigh/-low parity)
    conf_high, conf_low = {}, {}
    for i in range(N, n - N):
        if all(highs[i] > highs[i-N:i]) and all(highs[i] > highs[i+1:i+N+1]):
            conf_high[i + N] = (i, float(highs[i]))
        if all(lows[i] < lows[i-N:i]) and all(lows[i] < lows[i+1:i+N+1]):
            conf_low[i + N] = (i, float(lows[i]))

    mac = {"trend": 0, "ext": None, "ext_idx": None}

    def fresh_machine(side):
        return {"side": side, "state": 0,
                "p1_idx": None, "p1_price": None,
                "p2": None, "p2_idx": None,
                "prev_p2": None,             # persists across resets (Pine)
                "anchor": None,
                "p3": None, "p3_bar": None, "ttl": None,
                "fib": None, "c_watch": None}

    def reset_machine(m):
        # prev_p2 deliberately NOT cleared — Pine keeps prev_bull_p2 forever
        m.update(state=0, p1_idx=None, p1_price=None, p2=None, p2_idx=None,
                 anchor=None, p3=None, p3_bar=None, ttl=None, fib=None,
                 c_watch=None)

    mach = {"bull": fresh_machine("bull"), "bear": fresh_machine("bear")}
    pos  = {"bull": None, "bear": None}
    all_trades = []

    def close_position(side, xp, xr, xc):
        nonlocal equity
        po = pos[side]
        gross = (xp - po["entry"]) * po["size"] if side == "bull" \
                else (po["entry"] - xp) * po["size"]
        won = xr == "TP"
        fee = po["notional"] * 0.0002 + po["notional"] * (0.0002 if won else 0.00055)
        pnl = gross - fee
        equity += pnl
        all_trades.append({
            "id":         0,
            "direction":  "LONG" if side == "bull" else "SHORT",
            "p1_time":    po["p1_time"],
            "p2_time":    po["p2_time"],
            "p3_time":    po["p3_time"],
            "entry_time": timestamps[po["entry_candle"]],
            "exit_time":  timestamps[xc],
            "entry":      round(po["entry"], 6),
            "sl":         round(po["sl"], 6),
            "tp":         round(po["tp"], 6),
            "exit_price": round(xp, 6),
            "result":     xr,
            "gross_pnl":  round(gross, 4),
            "fee":        round(fee, 4),
            "pnl":        round(pnl, 4),
            "equity":     round(equity, 4),
            "won":        won,
            "p1":         round(po["p1"], 6),
            "p2":         round(po["p2"], 6),
            "p3":         round(po["p3"], 6),
            "fib_entry":  round(po["fib"], 6),
        })
        pos[side] = None

    def open_position(side, m, ci, entry_price, entry_candle, scan_start):
        sl_lvl = get_sl(entry_price, side, m["p2"], entry_candle)
        rpp = abs(entry_price - sl_lvl)
        if rpp <= 0: return
        rng = (m["p3"] - m["p2"]) if side == "bull" else (m["p2"] - m["p3"])
        if rng <= 0 or rng / max(min(m["p2"], m["p3"]), 1) < min_swing_pct: return
        tp = entry_price + rpp * rr if side == "bull" else entry_price - rpp * rr
        size = (equity * risk_pct) / rpp
        pos[side] = {
            "entry": entry_price, "sl": sl_lvl, "tp": tp,
            "entry_candle": entry_candle, "scan_start": scan_start,
            "size": size, "notional": size * entry_price,
            "p1": m["p1_price"], "p2": m["p2"], "p3": m["p3"], "fib": m["fib"],
            "p1_time": timestamps[m["p1_idx"]] if m["p1_idx"] is not None else None,
            "p2_time": timestamps[m["p2_idx"]] if m["p2_idx"] is not None else None,
            "p3_time": timestamps[m["p3_bar"]] if m["p3_bar"] is not None else None,
        }

    def step_machine(m, ci):
        side    = m["side"]
        c_high  = float(highs[ci]);  c_low  = float(lows[ci])
        c_close = float(closes[ci]); c_open = float(opens[ci])

        # ── NEW P1 (pivot confirms this bar; state 2 is locked) ──
        pv = conf_high.get(ci) if side == "bull" else conf_low.get(ci)
        if pv is not None and m["state"] != 2:
            p_idx, p_price = pv
            m["state"] = 1
            m["p1_idx"] = p_idx; m["p1_price"] = p_price
            if side == "bull":
                m["anchor"] = m["prev_p2"] if mac["trend"] == 1 else mac["ext"]
                m["p2"]     = float(min(lows[p_idx:ci + 1]))
                m["p2_idx"] = p_idx + int(np.argmin(lows[p_idx:ci + 1]))
            else:
                m["anchor"] = m["prev_p2"] if mac["trend"] == -1 else mac["ext"]
                m["p2"]     = float(max(highs[p_idx:ci + 1]))
                m["p2_idx"] = p_idx + int(np.argmax(highs[p_idx:ci + 1]))
            # falls through to state-1 processing this same bar (Pine)

        # ── STATE 1: float P2, INVALID vs anchor, hunt BOS ──
        if m["state"] == 1:
            if side == "bull" and c_low < m["p2"]:
                m["p2"] = c_low; m["p2_idx"] = ci
            elif side == "bear" and c_high > m["p2"]:
                m["p2"] = c_high; m["p2_idx"] = ci

            if m["anchor"] is not None:
                if side == "bull" and c_low < m["anchor"]:
                    reset_machine(m); return
                if side == "bear" and c_high > m["anchor"]:
                    reset_machine(m); return

            broke = False
            if side == "bull":
                broke = (c_close > m["p1_price"]) if bos_break == "close" else (c_high > m["p1_price"])
            else:
                broke = (c_close < m["p1_price"]) if bos_break == "close" else (c_low < m["p1_price"])

            if broke:
                m["state"] = 2
                mac["trend"]   = 1 if side == "bull" else -1
                mac["ext"]     = c_high if side == "bull" else c_low
                mac["ext_idx"] = ci
                m["prev_p2"]   = m["p2"]
                m["ttl"]       = (ci - m["p1_idx"]) * 2
                m["p3"]        = c_high if side == "bull" else c_low
                m["p3_bar"]    = ci
                # falls through to state-2 processing this same bar (Pine)

        # ── STATE 2: KILLED / float P3 / fib / FAILED / EXPIRED / trigger ──
        if m["state"] == 2:
            if (side == "bull" and mac["trend"] == -1) or (side == "bear" and mac["trend"] == 1):
                reset_machine(m); return

            if side == "bull" and c_high >= m["p3"]:
                m["p3"] = c_high; m["p3_bar"] = ci
            elif side == "bear" and c_low <= m["p3"]:
                m["p3"] = c_low; m["p3_bar"] = ci

            rng = (m["p3"] - m["p2"]) if side == "bull" else (m["p2"] - m["p3"])
            if rng > 0:
                m["fib"] = m["p3"] - rng * fib_level if side == "bull" \
                           else m["p3"] + rng * fib_level

            if side == "bull" and c_low < m["p2"]:
                reset_machine(m); return
            if side == "bear" and c_high > m["p2"]:
                reset_machine(m); return

            if m["ttl"] is not None and (ci - m["p3_bar"]) > m["ttl"]:
                reset_machine(m); return

            if m["fib"] is None:
                return

            fib = m["fib"]
            triggered = False; entry_price = None; entry_candle = None; scan_start = None

            if entry_mode == "rejection":
                # Pine BUY/SELL: wick into fib + close beyond + directional candle
                if side == "bull" and c_low <= fib and c_close > fib and c_close > c_open:
                    if ci + 1 < n:
                        triggered = True
                        entry_price  = float(opens[ci + 1])
                        entry_candle = ci + 1
                        scan_start   = ci + 1          # fill candle itself monitored
                elif side == "bear" and c_high >= fib and c_close < fib and c_close < c_open:
                    if ci + 1 < n:
                        triggered = True
                        entry_price  = float(opens[ci + 1])
                        entry_candle = ci + 1
                        scan_start   = ci + 1

            elif entry_mode == "touch":
                if side == "bull" and c_low <= fib:
                    triggered = True; entry_price = fib; entry_candle = ci; scan_start = ci + 1
                elif side == "bear" and c_high >= fib:
                    triggered = True; entry_price = fib; entry_candle = ci; scan_start = ci + 1

            elif entry_mode == "reclaim":
                if m["c_watch"] is None:
                    if side == "bull" and c_close < fib:   m["c_watch"] = ci
                    elif side == "bear" and c_close > fib: m["c_watch"] = ci
                else:
                    if (ci - m["c_watch"]) <= 2:
                        if side == "bull" and c_close > fib:
                            triggered = True; entry_price = c_close
                            entry_candle = ci; scan_start = ci + 1; m["c_watch"] = None
                        elif side == "bear" and c_close < fib:
                            triggered = True; entry_price = c_close
                            entry_candle = ci; scan_start = ci + 1; m["c_watch"] = None
                    else:
                        m["c_watch"] = None

            if not triggered or entry_price is None:
                return

            # EMA/ADX gate — blocked trigger leaves the zone armed (legacy)
            if not ema_allows(ci, side):
                return

            # Signal consumes the setup (Pine resets on BUY/SELL)…
            sig = dict(m)
            reset_machine(m)
            # …and opens a trade only if this direction's slot is free
            if pos[side] is None:
                open_position(side, sig, ci, entry_price, entry_candle, scan_start)

    # ── MAIN BAR LOOP — Pine source order per bar ──────────
    for ci in range(n):
        # 1) macro extreme tracker (top of Pine script)
        if mac["trend"] == 1:
            if mac["ext"] is None or highs[ci] > mac["ext"]:
                mac["ext"] = float(highs[ci]); mac["ext_idx"] = ci
        elif mac["trend"] == -1:
            if mac["ext"] is None or lows[ci] < mac["ext"]:
                mac["ext"] = float(lows[ci]); mac["ext_idx"] = ci

        # 2) manage open positions (SL before TP — conservative)
        for side in ("bull", "bear"):
            po = pos[side]
            if po is None or ci < po["scan_start"]:
                continue
            if side == "bull":
                if   lows[ci]  <= po["sl"]: close_position(side, po["sl"], "SL", ci)
                elif highs[ci] >= po["tp"]: close_position(side, po["tp"], "TP", ci)
            else:
                if   highs[ci] >= po["sl"]: close_position(side, po["sl"], "SL", ci)
                elif lows[ci]  <= po["tp"]: close_position(side, po["tp"], "TP", ci)

        # 3) state machines — bull block before bear block (Pine order)
        if ci < n - 1:
            step_machine(mach["bull"], ci)
            step_machine(mach["bear"], ci)

    # ── flush open positions at data end ────────────────────
    for side in ("bull", "bear"):
        if pos[side] is not None:
            close_position(side, float(closes[n - 1]), "TIMEOUT", n - 1)

    all_trades.sort(key=lambda t: t["entry_time"])
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
            **ema_kwargs, bos_break=req.bos_break,
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
            **ema_kwargs, bos_break=req.bos_break,
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
    return {"status": "Waddle Backtest API v11 — Structure Engine", "rules": "ema_cross+adx+pullback_p1reset"}

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

@app.post("/send-matrix-csv")
def send_matrix_csv():
    """Build full matrix CSV and send to Telegram as file."""
    import io, csv as csv_mod
    fields = ["pair","timeframe","engine","entry_mode","pivot_n","rr","fib_level",
              "ema_pair","adx_min","return_pct","cagr","max_dd","sharpe","profit_factor",
              "win_rate","trades","wins","losses","avg_win","avg_loss","kelly_full",
              "period_start","period_end"]
    try:
        all_rows = []
        offset = 0
        while True:
            q = f"select={','.join(fields)}&order=sharpe.desc.nullslast&limit=1000&offset={offset}"
            hdrs = {"apikey":SUPABASE_KEY,"Authorization":f"Bearer {SUPABASE_KEY}","Prefer":"count=none"}
            res = httpx.get(f"{SUPABASE_URL}/rest/v1/matrix_results?{q}",headers=hdrs,timeout=60)
            if res.status_code != 200: break
            rows = res.json()
            if not rows: break
            all_rows.extend(rows)
            if len(rows) < 1000: break
            offset += 1000

        buf = io.StringIO()
        w = csv_mod.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
        w.writeheader(); w.writerows(all_rows)
        csv_bytes = buf.getvalue().encode("utf-8")

        TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN","")
        TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID","")
        filename = f"waddle_matrix_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.csv"

        httpx.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
            data={"chat_id": TELEGRAM_CHAT_ID, "caption": f"Matrix results — {len(all_rows)} rows"},
            files={"document": (filename, csv_bytes, "text/csv")},
            timeout=120
        )
        return {"success": True, "rows": len(all_rows), "message": f"Sent {len(all_rows)} rows to Telegram"}
    except Exception as e:
        return {"success": False, "error": str(e)}

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
