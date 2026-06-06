#!/usr/bin/env python3
# ============================================================
# WADDLE MATRIX RUNNER v2
# Runs as a Railway background process
# Triggered by deploying this file via Telegram bot
#
# Flow:
#   1. Fetches candle data from Supabase/exchange per pair+tf
#   2. Runs all strategy combos in pure Python (no HTTP)
#   3. Saves results to Supabase matrix_results table
#   4. Sends Telegram progress updates + completion summary
# ============================================================

import os, time, json, httpx, itertools, io, csv
import numpy as np
from datetime import datetime, timezone

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SUPABASE_URL     = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY     = os.environ.get("SUPABASE_KEY", "")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal,resolution=ignore-duplicates",
}

# ── VARIABLE SPACE ─────────────────────────────────────────────
PAIRS       = ["DOGE/USDT","XLM/USDT","XRP/USDT","ADA/USDT","TRX/USDT","ARB/USDT"]
TIMEFRAMES  = ["5m","15m","1h"]
ENGINES     = ["original","bos_pivot","structure","pullback","ema_cross"]
ENTRY_MODES = ["touch","rejection","reclaim"]
PIVOT_NS    = [3,5,8]
RR_RATIOS   = [1.5,2.0,3.0,4.0]
FIB_LEVELS  = [0.382,0.5,0.618]
EMA_PAIRS   = ["off","34/55","55/89","89/144","144/169"]
ADX_MINS    = [0,15,25]
PERIOD_START= "2025-01-01"
PERIOD_END  = "2026-01-01"
RISK_PCT    = 0.02

# ── TELEGRAM ───────────────────────────────────────────────────
def send_telegram(msg):
    try:
        httpx.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        print(f"Telegram error: {e}")

# ── SUPABASE ───────────────────────────────────────────────────
def get_candles(symbol, timeframe):
    """Fetch all candles for symbol+timeframe from Supabase."""
    import ccxt
    # Try Supabase cache first
    try:
        start_ms = int(datetime.strptime(PERIOD_START, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
        end_ms   = int(datetime.strptime(PERIOD_END,   "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
        all_rows = []
        offset   = 0
        while True:
            q = (f"symbol=eq.{symbol}&timeframe=eq.{timeframe}"
                 f"&ts=gte.{start_ms}&ts=lte.{end_ms}"
                 f"&order=ts.asc&limit=10000&offset={offset}"
                 f"&select=ts,open,high,low,close,volume")
            res = httpx.get(f"{SUPABASE_URL}/rest/v1/candles?{q}", headers=HEADERS, timeout=30)
            if res.status_code == 200:
                rows = res.json()
                if not rows: break
                all_rows += rows
                if len(rows) < 10000: break
                offset += 10000
            else:
                break
        if len(all_rows) > 100:
            candles = [[r["ts"],r["open"],r["high"],r["low"],r["close"],r["volume"]] for r in all_rows]
            print(f"  Cache: {symbol} {timeframe} — {len(candles)} candles")
            return candles
    except Exception as e:
        print(f"  Supabase error: {e}")

    # Fallback — fetch live from exchange
    print(f"  Fetching live: {symbol} {timeframe}...")
    try:
        ex = ccxt.kucoin({"enableRateLimit": True})
        start_ms = int(datetime.strptime(PERIOD_START, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
        end_ms   = int(datetime.strptime(PERIOD_END,   "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
        all_candles = []
        since = start_ms
        while since < end_ms:
            batch = ex.fetch_ohlcv(symbol, timeframe, since=since, limit=1000)
            if not batch: break
            filtered = [c for c in batch if c[0] < end_ms]
            all_candles += filtered
            if batch[-1][0] >= end_ms: break
            since = batch[-1][0] + 1
            time.sleep(0.5)
        print(f"  Fetched: {symbol} {timeframe} — {len(all_candles)} candles")
        return all_candles
    except Exception as e:
        print(f"  Fetch error: {e}")
        return []

def save_batch(rows):
    if not rows: return
    try:
        httpx.post(
            f"{SUPABASE_URL}/rest/v1/matrix_results",
            json=rows, headers=HEADERS, timeout=30
        )
    except Exception as e:
        print(f"Save error: {e}")

def get_done_keys():
    try:
        res = httpx.get(
            f"{SUPABASE_URL}/rest/v1/matrix_results?select=combo_key&limit=500000",
            headers={**HEADERS, "Prefer": ""},
            timeout=30
        )
        if res.status_code == 200:
            return {r["combo_key"] for r in res.json()}
    except Exception as e:
        print(f"Done keys error: {e}")
    return set()

def update_status(status, completed, total, pair="", tf=""):
    try:
        httpx.post(
            f"{SUPABASE_URL}/rest/v1/matrix_status",
            json=[{"id":1,"status":status,"completed":completed,
                   "total":total,"current_pair":pair,"current_tf":tf,
                   "updated_at":datetime.now(timezone.utc).isoformat()}],
            headers={**HEADERS,"Prefer":"resolution=merge-duplicates"},
            timeout=10
        )
    except: pass

# ── ENGINE HELPERS ─────────────────────────────────────────────
def calc_ema_arr(arr, period):
    k   = 2 / (period + 1)
    ema = np.zeros(len(arr))
    ema[0] = arr[0]
    for i in range(1, len(arr)):
        ema[i] = arr[i] * k + ema[i-1] * (1 - k)
    return ema

def calc_adx_arr(highs, lows, closes, period):
    n = len(highs)
    adx  = np.zeros(n)
    pdi  = np.zeros(n)
    mdi  = np.zeros(n)
    tr_a = np.zeros(n)
    pdm  = np.zeros(n)
    mdm  = np.zeros(n)
    for i in range(1, n):
        pdm[i]  = max(highs[i]-highs[i-1], 0) if highs[i]-highs[i-1] > lows[i-1]-lows[i] else 0
        mdm[i]  = max(lows[i-1]-lows[i], 0)   if lows[i-1]-lows[i] > highs[i]-highs[i-1] else 0
        tr_a[i] = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
    sm_tr = sm_p = sm_m = sum(tr_a[1:period+1])
    sm_p  = sum(pdm[1:period+1])
    sm_m  = sum(mdm[1:period+1])
    dx_a  = np.zeros(n)
    for i in range(period+1, n):
        sm_tr = sm_tr - sm_tr/period + tr_a[i]
        sm_p  = sm_p  - sm_p/period  + pdm[i]
        sm_m  = sm_m  - sm_m/period  + mdm[i]
        p = (sm_p/sm_tr*100) if sm_tr > 0 else 0
        m = (sm_m/sm_tr*100) if sm_tr > 0 else 0
        pdi[i] = p; mdi[i] = m
        s = p + m
        dx_a[i] = abs(p-m)/s*100 if s > 0 else 0
    start = period*2
    adx_sum = sum(dx_a[period+1:start+1])
    if start < n: adx[start] = adx_sum/period
    for i in range(start+1, n):
        adx[i] = (adx[i-1]*(period-1)+dx_a[i])/period
    return adx

def find_pivots(highs, lows, N):
    pivots = []
    n = len(highs)
    for i in range(N, n-N):
        if highs[i] == max(highs[i-N:i+N+1]):
            pivots.append({"idx":i,"type":"H","price":float(highs[i])})
        elif lows[i] == min(lows[i-N:i+N+1]):
            pivots.append({"idx":i,"type":"L","price":float(lows[i])})
    deduped = []
    for p in pivots:
        if not deduped: deduped.append(p); continue
        last = deduped[-1]
        if last["type"] == p["type"]:
            if p["type"]=="H" and p["price"]>last["price"]: deduped[-1]=p
            elif p["type"]=="L" and p["price"]<last["price"]: deduped[-1]=p
        else:
            deduped.append(p)
    return deduped

# ── BACKTEST ENGINE ────────────────────────────────────────────
def run_backtest(candles, engine, entry_mode, pivot_n, rr, fib_level,
                 ema_fast, ema_slow, use_ema, adx_threshold, adx_period=14):
    if len(candles) < 60: return None

    highs  = np.array([c[2] for c in candles])
    lows   = np.array([c[3] for c in candles])
    closes = np.array([c[4] for c in candles])
    opens  = np.array([c[1] for c in candles])
    n      = len(candles)
    ts     = [c[0] for c in candles]

    # EMA arrays
    ema_f = calc_ema_arr(closes, ema_fast) if (use_ema or engine=="ema_cross") else None
    ema_s = calc_ema_arr(closes, ema_slow) if (use_ema or engine=="ema_cross") else None

    # ADX array
    adx_v = calc_adx_arr(highs, lows, closes, adx_period) if adx_threshold > 0 else None

    def allowed(idx, direction):
        if idx >= n: return False
        if use_ema and ema_f is not None:
            if direction=="bull" and ema_f[idx]<=ema_s[idx]: return False
            if direction=="bear" and ema_f[idx]>=ema_s[idx]: return False
        if adx_v is not None and adx_v[idx] < adx_threshold: return False
        return True

    pivots = find_pivots(highs, lows, pivot_n)
    N      = pivot_n
    equity = 100.0
    trades = []

    def make_trade(direction, entry, sl, tp, entry_idx, exit_idx, exit_price, p1, p2, p3, fib_e):
        nonlocal equity
        rpp  = abs(entry - sl)
        if rpp <= 0: return False
        pos  = (equity * RISK_PCT) / rpp
        notional = pos * entry
        gross = (exit_price-entry)*pos if direction=="LONG" else (entry-exit_price)*pos
        won   = exit_price == tp
        fee_e = notional * 0.0002
        fee_x = notional * 0.0002 if won else notional * 0.00055
        pnl   = gross - fee_e - fee_x
        equity += pnl
        trades.append({"won":won,"pnl":pnl,"equity":equity})
        return True

    def check_exit(direction, sl, tp, start_ci, max_ci):
        for ci in range(start_ci, min(start_ci+200, max_ci)):
            if direction == "LONG":
                if lows[ci]  <= sl: return sl, ci
                if highs[ci] >= tp: return tp, ci
            else:
                if highs[ci] >= sl: return sl, ci
                if lows[ci]  <= tp: return tp, ci
        return None, None

    # ── ENGINES ────────────────────────────────────────────────
    if engine == "original":
        bias = None
        used = -1
        for pi in range(2, len(pivots)):
            p1,p2,p3 = pivots[pi-2],pivots[pi-1],pivots[pi]
            st = None
            if p1["type"]=="H" and p2["type"]=="L" and p3["type"]=="H" and p3["price"]<p1["price"]: st="bear"
            elif p1["type"]=="L" and p2["type"]=="H" and p3["type"]=="L" and p3["price"]>p1["price"]: st="bull"
            if not st or p3["idx"]<=used: continue
            if bias and bias!=st: continue
            fh = p1["price"] if st=="bear" else p2["price"]
            fl = p2["price"] if st=="bear" else p1["price"]
            rng = fh-fl
            if rng<=0: continue
            fib_e = fl+rng*fib_level if st=="bear" else fh-rng*fib_level
            sl_l  = fh+rng*0.02 if st=="bear" else fl-rng*0.02
            search = p3["idx"]+N+1
            ec = None
            for ci in range(search, min(p3["idx"]+200, n-1)):
                if st=="bear":
                    if highs[ci]>fh: break
                    if highs[ci]>=fib_e: ec=ci; break
                else:
                    if lows[ci]<fl: break
                    if lows[ci]<=fib_e: ec=ci; break
            if ec is None: continue
            if not allowed(ec, st): continue
            entry = fib_e
            rpp = abs(entry-sl_l)
            if rpp<=0: continue
            tp = entry+rpp*rr if st=="bull" else entry-rpp*rr
            xp,xc = check_exit("LONG" if st=="bull" else "SHORT", sl_l, tp, ec+1, n)
            if xp is None: continue
            make_trade("LONG" if st=="bull" else "SHORT", entry, sl_l, tp, ec, xc, xp, p1["price"],p2["price"],p3["price"],fib_e)
            bias = None if xp==tp else ("bull" if st=="bear" else "bear")
            used = p3["idx"]

    elif engine in ("bos_pivot","pullback"):
        v2 = engine=="pullback"
        ph_idx = {p["idx"]:p["price"] for p in pivots if p["type"]=="H"}
        pl_idx = {p["idx"]:p["price"] for p in pivots if p["type"]=="L"}
        MIN_RNG = 0.002

        def build_setups(st):
            setups = []
            src = [p for p in pivots if p["type"]==("H" if st=="bull" else "L")]
            for p1 in src:
                pi1,pr1 = p1["idx"],p1["price"]
                for ci in range(pi1+1, n-1):
                    if st=="bull" and closes[ci]>pr1:
                        p2 = float(min(closes[pi1:ci+1]))
                        rng = closes[ci]-p2
                        if rng>0 and rng/max(p2,1)>=MIN_RNG:
                            fib_e = p2+rng*fib_level
                            setups.append({"st":st,"p1_idx":pi1,"p1_price":pr1,"p2":p2,
                                "p3_idx":ci,"p3_close":closes[ci],"rng":rng,"fib618":fib_e,"sl":p2})
                        break
                    elif st=="bear" and closes[ci]<pr1:
                        p2 = float(max(closes[pi1:ci+1]))
                        rng = p2-closes[ci]
                        if rng>0 and rng/max(p2,1)>=MIN_RNG:
                            fib_e = p2-rng*fib_level
                            setups.append({"st":st,"p1_idx":pi1,"p1_price":pr1,"p2":p2,
                                "p3_idx":ci,"p3_close":closes[ci],"rng":rng,"fib618":fib_e,"sl":p2})
                        break
                    if (st=="bull" and lows[ci]<pr1*0.90) or (st=="bear" and highs[ci]>pr1*1.10): break
            setups.sort(key=lambda x:x["p3_idx"])
            return setups

        for st in ["bull","bear"]:
            setups = build_setups(st)
            si = 0; active = None; last_p3 = -1; ci = 1
            while ci < n-1:
                if active is None:
                    while si < len(setups):
                        s = setups[si]; si+=1
                        if s["p3_idx"]<=last_p3: continue
                        active=s; ci=s["p3_idx"]+1; break
                    if active is None: break
                p2,fib_e,sl_l = active["p2"],active["fib618"],active["sl"]
                if v2 and ci in (ph_idx if st=="bull" else pl_idx):
                    nv = ph_idx[ci] if st=="bull" else pl_idx[ci]
                    # P1 reset: new extreme above original P1
                    if (st=="bull" and nv>active["p1_price"]) or (st=="bear" and nv<active["p1_price"]):
                        active["p1_idx"]=ci; active["p1_price"]=nv
                        active["p3_idx"]=ci; active["p3_close"]=nv
                        new_p2 = float(min(lows[active["p1_idx"]:ci+1])) if st=="bull" else float(max(highs[active["p1_idx"]:ci+1]))
                        rng = abs(nv-new_p2)
                        if rng>0 and rng/max(new_p2,1)>=MIN_RNG:
                            active["p2"]=new_p2; active["rng"]=rng
                            active["fib618"]=new_p2+rng*fib_level if st=="bull" else new_p2-rng*fib_level
                            active["sl"]=new_p2
                            p2=active["p2"]; fib_e=active["fib618"]; sl_l=active["sl"]
                        ci+=1; continue
                    elif (st=="bull" and nv>active["p3_close"]) or (st=="bear" and nv<active["p3_close"]):
                        new_p2 = float(min(lows[active["p3_idx"]:ci+1])) if st=="bull" else float(max(highs[active["p3_idx"]:ci+1]))
                        rng = abs(nv-new_p2)
                        if rng>0 and rng/max(new_p2,1)>=MIN_RNG:
                            active["p2"]=new_p2; active["p3_idx"]=ci; active["p3_close"]=nv
                            active["rng"]=rng
                            active["fib618"]=new_p2+rng*fib_level if st=="bull" else new_p2-rng*fib_level
                            active["sl"]=new_p2
                            p2=active["p2"]; fib_e=active["fib618"]; sl_l=active["sl"]
                        ci+=1; continue
                if (st=="bull" and lows[ci]<p2) or (st=="bear" and highs[ci]>p2):
                    active=None; ci+=1; continue
                if not allowed(ci, st): ci+=1; continue
                trig = False
                if entry_mode=="touch":
                    trig = lows[ci]<=fib_e if st=="bull" else highs[ci]>=fib_e
                elif entry_mode in ("rejection","reclaim"):
                    trig = (lows[ci]<=fib_e and closes[ci]>fib_e) if st=="bull" else (highs[ci]>=fib_e and closes[ci]<fib_e)
                if not trig: ci+=1; continue
                entry = fib_e
                sl_use = p2*(1-0.001) if st=="bull" else p2*(1+0.001)
                rpp = abs(entry-sl_use)
                if rpp<=0: active=None; ci+=1; continue
                tp = entry+rpp*rr if st=="bull" else entry-rpp*rr
                xp,xc = check_exit("LONG" if st=="bull" else "SHORT", sl_use, tp, ci+1, n)
                if xp is None: active=None; ci=n; continue
                make_trade("LONG" if st=="bull" else "SHORT",entry,sl_use,tp,ci,xc,xp,
                           active["p1_price"],p2,active["p3_close"],fib_e)
                last_p3=active["p3_idx"]; active=None; ci=xc+1

    elif engine == "structure":
        ph_list = [p for p in pivots if p["type"]=="H"]
        pl_list = [p for p in pivots if p["type"]=="L"]

        def run_dir_struct(st):
            src = ph_list if st=="bull" else pl_list
            for i in range(1, len(src)):
                pp,pc = src[i-1],src[i]
                if (st=="bull" and pc["price"]<=pp["price"]) or (st=="bear" and pc["price"]>=pp["price"]): continue
                if st=="bull":
                    p2  = float(min(lows[pp["idx"]:pc["idx"]+1]))
                    rng = pc["price"]-p2
                else:
                    p2  = float(max(highs[pp["idx"]:pc["idx"]+1]))
                    rng = p2-pc["price"]
                if rng<=0: continue
                fib_e = pc["price"]-rng*fib_level if st=="bull" else pc["price"]+rng*fib_level
                sl_l  = p2*(1-0.001) if st=="bull" else p2*(1+0.001)
                for ci in range(pc["idx"]+1, n-1):
                    if not allowed(ci, st): continue
                    if (st=="bull" and lows[ci]<sl_l) or (st=="bear" and highs[ci]>sl_l): break
                    trig = False
                    if entry_mode=="touch":
                        trig = lows[ci]<=fib_e if st=="bull" else highs[ci]>=fib_e
                    elif entry_mode in ("rejection","reclaim"):
                        trig = (lows[ci]<=fib_e and closes[ci]>fib_e) if st=="bull" else (highs[ci]>=fib_e and closes[ci]<fib_e)
                    if not trig: continue
                    entry = fib_e
                    rpp = abs(entry-sl_l)
                    if rpp<=0: break
                    tp = entry+rpp*rr if st=="bull" else entry-rpp*rr
                    xp,xc = check_exit("LONG" if st=="bull" else "SHORT", sl_l, tp, ci+1, n)
                    if xp is None: break
                    make_trade("LONG" if st=="bull" else "SHORT",entry,sl_l,tp,ci,xc,xp,pp["price"],p2,pc["price"],fib_e)
                    break

        for st in ["bull","bear"]:
            run_dir_struct(st)

    elif engine == "ema_cross":
        if ema_f is None or ema_s is None: return None
        atr_v = np.zeros(n)
        for i in range(1,n):
            tr = max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),abs(lows[i]-closes[i-1]))
            atr_v[i] = (atr_v[i-1]*(N-1)+tr)/N if i>=N else tr

        for st in ["bull","bear"]:
            in_trade=False; entry=sl_l=tp_l=pos_l=notional_l=None
            for ci in range(max(ema_slow,28)+1, n-1):
                ef,es = ema_f[ci],ema_s[ci]
                if in_trade:
                    xp_=None
                    if st=="bull":
                        if lows[ci]<=sl_l: xp_=sl_l
                        elif highs[ci]>=tp_l: xp_=tp_l
                    else:
                        if highs[ci]>=sl_l: xp_=sl_l
                        elif lows[ci]<=tp_l: xp_=tp_l
                    if xp_ is not None:
                        gross=(xp_-entry)*pos_l if st=="bull" else (entry-xp_)*pos_l
                        won=xp_==tp_l
                        fee_e=notional_l*0.0002; fee_x=notional_l*(0.0002 if won else 0.00055)
                        pnl=gross-fee_e-fee_x; equity+=pnl
                        trades.append({"won":won,"pnl":pnl,"equity":equity})
                        in_trade=False
                    continue
                if adx_v is not None and adx_v[ci]<adx_threshold: continue
                if st=="bull":
                    if not(closes[ci]>ef and closes[ci]>es and ef>es): continue
                    tch_f=lows[ci]<=ef and closes[ci]>ef
                    tch_s=lows[ci]<=es and closes[ci]>es
                    if not(tch_f or tch_s): continue
                    ep=ef if tch_f else es
                else:
                    if not(closes[ci]<ef and closes[ci]<es and ef<es): continue
                    tch_f=highs[ci]>=ef and closes[ci]<ef
                    tch_s=highs[ci]>=es and closes[ci]<es
                    if not(tch_f or tch_s): continue
                    ep=ef if tch_f else es
                atr=atr_v[ci]
                if atr<=0: continue
                sl=ep-atr*1.5 if st=="bull" else ep+atr*1.5
                rpp=abs(ep-sl)
                if rpp<=0: continue
                tp=ep+rpp*rr if st=="bull" else ep-rpp*rr
                pos_l=(equity*RISK_PCT)/rpp; notional_l=pos_l*ep
                entry=ep; sl_l=sl; tp_l=tp; in_trade=True

    # ── CALC STATS ─────────────────────────────────────────────
    if not trades: return None
    wins   = [t for t in trades if t["won"]]
    losses = [t for t in trades if not t["won"]]
    if not wins and not losses: return None
    final  = trades[-1]["equity"]
    tr     = (final-100)/100*100
    wr     = len(wins)/len(trades)*100
    peak=100; mdd=0
    for t in trades:
        if t["equity"]>peak: peak=t["equity"]
        mdd=max(mdd,(peak-t["equity"])/peak*100)
    rets   = [t["pnl"]/(t["equity"]-t["pnl"])*100 for t in trades]
    mean   = sum(rets)/len(rets)
    std    = (sum((r-mean)**2 for r in rets)/len(rets))**0.5
    sharpe = mean/std*(365**0.5) if std>0 else 0
    days   = 365
    cagr   = ((final/100)**(365/max(days,1))-1)*100
    gw     = sum(t["pnl"] for t in wins)
    gl     = abs(sum(t["pnl"] for t in losses))
    pf     = gw/gl if gl>0 else 999
    return {
        "trades":    len(trades),
        "wins":      len(wins),
        "wr":        round(wr,2),
        "return_pct":round(tr,2),
        "cagr":      round(cagr,2),
        "max_dd":    round(mdd,2),
        "sharpe":    round(sharpe,2),
        "pf":        round(pf,2),
        "avg_win":   round(gw/len(wins),4) if wins else 0,
        "avg_loss":  round(gl/len(losses),4) if losses else 0,
    }

# ── COMBO KEY ──────────────────────────────────────────────────
def combo_key(pair,tf,eng,em,pn,rr,fib,ep,ax):
    return f"{pair}|{tf}|{eng}|{em}|{pn}|{rr}|{fib}|{ep}|{ax}"

def parse_ema(ep):
    if ep=="off": return False,34,55
    f,s=ep.split("/"); return True,int(f),int(s)

# ── MAIN ───────────────────────────────────────────────────────
def main():
    print("🚀 Waddle Matrix Runner v2 starting...")
    send_telegram("🚀 <b>Matrix Runner Started</b>\nCalculating combos...")

    # Count total
    total_combos = (len(PAIRS)*len(TIMEFRAMES)*len(ENGINES)*len(ENTRY_MODES)*
                    len(PIVOT_NS)*len(RR_RATIOS)*len(FIB_LEVELS)*len(EMA_PAIRS)*len(ADX_MINS))
    print(f"Total combos: {total_combos:,}")

    done_keys  = get_done_keys()
    print(f"Already done: {len(done_keys):,}")

    send_telegram(f"""📊 <b>Matrix Runner</b>
Total combos: {total_combos:,}
Already done: {len(done_keys):,}
Remaining: {total_combos-len(done_keys):,}
Pairs: {len(PAIRS)} × TF: {len(TIMEFRAMES)}
Engines: {len(ENGINES)} × Entry: {len(ENTRY_MODES)}""")

    completed  = 0
    saved      = 0
    errors     = 0
    start_time = time.time()
    save_buf   = []
    last_tg    = time.time()

    for pair in PAIRS:
        for tf in TIMEFRAMES:
            print(f"\n{'='*50}")
            print(f"Loading: {pair} {tf}")
            candles = get_candles(pair, tf)
            if not candles or len(candles) < 100:
                print(f"  Skip — not enough candles")
                continue

            # Run all combos for this pair+tf
            pair_combos = list(itertools.product(
                ENGINES, ENTRY_MODES, PIVOT_NS, RR_RATIOS, FIB_LEVELS, EMA_PAIRS, ADX_MINS
            ))
            print(f"  Running {len(pair_combos):,} combos...")

            for eng,em,pn,rr,fib,ep,ax in pair_combos:
                key = combo_key(pair,tf,eng,em,pn,rr,fib,ep,ax)
                if key in done_keys:
                    completed += 1
                    continue

                use_ema, ema_fast, ema_slow = parse_ema(ep)
                try:
                    stats = run_backtest(
                        candles, eng, em, pn, rr, fib,
                        ema_fast, ema_slow, use_ema, float(ax)
                    )
                    row = {
                        "combo_key":   key,
                        "pair":        pair.replace("/USDT",""),
                        "timeframe":   tf,
                        "engine":      eng,
                        "entry_mode":  em,
                        "pivot_n":     pn,
                        "rr":          rr,
                        "fib_level":   fib,
                        "ema_pair":    ep,
                        "adx_min":     ax,
                        "period_start":PERIOD_START,
                        "period_end":  PERIOD_END,
                        "success":     stats is not None,
                        "return_pct":  stats["return_pct"] if stats else None,
                        "cagr":        stats["cagr"]       if stats else None,
                        "max_dd":      stats["max_dd"]     if stats else None,
                        "sharpe":      stats["sharpe"]     if stats else None,
                        "profit_factor":stats["pf"]        if stats else None,
                        "win_rate":    stats["wr"]         if stats else None,
                        "trades":      stats["trades"]     if stats else 0,
                        "avg_win":     stats["avg_win"]    if stats else None,
                        "avg_loss":    stats["avg_loss"]   if stats else None,
                        "computed_at": datetime.now(timezone.utc).isoformat(),
                    }
                    save_buf.append(row)
                    saved += 1
                except Exception as e:
                    errors += 1

                completed += 1

                # Save every 200 results
                if len(save_buf) >= 200:
                    save_batch(save_buf)
                    save_buf = []

                # Telegram update every 10 minutes
                if time.time() - last_tg > 600:
                    elapsed = time.time()-start_time
                    rate    = completed/elapsed if elapsed>0 else 0
                    eta     = (total_combos-completed)/rate if rate>0 else 0
                    send_telegram(
                        f"⏳ <b>Matrix Progress</b>\n"
                        f"Done: {completed:,}/{total_combos:,} ({completed/total_combos*100:.1f}%)\n"
                        f"Saved: {saved:,} | Errors: {errors}\n"
                        f"Current: {pair} {tf}\n"
                        f"ETA: {eta/3600:.1f}h"
                    )
                    update_status("running", completed, total_combos, pair, tf)
                    last_tg = time.time()

            # Save remaining after each pair+tf
            if save_buf:
                save_batch(save_buf)
                save_buf = []

    # Final save
    if save_buf:
        save_batch(save_buf)

    elapsed = time.time()-start_time
    update_status("done", completed, total_combos, "", "")

    summary = f"""✅ <b>Matrix Runner COMPLETE</b>

Total combos: {total_combos:,}
Saved: {saved:,}
Errors: {errors:,}
Time: {elapsed/3600:.2f}h

Download results from Backtest Lab → Matrix Runner tab."""

    send_telegram(summary)
    print(f"\n{'='*50}")
    print(summary.replace("<b>","").replace("</b>",""))

if __name__ == "__main__":
    main()
