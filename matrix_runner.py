#!/usr/bin/env python3
# ============================================================
# WADDLE MATRIX RUNNER v3
# Two-phase approach:
#   Phase 1: prefetch_candles() — download + cache to Supabase
#   Phase 2: run_computation()  — vectorised backtest, fast
#
# Triggered via /prefetch-candles or /run-matrix API endpoints
# Progress: Telegram every 10min + Supabase matrix_status table
# ============================================================

import os, time, httpx, itertools, io, csv as csv_mod
import numpy as np
from datetime import datetime, timezone
import ccxt

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

TOTAL_COMBOS = (len(PAIRS)*len(TIMEFRAMES)*len(ENGINES)*len(ENTRY_MODES)*
                len(PIVOT_NS)*len(RR_RATIOS)*len(FIB_LEVELS)*len(EMA_PAIRS)*len(ADX_MINS))

# ── TELEGRAM ───────────────────────────────────────────────────
def tg(msg):
    try:
        httpx.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"HTML"},
            timeout=10
        )
    except Exception as e:
        print(f"Telegram error: {e}")

# ── STATUS ─────────────────────────────────────────────────────
def update_status(phase, status, completed, total, detail=""):
    try:
        httpx.post(
            f"{SUPABASE_URL}/rest/v1/matrix_status",
            json=[{"id":1,"phase":phase,"status":status,"completed":completed,
                   "total":total,"detail":detail,
                   "updated_at":datetime.now(timezone.utc).isoformat()}],
            headers={**HEADERS,"Prefer":"resolution=merge-duplicates"},
            timeout=10
        )
    except: pass

# ── SUPABASE CANDLE HELPERS ────────────────────────────────────
def get_cached_candles(symbol, timeframe):
    start_ms = int(datetime.strptime(PERIOD_START,"%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
    end_ms   = int(datetime.strptime(PERIOD_END,  "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
    all_rows = []
    offset   = 0
    try:
        while True:
            q = (f"symbol=eq.{symbol}&timeframe=eq.{timeframe}"
                 f"&ts=gte.{start_ms}&ts=lte.{end_ms}"
                 f"&order=ts.asc&limit=10000&offset={offset}&select=ts,open,high,low,close")
            res = httpx.get(f"{SUPABASE_URL}/rest/v1/candles?{q}",headers=HEADERS,timeout=30)
            if res.status_code==200:
                rows=res.json()
                if not rows: break
                all_rows+=rows
                if len(rows)<10000: break
                offset+=10000
            else: break
    except Exception as e:
        print(f"Cache read error: {e}")
    if len(all_rows)>100:
        return [[r["ts"],r["open"],r["high"],r["low"],r["close"],0] for r in all_rows]
    return None

def save_candles_to_supabase(symbol, timeframe, candles):
    save_headers = {**HEADERS,"Prefer":"return=minimal,resolution=ignore-duplicates"}
    for i in range(0,len(candles),500):
        batch = candles[i:i+500]
        rows  = [{"symbol":symbol,"timeframe":timeframe,"ts":c[0],
                  "open":float(c[1]),"high":float(c[2]),"low":float(c[3]),
                  "close":float(c[4]),"volume":float(c[5] if len(c)>5 else 0)} for c in batch]
        try:
            httpx.post(f"{SUPABASE_URL}/rest/v1/candles",json=rows,headers=save_headers,timeout=30)
        except Exception as e:
            print(f"Save candles error: {e}")

def fetch_from_exchange(symbol, timeframe):
    ex = ccxt.kucoin({"enableRateLimit":True})
    start_ms = int(datetime.strptime(PERIOD_START,"%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
    end_ms   = int(datetime.strptime(PERIOD_END,  "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
    all_candles=[]
    since=start_ms
    empty_count=0
    while since<end_ms:
        try:
            batch=ex.fetch_ohlcv(symbol,timeframe,since=since,limit=1000)
            if not batch:
                empty_count+=1
                if empty_count>=3: break
                time.sleep(2); continue
            empty_count=0
            filtered=[c for c in batch if c[0]<end_ms]
            all_candles+=filtered
            if batch[-1][0]>=end_ms: break
            since=batch[-1][0]+1
            time.sleep(0.5)
        except Exception as e:
            if "429" in str(e) or "rate" in str(e).lower():
                print(f"Rate limited, waiting 10s..."); time.sleep(10)
            else:
                print(f"Fetch error: {e}"); break
    return all_candles

# ── PHASE 1: PREFETCH CANDLES ──────────────────────────────────
def prefetch_candles():
    """Download all pair+TF candles and save to Supabase."""
    combos = list(itertools.product(PAIRS, TIMEFRAMES))
    total  = len(combos)
    tg(f"""📥 <b>Phase 1: Prefetch Candles Started</b>
Pairs: {len(PAIRS)} × TF: {len(TIMEFRAMES)} = {total} combinations
Period: {PERIOD_START} → {PERIOD_END}
Will notify every 10 minutes.""")

    completed  = 0
    last_tg    = time.time()
    start_time = time.time()

    for symbol, tf in combos:
        detail = f"{symbol} {tf}"
        update_status("prefetch","running",completed,total,detail)
        print(f"\nChecking: {symbol} {tf}")

        # Check if already cached
        cached = get_cached_candles(symbol, tf)
        tf_ms  = {"5m":300000,"15m":900000,"1h":3600000}.get(tf,300000)
        start_ms = int(datetime.strptime(PERIOD_START,"%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
        end_ms   = int(datetime.strptime(PERIOD_END,  "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
        expected = (end_ms-start_ms)//tf_ms

        if cached and len(cached)>=expected*0.8:
            print(f"  Already cached: {len(cached)} candles ({len(cached)/expected*100:.0f}%)")
            completed+=1
        else:
            print(f"  Fetching live: {symbol} {tf}...")
            candles = fetch_from_exchange(symbol, tf)
            if candles:
                save_candles_to_supabase(symbol, tf, candles)
                print(f"  Saved: {len(candles)} candles")
            else:
                print(f"  Failed to fetch {symbol} {tf}")
            completed+=1

        # Telegram update every 10 min
        if time.time()-last_tg>600:
            elapsed = time.time()-start_time
            rate    = completed/elapsed if elapsed>0 else 0
            eta     = (total-completed)/rate if rate>0 else 0
            tg(f"📥 <b>Prefetch Progress</b>\n"
               f"Done: {completed}/{total}\n"
               f"Current: {detail}\n"
               f"ETA: {eta/60:.0f} min")
            last_tg = time.time()

    update_status("prefetch","done",total,total,"")
    tg(f"""✅ <b>Phase 1 Complete — Candles Cached</b>
All {total} pair+TF combinations saved to Supabase.
Now click <b>Run Computation</b> in the Runner tab.""")
    print("\nPhase 1 complete.")

# ── INDICATOR HELPERS ──────────────────────────────────────────
def calc_ema(closes, period):
    k   = 2/(period+1)
    ema = np.empty(len(closes))
    ema[0] = closes[0]
    for i in range(1,len(closes)):
        ema[i] = closes[i]*k + ema[i-1]*(1-k)
    return ema

def calc_adx(highs, lows, closes, period):
    n    = len(highs)
    adx  = np.zeros(n)
    pdm  = np.zeros(n)
    mdm  = np.zeros(n)
    tr_a = np.zeros(n)
    for i in range(1,n):
        pdm[i]  = max(highs[i]-highs[i-1],0) if highs[i]-highs[i-1]>lows[i-1]-lows[i] else 0
        mdm[i]  = max(lows[i-1]-lows[i],0) if lows[i-1]-lows[i]>highs[i]-highs[i-1] else 0
        tr_a[i] = max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),abs(lows[i]-closes[i-1]))
    sm_tr=sm_p=sm_m=0.0
    for i in range(1,period+1):
        sm_tr+=tr_a[i]; sm_p+=pdm[i]; sm_m+=mdm[i]
    dx_a=np.zeros(n)
    for i in range(period+1,n):
        sm_tr=sm_tr-sm_tr/period+tr_a[i]
        sm_p =sm_p -sm_p/period +pdm[i]
        sm_m =sm_m -sm_m/period +mdm[i]
        p=(sm_p/sm_tr*100) if sm_tr>0 else 0
        m=(sm_m/sm_tr*100) if sm_tr>0 else 0
        s=p+m
        dx_a[i]=abs(p-m)/s*100 if s>0 else 0
    start=period*2
    if start<n:
        adx[start]=sum(dx_a[period+1:start+1])/period
    for i in range(start+1,n):
        adx[i]=(adx[i-1]*(period-1)+dx_a[i])/period
    return adx

def find_pivots(highs, lows, N):
    n      = len(highs)
    pivots = []
    for i in range(N,n-N):
        if highs[i]==max(highs[i-N:i+N+1]):
            pivots.append({"idx":i,"type":"H","price":float(highs[i])})
        elif lows[i]==min(lows[i-N:i+N+1]):
            pivots.append({"idx":i,"type":"L","price":float(lows[i])})
    deduped=[]
    for p in pivots:
        if not deduped: deduped.append(p); continue
        last=deduped[-1]
        if last["type"]==p["type"]:
            if p["type"]=="H" and p["price"]>last["price"]: deduped[-1]=p
            elif p["type"]=="L" and p["price"]<last["price"]: deduped[-1]=p
        else: deduped.append(p)
    return deduped

# ── BACKTEST ENGINE ────────────────────────────────────────────
def run_backtest(highs, lows, closes, opens, n, pivots_cache,
                 engine, entry_mode, pivot_n, rr, fib_level,
                 ema_f, ema_s, use_ema, adx_v, adx_threshold):

    pivots = pivots_cache.get(pivot_n) or find_pivots(highs, lows, pivot_n)
    N      = pivot_n
    equity = 100.0
    trades = []

    def allowed(idx, direction):
        if idx>=n: return False
        if use_ema and ema_f is not None:
            if direction=="bull" and ema_f[idx]<=ema_s[idx]: return False
            if direction=="bear" and ema_f[idx]>=ema_s[idx]: return False
        if adx_v is not None and adx_v[idx]<adx_threshold: return False
        return True

    def check_exit(direction, sl, tp, start_ci):
        for ci in range(start_ci, min(start_ci+200,n)):
            if direction=="LONG":
                if lows[ci]<=sl:  return sl,ci
                if highs[ci]>=tp: return tp,ci
            else:
                if highs[ci]>=sl: return sl,ci
                if lows[ci]<=tp:  return tp,ci
        return None,None

    def record(entry,sl,tp,xp,won):
        nonlocal equity
        rpp=abs(entry-sl)
        if rpp<=0: return
        pos=equity*RISK_PCT/rpp
        notional=pos*entry
        gross=(xp-entry)*pos if won else (entry-xp)*pos if xp==sl else (xp-entry)*pos
        # Correct PnL direction
        if entry<xp: gross_=(xp-entry)*pos
        else:        gross_=(entry-xp)*pos
        gross=gross_ if (entry<tp and tp>sl) else -gross_  # bull
        # Simpler approach
        if sl<entry:  # LONG
            gross=(xp-entry)*pos
        else:         # SHORT
            gross=(entry-xp)*pos
        fee=notional*(0.0002+( 0.0002 if won else 0.00055))
        pnl=gross-fee
        equity+=pnl
        trades.append({"won":won,"pnl":pnl,"eq":equity})

    if engine=="original":
        bias=None; used=-1
        for pi in range(2,len(pivots)):
            p1,p2,p3=pivots[pi-2],pivots[pi-1],pivots[pi]
            st=None
            if p1["type"]=="H" and p2["type"]=="L" and p3["type"]=="H" and p3["price"]<p1["price"]: st="bear"
            elif p1["type"]=="L" and p2["type"]=="H" and p3["type"]=="L" and p3["price"]>p1["price"]: st="bull"
            if not st or p3["idx"]<=used: continue
            if bias and bias!=st: continue
            fh=p1["price"] if st=="bear" else p2["price"]
            fl=p2["price"] if st=="bear" else p1["price"]
            rng=fh-fl
            if rng<=0: continue
            fib_e=fl+rng*fib_level if st=="bear" else fh-rng*fib_level
            sl_l=fh+rng*0.02 if st=="bear" else fl-rng*0.02
            ec=None
            for ci in range(p3["idx"]+N+1,min(p3["idx"]+200,n-1)):
                if st=="bear":
                    if highs[ci]>fh: break
                    if highs[ci]>=fib_e: ec=ci; break
                else:
                    if lows[ci]<fl: break
                    if lows[ci]<=fib_e: ec=ci; break
            if ec is None: continue
            if not allowed(ec,st): continue
            rpp=abs(fib_e-sl_l)
            if rpp<=0: continue
            tp=fib_e+rpp*rr if st=="bull" else fib_e-rpp*rr
            xp,xc=check_exit("LONG" if st=="bull" else "SHORT",sl_l,tp,ec+1)
            if xp is None: continue
            won=xp==tp
            pos=equity*RISK_PCT/rpp
            notional=pos*fib_e
            gross=(xp-fib_e)*pos if st=="bull" else (fib_e-xp)*pos
            fee=notional*(0.0002+(0.0002 if won else 0.00055))
            pnl=gross-fee; equity+=pnl
            trades.append({"won":won,"pnl":pnl,"eq":equity})
            bias=None if won else ("bull" if st=="bear" else "bear")
            used=p3["idx"]

    elif engine in ("bos_pivot","pullback"):
        v2=engine=="pullback"
        MIN_RNG=0.002
        ph_d={p["idx"]:p["price"] for p in pivots if p["type"]=="H"}
        pl_d={p["idx"]:p["price"] for p in pivots if p["type"]=="L"}

        for st in ["bull","bear"]:
            src=[p for p in pivots if p["type"]==("H" if st=="bull" else "L")]
            setups=[]
            for p1 in src:
                pi1,pr1=p1["idx"],p1["price"]
                for ci in range(pi1+1,n-1):
                    if st=="bull" and closes[ci]>pr1:
                        p2=float(min(lows[pi1:ci+1]))
                        rng=closes[ci]-p2
                        if rng>0 and rng/max(p2,1)>=MIN_RNG:
                            setups.append({"st":st,"p1_idx":pi1,"p1_price":pr1,"p2":p2,
                                "p3_idx":ci,"p3_close":closes[ci],"fib618":p2+rng*fib_level,"sl":p2})
                        break
                    elif st=="bear" and closes[ci]<pr1:
                        p2=float(max(highs[pi1:ci+1]))
                        rng=p2-closes[ci]
                        if rng>0 and rng/max(p2,1)>=MIN_RNG:
                            setups.append({"st":st,"p1_idx":pi1,"p1_price":pr1,"p2":p2,
                                "p3_idx":ci,"p3_close":closes[ci],"fib618":p2-rng*fib_level,"sl":p2})
                        break
                    if (st=="bull" and lows[ci]<pr1*0.90) or (st=="bear" and highs[ci]>pr1*1.10): break
            setups.sort(key=lambda x:x["p3_idx"])
            si=0; active=None; last_p3=-1; ci=1
            while ci<n-1:
                if active is None:
                    while si<len(setups):
                        s=setups[si]; si+=1
                        if s["p3_idx"]<=last_p3: continue
                        active=s; ci=s["p3_idx"]+1; break
                    if active is None: break
                p2_v,fib_e,sl_v=active["p2"],active["fib618"],active["sl"]
                if v2:
                    piv_d=ph_d if st=="bull" else pl_d
                    if ci in piv_d:
                        nv=piv_d[ci]
                        if (st=="bull" and nv>active["p1_price"]) or (st=="bear" and nv<active["p1_price"]):
                            active["p1_idx"]=ci; active["p1_price"]=nv; active["p3_idx"]=ci; active["p3_close"]=nv
                            new_p2=float(min(lows[active["p1_idx"]:ci+1])) if st=="bull" else float(max(highs[active["p1_idx"]:ci+1]))
                            rng=abs(nv-new_p2)
                            if rng>0 and rng/max(new_p2,1)>=MIN_RNG:
                                active["p2"]=new_p2; active["fib618"]=new_p2+rng*fib_level if st=="bull" else new_p2-rng*fib_level
                                active["sl"]=new_p2; p2_v=active["p2"]; fib_e=active["fib618"]; sl_v=active["sl"]
                            ci+=1; continue
                        elif (st=="bull" and nv>active["p3_close"]) or (st=="bear" and nv<active["p3_close"]):
                            new_p2=float(min(lows[active["p3_idx"]:ci+1])) if st=="bull" else float(max(highs[active["p3_idx"]:ci+1]))
                            rng=abs(nv-new_p2)
                            if rng>0 and rng/max(new_p2,1)>=MIN_RNG:
                                active["p2"]=new_p2; active["p3_idx"]=ci; active["p3_close"]=nv
                                active["fib618"]=new_p2+rng*fib_level if st=="bull" else new_p2-rng*fib_level
                                active["sl"]=new_p2; p2_v=active["p2"]; fib_e=active["fib618"]; sl_v=active["sl"]
                            ci+=1; continue
                if (st=="bull" and lows[ci]<p2_v) or (st=="bear" and highs[ci]>p2_v):
                    active=None; ci+=1; continue
                if not allowed(ci,st): ci+=1; continue
                trig=False
                if entry_mode=="touch": trig=lows[ci]<=fib_e if st=="bull" else highs[ci]>=fib_e
                elif entry_mode in ("rejection","reclaim"):
                    trig=(lows[ci]<=fib_e and closes[ci]>fib_e) if st=="bull" else (highs[ci]>=fib_e and closes[ci]<fib_e)
                if not trig: ci+=1; continue
                sl_use=p2_v*(1-0.001) if st=="bull" else p2_v*(1+0.001)
                rpp=abs(fib_e-sl_use)
                if rpp<=0: active=None; ci+=1; continue
                tp=fib_e+rpp*rr if st=="bull" else fib_e-rpp*rr
                xp,xc=check_exit("LONG" if st=="bull" else "SHORT",sl_use,tp,ci+1)
                if xp is None: active=None; ci=n; continue
                won=xp==tp
                pos=equity*RISK_PCT/rpp; notional=pos*fib_e
                gross=(xp-fib_e)*pos if st=="bull" else (fib_e-xp)*pos
                fee=notional*(0.0002+(0.0002 if won else 0.00055))
                pnl=gross-fee; equity+=pnl
                trades.append({"won":won,"pnl":pnl,"eq":equity})
                last_p3=active["p3_idx"]; active=None; ci=xc+1

    elif engine=="structure":
        ph_l=[p for p in pivots if p["type"]=="H"]
        pl_l=[p for p in pivots if p["type"]=="L"]
        for st in ["bull","bear"]:
            src=ph_l if st=="bull" else pl_l
            for i in range(1,len(src)):
                pp,pc=src[i-1],src[i]
                if (st=="bull" and pc["price"]<=pp["price"]) or (st=="bear" and pc["price"]>=pp["price"]): continue
                p2=float(min(lows[pp["idx"]:pc["idx"]+1])) if st=="bull" else float(max(highs[pp["idx"]:pc["idx"]+1]))
                rng=abs(pc["price"]-p2)
                if rng<=0: continue
                fib_e=pc["price"]-rng*fib_level if st=="bull" else pc["price"]+rng*fib_level
                sl_l=p2*(1-0.001) if st=="bull" else p2*(1+0.001)
                for ci in range(pc["idx"]+1,n-1):
                    if not allowed(ci,st): continue
                    if (st=="bull" and lows[ci]<sl_l) or (st=="bear" and highs[ci]>sl_l): break
                    trig=False
                    if entry_mode=="touch": trig=lows[ci]<=fib_e if st=="bull" else highs[ci]>=fib_e
                    elif entry_mode in ("rejection","reclaim"):
                        trig=(lows[ci]<=fib_e and closes[ci]>fib_e) if st=="bull" else (highs[ci]>=fib_e and closes[ci]<fib_e)
                    if not trig: continue
                    rpp=abs(fib_e-sl_l)
                    if rpp<=0: break
                    tp=fib_e+rpp*rr if st=="bull" else fib_e-rpp*rr
                    xp,xc=check_exit("LONG" if st=="bull" else "SHORT",sl_l,tp,ci+1)
                    if xp is None: break
                    won=xp==tp
                    pos=equity*RISK_PCT/rpp; notional=pos*fib_e
                    gross=(xp-fib_e)*pos if st=="bull" else (fib_e-xp)*pos
                    fee=notional*(0.0002+(0.0002 if won else 0.00055))
                    pnl=gross-fee; equity+=pnl
                    trades.append({"won":won,"pnl":pnl,"eq":equity})
                    break

    elif engine=="ema_cross":
        if ema_f is None or ema_s is None: return None
        atr_v=np.zeros(n)
        for i in range(1,n):
            tr=max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),abs(lows[i]-closes[i-1]))
            atr_v[i]=(atr_v[i-1]*(N-1)+tr)/N if i>=N else tr
        for st in ["bull","bear"]:
            in_t=False; entry=sl_l=tp_l=pos_l=notional_l=None
            for ci in range(max(50,int(len(closes)*0.05)),n-1):
                ef,es=ema_f[ci],ema_s[ci]
                if in_t:
                    xp_=None
                    if st=="bull":
                        if lows[ci]<=sl_l: xp_=sl_l
                        elif highs[ci]>=tp_l: xp_=tp_l
                    else:
                        if highs[ci]>=sl_l: xp_=sl_l
                        elif lows[ci]<=tp_l: xp_=tp_l
                    if xp_ is not None:
                        won=xp_==tp_l
                        gross=(xp_-entry)*pos_l if st=="bull" else (entry-xp_)*pos_l
                        fee=notional_l*(0.0002+(0.0002 if won else 0.00055))
                        pnl=gross-fee; equity+=pnl
                        trades.append({"won":won,"pnl":pnl,"eq":equity})
                        in_t=False
                    continue
                if adx_v is not None and adx_v[ci]<adx_threshold: continue
                if st=="bull":
                    if not(closes[ci]>ef and closes[ci]>es and ef>es): continue
                    tf=lows[ci]<=ef and closes[ci]>ef
                    ts=lows[ci]<=es and closes[ci]>es
                    if not(tf or ts): continue
                    ep=ef if tf else es
                else:
                    if not(closes[ci]<ef and closes[ci]<es and ef<es): continue
                    tf=highs[ci]>=ef and closes[ci]<ef
                    ts=highs[ci]>=es and closes[ci]<es
                    if not(tf or ts): continue
                    ep=ef if tf else es
                atr=atr_v[ci]
                if atr<=0: continue
                sl=ep-atr*1.5 if st=="bull" else ep+atr*1.5
                rpp=abs(ep-sl)
                if rpp<=0: continue
                tp=ep+rpp*rr if st=="bull" else ep-rpp*rr
                pos_l=equity*RISK_PCT/rpp; notional_l=pos_l*ep
                entry=ep; sl_l=sl; tp_l=tp; in_t=True

    # Stats
    if not trades: return None
    wins=[t for t in trades if t["won"]]
    losses=[t for t in trades if not t["won"]]
    final=trades[-1]["eq"]
    tr=(final-100)/100*100
    wr=len(wins)/len(trades)*100 if trades else 0
    peak=100; mdd=0
    for t in trades:
        if t["eq"]>peak: peak=t["eq"]
        mdd=max(mdd,(peak-t["eq"])/peak*100)
    rets=[t["pnl"]/(t["eq"]-t["pnl"])*100 for t in trades if (t["eq"]-t["pnl"])!=0]
    mean=sum(rets)/len(rets) if rets else 0
    std=(sum((r-mean)**2 for r in rets)/len(rets))**0.5 if rets else 0
    sharpe=mean/std*(365**0.5) if std>0 else 0
    gw=sum(t["pnl"] for t in wins)
    gl=abs(sum(t["pnl"] for t in losses))
    pf=gw/gl if gl>0 else 999
    kf=(wr/100-(1-wr/100)/(gw/len(wins)/(gl/len(losses)))) if wins and losses else 0
    total_fees=sum(abs(t["pnl"])*(0.04 if t["won"] else 0.075)/abs(t["pnl"]) for t in trades if t["pnl"]!=0)
    return {
        "trades":len(trades),"wins":len(wins),"losses":len(losses),
        "wr":round(wr,2),"return_pct":round(tr,2),"cagr":round(((final/100)**(365/365)-1)*100,2),
        "max_dd":round(mdd,2),"sharpe":round(sharpe,2),"pf":round(pf,2),
        "avg_win":round(gw/len(wins),4) if wins else 0,
        "avg_loss":round(gl/len(losses),4) if losses else 0,
        "kelly_full":round(kf,3),
        "total_fees":round(total_fees,4),
    }

# ── PHASE 2: COMPUTATION ───────────────────────────────────────
def run_computation():
    """Load cached candles, run all combos, save results."""
    # Clear old results first
    try:
        httpx.delete(f"{SUPABASE_URL}/rest/v1/matrix_results?id=gt.0",
                     headers=HEADERS,timeout=30)
        print("Cleared old matrix_results")
    except: pass

    tg(f"""🔢 <b>Phase 2: Computation Started</b>
Total combos: {TOTAL_COMBOS:,}
Pairs: {len(PAIRS)} × TF: {len(TIMEFRAMES)} × Engines: {len(ENGINES)}
EMA pairs: {len(EMA_PAIRS)} × ADX: {len(ADX_MINS)}
Period: {PERIOD_START} → {PERIOD_END}

Variables being tested:
• Engines: {', '.join(ENGINES)}
• Entry: {', '.join(ENTRY_MODES)}
• Pivot N: {PIVOT_NS}
• RR: {RR_RATIOS}
• Fib: {FIB_LEVELS}
• EMA: {EMA_PAIRS}
• ADX min: {ADX_MINS}""")

    completed  = 0
    saved      = 0
    errors     = 0
    start_time = time.time()
    last_tg    = time.time()
    save_buf   = []

    for symbol,tf in itertools.product(PAIRS,TIMEFRAMES):
        print(f"\nLoading: {symbol} {tf}")
        candles=get_cached_candles(symbol,tf)
        if not candles or len(candles)<100:
            print(f"  No candles — skip"); continue

        highs  = np.array([c[2] for c in candles],dtype=float)
        lows   = np.array([c[3] for c in candles],dtype=float)
        closes = np.array([c[4] for c in candles],dtype=float)
        opens  = np.array([c[1] for c in candles],dtype=float)
        n      = len(candles)

        # Precompute indicators once per pair+TF
        ema_cache  = {}  # (fast,slow) → (ema_f,ema_s)
        adx_cache  = {}  # period → adx_arr
        piv_cache  = {}  # N → pivots

        for pn in PIVOT_NS:
            piv_cache[pn]=find_pivots(highs,lows,pn)
        for ep in EMA_PAIRS:
            if ep!="off":
                f,s=map(int,ep.split("/"))
                if (f,s) not in ema_cache:
                    ema_cache[(f,s)]=(calc_ema(closes,f),calc_ema(closes,s))
        for ax in ADX_MINS:
            if ax>0 and 14 not in adx_cache:
                adx_cache[14]=calc_adx(highs,lows,closes,14)

        pair_combos=list(itertools.product(ENGINES,ENTRY_MODES,PIVOT_NS,RR_RATIOS,FIB_LEVELS,EMA_PAIRS,ADX_MINS))
        print(f"  Running {len(pair_combos):,} combos on {n} candles...")

        for eng,em,pn,rr,fib,ep,ax in pair_combos:
            try:
                use_ema=ep!="off"
                if use_ema:
                    f,s=map(int,ep.split("/"))
                    ema_f,ema_s=ema_cache.get((f,s),(None,None))
                else:
                    ema_f=ema_s=None; f=s=0

                adx_v=adx_cache.get(14) if ax>0 else None

                stats=run_backtest(highs,lows,closes,opens,n,piv_cache,
                                   eng,em,pn,rr,fib,ema_f,ema_s,use_ema,adx_v,float(ax))

                row={
                    "combo_key":f"{symbol}|{tf}|{eng}|{em}|{pn}|{rr}|{fib}|{ep}|{ax}",
                    "pair":symbol.replace("/USDT",""),
                    "timeframe":tf,"engine":eng,"entry_mode":em,
                    "pivot_n":pn,"rr":rr,"fib_level":fib,
                    "ema_pair":ep,"adx_min":ax,
                    "period_start":PERIOD_START,"period_end":PERIOD_END,
                    "success":stats is not None,
                    "return_pct":stats["return_pct"] if stats else None,
                    "cagr":stats["cagr"] if stats else None,
                    "max_dd":stats["max_dd"] if stats else None,
                    "sharpe":stats["sharpe"] if stats else None,
                    "profit_factor":stats["pf"] if stats else None,
                    "win_rate":stats["wr"] if stats else None,
                    "trades":stats["trades"] if stats else 0,
                    "wins":stats["wins"] if stats else 0,
                    "losses":stats["losses"] if stats else 0,
                    "avg_win":stats["avg_win"] if stats else None,
                    "avg_loss":stats["avg_loss"] if stats else None,
                    "kelly_full":stats["kelly_full"] if stats else None,
                    "total_fees":stats["total_fees"] if stats else None,
                    "computed_at":datetime.now(timezone.utc).isoformat(),
                }
                save_buf.append(row)
                if stats: saved+=1
            except Exception as e:
                errors+=1

            completed+=1

            # Save every 500 results
            if len(save_buf)>=500:
                try:
                    httpx.post(f"{SUPABASE_URL}/rest/v1/matrix_results",
                               json=save_buf,headers=HEADERS,timeout=30)
                except Exception as e:
                    print(f"Save error: {e}")
                save_buf=[]

            # Telegram every 10 min
            if time.time()-last_tg>600:
                elapsed=time.time()-start_time
                rate=completed/elapsed if elapsed>0 else 0
                eta=(TOTAL_COMBOS-completed)/rate if rate>0 else 0
                tg(f"⏳ <b>Computation Progress</b>\n"
                   f"Done: {completed:,}/{TOTAL_COMBOS:,} ({completed/TOTAL_COMBOS*100:.1f}%)\n"
                   f"Saved: {saved:,} | Errors: {errors}\n"
                   f"Current: {symbol} {tf}\n"
                   f"Rate: {rate:.0f}/s | ETA: {eta/60:.0f} min")
                update_status("compute","running",completed,TOTAL_COMBOS,f"{symbol} {tf}")
                last_tg=time.time()

        # Save remaining for this pair+TF
        if save_buf:
            try:
                httpx.post(f"{SUPABASE_URL}/rest/v1/matrix_results",
                           json=save_buf,headers=HEADERS,timeout=30)
            except Exception as e:
                print(f"Save error: {e}")
            save_buf=[]

    elapsed=time.time()-start_time
    update_status("compute","done",TOTAL_COMBOS,TOTAL_COMBOS,"")
    tg(f"""✅ <b>Phase 2 Complete — Computation Done</b>

Total combos: {TOTAL_COMBOS:,}
Saved: {saved:,} results
Errors: {errors:,}
Time: {elapsed/3600:.2f}h

Download results from Backtest Lab → Runner tab.""")
    print(f"\nComputation complete. Saved {saved:,} results in {elapsed/3600:.2f}h")

# ── ENTRY POINTS ───────────────────────────────────────────────
def main_prefetch():
    prefetch_candles()

def main_compute():
    run_computation()

def main():
    """Default: run both phases."""
    prefetch_candles()
    run_computation()

if __name__=="__main__":
    import sys
    if len(sys.argv)>1 and sys.argv[1]=="prefetch":
        main_prefetch()
    elif len(sys.argv)>1 and sys.argv[1]=="compute":
        main_compute()
    else:
        main()
