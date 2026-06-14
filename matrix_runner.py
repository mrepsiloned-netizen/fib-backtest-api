#!/usr/bin/env python3
# ============================================================
# WADDLE MATRIX RUNNER v8
# Default: EMA Cross only (Engine 6)
# Args:
#   (none)    → EMA Cross only
#   bos       → BOS Pullback only
#   all       → both engines
#   prefetch  → fetch candles
# ============================================================

import os, time, httpx, itertools, io, csv as csv_mod
import numpy as np
from datetime import datetime, timezone

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN","")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID","")
SUPABASE_URL     = os.environ.get("SUPABASE_URL","")
SUPABASE_KEY     = os.environ.get("SUPABASE_KEY","")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal,resolution=ignore-duplicates",
}

# ── BOS PULLBACK VARIABLE SPACE ────────────────────────────────
BOS_PAIRS       = ["DOGE/USDT","XLM/USDT","XRP/USDT","ADA/USDT","TRX/USDT","ARB/USDT"]
BOS_TIMEFRAMES  = ["5m","15m","1h"]
BOS_ENTRY_MODES = ["rejection","reclaim"]
BOS_PIVOT_NS    = [3,5,8]
BOS_RR_RATIOS   = [1.5,2.0,3.0,4.0]
BOS_FIB_LEVELS  = [0.382,0.5,0.618]
BOS_EMA_PAIRS   = ["off","34/55","55/89","89/144","144/169"]
BOS_ADX_MINS    = [0,15,25]
PERIOD_START    = "2025-01-01"
PERIOD_END      = "2026-01-01"
BOS_TOTAL = (len(BOS_PAIRS)*len(BOS_TIMEFRAMES)*len(BOS_ENTRY_MODES)*
             len(BOS_PIVOT_NS)*len(BOS_RR_RATIOS)*len(BOS_FIB_LEVELS)*
             len(BOS_EMA_PAIRS)*len(BOS_ADX_MINS))

# ── EMA CROSS VARIABLE SPACE ───────────────────────────────────
EMA_PAIRS_LIST  = ["DOGE/USDT","XLM/USDT","XRP/USDT","TRX/USDT","ARB/USDT"]
EMA_TIMEFRAMES  = ["1m","5m","15m"]
EMA_FAST_LIST   = [9, 12]
EMA_SLOW_LIST   = [21, 26]
EMA_RR_LIST     = [1.5, 2.0]
EMA_VOL_OPTS    = [False, True]
EMA_GAP_OPTS    = [False, True]
EMA_HTF_OPTS    = [False, True]
EMA_TOTAL = (len(EMA_PAIRS_LIST)*len(EMA_TIMEFRAMES)*len(EMA_FAST_LIST)*
             len(EMA_SLOW_LIST)*len(EMA_RR_LIST)*
             len(EMA_VOL_OPTS)*len(EMA_GAP_OPTS)*len(EMA_HTF_OPTS))

RISK_PCT  = 0.02
MIN_SWING = 0.002
STOP_BUF  = 0.001

# ── HELPERS ────────────────────────────────────────────────────
def tg(msg):
    try:
        httpx.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                   json={"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"HTML"},timeout=10)
    except: pass

def set_status(phase, status, done, total, detail=""):
    try:
        httpx.post(f"{SUPABASE_URL}/rest/v1/matrix_status",
                   json=[{"id":1,"phase":phase,"status":status,"completed":done,
                          "total":total,"detail":detail,
                          "updated_at":datetime.now(timezone.utc).isoformat()}],
                   headers={**HEADERS,"Prefer":"resolution=merge-duplicates"},timeout=10)
    except: pass

def get_candles(symbol, timeframe, ps=None, pe=None):
    ps = ps or PERIOD_START
    pe = pe or PERIOD_END
    start_ms=int(datetime.strptime(ps,"%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
    end_ms  =int(datetime.strptime(pe,"%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
    rows=[]; offset=0
    while True:
        q=(f"symbol=eq.{symbol}&timeframe=eq.{timeframe}"
           f"&ts=gte.{start_ms}&ts=lte.{end_ms}"
           f"&order=ts.asc&limit=1000&offset={offset}&select=open,high,low,close,volume")
        res=httpx.get(f"{SUPABASE_URL}/rest/v1/candles?{q}",headers=HEADERS,timeout=60)
        if res.status_code==200:
            batch=res.json()
            if not batch: break
            rows+=batch
            if len(batch)<1000: break
            offset+=len(batch)
        else: break
    if len(rows)<50: return None
    return rows

def save_rows(rows):
    if not rows: return
    try:
        httpx.post(f"{SUPABASE_URL}/rest/v1/matrix_results",
                   json=rows,headers=HEADERS,timeout=30)
    except Exception as e:
        print(f"Save error: {e}")

# ── INDICATORS ─────────────────────────────────────────────────
def calc_ema(arr, period):
    k=2/(period+1); out=np.empty(len(arr)); out[0]=arr[0]
    for i in range(1,len(arr)): out[i]=arr[i]*k+out[i-1]*(1-k)
    return out

def calc_adx(H,L,C,period):
    n=len(H); adx_=np.zeros(n); pdm=np.zeros(n); mdm=np.zeros(n); tr=np.zeros(n)
    for i in range(1,n):
        pdm[i]=max(H[i]-H[i-1],0) if H[i]-H[i-1]>L[i-1]-L[i] else 0
        mdm[i]=max(L[i-1]-L[i],0) if L[i-1]-L[i]>H[i]-H[i-1] else 0
        tr[i]=max(H[i]-L[i],abs(H[i]-C[i-1]),abs(L[i]-C[i-1]))
    st_=sum(tr[1:period+1]); sp=sum(pdm[1:period+1]); sm=sum(mdm[1:period+1])
    dx=np.zeros(n)
    for i in range(period+1,n):
        st_=st_-st_/period+tr[i]; sp=sp-sp/period+pdm[i]; sm=sm-sm/period+mdm[i]
        pi_=(sp/st_*100) if st_>0 else 0; mi_=(sm/st_*100) if st_>0 else 0
        s=pi_+mi_; dx[i]=abs(pi_-mi_)/s*100 if s>0 else 0
    s2=period*2
    if s2<n: adx_[s2]=sum(dx[period+1:s2+1])/period
    for i in range(s2+1,n): adx_[i]=(adx_[i-1]*(period-1)+dx[i])/period
    return adx_

# ── BACKTEST — Pine Script P1-P2-P3 v6.5 ──────────────────────
def backtest(H, L, C, O, n, N, rr, fib_level, entry_mode,
             ema_f, ema_s, use_ema, adx_v, adx_thr):

    # Strict fractals — confirmed at bar i+N (no lookahead)
    conf_high, conf_low = {}, {}
    for i in range(N, n-N):
        if all(H[i]>H[i-N:i]) and all(H[i]>H[i+1:i+N+1]):
            conf_high[i+N] = (i, float(H[i]))
        if all(L[i]<L[i-N:i]) and all(L[i]<L[i+1:i+N+1]):
            conf_low[i+N]  = (i, float(L[i]))

    def ema_ok(idx, side):
        if idx>=n: return False
        if use_ema and ema_f is not None:
            if side=="bull" and ema_f[idx]<=ema_s[idx]: return False
            if side=="bear" and ema_f[idx]>=ema_s[idx]: return False
        if adx_v is not None and adx_v[idx]<adx_thr: return False
        return True

    mac = {"trend":0,"ext":None,"ext_idx":None}

    def fresh(side):
        return {"side":side,"state":0,"p1_idx":None,"p1_price":None,
                "p2":None,"p2_idx":None,"prev_p2":None,"anchor":None,
                "p3":None,"p3_bar":None,"ttl":None,"fib":None,"c_watch":None}

    def reset(m):
        m.update(state=0,p1_idx=None,p1_price=None,p2=None,p2_idx=None,
                 anchor=None,p3=None,p3_bar=None,ttl=None,fib=None,c_watch=None)

    mach = {"bull":fresh("bull"), "bear":fresh("bear")}
    pos  = {"bull":None, "bear":None}
    equity = 100.0
    trades = []

    def close_pos(side, xp, xr, xc):
        nonlocal equity
        po=pos[side]
        gross=(xp-po["entry"])*po["size"] if side=="bull" else (po["entry"]-xp)*po["size"]
        won=xr=="TP"
        fee=po["notional"]*0.0002+po["notional"]*(0.0002 if won else 0.00055)
        pnl=gross-fee; equity+=pnl
        trades.append({"won":won,"pnl":pnl,"eq":equity})
        pos[side]=None

    def open_pos(side, m, entry_price, entry_candle, scan_start):
        nonlocal equity
        sl=m["p2"]*(1+STOP_BUF) if side=="bear" else m["p2"]*(1-STOP_BUF)
        rpp=abs(entry_price-sl)
        if rpp<=0: return
        rng=(m["p3"]-m["p2"]) if side=="bull" else (m["p2"]-m["p3"])
        if rng<=0 or rng/max(min(m["p2"],m["p3"]),1)<MIN_SWING: return
        tp=entry_price+rpp*rr if side=="bull" else entry_price-rpp*rr
        sz=equity*RISK_PCT/rpp
        pos[side]={"entry":entry_price,"sl":sl,"tp":tp,
                   "entry_candle":entry_candle,"scan_start":scan_start,
                   "size":sz,"notional":sz*entry_price}

    def step(m, ci):
        side=m["side"]
        ch=float(H[ci]); cl=float(L[ci])
        cc=float(C[ci]); co=float(O[ci])

        # New P1 — state 2 locked (allowStale=False)
        pv=conf_high.get(ci) if side=="bull" else conf_low.get(ci)
        if pv is not None and m["state"]!=2:
            p_idx,p_price=pv
            m["state"]=1; m["p1_idx"]=p_idx; m["p1_price"]=p_price
            if side=="bull":
                m["anchor"]=m["prev_p2"] if mac["trend"]==1 else mac["ext"]
                m["p2"]=float(min(L[p_idx:ci+1]))
                m["p2_idx"]=p_idx+int(np.argmin(L[p_idx:ci+1]))
            else:
                m["anchor"]=m["prev_p2"] if mac["trend"]==-1 else mac["ext"]
                m["p2"]=float(max(H[p_idx:ci+1]))
                m["p2_idx"]=p_idx+int(np.argmax(H[p_idx:ci+1]))

        # State 1 — float P2, INVALID, hunt BOS
        if m["state"]==1:
            if side=="bull" and cl<m["p2"]: m["p2"]=cl; m["p2_idx"]=ci
            elif side=="bear" and ch>m["p2"]: m["p2"]=ch; m["p2_idx"]=ci
            if m["anchor"] is not None:
                if side=="bull" and cl<m["anchor"]: reset(m); return
                if side=="bear" and ch>m["anchor"]: reset(m); return
            broke=(cc>m["p1_price"]) if side=="bull" else (cc<m["p1_price"])
            if broke:
                m["state"]=2
                mac["trend"]=1 if side=="bull" else -1
                mac["ext"]=ch if side=="bull" else cl
                mac["ext_idx"]=ci
                m["prev_p2"]=m["p2"]
                m["ttl"]=(ci-m["p1_idx"])*2
                m["p3"]=ch if side=="bull" else cl
                m["p3_bar"]=ci

        # State 2 — KILLED / float P3 / fib / FAILED / EXPIRED / trigger
        if m["state"]==2:
            if (side=="bull" and mac["trend"]==-1) or (side=="bear" and mac["trend"]==1):
                reset(m); return
            if side=="bull" and ch>=m["p3"]: m["p3"]=ch; m["p3_bar"]=ci
            elif side=="bear" and cl<=m["p3"]: m["p3"]=cl; m["p3_bar"]=ci
            rng=(m["p3"]-m["p2"]) if side=="bull" else (m["p2"]-m["p3"])
            if rng>0:
                m["fib"]=m["p3"]-rng*fib_level if side=="bull" else m["p3"]+rng*fib_level
            if side=="bull" and cl<m["p2"]: reset(m); return
            if side=="bear" and ch>m["p2"]: reset(m); return
            if m["ttl"] and (ci-m["p3_bar"])>m["ttl"]: reset(m); return
            if m["fib"] is None: return

            fib=m["fib"]; trig=False; ep=None; ec=None; ss=None

            if entry_mode=="rejection":
                if side=="bull" and cl<=fib and cc>fib and cc>co:
                    if ci+1<n: trig=True; ep=float(O[ci+1]); ec=ci+1; ss=ci+1
                elif side=="bear" and ch>=fib and cc<fib and cc<co:
                    if ci+1<n: trig=True; ep=float(O[ci+1]); ec=ci+1; ss=ci+1
            elif entry_mode=="reclaim":
                if m["c_watch"] is None:
                    if side=="bull" and cc<fib: m["c_watch"]=ci
                    elif side=="bear" and cc>fib: m["c_watch"]=ci
                else:
                    if (ci-m["c_watch"])<=2:
                        if side=="bull" and cc>fib:
                            trig=True; ep=cc; ec=ci; ss=ci+1; m["c_watch"]=None
                        elif side=="bear" and cc<fib:
                            trig=True; ep=cc; ec=ci; ss=ci+1; m["c_watch"]=None
                    else:
                        m["c_watch"]=None

            if not trig or ep is None: return
            if not ema_ok(ci,side): return

            sig=dict(m); reset(m)
            if pos[side] is None:
                open_pos(side,sig,ep,ec,ss)

    # Main bar loop
    for ci in range(n):
        if mac["trend"]==1:
            if mac["ext"] is None or H[ci]>mac["ext"]: mac["ext"]=float(H[ci]); mac["ext_idx"]=ci
        elif mac["trend"]==-1:
            if mac["ext"] is None or L[ci]<mac["ext"]: mac["ext"]=float(L[ci]); mac["ext_idx"]=ci

        for side in ("bull","bear"):
            po=pos[side]
            if po is None or ci<po["scan_start"]: continue
            if side=="bull":
                if L[ci]<=po["sl"]: close_pos(side,po["sl"],"SL",ci)
                elif H[ci]>=po["tp"]: close_pos(side,po["tp"],"TP",ci)
            else:
                if H[ci]>=po["sl"]: close_pos(side,po["sl"],"SL",ci)
                elif L[ci]<=po["tp"]: close_pos(side,po["tp"],"TP",ci)

        if ci<n-1:
            step(mach["bull"],ci)
            step(mach["bear"],ci)

    for side in ("bull","bear"):
        if pos[side] is not None:
            close_pos(side,float(C[n-1]),"TIMEOUT",n-1)

    if not trades: return None
    W=[t for t in trades if t["won"]]; L_=[t for t in trades if not t["won"]]
    final=trades[-1]["eq"]; tr=(final-100)/100*100
    wr=len(W)/len(trades)*100
    peak=100; mdd=0
    for t in trades:
        if t["eq"]>peak: peak=t["eq"]
        mdd=max(mdd,(peak-t["eq"])/peak*100)
    rets=[t["pnl"]/(t["eq"]-t["pnl"])*100 for t in trades if t["eq"]!=t["pnl"]]
    mean=sum(rets)/len(rets) if rets else 0
    std=(sum((r-mean)**2 for r in rets)/len(rets))**0.5 if rets else 0
    sharpe=mean/std*(365**0.5) if std>0 else 0
    gw=sum(t["pnl"] for t in W); gl=abs(sum(t["pnl"] for t in L_))
    pf=gw/gl if gl>0 else 999
    kf=(wr/100-(1-wr/100)/(gw/len(W)/(gl/len(L_)))) if W and L_ else 0
    return {
        "trades":len(trades),"wins":len(W),"losses":len(L_),
        "wr":round(wr,2),"return_pct":round(tr,2),
        "cagr":round(((final/100)**(365/365)-1)*100,2),
        "max_dd":round(mdd,2),"sharpe":round(sharpe,2),"pf":round(pf,2),
        "avg_win":round(gw/len(W),4) if W else 0,
        "avg_loss":round(gl/len(L_),4) if L_ else 0,
        "kelly":round(kf,3),
    }

# ── EMA CROSS BACKTEST ─────────────────────────────────────────
def backtest_ema(H, L, C, O, V, n, rr,
                 ema_fast, ema_slow,
                 use_volume, use_ema_gap, use_htf_mult,
                 htf_mult=5):
    def calc_ema(arr, period):
        k=2/(period+1); out=np.empty(len(arr)); out[0]=arr[0]
        for i in range(1,len(arr)): out[i]=arr[i]*k+out[i-1]*(1-k)
        return out

    ef=calc_ema(C,ema_fast); es=calc_ema(C,ema_slow)
    hf=calc_ema(C,ema_fast*htf_mult) if use_htf_mult else None
    hs=calc_ema(C,ema_slow*htf_mult) if use_htf_mult else None

    vol_ma=np.zeros(n)
    for i in range(20,n): vol_ma[i]=np.mean(V[i-20:i])

    trades=[]; equity=100.0; pos=None
    EMA_GAP_MIN=0.0005

    for i in range(ema_slow*2, n-1):
        if pos is not None:
            if pos["side"]=="long":
                if L[i]<=pos["sl"]:
                    gross=(pos["sl"]-pos["entry"])*pos["size"]
                    fee=pos["notional"]*0.0002+pos["notional"]*0.00055
                    trades.append({"won":False,"pnl":gross-fee,"eq":equity+(gross-fee)}); equity+=gross-fee; pos=None
                elif H[i]>=pos["tp"]:
                    gross=(pos["tp"]-pos["entry"])*pos["size"]
                    fee=pos["notional"]*0.0002+pos["notional"]*0.0002
                    trades.append({"won":True,"pnl":gross-fee,"eq":equity+(gross-fee)}); equity+=gross-fee; pos=None
            else:
                if H[i]>=pos["sl"]:
                    gross=(pos["entry"]-pos["sl"])*pos["size"]
                    fee=pos["notional"]*0.0002+pos["notional"]*0.00055
                    trades.append({"won":False,"pnl":gross-fee,"eq":equity+(gross-fee)}); equity+=gross-fee; pos=None
                elif L[i]<=pos["tp"]:
                    gross=(pos["entry"]-pos["tp"])*pos["size"]
                    fee=pos["notional"]*0.0002+pos["notional"]*0.0002
                    trades.append({"won":True,"pnl":gross-fee,"eq":equity+(gross-fee)}); equity+=gross-fee; pos=None

        if pos is not None: continue

        prev_bull=ef[i-1]>es[i-1]; curr_bull=ef[i]>es[i]
        cross_up=not prev_bull and curr_bull
        cross_dn=prev_bull and not curr_bull
        if not cross_up and not cross_dn: continue
        side="long" if cross_up else "short"

        if use_volume and vol_ma[i]>0:
            if V[i]<=vol_ma[i]: continue
        if use_ema_gap:
            if abs(ef[i]-es[i])/C[i]<EMA_GAP_MIN: continue
        if use_htf_mult and hf is not None:
            if side=="long"  and hf[i]<=hs[i]: continue
            if side=="short" and hf[i]>=hs[i]: continue

        ei=i+1
        if ei>=n: continue
        ep=float(O[ei])
        sl=(float(np.min(L[max(0,i-2):i+1]))*(1-STOP_BUF) if side=="long"
            else float(np.max(H[max(0,i-2):i+1]))*(1+STOP_BUF))
        rpp=abs(ep-sl)
        if rpp<=0: continue
        tp=(ep+rpp*rr) if side=="long" else (ep-rpp*rr)
        sz=equity*RISK_PCT/rpp
        pos={"side":side,"entry":ep,"sl":sl,"tp":tp,"size":sz,"notional":sz*ep}

    if pos is not None:
        xp=float(C[n-1])
        gross=(xp-pos["entry"])*pos["size"] if pos["side"]=="long" else (pos["entry"]-xp)*pos["size"]
        fee=pos["notional"]*0.0002+pos["notional"]*0.00055
        trades.append({"won":gross-fee>0,"pnl":gross-fee,"eq":equity+(gross-fee)})

    if not trades: return None
    W=[t for t in trades if t["won"]]; L_=[t for t in trades if not t["won"]]
    final=trades[-1]["eq"]; tr=(final-100)/100*100; wr=len(W)/len(trades)*100
    peak=100; mdd=0
    for t in trades:
        if t["eq"]>peak: peak=t["eq"]
        mdd=max(mdd,(peak-t["eq"])/peak*100)
    rets=[t["pnl"]/(t["eq"]-t["pnl"])*100 for t in trades if t["eq"]!=t["pnl"]]
    mean=sum(rets)/len(rets) if rets else 0
    std=(sum((r-mean)**2 for r in rets)/len(rets))**0.5 if rets else 0
    sharpe=mean/std*(365**0.5) if std>0 else 0
    gw=sum(t["pnl"] for t in W); gl=abs(sum(t["pnl"] for t in L_))
    pf=gw/gl if gl>0 else 999
    kf=(wr/100-(1-wr/100)/(gw/len(W)/(gl/len(L_)))) if W and L_ else 0
    return {
        "trades":len(trades),"wins":len(W),"losses":len(L_),
        "wr":round(wr,2),"return_pct":round(tr,2),
        "cagr":round(((final/100)**(365/365)-1)*100,2),
        "max_dd":round(mdd,2),"sharpe":round(sharpe,2),"pf":round(pf,2),
        "avg_win":round(gw/len(W),4) if W else 0,
        "avg_loss":round(gl/len(L_),4) if L_ else 0,
        "kelly":round(kf,3),
    }


# ── PHASE: BOS PULLBACK ────────────────────────────────────────
def run_bos(grand_total, done, saved, errors, buf, start, last_tg):
    print("\n=== BOS Pullback ===")
    for symbol,tf in itertools.product(BOS_PAIRS, BOS_TIMEFRAMES):
        print(f"\nLoading {symbol} {tf}...")
        set_status("compute","running",done,grand_total,f"BOS {symbol} {tf}")
        rows=get_candles(symbol,tf)
        if not rows: print("  No candles — skip"); continue

        H=np.array([r["high"]  for r in rows],dtype=float)
        L=np.array([r["low"]   for r in rows],dtype=float)
        C=np.array([r["close"] for r in rows],dtype=float)
        O=np.array([r["open"]  for r in rows],dtype=float)
        n=len(rows); print(f"  {n} candles")

        ema_cache={}
        for ep in BOS_EMA_PAIRS:
            if ep!="off":
                f,s=map(int,ep.split("/")); ema_cache[ep]=(calc_ema(C,f),calc_ema(C,s))
        adx_cache=calc_adx(H,L,C,14)

        for em,N,rr,fib,ep,ax in itertools.product(BOS_ENTRY_MODES,BOS_PIVOT_NS,BOS_RR_RATIOS,BOS_FIB_LEVELS,BOS_EMA_PAIRS,BOS_ADX_MINS):
            use_ema=ep!="off"
            ef,es=ema_cache[ep] if use_ema else (None,None)
            adx_v=adx_cache if ax>0 else None
            try:
                s=backtest(H,L,C,O,n,N,rr,fib,em,ef,es,use_ema,adx_v,float(ax))
            except Exception as e:
                s=None; errors+=1

            buf.append({
                "combo_key":f"{symbol}|{tf}|bos_pullback|{em}|{N}|{rr}|{fib}|{ep}|{ax}",
                "pair":symbol.replace("/USDT",""),"timeframe":tf,
                "engine":"bos_pullback","entry_mode":em,
                "pivot_n":N,"rr":rr,"fib_level":fib,"ema_pair":ep,"adx_min":ax,"filters":None,
                "period_start":PERIOD_START,"period_end":PERIOD_END,
                "success":s is not None,
                "return_pct":s["return_pct"] if s else None,"cagr":s["cagr"] if s else None,
                "max_dd":s["max_dd"] if s else None,"sharpe":s["sharpe"] if s else None,
                "profit_factor":s["pf"] if s else None,"win_rate":s["wr"] if s else None,
                "trades":s["trades"] if s else 0,"wins":s["wins"] if s else 0,
                "losses":s["losses"] if s else 0,"avg_win":s["avg_win"] if s else None,
                "avg_loss":s["avg_loss"] if s else None,"kelly_full":s["kelly"] if s else None,
                "computed_at":datetime.now(timezone.utc).isoformat(),
            })
            if s: saved+=1
            done+=1
            if len(buf)>=500: save_rows(buf); buf=[]
            if time.time()-last_tg>600:
                el=time.time()-start; rate=done/el if el>0 else 0; eta=(grand_total-done)/rate if rate>0 else 0
                tg(f"⏳ BOS Phase\nDone: {done:,}/{grand_total:,} ({done/grand_total*100:.1f}%)\nETA: {eta/60:.0f}min\n{symbol} {tf}")
                set_status("compute","running",done,grand_total,f"BOS {symbol} {tf}")
                last_tg=time.time()

        if buf: save_rows(buf); buf=[]
    return done, saved, errors, buf, last_tg


# ── PHASE: EMA CROSS ───────────────────────────────────────────
def run_ema(grand_total, done, saved, errors, buf, start, last_tg):
    print("\n=== EMA Cross ===")
    for symbol,tf in itertools.product(EMA_PAIRS_LIST, EMA_TIMEFRAMES):
        print(f"\nLoading {symbol} {tf}...")
        set_status("compute","running",done,grand_total,f"EMA {symbol} {tf}")
        rows=get_candles(symbol,tf)
        if not rows: print("  No candles — skip"); continue

        H=np.array([r["high"]  for r in rows],dtype=float)
        L=np.array([r["low"]   for r in rows],dtype=float)
        C=np.array([r["close"] for r in rows],dtype=float)
        O=np.array([r["open"]  for r in rows],dtype=float)
        V=np.array([r.get("volume",0) for r in rows],dtype=float)
        n=len(rows); print(f"  {n} candles")

        for ef_p,es_p,rr,use_vol,use_gap,use_htf in itertools.product(
            EMA_FAST_LIST,EMA_SLOW_LIST,EMA_RR_LIST,
            EMA_VOL_OPTS,EMA_GAP_OPTS,EMA_HTF_OPTS
        ):
            filters="+".join(f for f,v in [("vol",use_vol),("gap",use_gap),("htf",use_htf)] if v) or "none"
            try:
                s=backtest_ema(H,L,C,O,V,n,rr,ef_p,es_p,use_vol,use_gap,use_htf)
            except Exception as e:
                s=None; errors+=1; print(f"  EMA error: {e}")

            buf.append({
                "combo_key":f"{symbol}|{tf}|ema_cross|cross|{ef_p}/{es_p}|{rr}|0|{filters}|0",
                "pair":symbol.replace("/USDT",""),"timeframe":tf,
                "engine":"ema_cross","entry_mode":"cross",
                "pivot_n":0,"rr":rr,"fib_level":0,
                "ema_pair":f"{ef_p}/{es_p}","adx_min":0,"filters":filters,
                "period_start":PERIOD_START,"period_end":PERIOD_END,
                "success":s is not None,
                "return_pct":s["return_pct"] if s else None,"cagr":s["cagr"] if s else None,
                "max_dd":s["max_dd"] if s else None,"sharpe":s["sharpe"] if s else None,
                "profit_factor":s["pf"] if s else None,"win_rate":s["wr"] if s else None,
                "trades":s["trades"] if s else 0,"wins":s["wins"] if s else 0,
                "losses":s["losses"] if s else 0,"avg_win":s["avg_win"] if s else None,
                "avg_loss":s["avg_loss"] if s else None,"kelly_full":s["kelly"] if s else None,
                "computed_at":datetime.now(timezone.utc).isoformat(),
            })
            if s: saved+=1
            done+=1
            if len(buf)>=500: save_rows(buf); buf=[]
            if time.time()-last_tg>600:
                el=time.time()-start; rate=done/el if el>0 else 0; eta=(grand_total-done)/rate if rate>0 else 0
                tg(f"⏳ EMA Phase\nDone: {done:,}/{grand_total:,} ({done/grand_total*100:.1f}%)\nETA: {eta/60:.0f}min\n{symbol} {tf}")
                set_status("compute","running",done,grand_total,f"EMA {symbol} {tf}")
                last_tg=time.time()

        if buf: save_rows(buf); buf=[]
        print(f"  Done {symbol} {tf} — {saved:,} saved so far")
    return done, saved, errors, buf, last_tg


# ── MAIN COMPUTE ───────────────────────────────────────────────
def main_compute(mode="ema"):
    # Clear only the engine(s) we're about to rerun
    engines_to_clear = []
    if mode in ("ema","all"): engines_to_clear.append("ema_cross")
    if mode in ("bos","all"): engines_to_clear.append("bos_pullback")

    for eng in engines_to_clear:
        try:
            httpx.delete(f"{SUPABASE_URL}/rest/v1/matrix_results?engine=eq.{eng}",
                         headers=HEADERS, timeout=30)
            print(f"Cleared old {eng} results")
        except: pass

    grand_total = (EMA_TOTAL if mode=="ema" else BOS_TOTAL if mode=="bos" else EMA_TOTAL+BOS_TOTAL)

    engine_label = ("EMA Cross only" if mode=="ema" else
                    "BOS Pullback only" if mode=="bos" else
                    "BOS Pullback + EMA Cross")
    tg(f"""🔢 <b>Matrix Runner v8 — {engine_label}</b>
Total combos: {grand_total:,}
Period: {PERIOD_START} → {PERIOD_END}""")

    saved=0; errors=0; done=0
    start=time.time(); last_tg=time.time(); buf=[]

    if mode in ("bos","all"):
        done,saved,errors,buf,last_tg = run_bos(grand_total,done,saved,errors,buf,start,last_tg)
    if mode in ("ema","all"):
        done,saved,errors,buf,last_tg = run_ema(grand_total,done,saved,errors,buf,start,last_tg)

    if buf: save_rows(buf)
    el=time.time()-start
    set_status("compute","done",grand_total,grand_total,"")
    tg(f"""✅ <b>Matrix v8 Complete — {engine_label}</b>
Combos: {grand_total:,} | Saved: {saved:,} | Errors: {errors}
Time: {el/60:.1f}min
Download from Runner tab.""")
    print(f"\nDone. {saved:,} saved in {el/60:.1f}min")


def main_prefetch():
    import ccxt
    ex=ccxt.kucoin({"enableRateLimit":True})
    tg("📥 <b>Prefetch Started — all TFs including 1m</b>")
    sh={**HEADERS,"Prefer":"return=minimal,resolution=ignore-duplicates"}
    all_pairs=list(set(BOS_PAIRS)|set(EMA_PAIRS_LIST))
    all_tfs  =list(set(BOS_TIMEFRAMES)|set(EMA_TIMEFRAMES))
    s_ms=int(datetime.strptime(PERIOD_START,"%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
    e_ms=int(datetime.strptime(PERIOD_END,  "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
    for sym,tf in itertools.product(all_pairs,all_tfs):
        print(f"Fetching {sym} {tf}...")
        candles=[]; since=s_ms; ec=0
        while since<e_ms:
            try:
                b=ex.fetch_ohlcv(sym,tf,since=since,limit=1000)
                if not b: ec+=1
                if ec>=3: break
                f_=[c for c in b if c[0]<e_ms]; candles+=f_
                if b[-1][0]>=e_ms: break
                since=b[-1][0]+1; time.sleep(0.3); ec=0
            except Exception as e:
                print(f"Error: {e}"); time.sleep(5)
        if candles:
            for i in range(0,len(candles),500):
                batch=candles[i:i+500]
                httpx.post(f"{SUPABASE_URL}/rest/v1/candles",
                           json=[{"symbol":sym,"timeframe":tf,"ts":c[0],
                                  "open":c[1],"high":c[2],"low":c[3],
                                  "close":c[4],"volume":c[5] if len(c)>5 else 0}
                                 for c in batch],
                           headers=sh,timeout=30)
            print(f"  {len(candles)} candles saved")
    tg("✅ <b>Prefetch Done</b>")


def main():
    import sys
    arg = sys.argv[1] if len(sys.argv)>1 else "ema"
    if arg == "prefetch":
        main_prefetch()
    elif arg == "bos":
        main_compute("bos")
    elif arg == "all":
        main_compute("all")
    else:
        main_compute("ema")  # default

if __name__=="__main__":
    main()
