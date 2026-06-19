#!/usr/bin/env python3
# ============================================================
# WADDLE MATRIX RUNNER v2.0 — clean rebuild
# Single `params` JSONB column per result row — works for any
# strategy without schema changes. Only validated rows get saved
# (min 30 trades, Sharpe 0.5-2.5, max drawdown <=35%) except for
# monthly-stage rows on locked configs, which save in full for
# visibility into consistency even on weaker months.
#
# Usage: /run-matrix?engine=bos&stage=sweep
#        /run-matrix?engine=bos&stage=stability
#        /run-matrix?engine=bos&stage=monthly
#        /run-matrix?engine=ema&stage=sweep|stability|monthly
#        /run-matrix?engine=div&stage=sweep|stability|monthly
#        /run-matrix?engine=all&stage=sweep   (runs sweep for all 3)
# ============================================================

import os, time, httpx, itertools, io, csv as csv_mod, math, statistics, json, bisect
import numpy as np
from datetime import datetime, timezone, timedelta

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

RISK_PCT  = 0.02
MIN_SWING = 0.002
STOP_BUF  = 0.001
PERIOD_START = "2025-01-01"

# ── VALIDATION FILTER (Algovibes methodology) ──────────────────
def passes_filter(s):
    if s is None: return False
    if s["trades"] < 30: return False
    if not (0.5 <= s["sharpe"] <= 2.5): return False
    if s["max_dd"] > 35: return False
    return True

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
    pe = pe or datetime.now(timezone.utc).strftime("%Y-%m-%d")
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
        else:
            break
    return rows

def save_rows(rows):
    if not rows: return True
    try:
        res = httpx.post(f"{SUPABASE_URL}/rest/v1/matrix_results", json=rows,
                          headers=HEADERS, timeout=30)
        if res.status_code not in (200, 201, 204):
            print(f"save_rows FAILED: {res.status_code} — {res.text[:500]}")
            tg(f"⚠️ <b>Save failed</b>\nStatus: {res.status_code}\n{res.text[:300]}")
            return False
        return True
    except Exception as e:
        print(f"save_rows error: {e}")
        tg(f"⚠️ <b>Save exception</b>\n{str(e)[:300]}")
        return False

def make_row(pair, tf, engine, stage, period_label, ps, pe, params, s):
    return {
        "pair": pair, "timeframe": tf, "engine": engine, "stage": stage,
        "period_label": period_label, "period_start": ps, "period_end": pe,
        "params": params,
        "return_pct": s["return_pct"] if s else None,
        "cagr": s["cagr"] if s else None,
        "max_dd": s["max_dd"] if s else None,
        "sharpe": s["sharpe"] if s else None,
        "profit_factor": s["pf"] if s else None,
        "win_rate": s["wr"] if s else None,
        "trades": s["trades"] if s else 0,
        "wins": s["wins"] if s else 0,
        "losses": s["losses"] if s else 0,
        "avg_win": s["avg_win"] if s else None,
        "avg_loss": s["avg_loss"] if s else None,
        "kelly_full": s["kelly"] if s else None,
        "total_fees": s.get("total_fees") if s else None,
        "passed_filter": passes_filter(s) if s else False,
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }

# ── INDICATORS ─────────────────────────────────────────────────
def calc_ema(arr, period):
    k=2/(period+1); out=np.empty(len(arr)); out[0]=arr[0]
    for i in range(1,len(arr)): out[i]=arr[i]*k+out[i-1]*(1-k)
    return out

def calc_adx(H,L,C,period):
    n=len(H); adx_=np.zeros(n); pdm=np.zeros(n); mdm=np.zeros(n); tr=np.zeros(n)
    if n<=period*2+1: return adx_  # not enough data — return zeros (no trend signal)
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

def calc_rsi(C, period=14):
    n=len(C); rsi=np.full(n,50.0)
    if n<=period: return rsi  # not enough data — return neutral RSI
    g=np.zeros(n); l_=np.zeros(n)
    for i in range(1,n):
        d=C[i]-C[i-1]; g[i]=max(d,0); l_[i]=max(-d,0)
    ag=al=0.0
    for i in range(1,period+1): ag+=g[i]; al+=l_[i]
    ag/=period; al/=period
    rsi[period]=100 if al==0 else 100-100/(1+ag/al)
    for i in range(period+1,n):
        ag=(ag*(period-1)+g[i])/period; al=(al*(period-1)+l_[i])/period
        rsi[i]=100 if al==0 else 100-100/(1+ag/al)
    return rsi

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


# ── DIVERGENCE BACKTEST ENGINE ─────────────────────────────────
def calc_macd_histogram(C, fast=12, slow=26, signal=9):
    ef=calc_ema(C,fast); es=calc_ema(C,slow)
    macd=ef-es; sig=calc_ema(macd,signal)
    return macd-sig  # histogram

def precompute_divergence_inputs(H, L, C, pivot_n, rsi_period, use_macd_conf, adx_max):
    """
    Cacheable pre-computation — call ONCE per (pivot_n, rsi_period, macd, adx_max)
    combination per symbol/timeframe, not once per full parameter sweep combo.
    Returns dict with pivot_lows, pivot_highs, rsi_v, hist, adx_v.
    """
    n=len(C)
    rsi_v  = calc_rsi(C, rsi_period)
    hist   = calc_macd_histogram(C) if use_macd_conf else None
    adx_v  = calc_adx(H,L,C,14) if adx_max>0 else None

    def is_pivot_low(i):
        return (i>=pivot_n and i<n-pivot_n and
                all(L[i]<=L[i-j] for j in range(1,pivot_n+1)) and
                all(L[i]<=L[i+j] for j in range(1,pivot_n+1)))
    def is_pivot_high(i):
        return (i>=pivot_n and i<n-pivot_n and
                all(H[i]>=H[i-j] for j in range(1,pivot_n+1)) and
                all(H[i]>=H[i+j] for j in range(1,pivot_n+1)))

    pivot_lows  = [i for i in range(pivot_n, n-pivot_n) if is_pivot_low(i)]
    pivot_highs = [i for i in range(pivot_n, n-pivot_n) if is_pivot_high(i)]

    return {"pivot_lows":pivot_lows,"pivot_highs":pivot_highs,
            "rsi_v":rsi_v,"hist":hist,"adx_v":adx_v}


def backtest_divergence(H, L, C, O, n, rr, tf,
                        rsi_period, rsi_thresh, pivot_n, lookback,
                        use_macd_conf, adx_max, precomputed=None):
    """
    RSI Divergence Engine:
    - Bullish: price makes lower low, RSI makes higher low, RSI < rsi_thresh → long
    - Bearish: price makes higher high, RSI makes lower high, RSI > (100-rsi_thresh) → short
    - Optional: require MACD histogram divergence to match (double divergence filter)
    - Optional: ADX < adx_max to avoid trading in strong trends (0=off)
    - SL: swing low/high × buffer
    - TP: fixed RR

    Pass `precomputed` (from precompute_divergence_inputs) to skip redundant
    pivot/RSI/MACD/ADX recalculation across rr sweeps — these don't depend on rr.
    """
    min_required = max(pivot_n*2+lookback, rsi_period*2+1, 60)
    if n < min_required:
        return None  # not enough candles for reliable signal detection

    if precomputed is not None:
        pivot_lows  = precomputed["pivot_lows"]
        pivot_highs = precomputed["pivot_highs"]
        rsi_v       = precomputed["rsi_v"]
        hist        = precomputed["hist"]
        adx_v       = precomputed["adx_v"]
    else:
        pre = precompute_divergence_inputs(H,L,C,pivot_n,rsi_period,use_macd_conf,adx_max)
        pivot_lows, pivot_highs = pre["pivot_lows"], pre["pivot_highs"]
        rsi_v, hist, adx_v = pre["rsi_v"], pre["hist"], pre["adx_v"]

    balance=1000.0; peak=1000.0
    wins=losses=trades=0
    gross_win=gross_loss=total_fees=0.0
    pnl_series=[]
    in_trade=False; direction=None
    ep=sl=tp=pos_size=notional=0.0

    STOP_BUF=0.001
    FEE_ENTRY=0.0002; FEE_TP=0.0002; FEE_SL=0.00055

    import bisect
    start_i = max(pivot_n*2+lookback, rsi_period*2+1)

    for ci in range(start_i, n-1):
        # Exit check
        if in_trade:
            hi_c=H[ci]; lo_c=L[ci]
            if direction=="long":
                if lo_c<=sl:
                    gross=(sl-ep)*pos_size
                    fee=notional*FEE_SL
                    pnl=gross-fee
                    balance+=pnl; losses+=1; gross_loss+=abs(pnl); total_fees+=fee
                    pnl_series.append(pnl); in_trade=False
                elif hi_c>=tp:
                    gross=(tp-ep)*pos_size
                    fee=notional*FEE_TP
                    pnl=gross-fee
                    balance+=pnl; wins+=1; gross_win+=pnl; total_fees+=fee
                    pnl_series.append(pnl); in_trade=False
            else:
                if hi_c>=sl:
                    gross=(ep-sl)*pos_size
                    fee=notional*FEE_SL
                    pnl=gross-fee
                    balance+=pnl; losses+=1; gross_loss+=abs(pnl); total_fees+=fee
                    pnl_series.append(pnl); in_trade=False
                elif lo_c<=tp:
                    gross=(ep-tp)*pos_size
                    fee=notional*FEE_TP
                    pnl=gross-fee
                    balance+=pnl; wins+=1; gross_win+=pnl; total_fees+=fee
                    pnl_series.append(pnl); in_trade=False
            if balance>peak: peak=balance
            if in_trade: continue

        if in_trade: continue

        # ADX filter — only fire in low-trend regime
        if adx_max>0 and adx_v is not None and adx_v[ci]>adx_max:
            continue

        # Find recent pivot lows within lookback for bullish divergence (bisect: O(log p))
        lo_bound = bisect.bisect_left(pivot_lows, ci-lookback)
        hi_bound = bisect.bisect_left(pivot_lows, ci-pivot_n)
        recent_lows = pivot_lows[lo_bound:hi_bound]
        if len(recent_lows)>=2:
            p2=recent_lows[-1]; p1=recent_lows[-2]  # p2=newer, p1=older
            # Bullish: price lower low, RSI higher low, RSI oversold
            if (L[p2]<L[p1] and rsi_v[p2]>rsi_v[p1] and rsi_v[ci]<rsi_thresh):
                # Optional MACD histogram confirmation
                macd_ok=True
                if use_macd_conf and hist is not None:
                    macd_ok=(hist[p2]>hist[p1])  # histogram also diverging
                if macd_ok:
                    entry_p=float(O[ci+1]) if ci+1<n else float(C[ci])
                    sl_p=float(L[p2])*(1-STOP_BUF)
                    rpp=abs(entry_p-sl_p)
                    if rpp>0 and entry_p>sl_p:
                        tp_p=entry_p+rpp*rr
                        ep=entry_p; sl=sl_p; tp=tp_p
                        pos_size=balance*RISK_PCT/rpp
                        notional=pos_size*entry_p
                        fee=notional*FEE_ENTRY
                        balance-=fee; total_fees+=fee
                        in_trade=True; direction="long"; trades+=1
                        continue

        # Find recent pivot highs within lookback for bearish divergence (bisect: O(log p))
        lo_bound_h = bisect.bisect_left(pivot_highs, ci-lookback)
        hi_bound_h = bisect.bisect_left(pivot_highs, ci-pivot_n)
        recent_highs = pivot_highs[lo_bound_h:hi_bound_h]
        if len(recent_highs)>=2:
            p2=recent_highs[-1]; p1=recent_highs[-2]
            # Bearish: price higher high, RSI lower high, RSI overbought
            if (H[p2]>H[p1] and rsi_v[p2]<rsi_v[p1] and rsi_v[ci]>(100-rsi_thresh)):
                macd_ok=True
                if use_macd_conf and hist is not None:
                    macd_ok=(hist[p2]<hist[p1])
                if macd_ok:
                    entry_p=float(O[ci+1]) if ci+1<n else float(C[ci])
                    sl_p=float(H[p2])*(1+STOP_BUF)
                    rpp=abs(sl_p-entry_p)
                    if rpp>0 and entry_p<sl_p:
                        tp_p=entry_p-rpp*rr
                        ep=entry_p; sl=sl_p; tp=tp_p
                        pos_size=balance*RISK_PCT/rpp
                        notional=pos_size*entry_p
                        fee=notional*FEE_ENTRY
                        balance-=fee; total_fees+=fee
                        in_trade=True; direction="short"; trades+=1

    if trades==0: return None
    total=wins+losses
    wr=wins/total*100 if total>0 else 0
    ret=(balance-1000)/1000*100
    TF_MINS={"1m":1,"5m":5,"15m":15,"1h":60,"4h":240}
    days=(n*TF_MINS.get(tf,60))/1440
    cagr_=((balance/1000)**(365/days)-1)*100 if days>0 else 0
    dd=0.0; pk=1000.0
    for p in pnl_series:
        pk=max(pk,pk+p); dd=max(dd,(pk-(pk+p))/pk*100 if pk>0 else 0)
    pf=gross_win/gross_loss if gross_loss>0 else 999
    avg_win=gross_win/wins if wins>0 else 0
    avg_loss=gross_loss/losses if losses>0 else 0
    if len(pnl_series)>1:
        mu=statistics.mean(pnl_series); sd=statistics.stdev(pnl_series)
        sharpe_=(mu/sd)*math.sqrt(252) if sd>0 else 0
    else: sharpe_=0
    kf=(wr/100-(1-wr/100)/rr)*100 if rr>0 else 0
    return {"return_pct":round(ret,4),"cagr":round(cagr_,4),
            "max_dd":round(dd,4),"sharpe":round(sharpe_,4),
            "pf":round(pf,4),"wr":round(wr,4),
            "trades":trades,"wins":wins,"losses":losses,
            "avg_win":round(avg_win,4),"avg_loss":round(avg_loss,4),
            "total_fees":round(total_fees,4),"kelly":round(kf,3)}



# ── VARIABLE SPACES ──────────────────────────────────────────
BOS_PAIRS       = ["DOGE/USDT","XLM/USDT","XRP/USDT","ADA/USDT","TRX/USDT","ARB/USDT"]
BOS_TIMEFRAMES  = ["5m","15m","1h"]
BOS_ENTRY_MODES = ["rejection","reclaim"]
BOS_PIVOT_NS    = [3,5,8]
BOS_RR_RATIOS   = [1.5,2.0,3.0,4.0]
BOS_FIB_LEVELS  = [0.382,0.5,0.618]
BOS_EMA_PAIRS   = ["off","34/55","55/89","89/144","144/169"]
BOS_ADX_MINS    = [0,15,25]

EMA_PAIRS_LIST  = ["DOGE/USDT","XLM/USDT","XRP/USDT","TRX/USDT","ARB/USDT"]
EMA_TIMEFRAMES  = ["1m","5m","15m"]
EMA_FAST_LIST   = [9, 12]
EMA_SLOW_LIST   = [21, 26]
EMA_RR_LIST     = [1.5, 2.0]
EMA_VOL_OPTS    = [False, True]
EMA_GAP_OPTS    = [False, True]
EMA_HTF_OPTS    = [False, True]

DIV_PAIRS       = ["DOGE/USDT","XLM/USDT","XRP/USDT","ADA/USDT","TRX/USDT","ARB/USDT"]
DIV_TIMEFRAMES  = ["15m","1h","4h"]
DIV_RSI_PERIODS = [10, 14]
DIV_RSI_THRESH  = [30, 40]
DIV_PIVOT_NS    = [3, 5]
DIV_LOOKBACKS   = [30, 60]
DIV_MACD_CONF   = [False, True]
DIV_ADX_MAXS    = [0, 20]
DIV_RR_RATIOS   = [1.5, 2.0, 3.0]

# Three test periods for stability stage
_TODAY      = datetime.now(timezone.utc).strftime("%Y-%m-%d")
_L30D_START = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
STABILITY_PERIODS = [
    ("2025_full", "2025-01-01", "2026-01-01"),
    ("2026_ytd",  "2026-01-01", _TODAY),
    ("l30d",      _L30D_START,  _TODAY),
]

def gen_monthly_periods():
    periods = []
    y, m = 2025, 1
    now = datetime.now(timezone.utc)
    while (y, m) <= (now.year, now.month):
        start = f"{y}-{m:02d}-01"
        ny, nm = (y+1, 1) if m == 12 else (y, m+1)
        end_dt = datetime(ny, nm, 1, tzinfo=timezone.utc)
        if end_dt > now: end_dt = now
        end = end_dt.strftime("%Y-%m-%d")
        if end > start: periods.append((f"{y}-{m:02d}", start, end))
        y, m = ny, nm
    return periods

MONTHLY_PERIODS = gen_monthly_periods()

# ── LOCKED CONFIGS (validated, currently live in paper_trader.py) ──
BOS_LOCKED = [
    {"pair":"ADA/USDT", "timeframe":"15m","pivot_n":8,"rr":1.5,"fib_level":0.382,"entry_mode":"reclaim",  "ema_pair":"89/144","adx_min":25},
    {"pair":"DOGE/USDT","timeframe":"15m","pivot_n":8,"rr":1.5,"fib_level":0.618,"entry_mode":"reclaim",  "ema_pair":"34/55", "adx_min":15},
    {"pair":"XLM/USDT", "timeframe":"15m","pivot_n":5,"rr":4.0,"fib_level":0.382,"entry_mode":"reclaim",  "ema_pair":"89/144","adx_min":25},
    {"pair":"TRX/USDT", "timeframe":"1h", "pivot_n":3,"rr":1.5,"fib_level":0.618,"entry_mode":"rejection","ema_pair":"89/144","adx_min":15},
    {"pair":"XRP/USDT", "timeframe":"15m","pivot_n":3,"rr":2.0,"fib_level":0.5,  "entry_mode":"reclaim",  "ema_pair":"55/89", "adx_min":25},
]
EMA_LOCKED = [
    {"pair":"ARB/USDT", "timeframe":"5m", "ema_fast":12,"ema_slow":26,"rr":2.0,"use_vol":True, "use_gap":True, "use_htf":False},
    {"pair":"XLM/USDT", "timeframe":"15m","ema_fast":12,"ema_slow":26,"rr":2.0,"use_vol":False,"use_gap":True, "use_htf":True},
]

# ── EXECUTION HELPERS — run one backtest given engine+params ───
def run_bos_backtest(H,L,C,O,n, p):
    ep=p.get("ema_pair","off"); ax=p.get("adx_min",0)
    use_ema = ep!="off"
    ef=es=adx_v=None
    if use_ema:
        f,s_=map(int,ep.split("/")); ef,es=calc_ema(C,f),calc_ema(C,s_)
    if ax>0:
        adx_v=calc_adx(H,L,C,14)
    return backtest(H,L,C,O,n,p["pivot_n"],p["rr"],p["fib_level"],p["entry_mode"],
                    ef,es,use_ema,adx_v,float(ax))

def run_ema_backtest(H,L,C,O,V,n, p):
    return backtest_ema(H,L,C,O,V,n,p["rr"],p["ema_fast"],p["ema_slow"],
                        p["use_vol"],p["use_gap"],p["use_htf"])

def run_div_backtest(H,L,C,O,n,tf, p, precomputed=None):
    return backtest_divergence(H,L,C,O,n,p["rr"],tf,
                               p["rsi_period"],p["rsi_thresh"],p["pivot_n"],
                               p["lookback"],p["use_macd"],p["adx_max"],
                               precomputed=precomputed)

# ── PHASE: SWEEP (Stage 1 — broad parameter search per engine) ──
def run_sweep(engine, grand_total, done, saved, errors, buf, start, last_tg):
    if engine=="bos":
        pairs, tfs = BOS_PAIRS, BOS_TIMEFRAMES
        param_iter = lambda: itertools.product(BOS_ENTRY_MODES,BOS_PIVOT_NS,BOS_RR_RATIOS,
                                                BOS_FIB_LEVELS,BOS_EMA_PAIRS,BOS_ADX_MINS)
    elif engine=="ema":
        pairs, tfs = EMA_PAIRS_LIST, EMA_TIMEFRAMES
        param_iter = lambda: itertools.product(EMA_FAST_LIST,EMA_SLOW_LIST,EMA_RR_LIST,
                                                EMA_VOL_OPTS,EMA_GAP_OPTS,EMA_HTF_OPTS)
    elif engine=="div":
        pairs, tfs = DIV_PAIRS, DIV_TIMEFRAMES
    else:
        return done,saved,errors,buf,last_tg

    for symbol, tf in itertools.product(pairs, tfs):
        set_status("compute","running",done,grand_total,f"{engine} sweep {symbol} {tf}")
        rows=get_candles(symbol,tf)
        if not rows: continue
        H=np.array([r["high"] for r in rows],dtype=float)
        L=np.array([r["low"] for r in rows],dtype=float)
        C=np.array([r["close"] for r in rows],dtype=float)
        O=np.array([r["open"] for r in rows],dtype=float)
        V=np.array([r.get("volume",0) for r in rows],dtype=float)
        n=len(rows)
        pair=symbol.replace("/USDT","")

        if engine=="bos":
            for em,N,rr,fib,ep,ax in param_iter():
                p={"entry_mode":em,"pivot_n":N,"rr":rr,"fib_level":fib,"ema_pair":ep,"adx_min":ax}
                try: s=run_bos_backtest(H,L,C,O,n,p)
                except Exception as e: s=None; errors+=1
                if s and passes_filter(s):
                    buf.append(make_row(pair,tf,"bos_pullback","sweep",None,PERIOD_START,_TODAY,p,s))
                    saved+=1
                done+=1
                if len(buf)>=300: save_rows(buf); buf=[]
                if time.time()-last_tg>600:
                    el=time.time()-start; rate=done/el if el>0 else 0; eta=(grand_total-done)/rate if rate>0 else 0
                    tg(f"⏳ BOS sweep\nDone: {done:,}/{grand_total:,} ({done/grand_total*100:.1f}%)\nETA: {eta/60:.0f}min\n{symbol} {tf}")
                    last_tg=time.time()

        elif engine=="ema":
            for ef_p,es_p,rr,uv,ug,uh in param_iter():
                p={"ema_fast":ef_p,"ema_slow":es_p,"rr":rr,"use_vol":uv,"use_gap":ug,"use_htf":uh}
                try: s=run_ema_backtest(H,L,C,O,V,n,p)
                except Exception as e: s=None; errors+=1
                if s and passes_filter(s):
                    buf.append(make_row(pair,tf,"ema_cross","sweep",None,PERIOD_START,_TODAY,p,s))
                    saved+=1
                done+=1
                if len(buf)>=300: save_rows(buf); buf=[]
                if time.time()-last_tg>600:
                    el=time.time()-start; rate=done/el if el>0 else 0; eta=(grand_total-done)/rate if rate>0 else 0
                    tg(f"⏳ EMA sweep\nDone: {done:,}/{grand_total:,} ({done/grand_total*100:.1f}%)\nETA: {eta/60:.0f}min\n{symbol} {tf}")
                    last_tg=time.time()

        elif engine=="div":
            for rsi_p,piv_n,macd_c,adx_mx in itertools.product(DIV_RSI_PERIODS,DIV_PIVOT_NS,DIV_MACD_CONF,DIV_ADX_MAXS):
                try:
                    precomputed = precompute_divergence_inputs(H,L,C,piv_n,rsi_p,macd_c,adx_mx)
                except Exception:
                    precomputed=None; errors+=1; continue
                for rsi_th,lb,rr in itertools.product(DIV_RSI_THRESH,DIV_LOOKBACKS,DIV_RR_RATIOS):
                    p={"rsi_period":rsi_p,"rsi_thresh":rsi_th,"pivot_n":piv_n,"lookback":lb,
                       "use_macd":macd_c,"adx_max":adx_mx,"rr":rr}
                    try: s=run_div_backtest(H,L,C,O,n,tf,p,precomputed=precomputed)
                    except Exception as e: s=None; errors+=1
                    if s and passes_filter(s):
                        buf.append(make_row(pair,tf,"rsi_divergence","sweep",None,PERIOD_START,_TODAY,p,s))
                        saved+=1
                    done+=1
                    if len(buf)>=300: save_rows(buf); buf=[]
            if time.time()-last_tg>600:
                el=time.time()-start; rate=done/el if el>0 else 0; eta=(grand_total-done)/rate if rate>0 else 0
                tg(f"⏳ DIV sweep\nDone: {done:,}/{grand_total:,} ({done/grand_total*100:.1f}%)\nETA: {eta/60:.0f}min\n{symbol} {tf}")
                last_tg=time.time()

        if buf: save_rows(buf); buf=[]

    return done, saved, errors, buf, last_tg

# ── PHASE: STABILITY (Stage 2 — locked configs across 3 periods) ──
def run_stability(engine, configs, grand_total, done, saved, errors, buf, start, last_tg):
    for cfg in configs:
        symbol, tf = cfg["pair"], cfg["timeframe"]
        pair = symbol.replace("/USDT","")
        for period_label, ps, pe in STABILITY_PERIODS:
            set_status("compute","running",done,grand_total,f"{engine} stability {symbol} {tf} {period_label}")
            rows=get_candles(symbol,tf,ps,pe)
            if not rows: done+=1; continue
            H=np.array([r["high"] for r in rows],dtype=float)
            L=np.array([r["low"] for r in rows],dtype=float)
            C=np.array([r["close"] for r in rows],dtype=float)
            O=np.array([r["open"] for r in rows],dtype=float)
            V=np.array([r.get("volume",0) for r in rows],dtype=float)
            n=len(rows)

            try:
                if engine=="bos": s=run_bos_backtest(H,L,C,O,n,cfg)
                elif engine=="ema": s=run_ema_backtest(H,L,C,O,V,n,cfg)
                elif engine=="div": s=run_div_backtest(H,L,C,O,n,tf,cfg)
            except Exception as e:
                s=None; errors+=1

            # Stability stage saves full results regardless of filter — we WANT
            # to see weaker periods, that's the point of this check.
            buf.append(make_row(pair,tf,
                                {"bos":"bos_pullback","ema":"ema_cross","div":"rsi_divergence"}[engine],
                                "stability",period_label,ps,pe,cfg,s))
            if s and passes_filter(s): saved+=1
            done+=1
            set_status("compute","running",done,grand_total,f"{engine} stability {symbol} {tf} {period_label}")

    if buf: save_rows(buf); buf=[]
    return done, saved, errors, buf, last_tg

# ── PHASE: MONTHLY (Stage 3 — locked configs, 18 calendar months) ──
def run_monthly(engine, configs, grand_total, done, saved, errors, buf, start, last_tg):
    for cfg in configs:
        symbol, tf = cfg["pair"], cfg["timeframe"]
        pair = symbol.replace("/USDT","")
        for month_label, ps, pe in MONTHLY_PERIODS:
            set_status("compute","running",done,grand_total,f"{engine} monthly {symbol} {tf} {month_label}")
            rows=get_candles(symbol,tf,ps,pe)
            if not rows: done+=1; continue
            H=np.array([r["high"] for r in rows],dtype=float)
            L=np.array([r["low"] for r in rows],dtype=float)
            C=np.array([r["close"] for r in rows],dtype=float)
            O=np.array([r["open"] for r in rows],dtype=float)
            V=np.array([r.get("volume",0) for r in rows],dtype=float)
            n=len(rows)

            try:
                if engine=="bos": s=run_bos_backtest(H,L,C,O,n,cfg)
                elif engine=="ema": s=run_ema_backtest(H,L,C,O,V,n,cfg)
                elif engine=="div": s=run_div_backtest(H,L,C,O,n,tf,cfg)
            except Exception as e:
                s=None; errors+=1

            buf.append(make_row(pair,tf,
                                {"bos":"bos_pullback","ema":"ema_cross","div":"rsi_divergence"}[engine],
                                "monthly",month_label,ps,pe,cfg,s))
            if s and passes_filter(s): saved+=1
            done+=1
            set_status("compute","running",done,grand_total,f"{engine} monthly {symbol} {tf} {month_label}")

    if buf: save_rows(buf); buf=[]
    return done, saved, errors, buf, last_tg

# ── COMBO COUNTING (for progress tracking) ─────────────────────
def count_sweep(engine):
    if engine=="bos":
        return (len(BOS_PAIRS)*len(BOS_TIMEFRAMES)*len(BOS_ENTRY_MODES)*len(BOS_PIVOT_NS)*
                len(BOS_RR_RATIOS)*len(BOS_FIB_LEVELS)*len(BOS_EMA_PAIRS)*len(BOS_ADX_MINS))
    if engine=="ema":
        return (len(EMA_PAIRS_LIST)*len(EMA_TIMEFRAMES)*len(EMA_FAST_LIST)*len(EMA_SLOW_LIST)*
                len(EMA_RR_LIST)*len(EMA_VOL_OPTS)*len(EMA_GAP_OPTS)*len(EMA_HTF_OPTS))
    if engine=="div":
        return (len(DIV_PAIRS)*len(DIV_TIMEFRAMES)*len(DIV_RSI_PERIODS)*len(DIV_RSI_THRESH)*
                len(DIV_PIVOT_NS)*len(DIV_LOOKBACKS)*len(DIV_MACD_CONF)*len(DIV_ADX_MAXS)*len(DIV_RR_RATIOS))
    return 0

def count_stability(engine):
    n = {"bos":len(BOS_LOCKED), "ema":len(EMA_LOCKED), "div":0}.get(engine,0)
    return n * len(STABILITY_PERIODS)

def count_monthly(engine):
    n = {"bos":len(BOS_LOCKED), "ema":len(EMA_LOCKED), "div":0}.get(engine,0)
    return n * len(MONTHLY_PERIODS)

# ── MAIN COMPUTE ────────────────────────────────────────────────
def main_compute(engine="bos", stage="sweep"):
    engines = ["bos","ema","div"] if engine=="all" else [engine]

    grand_total = sum(
        {"sweep":count_sweep,"stability":count_stability,"monthly":count_monthly}[stage](e)
        for e in engines
    )

    tg(f"""🔢 <b>Matrix Runner v2.0 — {engine}/{stage}</b>
Total combos: {grand_total:,}
Filter: min 30 trades, Sharpe 0.5-2.5, maxDD≤35%
Only passing combos saved (sweep), full visibility (stability/monthly)""")

    saved=0; errors=0; done=0
    start=time.time(); last_tg=time.time(); buf=[]

    for e in engines:
        if stage=="sweep":
            done,saved,errors,buf,last_tg = run_sweep(e,grand_total,done,saved,errors,buf,start,last_tg)
        elif stage=="stability":
            configs = {"bos":BOS_LOCKED,"ema":EMA_LOCKED,"div":[]}.get(e,[])
            if configs:
                done,saved,errors,buf,last_tg = run_stability(e,configs,grand_total,done,saved,errors,buf,start,last_tg)
        elif stage=="monthly":
            configs = {"bos":BOS_LOCKED,"ema":EMA_LOCKED,"div":[]}.get(e,[])
            if configs:
                done,saved,errors,buf,last_tg = run_monthly(e,configs,grand_total,done,saved,errors,buf,start,last_tg)

    if buf: save_rows(buf)
    el=time.time()-start
    set_status("compute","done",grand_total,grand_total,"")
    tg(f"""✅ <b>Matrix v2.0 Complete — {engine}/{stage}</b>
Combos: {grand_total:,} | Saved (passed filter): {saved:,} | Errors: {errors}
Time: {el/60:.1f}min""")
    print(f"Done. {saved:,} saved in {el/60:.1f}min")


def main_prefetch():
    import ccxt
    ex=ccxt.kucoin({"enableRateLimit":True})
    tg("📥 <b>Prefetch Started</b>")
    sh={**HEADERS,"Prefer":"return=minimal,resolution=ignore-duplicates"}
    all_pairs=list(set(BOS_PAIRS)|set(EMA_PAIRS_LIST)|set(DIV_PAIRS))
    all_tfs  =list(set(BOS_TIMEFRAMES)|set(EMA_TIMEFRAMES)|set(DIV_TIMEFRAMES))
    s_ms=int(datetime.strptime(PERIOD_START,"%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
    e_ms=int(datetime.now(timezone.utc).timestamp()*1000)
    for sym,tf in itertools.product(all_pairs,all_tfs):
        print(f"Fetching {sym} {tf}...")
        try:
            since=s_ms; all_candles=[]
            while since<e_ms:
                batch=ex.fetch_ohlcv(sym,tf,since=since,limit=1000)
                if not batch: break
                all_candles+=batch
                since=batch[-1][0]+1
                if len(batch)<1000: break
            rows=[{"symbol":sym,"timeframe":tf,"ts":c[0],"open":c[1],"high":c[2],
                   "low":c[3],"close":c[4],"volume":c[5]} for c in all_candles]
            for i in range(0,len(rows),500):
                httpx.post(f"{SUPABASE_URL}/rest/v1/candles",json=rows[i:i+500],headers=sh,timeout=30)
            print(f"  {len(rows)} candles saved")
        except Exception as ex2:
            print(f"  Error: {ex2}")
    tg("✅ <b>Prefetch Complete</b>")


def main():
    import sys
    mode = sys.argv[1] if len(sys.argv)>1 else "bos"
    if mode=="prefetch":
        main_prefetch()
    else:
        stage = sys.argv[2] if len(sys.argv)>2 else "sweep"
        main_compute(mode, stage)

if __name__=="__main__":
    main()
