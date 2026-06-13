#!/usr/bin/env python3
# ============================================================
# WADDLE MATRIX RUNNER v6
# Pine Script P1-P2-P3 Master Algorithm v6.5 faithful port
# Pulls candles from Supabase, runs all combos in memory
# No HTTP per combo — full speed pure Python
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

# ── VARIABLE SPACE ─────────────────────────────────────────────
PAIRS       = ["DOGE/USDT","XLM/USDT","XRP/USDT","ADA/USDT","TRX/USDT","ARB/USDT"]
TIMEFRAMES  = ["5m","15m","1h"]
ENTRY_MODES = ["rejection","reclaim"]
PIVOT_NS    = [3,5,8]
RR_RATIOS   = [1.5,2.0,3.0,4.0]
FIB_LEVELS  = [0.382,0.5,0.618]
EMA_PAIRS   = ["off","34/55","55/89","89/144","144/169"]
ADX_MINS    = [0,15,25]
PERIOD_START= "2025-01-01"
PERIOD_END  = "2026-01-01"
RISK_PCT    = 0.02
MIN_SWING   = 0.002
STOP_BUF    = 0.001

TOTAL = (len(PAIRS)*len(TIMEFRAMES)*len(ENTRY_MODES)*
         len(PIVOT_NS)*len(RR_RATIOS)*len(FIB_LEVELS)*
         len(EMA_PAIRS)*len(ADX_MINS))

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

def get_candles(symbol, timeframe):
    start_ms=int(datetime.strptime(PERIOD_START,"%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
    end_ms  =int(datetime.strptime(PERIOD_END,  "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
    rows=[]; offset=0
    while True:
        q=(f"symbol=eq.{symbol}&timeframe=eq.{timeframe}"
           f"&ts=gte.{start_ms}&ts=lte.{end_ms}"
           f"&order=ts.asc&limit=10000&offset={offset}&select=open,high,low,close")
        res=httpx.get(f"{SUPABASE_URL}/rest/v1/candles?{q}",headers=HEADERS,timeout=60)
        if res.status_code==200:
            batch=res.json()
            if not batch: break
            rows+=batch
            if len(batch)<10000: break
            offset+=10000
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

# ── MAIN COMPUTE ───────────────────────────────────────────────
def main_compute():
    try:
        httpx.delete(f"{SUPABASE_URL}/rest/v1/matrix_results?id=gt.0",
                     headers=HEADERS,timeout=30)
        print("Cleared old results")
    except: pass

    tg(f"""🔢 <b>Matrix Runner v6 — Pine Script P1-P2-P3</b>
Total combos: {TOTAL:,}
Pairs: {', '.join(p.replace('/USDT','') for p in PAIRS)}
TFs: {', '.join(TIMEFRAMES)}
Entry: {', '.join(ENTRY_MODES)}
EMA: {len(EMA_PAIRS)} options × ADX: {len(ADX_MINS)} options
Period: {PERIOD_START} → {PERIOD_END}""")

    saved=0; errors=0; done=0
    start=time.time(); last_tg=time.time()
    buf=[]

    for symbol,tf in itertools.product(PAIRS,TIMEFRAMES):
        print(f"\nLoading {symbol} {tf}...")
        set_status("compute","running",done,TOTAL,f"{symbol} {tf}")
        rows=get_candles(symbol,tf)
        if not rows:
            print(f"  No candles — skip")
            continue

        H=np.array([r["high"]  for r in rows],dtype=float)
        L=np.array([r["low"]   for r in rows],dtype=float)
        C=np.array([r["close"] for r in rows],dtype=float)
        O=np.array([r["open"]  for r in rows],dtype=float)
        n=len(rows)
        print(f"  {n} candles")

        # Precompute EMA and ADX for all combinations
        ema_cache={}
        for ep in EMA_PAIRS:
            if ep!="off":
                f,s=map(int,ep.split("/"))
                ema_cache[ep]=(calc_ema(C,f),calc_ema(C,s))
        adx_cache=calc_adx(H,L,C,14)

        pt_combos=list(itertools.product(
            ENTRY_MODES,PIVOT_NS,RR_RATIOS,FIB_LEVELS,EMA_PAIRS,ADX_MINS
        ))
        print(f"  Running {len(pt_combos):,} combos...")

        for em,N,rr,fib,ep,ax in pt_combos:
            use_ema=ep!="off"
            ef,es=ema_cache[ep] if use_ema else (None,None)
            adx_v=adx_cache if ax>0 else None

            try:
                s=backtest(H,L,C,O,n,N,rr,fib,em,ef,es,use_ema,adx_v,float(ax))
            except Exception as e:
                s=None; errors+=1; print(f"  Error {symbol} {tf} {em} N={N}: {e}")

            buf.append({
                "combo_key":f"{symbol}|{tf}|structure|{em}|{N}|{rr}|{fib}|{ep}|{ax}",
                "pair":symbol.replace("/USDT",""),
                "timeframe":tf,"engine":"structure","entry_mode":em,
                "pivot_n":N,"rr":rr,"fib_level":fib,
                "ema_pair":ep,"adx_min":ax,
                "period_start":PERIOD_START,"period_end":PERIOD_END,
                "success":s is not None,
                "return_pct":s["return_pct"] if s else None,
                "cagr":s["cagr"] if s else None,
                "max_dd":s["max_dd"] if s else None,
                "sharpe":s["sharpe"] if s else None,
                "profit_factor":s["pf"] if s else None,
                "win_rate":s["wr"] if s else None,
                "trades":s["trades"] if s else 0,
                "wins":s["wins"] if s else 0,
                "losses":s["losses"] if s else 0,
                "avg_win":s["avg_win"] if s else None,
                "avg_loss":s["avg_loss"] if s else None,
                "kelly_full":s["kelly"] if s else None,
                "computed_at":datetime.now(timezone.utc).isoformat(),
            })
            if s: saved+=1
            done+=1

            if len(buf)>=500:
                save_rows(buf); buf=[]

            if time.time()-last_tg>600:
                el=time.time()-start
                rate=done/el if el>0 else 0
                eta=(TOTAL-done)/rate if rate>0 else 0
                tg(f"⏳ <b>Progress</b>\n"
                   f"Done: {done:,}/{TOTAL:,} ({done/TOTAL*100:.1f}%)\n"
                   f"Saved: {saved:,} | Errors: {errors}\n"
                   f"Rate: {rate:.0f}/s | ETA: {eta/60:.0f}min\n"
                   f"Current: {symbol} {tf}")
                set_status("compute","running",done,TOTAL,f"{symbol} {tf}")
                last_tg=time.time()

        if buf: save_rows(buf); buf=[]
        print(f"  Done {symbol} {tf} — {saved:,} saved so far")

    if buf: save_rows(buf)
    el=time.time()-start
    set_status("compute","done",TOTAL,TOTAL,"")
    tg(f"""✅ <b>Matrix Complete</b>
Combos: {TOTAL:,} | Saved: {saved:,} | Errors: {errors}
Time: {el/60:.1f}min
Download from Runner tab.""")
    print(f"\nDone. {saved:,} saved in {el/60:.1f}min")

def main_prefetch():
    import ccxt
    ex=ccxt.kucoin({"enableRateLimit":True})
    tg("📥 <b>Prefetch Started</b>")
    sh={**HEADERS,"Prefer":"return=minimal,resolution=ignore-duplicates"}
    s_ms=int(datetime.strptime(PERIOD_START,"%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
    e_ms=int(datetime.strptime(PERIOD_END,  "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
    for sym,tf in itertools.product(PAIRS,TIMEFRAMES):
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
    main_compute()

if __name__=="__main__":
    import sys
    if len(sys.argv)>1 and sys.argv[1]=="prefetch":
        main_prefetch()
    else:
        main_compute()
