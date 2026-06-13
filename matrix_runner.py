#!/usr/bin/env python3
# ============================================================
# WADDLE MATRIX RUNNER v5
# Pure standalone — loads candles from Supabase, runs all
# combos in memory, saves results. No HTTP per combo.
# Estimated runtime: 15-30 minutes for 19,440 combos
# ============================================================

import os, time, httpx, itertools, csv, io
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

PAIRS       = ["DOGE/USDT","XLM/USDT","XRP/USDT","ADA/USDT","TRX/USDT","ARB/USDT"]
TIMEFRAMES  = ["5m","15m","1h"]
ENGINES     = ["structure"]
ENTRY_MODES = ["rejection","reclaim"]
PIVOT_NS    = [3,5,8]
RR_RATIOS   = [1.5,2.0,3.0,4.0]
FIB_LEVELS  = [0.382,0.5,0.618]
EMA_PAIRS   = ["off","34/55","55/89","89/144","144/169"]
ADX_MINS    = [0,15,25]
PERIOD_START= "2025-01-01"
PERIOD_END  = "2026-01-01"
RISK_PCT    = 0.02

TOTAL = (len(PAIRS)*len(TIMEFRAMES)*len(ENGINES)*len(ENTRY_MODES)*
         len(PIVOT_NS)*len(RR_RATIOS)*len(FIB_LEVELS)*len(EMA_PAIRS)*len(ADX_MINS))

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
def ema(arr, p):
    k=2/(p+1); out=np.empty(len(arr)); out[0]=arr[0]
    for i in range(1,len(arr)): out[i]=arr[i]*k+out[i-1]*(1-k)
    return out

def adx(H,L,C,p):
    n=len(H); adx_=np.zeros(n); pdm=np.zeros(n); mdm=np.zeros(n); tr=np.zeros(n)
    for i in range(1,n):
        pdm[i]=max(H[i]-H[i-1],0) if H[i]-H[i-1]>L[i-1]-L[i] else 0
        mdm[i]=max(L[i-1]-L[i],0) if L[i-1]-L[i]>H[i]-H[i-1] else 0
        tr[i]=max(H[i]-L[i],abs(H[i]-C[i-1]),abs(L[i]-C[i-1]))
    st=sp=sm=sum(tr[1:p+1]); sp=sum(pdm[1:p+1]); sm=sum(mdm[1:p+1])
    dx=np.zeros(n)
    for i in range(p+1,n):
        st=st-st/p+tr[i]; sp=sp-sp/p+pdm[i]; sm=sm-sm/p+mdm[i]
        pi_=(sp/st*100) if st>0 else 0; mi_=(sm/st*100) if st>0 else 0
        s=pi_+mi_; dx[i]=abs(pi_-mi_)/s*100 if s>0 else 0
    s2=p*2
    if s2<n: adx_[s2]=sum(dx[p+1:s2+1])/p
    for i in range(s2+1,n): adx_[i]=(adx_[i-1]*(p-1)+dx[i])/p
    return adx_

def pivots(H,L,N):
    pv=[]
    for i in range(N,len(H)-N):
        if H[i]==max(H[i-N:i+N+1]): pv.append({"i":i,"t":"H","p":float(H[i])})
        elif L[i]==min(L[i-N:i+N+1]): pv.append({"i":i,"t":"L","p":float(L[i])})
    dd=[]
    for p in pv:
        if not dd: dd.append(p); continue
        last=dd[-1]
        if last["t"]==p["t"]:
            if p["t"]=="H" and p["p"]>last["p"]: dd[-1]=p
            elif p["t"]=="L" and p["p"]<last["p"]: dd[-1]=p
        else: dd.append(p)
    return dd

# ── BACKTEST ───────────────────────────────────────────────────
def backtest(H,L,C,n,pv,eng,em,rr,fib,ef,es,use_ema,adx_v,adx_thr,N):
    equity=100.0; trades=[]
    MIN=0.002

    def ok(i,d):
        if use_ema and ef is not None:
            if d=="bull" and ef[i]<=es[i]: return False
            if d=="bear" and ef[i]>=es[i]: return False
        if adx_v is not None and adx_v[i]<adx_thr: return False
        return True

    def exit_(d,sl,tp,start):
        for i in range(start,min(start+300,n)):
            if d=="bull":
                if L[i]<=sl: return sl,i
                if H[i]>=tp: return tp,i
            else:
                if H[i]>=sl: return sl,i
                if L[i]<=tp: return tp,i
        return None,None

    def trade(entry,sl,tp,won_price):
        nonlocal equity
        rpp=abs(entry-sl)
        if rpp<=0: return
        pos=equity*RISK_PCT/rpp
        won=won_price==tp
        gross=(won_price-entry)*pos if sl<entry else (entry-won_price)*pos
        fee=pos*entry*(0.0004 if won else 0.00075)
        pnl=gross-fee; equity+=pnl
        trades.append({"w":won,"p":pnl,"e":equity})

    if eng=="original":
        used=-1; bias=None
        for pi in range(2,len(pv)):
            a,b,c_=pv[pi-2],pv[pi-1],pv[pi]
            st=None
            if a["t"]=="H" and b["t"]=="L" and c_["t"]=="H" and c_["p"]<a["p"]: st="bear"
            elif a["t"]=="L" and b["t"]=="H" and c_["t"]=="L" and c_["p"]>a["p"]: st="bull"
            if not st or c_["i"]<=used: continue
            if bias and bias!=st: continue
            fh=a["p"] if st=="bear" else b["p"]
            fl=b["p"] if st=="bear" else a["p"]
            rng=fh-fl
            if rng<=0: continue
            fe=fl+rng*fib if st=="bear" else fh-rng*fib
            sl_=fh+rng*0.02 if st=="bear" else fl-rng*0.02
            ec=None
            for ci in range(c_["i"]+N+1,min(c_["i"]+200,n-1)):
                if st=="bear":
                    if H[ci]>fh: break
                    if H[ci]>=fe: ec=ci; break
                else:
                    if L[ci]<fl: break
                    if L[ci]<=fe: ec=ci; break
            if ec is None: continue
            if not ok(ec,st): continue
            rpp=abs(fe-sl_)
            if rpp<=0: continue
            tp=fe+rpp*rr if st=="bull" else fe-rpp*rr
            xp,xc=exit_(st,sl_,tp,ec+1)
            if xp is None: continue
            trade(fe,sl_,tp,xp)
            bias=None if xp==tp else ("bull" if st=="bear" else "bear")
            used=c_["i"]

    elif eng in ("bos_pivot","pullback"):
        v2=eng=="pullback"
        ph={p["i"]:p["p"] for p in pv if p["t"]=="H"}
        pl={p["i"]:p["p"] for p in pv if p["t"]=="L"}
        for st in ["bull","bear"]:
            src=[p for p in pv if p["t"]==("H" if st=="bull" else "L")]
            setups=[]
            for p1 in src:
                for ci in range(p1["i"]+1,n-1):
                    if st=="bull" and C[ci]>p1["p"]:
                        p2=float(min(L[p1["i"]:ci+1]))
                        rng=C[ci]-p2
                        if rng>0 and rng/max(p2,1)>=MIN:
                            setups.append({"p1i":p1["i"],"p1p":p1["p"],"p2":p2,
                                "p3i":ci,"p3c":C[ci],"fe":p2+rng*fib,"sl":p2})
                        break
                    elif st=="bear" and C[ci]<p1["p"]:
                        p2=float(max(H[p1["i"]:ci+1]))
                        rng=p2-C[ci]
                        if rng>0 and rng/max(p2,1)>=MIN:
                            setups.append({"p1i":p1["i"],"p1p":p1["p"],"p2":p2,
                                "p3i":ci,"p3c":C[ci],"fe":p2-rng*fib,"sl":p2})
                        break
                    if (st=="bull" and L[ci]<p1["p"]*0.90) or (st=="bear" and H[ci]>p1["p"]*1.10): break
            setups.sort(key=lambda x:x["p3i"])
            si=0; act=None; lp=-1; ci=1
            while ci<n-1:
                if act is None:
                    while si<len(setups):
                        s=setups[si]; si+=1
                        if s["p3i"]<=lp: continue
                        act=s; ci=s["p3i"]+1; break
                    if act is None: break
                fe_,sl_=act["fe"],act["sl"]
                if v2:
                    pd=ph if st=="bull" else pl
                    if ci in pd:
                        nv=pd[ci]
                        if (st=="bull" and nv>act["p1p"]) or (st=="bear" and nv<act["p1p"]):
                            act["p1i"]=ci; act["p1p"]=nv; act["p3i"]=ci; act["p3c"]=nv
                            np2=float(min(L[act["p1i"]:ci+1])) if st=="bull" else float(max(H[act["p1i"]:ci+1]))
                            rng=abs(nv-np2)
                            if rng>0 and rng/max(np2,1)>=MIN:
                                act["p2"]=np2; act["fe"]=np2+rng*fib if st=="bull" else np2-rng*fib
                                act["sl"]=np2; fe_=act["fe"]; sl_=act["sl"]
                            ci+=1; continue
                        elif (st=="bull" and nv>act["p3c"]) or (st=="bear" and nv<act["p3c"]):
                            np2=float(min(L[act["p3i"]:ci+1])) if st=="bull" else float(max(H[act["p3i"]:ci+1]))
                            rng=abs(nv-np2)
                            if rng>0 and rng/max(np2,1)>=MIN:
                                act["p2"]=np2; act["p3i"]=ci; act["p3c"]=nv
                                act["fe"]=np2+rng*fib if st=="bull" else np2-rng*fib
                                act["sl"]=np2; fe_=act["fe"]; sl_=act["sl"]
                            ci+=1; continue
                if (st=="bull" and L[ci]<act["p2"]) or (st=="bear" and H[ci]>act["p2"]):
                    act=None; ci+=1; continue
                if not ok(ci,st): ci+=1; continue
                trig=False
                if em=="touch": trig=L[ci]<=fe_ if st=="bull" else H[ci]>=fe_
                else: trig=(L[ci]<=fe_ and C[ci]>fe_) if st=="bull" else (H[ci]>=fe_ and C[ci]<fe_)
                if not trig: ci+=1; continue
                sl_u=act["p2"]*(0.999 if st=="bull" else 1.001)
                rpp=abs(fe_-sl_u)
                if rpp<=0: act=None; ci+=1; continue
                tp=fe_+rpp*rr if st=="bull" else fe_-rpp*rr
                xp,xc=exit_(st,sl_u,tp,ci+1)
                if xp is None: act=None; ci=n; continue
                trade(fe_,sl_u,tp,xp)
                lp=act["p3i"]; act=None; ci=xc+1

    elif eng=="structure":
        ph=[p for p in pv if p["t"]=="H"]
        pl=[p for p in pv if p["t"]=="L"]
        for st in ["bull","bear"]:
            src=ph if st=="bull" else pl
            for i in range(1,len(src)):
                pp,pc=src[i-1],src[i]
                if (st=="bull" and pc["p"]<=pp["p"]) or (st=="bear" and pc["p"]>=pp["p"]): continue
                p2=float(min(L[pp["i"]:pc["i"]+1])) if st=="bull" else float(max(H[pp["i"]:pc["i"]+1]))
                rng=abs(pc["p"]-p2)
                if rng<=0: continue
                fe_=pc["p"]-rng*fib if st=="bull" else pc["p"]+rng*fib
                sl_=p2*(0.999 if st=="bull" else 1.001)
                for ci in range(pc["i"]+1,n-1):
                    if not ok(ci,st): continue
                    if (st=="bull" and L[ci]<sl_) or (st=="bear" and H[ci]>sl_): break
                    trig=False
                    if em=="touch": trig=L[ci]<=fe_ if st=="bull" else H[ci]>=fe_
                    else: trig=(L[ci]<=fe_ and C[ci]>fe_) if st=="bull" else (H[ci]>=fe_ and C[ci]<fe_)
                    if not trig: continue
                    rpp=abs(fe_-sl_)
                    if rpp<=0: break
                    tp=fe_+rpp*rr if st=="bull" else fe_-rpp*rr
                    xp,xc=exit_(st,sl_,tp,ci+1)
                    if xp is None: break
                    trade(fe_,sl_,tp,xp); break

    elif eng=="ema_cross":
        if ef is None: return None
        av=np.zeros(n)
        for i in range(1,n):
            tr=max(H[i]-L[i],abs(H[i]-C[i-1]),abs(L[i]-C[i-1]))
            av[i]=(av[i-1]*(N-1)+tr)/N if i>=N else tr
        for st in ["bull","bear"]:
            in_t=False; en_=sl_=tp_=pos_=nt_=None
            for ci in range(max(50,int(n*0.05)),n-1):
                f_,s_=ef[ci],es[ci]
                if in_t:
                    xp_=None
                    if st=="bull":
                        if L[ci]<=sl_: xp_=sl_
                        elif H[ci]>=tp_: xp_=tp_
                    else:
                        if H[ci]>=sl_: xp_=sl_
                        elif L[ci]<=tp_: xp_=tp_
                    if xp_ is not None:
                        won=xp_==tp_
                        gross=(xp_-en_)*pos_ if st=="bull" else (en_-xp_)*pos_
                        fee=nt_*(0.0004 if won else 0.00075)
                        pnl=gross-fee; equity+=pnl
                        trades.append({"w":won,"p":pnl,"e":equity})
                        in_t=False
                    continue
                if adx_v is not None and adx_v[ci]<adx_thr: continue
                if st=="bull":
                    if not(C[ci]>f_ and C[ci]>s_ and f_>s_): continue
                    tf_=L[ci]<=f_ and C[ci]>f_; ts_=L[ci]<=s_ and C[ci]>s_
                    if not(tf_ or ts_): continue
                    ep=f_ if tf_ else s_
                else:
                    if not(C[ci]<f_ and C[ci]<s_ and f_<s_): continue
                    tf_=H[ci]>=f_ and C[ci]<f_; ts_=H[ci]>=s_ and C[ci]<s_
                    if not(tf_ or ts_): continue
                    ep=f_ if tf_ else s_
                at=av[ci]
                if at<=0: continue
                sl=ep-at*1.5 if st=="bull" else ep+at*1.5
                rpp=abs(ep-sl)
                if rpp<=0: continue
                tp=ep+rpp*rr if st=="bull" else ep-rpp*rr
                pos_=equity*RISK_PCT/rpp; nt_=pos_*ep
                en_=ep; sl_=sl; tp_=tp; in_t=True

    if not trades: return None
    W=[t for t in trades if t["w"]]; L_=[t for t in trades if not t["w"]]
    final=trades[-1]["e"]; tr=(final-100)/100*100; wr=len(W)/len(trades)*100
    peak=100; mdd=0
    for t in trades:
        if t["e"]>peak: peak=t["e"]
        mdd=max(mdd,(peak-t["e"])/peak*100)
    rets=[t["p"]/(t["e"]-t["p"])*100 for t in trades if t["e"]!=t["p"]]
    mean=sum(rets)/len(rets) if rets else 0
    std=(sum((r-mean)**2 for r in rets)/len(rets))**0.5 if rets else 0
    sharpe=mean/std*(365**0.5) if std>0 else 0
    gw=sum(t["p"] for t in W); gl=abs(sum(t["p"] for t in L_))
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

# ── MAIN ───────────────────────────────────────────────────────
def main_compute():
    # Clear old results
    try:
        httpx.delete(f"{SUPABASE_URL}/rest/v1/matrix_results?id=gt.0",
                     headers=HEADERS,timeout=30)
        print("Cleared old results")
    except: pass

    tg(f"""🔢 <b>Matrix Runner v5 — Pine Script Aligned</b>
{TOTAL:,} combos | Pure Python — no HTTP per combo
Pairs: {len(PAIRS)} × TF: {len(TIMEFRAMES)}
Engines: {len(ENGINES)} × Entry: {len(ENTRY_MODES)}
EMA: {len(EMA_PAIRS)} × ADX: {len(ADX_MINS)}
Est: 15-30 min""")

    saved=0; errors=0; done=0
    start=time.time(); last_tg=time.time()
    buf=[]

    for sym,tf in itertools.product(PAIRS,TIMEFRAMES):
        print(f"\nLoading {sym} {tf}...")
        set_status("compute","running",done,TOTAL,f"{sym} {tf}")
        rows=get_candles(sym,tf)
        if not rows:
            print(f"  No candles — skip")
            continue

        H=np.array([r["high"]  for r in rows],dtype=float)
        L=np.array([r["low"]   for r in rows],dtype=float)
        C=np.array([r["close"] for r in rows],dtype=float)
        n=len(rows)
        print(f"  {n} candles — running combos...")

        # Precompute per pair+TF
        pv_cache={N:pivots(H,L,N) for N in PIVOT_NS}
        ema_cache={ep:(ema(C,int(ep.split("/")[0])),ema(C,int(ep.split("/")[1])))
                   for ep in EMA_PAIRS if ep!="off"}
        adx_cache=adx(H,L,C,14)

        pt_combos=list(itertools.product(ENGINES,ENTRY_MODES,PIVOT_NS,RR_RATIOS,FIB_LEVELS,EMA_PAIRS,ADX_MINS))

        for eng,em,N,rr,fib,ep,ax in pt_combos:
            use_ema=ep!="off"
            ef,es=(ema_cache[ep] if use_ema else (None,None))
            adx_v=adx_cache if ax>0 else None

            try:
                s=backtest(H,L,C,n,pv_cache[N],eng,em,rr,fib,ef,es,use_ema,adx_v,float(ax),N)
            except Exception as e:
                s=None; errors+=1

            buf.append({
                "combo_key":f"{sym}|{tf}|{eng}|{em}|{N}|{rr}|{fib}|{ep}|{ax}",
                "pair":sym.replace("/USDT",""),"timeframe":tf,
                "engine":eng,"entry_mode":em,"pivot_n":N,"rr":rr,
                "fib_level":fib,"ema_pair":ep,"adx_min":ax,
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
                   f"Current: {sym} {tf}")
                set_status("compute","running",done,TOTAL,f"{sym} {tf}")
                last_tg=time.time()

        if buf: save_rows(buf); buf=[]
        print(f"  Done {sym} {tf}")

    if buf: save_rows(buf)
    el=time.time()-start
    set_status("compute","done",TOTAL,TOTAL,"")
    tg(f"""✅ <b>Matrix Complete</b>
Combos: {TOTAL:,}
Saved: {saved:,} | Errors: {errors}
Time: {el/60:.1f}min
Download from Runner tab → All Results""")
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
                if not b: ec+=1;
                if ec>=3: break
                f=[c for c in b if c[0]<e_ms]; candles+=f
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
