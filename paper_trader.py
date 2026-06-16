# ============================================================
# WADDLE PAPER TRADER v11
# Dual Engine: BOS Pullback + EMA Cross
#   BOS Pullback — Pine Script P1-P2-P3 v6.5
#     5 pairs: DOGE/15m, XLM/5m, TRX/1h, ARB/15m, XRP/1h
#   EMA Cross — Engine 6, stability-tested combos
#     3 pairs: ARB/5m, XLM/5m, TRX/15m
# ============================================================

import ccxt
import numpy as np
import time
import os
import httpx
from datetime import datetime, timezone, timedelta

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN","")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID","")
SUPABASE_URL     = os.environ.get("SUPABASE_URL","")
SUPABASE_KEY     = os.environ.get("SUPABASE_KEY","")

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}

START_BALANCE     = 100.0
RISK_PCT          = 0.02
MIN_SWING         = 0.002
STOP_BUF          = 0.001
MAX_CONSEC_LOSSES = 10

SCAN_INTERVALS = {
    "5m":300,"15m":900,"1h":3600,
}

# ── WATCHLISTS ────────────────────────────────────────────────
# BOS Pullback — final 5 configs (1 per pair), selected via:
#   Stage 1: full sweep (19,440 combos) → top-5 per pair by Sharpe
#   Stage 2: top-5 stability check across 2025/2026YTD/L30D
#   Stage 3: 18-month consistency check (% profitable months)
# ARB excluded from BOS (already covered by EMA Cross below).
BOS_WATCHLIST = [
    # ADA — 89% of 18 months profitable, avg +6.09%/mo, std 6.08% (best ratio)
    {"symbol":"ADA/USDT", "timeframe":"15m","pivot_n":8,"rr":1.5,"fib_level":0.382,"entry_mode":"reclaim",  "ema_pair":"89/144","adx_min":25,"label":"ADA 15m BOS", "engine":"bos_pullback"},
    # DOGE — upgraded from runner-up: 72% profitable, 132 trades/18mo
    {"symbol":"DOGE/USDT","timeframe":"15m","pivot_n":8,"rr":1.5,"fib_level":0.618,"entry_mode":"reclaim",  "ema_pair":"34/55", "adx_min":15,"label":"DOGE 15m BOS","engine":"bos_pullback"},
    # XLM — 78% profitable, avg +5.99%/mo
    {"symbol":"XLM/USDT", "timeframe":"15m","pivot_n":5,"rr":4.0,"fib_level":0.382,"entry_mode":"reclaim",  "ema_pair":"89/144","adx_min":25,"label":"XLM 15m BOS", "engine":"bos_pullback"},
    # TRX — upgraded from runner-up: 78% profitable (was 67%)
    {"symbol":"TRX/USDT", "timeframe":"1h", "pivot_n":3,"rr":1.5,"fib_level":0.618,"entry_mode":"rejection","ema_pair":"89/144","adx_min":15,"label":"TRX 1h BOS",  "engine":"bos_pullback"},
    # XRP — upgraded from runner-up: 72% profitable (was 61%), 133 trades/18mo
    {"symbol":"XRP/USDT", "timeframe":"15m","pivot_n":3,"rr":2.0,"fib_level":0.5,  "entry_mode":"reclaim",  "ema_pair":"55/89", "adx_min":25,"label":"XRP 15m BOS", "engine":"bos_pullback"},
]

# EMA Cross — final 2 configs, from full 3-stage validation:
#   Stage 1: full sweep → top-5 per pair
#   Stage 2: 3-period stability (2025/2026YTD/L30D)
#   Stage 3: 18-month consistency — ARB 72%, XLM 61% profitable months
# TRX dropped (4 zero-trade months, only 34 trades/18mo)
# XRP dropped (44% profitable months — worse than coin flip)
# DOGE dropped (all configs failed 2026 YTD — regime change)
EMA_WATCHLIST = [
    {"symbol":"ARB/USDT","timeframe":"5m", "ema_fast":12,"ema_slow":26,"rr":2.0,"use_vol":True, "use_gap":True,"use_htf":False,"label":"ARB 5m EMA12/26 vol+gap","engine":"ema_cross"},
    {"symbol":"XLM/USDT","timeframe":"15m","ema_fast":12,"ema_slow":26,"rr":2.0,"use_vol":False,"use_gap":True,"use_htf":True, "label":"XLM 15m EMA12/26 gap+htf","engine":"ema_cross"},
]

ALL_WATCHLIST = BOS_WATCHLIST + EMA_WATCHLIST


# ── SUPABASE ──────────────────────────────────────────────────
def get_account():
    try:
        res = httpx.get(f"{SUPABASE_URL}/rest/v1/paper_account?id=eq.1&select=*",
                        headers=SUPABASE_HEADERS, timeout=10)
        if res.status_code==200 and res.json(): return res.json()[0]
    except Exception as e: print(f"Get account error: {e}")
    return None

def init_account():
    try:
        existing = get_account()
        if existing: return existing
        row = {"id":1,"balance":START_BALANCE,"total_trades":0,"wins":0,"losses":0,
               "total_pnl":0.0,"peak_balance":START_BALANCE,"max_drawdown":0.0,
               "created_at":datetime.now(timezone.utc).isoformat()}
        httpx.post(f"{SUPABASE_URL}/rest/v1/paper_account",json=row,
                   headers=SUPABASE_HEADERS,timeout=10)
        return row
    except Exception as e:
        print(f"Init account error: {e}")
        return {"balance":START_BALANCE,"total_trades":0,"wins":0,"losses":0,
                "total_pnl":0.0,"peak_balance":START_BALANCE,"max_drawdown":0.0}

def update_account(balance, won, pnl):
    try:
        acc    = get_account()
        if not acc: acc = init_account()
        peak   = max(acc["peak_balance"], balance)
        dd     = round((peak-balance)/peak*100, 2)
        max_dd = max(acc["max_drawdown"], dd)
        updates = {
            "balance":      round(balance,4),
            "total_trades": acc["total_trades"]+1,
            "wins":         acc["wins"]+(1 if won else 0),
            "losses":       acc["losses"]+(0 if won else 1),
            "total_pnl":    round(acc["total_pnl"]+pnl,4),
            "peak_balance": round(peak,4),
            "max_drawdown": max_dd,
        }
        httpx.patch(f"{SUPABASE_URL}/rest/v1/paper_account?id=eq.1",
                    json=updates,headers=SUPABASE_HEADERS,timeout=10)
        return {**acc,**updates}
    except Exception as e:
        print(f"Update account error: {e}")
        return None

def log_trade(trade_data):
    try:
        httpx.post(f"{SUPABASE_URL}/rest/v1/paper_trades",json=trade_data,
                   headers={**SUPABASE_HEADERS,"Prefer":"return=minimal"},timeout=10)
    except Exception as e: print(f"Log trade error: {e}")

def get_today_trades():
    try:
        since = (datetime.now(timezone.utc)-timedelta(hours=24)).isoformat()
        res = httpx.get(
            f"{SUPABASE_URL}/rest/v1/paper_trades?created_at=gte.{since}&select=*&order=created_at.desc",
            headers=SUPABASE_HEADERS,timeout=10)
        if res.status_code==200: return res.json()
    except: pass
    return []

# ── TELEGRAM ──────────────────────────────────────────────────
def tg(msg):
    try:
        httpx.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                   json={"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"HTML"},timeout=10)
    except Exception as e: print(f"Telegram error: {e}")

def send_entry(w, signal, acc):
    if w["engine"]=="ema_cross":
        send_entry_ema(w, signal, acc)
    else:
        send_entry_bos(w, signal, acc)

def send_entry_bos(w, signal, acc):
    balance  = acc["balance"]
    risk_amt = round(balance*RISK_PCT,4)
    rpp      = abs(signal["entry"]-signal["sl"])
    pos_size = round(risk_amt/rpp,6) if rpp>0 else 0
    pos_val  = round(pos_size*signal["entry"],2)
    total_ret= round((balance-START_BALANCE)/START_BALANCE*100,2)
    wr       = round(acc["wins"]/acc["total_trades"]*100,1) if acc["total_trades"]>0 else 0
    emoji    = "🟢" if signal["direction"]=="LONG" else "🔴"
    ema_pair = w.get("ema_pair","off")
    adx_min  = w.get("adx_min",0)
    filt_str = []
    if ema_pair!="off": filt_str.append(f"EMA{ema_pair}")
    if adx_min>0: filt_str.append(f"ADX≥{adx_min}")
    filt_str = " + ".join(filt_str) if filt_str else "none"

    tg(f"""{emoji} <b>ENTRY — {w['symbol']} {w['timeframe'].upper()}</b>

<b>Variant:</b>  {w['label']}
<b>Strategy:</b> BOS Pullback | {signal['direction']}
<b>Entry mode:</b> {w['entry_mode'].capitalize()}
<b>Filters:</b>  {filt_str}

<b>P1:</b> ${signal['p1']}
<b>P2:</b> ${signal['p2']} (SL anchor)
<b>P3:</b> ${signal['p3']} (BOS level)
<b>Fib entry:</b> ${signal['entry']}

<b>Entry:</b>  ${signal['entry']}
<b>SL:</b>     ${signal['sl']}
<b>TP:</b>     ${signal['tp']}
<b>RR:</b>     1:{w['rr']}R

<b>Account:</b>  ${balance:.2f}
<b>Risk:</b>     2% = ${risk_amt:.2f}
<b>Position:</b> ${pos_val:.2f} of {w['symbol'].split('/')[0]}

<b>Stats:</b> {acc['total_trades']} trades · {acc['wins']}W {acc['losses']}L · {wr}% WR
<b>Total return:</b> {'+' if total_ret>=0 else ''}{total_ret}%
⏰ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
📊 Paper trade v11""")

def send_entry_ema(w, signal, acc):
    balance  = acc["balance"]
    risk_amt = round(balance*RISK_PCT,4)
    rpp      = abs(signal["entry"]-signal["sl"])
    pos_size = round(risk_amt/rpp,6) if rpp>0 else 0
    pos_val  = round(pos_size*signal["entry"],2)
    total_ret= round((balance-START_BALANCE)/START_BALANCE*100,2)
    wr       = round(acc["wins"]/acc["total_trades"]*100,1) if acc["total_trades"]>0 else 0
    emoji    = "🟢" if signal["direction"]=="LONG" else "🔴"

    tg(f"""{emoji} <b>ENTRY — {w['symbol']} {w['timeframe'].upper()}</b>

<b>Strategy:</b> EMA Cross | {signal['direction']}
<b>EMA pair:</b> {signal['ema_fast']}/{signal['ema_slow']}
<b>Filters:</b> {signal['filters']}

<b>Entry:</b>  ${signal['entry']}
<b>SL:</b>     ${signal['sl']} (3-bar swing)
<b>TP:</b>     ${signal['tp']}
<b>RR:</b>     1:{w['rr']}R

<b>Account:</b>  ${balance:.2f}
<b>Risk:</b>     2% = ${risk_amt:.2f}
<b>Position:</b> ${pos_val:.2f} of {w['symbol'].split('/')[0]}

<b>Stats:</b> {acc['total_trades']} trades · {acc['wins']}W {acc['losses']}L · {wr}% WR
<b>Total return:</b> {'+' if total_ret>=0 else ''}{total_ret}%
⏰ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
📊 Paper trade v11""")

def send_exit(w, signal, exit_price, won, pnl, acc):
    emoji    = "✅" if won else "❌"
    result   = "TAKE PROFIT" if won else "STOP LOSS"
    total_ret= round((acc["balance"]-START_BALANCE)/START_BALANCE*100,2)
    wr       = round(acc["wins"]/acc["total_trades"]*100,1) if acc["total_trades"]>0 else 0
    strategy = "EMA Cross" if w["engine"]=="ema_cross" else "BOS Pullback"

    tg(f"""{emoji} <b>TRADE CLOSED — {w['symbol']} {w['timeframe'].upper()}</b>

<b>Variant:</b>  {w['label']}
<b>Strategy:</b> {strategy}
<b>Result:</b>    {result}
<b>Direction:</b> {signal['direction']}

<b>Entry:</b>  ${signal['entry']}
<b>Exit:</b>   ${exit_price:.6f}
<b>P&L:</b>    {'+' if pnl>=0 else ''}${pnl:.4f}
<b>RR:</b>     {w['rr']}R {'✅' if won else '❌'}

<b>Previous balance:</b> ${round(acc['balance']-pnl,2):.2f}
<b>Current balance:</b>  ${acc['balance']:.2f}
<b>Total return:</b>     {'+' if total_ret>=0 else ''}{total_ret}%
<b>Max drawdown:</b>     -{acc['max_drawdown']:.2f}%

<b>All time:</b> {acc['total_trades']} trades · {acc['wins']}W {acc['losses']}L · {wr}% WR
⏰ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
📊 Paper trade v11""")

def send_daily_summary(open_signals):
    try:
        acc    = get_account()
        trades = get_today_trades()
        if not acc: return
        today_pnl  = sum(t.get("pnl",0) for t in trades)
        today_wins = sum(1 for t in trades if t.get("won"))
        today_loss = sum(1 for t in trades if not t.get("won"))
        total_ret  = round((acc["balance"]-START_BALANCE)/START_BALANCE*100,2)
        wr         = round(acc["wins"]/acc["total_trades"]*100,1) if acc["total_trades"]>0 else 0
        open_str   = ""
        if open_signals:
            open_str = "\n\n<b>Open positions:</b>"
            for key,sig in open_signals.items():
                open_str += f"\n• {sig['symbol']} {sig['timeframe'].upper()} {sig['direction']} @ ${sig['entry']}"
        tg(f"""📊 <b>DAILY REPORT — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}</b>

<b>Today:</b> {len(trades)} trades · {today_wins}W {today_loss}L
<b>P&L today:</b> {'+' if today_pnl>=0 else ''}${today_pnl:.4f}

<b>Account:</b>   ${acc['balance']:.2f}
<b>Total return:</b> {'+' if total_ret>=0 else ''}{total_ret}%
<b>Max drawdown:</b> -{acc['max_drawdown']:.2f}%

<b>All time:</b> {acc['total_trades']} trades · {acc['wins']}W {acc['losses']}L · {wr}% WR{open_str}""")
    except Exception as e: print(f"Daily summary error: {e}")

# ── BOS PULLBACK STATE MACHINE ─────────────────────────────────
def make_machine(side):
    return {"side":side,"state":0,
            "p1_idx":None,"p1_price":None,
            "p2":None,"p2_idx":None,
            "prev_p2":None,"anchor":None,
            "p3":None,"p3_bar":None,
            "ttl":None,"fib":None,"c_watch":None}

def reset_machine(m):
    m.update(state=0,p1_idx=None,p1_price=None,
             p2=None,p2_idx=None,anchor=None,
             p3=None,p3_bar=None,ttl=None,fib=None,c_watch=None)

def step_machine(m, ci, H, L, C, O, n, N,
                 conf_high, conf_low, fib_level, entry_mode, mac,
                 ema_f=None, ema_s=None, use_ema=False, adx_v=None, adx_thr=0.0):
    """
    Returns a signal dict if entry triggered, else None.
    Modifies m (state machine) and mac (macro trend) in place.
    """
    side = m["side"]
    ch=float(H[ci]); cl=float(L[ci])
    cc=float(C[ci]); co=float(O[ci])

    # New confirmed pivot — only update if not locked in state 2
    pv = conf_high.get(ci) if side=="bull" else conf_low.get(ci)
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

    # State 1 — float P2, check INVALID, hunt BOS
    if m["state"]==1:
        if side=="bull" and cl<m["p2"]: m["p2"]=cl; m["p2_idx"]=ci
        elif side=="bear" and ch>m["p2"]: m["p2"]=ch; m["p2_idx"]=ci
        if m["anchor"] is not None:
            if side=="bull" and cl<m["anchor"]: reset_machine(m); return None
            if side=="bear" and ch>m["anchor"]: reset_machine(m); return None
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

    # State 2 — KILLED / FAILED / EXPIRED / entry watch
    if m["state"]==2:
        # KILLED
        if (side=="bull" and mac["trend"]==-1) or (side=="bear" and mac["trend"]==1):
            reset_machine(m); return None
        # Float P3
        if side=="bull" and ch>=m["p3"]: m["p3"]=ch; m["p3_bar"]=ci
        elif side=="bear" and cl<=m["p3"]: m["p3"]=cl; m["p3_bar"]=ci
        # Redraw fib
        rng=(m["p3"]-m["p2"]) if side=="bull" else (m["p2"]-m["p3"])
        if rng>0:
            m["fib"]=m["p3"]-rng*fib_level if side=="bull" else m["p3"]+rng*fib_level
        # FAILED
        if side=="bull" and cl<m["p2"]: reset_machine(m); return None
        if side=="bear" and ch>m["p2"]: reset_machine(m); return None
        # EXPIRED
        if m["ttl"] and (ci-m["p3_bar"])>m["ttl"]: reset_machine(m); return None
        if m["fib"] is None: return None

        fib=m["fib"]; trig=False; ep=None

        if entry_mode=="rejection":
            if side=="bull" and cl<=fib and cc>fib and cc>co:
                trig=True; ep=float(O[min(ci+1,n-1)])
            elif side=="bear" and ch>=fib and cc<fib and cc<co:
                trig=True; ep=float(O[min(ci+1,n-1)])

        elif entry_mode=="reclaim":
            if m["c_watch"] is None:
                if side=="bull" and cc<fib: m["c_watch"]=ci
                elif side=="bear" and cc>fib: m["c_watch"]=ci
            else:
                if (ci-m["c_watch"])<=2:
                    if side=="bull" and cc>fib: trig=True; ep=cc; m["c_watch"]=None
                    elif side=="bear" and cc<fib: trig=True; ep=cc; m["c_watch"]=None
                else:
                    m["c_watch"]=None

        if not trig or ep is None: return None

        # EMA / ADX filter check (Pine Script aligned)
        if use_ema and ema_f is not None:
            if side=="bull" and ema_f[ci]<=ema_s[ci]: return None
            if side=="bear" and ema_f[ci]>=ema_s[ci]: return None
        if adx_v is not None and adx_v[ci]<adx_thr: return None

        # Build signal
        sl=m["p2"]*(1+STOP_BUF) if side=="bear" else m["p2"]*(1-STOP_BUF)
        rng2=(m["p3"]-m["p2"]) if side=="bull" else (m["p2"]-m["p3"])
        if rng2<=0 or rng2/max(min(m["p2"],m["p3"]),1)<MIN_SWING:
            return None

        sig = {
            "direction": "LONG" if side=="bull" else "SHORT",
            "entry":     round(ep,6),
            "sl":        round(sl,6),
            "p1":        round(m["p1_price"],6),
            "p2":        round(m["p2"],6),
            "p3":        round(m["p3"],6),
            "fib":       round(fib,6),
        }

        # Update macro trend
        mac["trend"]=1 if side=="bull" else -1
        reset_machine(m)
        return sig

    return None

def detect_signal_bos(candles, N, fib_level, entry_mode, rr, ema_pair="off", adx_min=0):
    """
    Run BOS Pullback state machine on candle history.
    Returns signal dict or None.
    Uses last confirmed candle (candles[-2]) as current.
    ema_pair: "off" or "fast/slow" e.g. "144/169" — only trade in EMA trend direction
    adx_min:  0 = off, else minimum ADX(14) to allow entry
    """
    if len(candles) < N*2+10: return None

    H=np.array([c[2] for c in candles],dtype=float)
    L=np.array([c[3] for c in candles],dtype=float)
    C=np.array([c[4] for c in candles],dtype=float)
    O=np.array([c[1] for c in candles],dtype=float)
    n=len(candles)

    use_ema = ema_pair!="off"
    ema_f=ema_s=adx_v=None
    if use_ema:
        f,s_=map(int,ema_pair.split("/"))
        ema_f=calc_ema(C,f); ema_s=calc_ema(C,s_)
    adx_thr=float(adx_min)
    if adx_min>0:
        adx_v=calc_adx(H,L,C,14)

    # Strict confirmed fractals (confirmed at bar i+N)
    conf_high,conf_low={},{}
    for i in range(N,n-N):
        if all(H[i]>H[i-N:i]) and all(H[i]>H[i+1:i+N+1]):
            conf_high[i+N]=(i,float(H[i]))
        if all(L[i]<L[i-N:i]) and all(L[i]<L[i+1:i+N+1]):
            conf_low[i+N]=(i,float(L[i]))

    mac  = {"trend":0,"ext":None,"ext_idx":None}
    bull = make_machine("bull")
    bear = make_machine("bear")
    last_signal = None

    for ci in range(n-1):  # stop at n-2 (last closed candle)
        # Update macro ext
        if mac["trend"]==1:
            if mac["ext"] is None or H[ci]>mac["ext"]: mac["ext"]=float(H[ci]); mac["ext_idx"]=ci
        elif mac["trend"]==-1:
            if mac["ext"] is None or L[ci]<mac["ext"]: mac["ext"]=float(L[ci]); mac["ext_idx"]=ci

        bs=step_machine(bull,ci,H,L,C,O,n,N,conf_high,conf_low,fib_level,entry_mode,mac,
                        ema_f,ema_s,use_ema,adx_v,adx_thr)
        be=step_machine(bear,ci,H,L,C,O,n,N,conf_high,conf_low,fib_level,entry_mode,mac,
                        ema_f,ema_s,use_ema,adx_v,adx_thr)

        if bs: last_signal=bs
        if be: last_signal=be

    # Only return signal from last confirmed candle (n-2)
    # Re-run on last confirmed candle to get live signal
    ci=n-2
    if mac["trend"]==1:
        if mac["ext"] is None or H[ci]>mac["ext"]: mac["ext"]=float(H[ci])
    elif mac["trend"]==-1:
        if mac["ext"] is None or L[ci]<mac["ext"]: mac["ext"]=float(L[ci])

    bs=step_machine(bull,ci,H,L,C,O,n,N,conf_high,conf_low,fib_level,entry_mode,mac)
    be=step_machine(bear,ci,H,L,C,O,n,N,conf_high,conf_low,fib_level,entry_mode,mac)

    sig = bs or be
    if sig:
        rpp=abs(sig["entry"]-sig["sl"])
        if rpp<=0: return None
        tp=sig["entry"]+rpp*rr if sig["direction"]=="LONG" else sig["entry"]-rpp*rr
        sig["tp"]=round(tp,6)
        sig["rr"]=rr
        return sig
    return None

# ── EMA CROSS SIGNAL DETECTION ─────────────────────────────────
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

def detect_signal_ema_cross(candles, ema_fast, ema_slow, rr,
                             use_vol, use_gap, use_htf, htf_mult=5):
    """
    EMA Cross — Engine 6
    Detects a cross on the last confirmed candle (n-2).
    Entry = open of the most recent candle (n-1), matching backtest "next open".
    SL = 3-bar swing extreme. TP = entry +/- RR * risk.
    """
    if len(candles) < ema_slow*2+10: return None

    H=np.array([c[2] for c in candles],dtype=float)
    L=np.array([c[3] for c in candles],dtype=float)
    C=np.array([c[4] for c in candles],dtype=float)
    O=np.array([c[1] for c in candles],dtype=float)
    V=np.array([c[5] if len(c)>5 else 0 for c in candles],dtype=float)
    n=len(candles)

    ef=calc_ema(C,ema_fast); es=calc_ema(C,ema_slow)
    hf=calc_ema(C,ema_fast*htf_mult) if use_htf else None
    hs=calc_ema(C,ema_slow*htf_mult) if use_htf else None

    vol_ma=np.zeros(n)
    for i in range(20,n): vol_ma[i]=np.mean(V[i-20:i])

    EMA_GAP_MIN=0.0005
    i=n-2  # last confirmed candle

    prev_bull=ef[i-1]>es[i-1]; curr_bull=ef[i]>es[i]
    cross_up=not prev_bull and curr_bull
    cross_dn=prev_bull and not curr_bull
    if not cross_up and not cross_dn: return None
    side="long" if cross_up else "short"

    if use_vol and vol_ma[i]>0:
        if V[i]<=vol_ma[i]: return None
    if use_gap:
        if abs(ef[i]-es[i])/C[i] < EMA_GAP_MIN: return None
    if use_htf and hf is not None:
        if side=="long"  and hf[i]<=hs[i]: return None
        if side=="short" and hf[i]>=hs[i]: return None

    ei=n-1  # entry on most recent candle's open
    ep=float(O[ei])
    sl=(float(np.min(L[max(0,i-2):i+1]))*(1-STOP_BUF) if side=="long"
        else float(np.max(H[max(0,i-2):i+1]))*(1+STOP_BUF))
    rpp=abs(ep-sl)
    if rpp<=0: return None
    tp=(ep+rpp*rr) if side=="long" else (ep-rpp*rr)

    return {
        "direction":"LONG" if side=="long" else "SHORT",
        "entry":round(ep,6),"sl":round(sl,6),"tp":round(tp,6),
        "rr":rr,"ema_fast":ema_fast,"ema_slow":ema_slow,
        "filters":"+".join(f for f,v in [("vol",use_vol),("gap",use_gap),("htf",use_htf)] if v) or "none",
    }

# ── MAIN LOOP ─────────────────────────────────────────────────
def run():
    print("🤖 Waddle Paper Trader v11 — Dual Engine starting...")
    acc = init_account()
    bos_str = "\n".join([f"• {w['label']}: {w['symbol']} {w['timeframe'].upper()} N={w['pivot_n']} {w['rr']}R {w['entry_mode']} fib={w['fib_level']}"
                          + (f" EMA{w['ema_pair']}" if w.get('ema_pair','off')!='off' else "")
                          + (f" ADX≥{w['adx_min']}" if w.get('adx_min',0)>0 else "")
                          for w in BOS_WATCHLIST])
    ema_str = "\n".join([f"• {w['label']}: {w['symbol']} {w['timeframe'].upper()} EMA{w['ema_fast']}/{w['ema_slow']} {w['rr']}R "
                          f"[{'+'.join(f for f,v in [('vol',w['use_vol']),('gap',w['use_gap']),('htf',w['use_htf'])] if v) or 'none'}]"
                          for w in EMA_WATCHLIST])
    tg(f"""🤖 <b>Waddle Paper Trader v11 LIVE</b>
<b>Dual Engine:</b> BOS Pullback (5 configs, 1 per pair — 18mo validated) + EMA Cross (3 configs)

<b>Account:</b> ${acc['balance']:.2f}
<b>Risk per trade:</b> {RISK_PCT*100:.0f}%

<b>BOS Pullback watchlist:</b>
{bos_str}

<b>EMA Cross watchlist:</b>
{ema_str}

📊 Each trade tagged with its variant label — query paper_trades grouped by 'label' for post-mortem comparison after ~1 month.""")

    exchange      = ccxt.kucoin({"enableRateLimit":True})
    open_signals  = {}
    last_scan     = {}
    last_signal   = {}
    last_daily    = 0
    last_heartbeat= 0
    consec_losses = 0

    while True:
        try:
            now     = time.time()
            now_utc = datetime.now(timezone.utc)
            now_str = now_utc.strftime("%H:%M:%S")

            # Circuit breaker
            if consec_losses >= MAX_CONSEC_LOSSES:
                tg(f"""🛑 <b>CIRCUIT BREAKER</b>
{consec_losses} consecutive losses. Bot stopped. Manual restart required.""")
                print(f"[{now_str}] Circuit breaker — {consec_losses} losses")
                break

            # Daily summary at 8AM UTC
            if now_utc.hour==8 and now_utc.minute<1 and now-last_daily>3600:
                send_daily_summary(open_signals)
                last_daily=now

            # Hourly heartbeat
            if now-last_heartbeat>3600:
                acc=init_account()
                total_ret=round((acc["balance"]-START_BALANCE)/START_BALANCE*100,2)
                open_str=f"\nOpen: {len(open_signals)}"+"".join([f"\n• {s['symbol']} {s['timeframe'].upper()} {s['direction']} @ ${s['entry']}" for s in open_signals.values()])
                tg(f"💓 <b>Bot Alive</b> — {now_utc.strftime('%H:%M UTC')}\nBalance: ${acc['balance']:.2f} ({'+' if total_ret>=0 else ''}{total_ret}%)\nScanning {len(ALL_WATCHLIST)} pairs (BOS:{len(BOS_WATCHLIST)} + EMA:{len(EMA_WATCHLIST)}){open_str}")
                last_heartbeat=now

            # Scan watchlist (both engines)
            for w in ALL_WATCHLIST:
                symbol    = w["symbol"]
                timeframe = w["timeframe"]
                engine    = w["engine"]
                key       = f"{symbol}_{timeframe}_{engine}"
                interval  = SCAN_INTERVALS.get(timeframe,60)

                if now-last_scan.get(key,0)<interval: continue
                last_scan[key]=now

                try:
                    candles=exchange.fetch_ohlcv(symbol,timeframe,limit=500)
                    if not candles or len(candles)<50:
                        print(f"[{now_str}] {symbol} {timeframe} — not enough candles")
                        continue

                    if engine=="ema_cross":
                        signal=detect_signal_ema_cross(
                            candles, w["ema_fast"], w["ema_slow"], w["rr"],
                            w["use_vol"], w["use_gap"], w["use_htf"]
                        )
                    else:
                        signal=detect_signal_bos(
                            candles, w["pivot_n"], w["fib_level"],
                            w["entry_mode"], w["rr"],
                            w.get("ema_pair","off"), w.get("adx_min",0)
                        )

                    if signal and key not in open_signals:
                        cooldown=interval*3
                        if now-last_signal.get(key,0)>cooldown:
                            acc=init_account()
                            send_entry(w,signal,acc)
                            last_signal[key]=now
                            open_signals[key]={
                                **signal,
                                "symbol":symbol,"timeframe":timeframe,
                                "label":w["label"],"engine":engine,"entry_time":now,
                                "entry_balance":acc["balance"]
                            }
                            print(f"[{now_str}] ✅ ENTRY ({engine}): {symbol} {timeframe} {signal['direction']} @ ${signal['entry']}")
                    else:
                        state=f"open" if key in open_signals else "no signal"
                        print(f"[{now_str}] {symbol} {timeframe} ({engine}) — {state}")

                    time.sleep(0.5)

                except Exception as e:
                    print(f"[{now_str}] Scan error {symbol} {timeframe} ({engine}): {e}")
                    time.sleep(2)

            # Check SL/TP on open positions
            closed=[]
            for key,sig in open_signals.items():
                try:
                    ohlcv=exchange.fetch_ohlcv(sig["symbol"],sig["timeframe"],limit=2)
                    if not ohlcv: continue
                    c=ohlcv[-1]; ch=c[2]; cl=c[3]
                    direction=sig["direction"]
                    won=False; hit=False; exit_price=None

                    if direction=="LONG":
                        if cl<=sig["sl"]:   won=False; hit=True; exit_price=sig["sl"]
                        elif ch>=sig["tp"]: won=True;  hit=True; exit_price=sig["tp"]
                    else:
                        if ch>=sig["sl"]:   won=False; hit=True; exit_price=sig["sl"]
                        elif cl<=sig["tp"]: won=True;  hit=True; exit_price=sig["tp"]

                    if hit:
                        acc       = init_account()
                        entry_bal = sig.get("entry_balance",acc["balance"])
                        risk_amt  = entry_bal*RISK_PCT
                        rpp       = abs(sig["entry"]-sig["sl"])
                        pos_size  = risk_amt/rpp if rpp>0 else 0
                        pnl       = round((exit_price-sig["entry"])*pos_size if direction=="LONG"
                                         else (sig["entry"]-exit_price)*pos_size, 4)
                        # Subtract fees (Bybit notional: 0.02% entry + 0.02% TP or 0.055% SL)
                        notional  = pos_size*sig["entry"]
                        fee       = notional*0.0002 + notional*(0.0002 if won else 0.00055)
                        pnl       = round(pnl-fee, 4)
                        new_bal   = round(acc["balance"]+pnl,4)
                        acc       = update_account(new_bal,won,pnl)

                        w_info = next((w for w in ALL_WATCHLIST
                                       if f"{w['symbol']}_{w['timeframe']}_{w['engine']}"==key), ALL_WATCHLIST[0])
                        log_trade({
                            "symbol":sig["symbol"],"timeframe":sig["timeframe"],
                            "direction":direction,"entry":sig["entry"],
                            "exit_price":exit_price,"sl":sig["sl"],"tp":sig["tp"],
                            "rr":sig["rr"],"pnl":pnl,"won":won,"balance":new_bal,
                            "label":sig["label"],"engine":sig["engine"],
                            "created_at":datetime.now(timezone.utc).isoformat(),
                        })

                        send_exit(w_info,sig,exit_price,won,pnl,acc)
                        closed.append(key)
                        print(f"[{now_str}] {'✅TP' if won else '❌SL'}: {sig['symbol']} {sig['timeframe']} PnL=${pnl}")

                        if won:
                            consec_losses=0
                        else:
                            consec_losses+=1
                            if consec_losses>=MAX_CONSEC_LOSSES:
                                tg(f"🚨 <b>WARNING</b> — {consec_losses} consecutive losses. Circuit breaker next loop.")

                except Exception as e:
                    print(f"[{now_str}] Exit check error {key}: {e}")

            for key in closed:
                del open_signals[key]

            time.sleep(30)

        except KeyboardInterrupt:
            print("Bot stopped"); break
        except Exception as e:
            print(f"[{now_str}] Main loop error: {e}")
            time.sleep(60)

if __name__=="__main__":
    run()
