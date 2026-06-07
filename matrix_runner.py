#!/usr/bin/env python3
# ============================================================
# WADDLE MATRIX RUNNER v4
# Calls the /backtest API endpoint for each combo
# Saves results to Supabase matrix_results table
# ============================================================

import os, time, httpx, itertools, io, csv as csv_mod
from datetime import datetime, timezone

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SUPABASE_URL     = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY     = os.environ.get("SUPABASE_KEY", "")
API_URL          = os.environ.get("BACKTEST_API_URL", "http://localhost:8080")

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

TOTAL_COMBOS = (len(PAIRS)*len(TIMEFRAMES)*len(ENGINES)*len(ENTRY_MODES)*
                len(PIVOT_NS)*len(RR_RATIOS)*len(FIB_LEVELS)*len(EMA_PAIRS)*len(ADX_MINS))

def tg(msg):
    try:
        httpx.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"HTML"},
            timeout=10
        )
    except: pass

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

def parse_ema(ep):
    if ep=="off": return False,34,55
    f,s=ep.split("/"); return True,int(f),int(s)

def get_done_keys():
    try:
        all_keys=set()
        offset=0
        while True:
            res=httpx.get(
                f"{SUPABASE_URL}/rest/v1/matrix_results?select=combo_key&limit=10000&offset={offset}",
                headers={**HEADERS,"Prefer":""},timeout=30
            )
            if res.status_code==200:
                rows=res.json()
                if not rows: break
                all_keys.update(r["combo_key"] for r in rows)
                if len(rows)<10000: break
                offset+=10000
            else: break
        return all_keys
    except Exception as e:
        print(f"Done keys error: {e}")
        return set()

def save_batch(rows):
    if not rows: return
    try:
        res=httpx.post(
            f"{SUPABASE_URL}/rest/v1/matrix_results",
            json=rows,headers=HEADERS,timeout=30
        )
        if res.status_code not in [200,201,204]:
            print(f"Save error {res.status_code}: {res.text[:200]}")
    except Exception as e:
        print(f"Save error: {e}")

BATCH_SIZE = 20  # combos per API batch call

def run_batch_api(combos):
    """Call /batch API with multiple combos at once."""
    configs = []
    for combo in combos:
        sym,tf,eng,em,pn,rr,fib,ep,ax = combo
        use_ema,ema_fast,ema_slow = parse_ema(ep)
        configs.append({
            "symbol":sym,"timeframe":tf,"engine":eng,"entry_mode":em,
            "pivot_n":pn,"rr":rr,"fib_level":fib,
            "start_date":PERIOD_START,"end_date":PERIOD_END,
            "risk_method":"fixed","risk_pct":0.02,
            "use_ema_filter":use_ema,"ema_fast":ema_fast,"ema_slow":ema_slow,
            "adx_period":14,"adx_threshold":float(ax),
            "max_bars":500,"max_hold":500,
        })
    try:
        res = httpx.post(
            f"{API_URL}/batch",
            json={"configs":configs},
            timeout=300
        )
        if res.status_code==200:
            return res.json().get("results",[])
    except Exception as e:
        print(f"Batch API error: {e}")
    return [None]*len(combos)

def main_compute():
    # Clear old results
    try:
        httpx.delete(f"{SUPABASE_URL}/rest/v1/matrix_results?id=gt.0",
                     headers=HEADERS,timeout=30)
        print("Cleared old results")
    except: pass

    tg(f"""🔢 <b>Matrix Runner v4 Started</b>
Total combos: {TOTAL_COMBOS:,}
Pairs: {', '.join(p.replace('/USDT','') for p in PAIRS)}
TFs: {', '.join(TIMEFRAMES)}
Engines: {', '.join(ENGINES)}
EMA pairs: {', '.join(EMA_PAIRS)}
ADX: {ADX_MINS}
Period: {PERIOD_START} → {PERIOD_END}
Will update every 10 minutes.""")

    done_keys  = get_done_keys()
    completed  = len(done_keys)
    saved      = 0
    errors     = 0
    start_time = time.time()
    last_tg    = time.time()
    save_buf   = []

    all_combos = list(itertools.product(
        PAIRS,TIMEFRAMES,ENGINES,ENTRY_MODES,PIVOT_NS,RR_RATIOS,FIB_LEVELS,EMA_PAIRS,ADX_MINS
    ))

    print(f"Total: {len(all_combos):,} | Already done: {completed:,}")

    # Filter out done combos
    pending = [c for c in all_combos
               if f"{c[0]}|{c[1]}|{c[2]}|{c[3]}|{c[4]}|{c[5]}|{c[6]}|{c[7]}|{c[8]}" not in done_keys]
    print(f"Pending: {len(pending):,}")

    # Run in batches
    for i in range(0, len(pending), BATCH_SIZE):
        batch   = pending[i:i+BATCH_SIZE]
        results = run_batch_api(batch)

        for combo, result in zip(batch, results):
            sym,tf,eng,em,pn,rr,fib,ep,ax = combo
            key = f"{sym}|{tf}|{eng}|{em}|{pn}|{rr}|{fib}|{ep}|{ax}"
            s   = result.get("stats") if result and result.get("success") else None
            row = {
                "combo_key":    key,
                "pair":         sym.replace("/USDT",""),
                "timeframe":    tf,"engine":eng,"entry_mode":em,
                "pivot_n":      pn,"rr":rr,"fib_level":fib,
                "ema_pair":     ep,"adx_min":ax,
                "period_start": PERIOD_START,"period_end":PERIOD_END,
                "success":      s is not None,
                "return_pct":   s["total_return"]    if s else None,
                "cagr":         s["cagr"]             if s else None,
                "max_dd":       s["max_drawdown"]     if s else None,
                "sharpe":       s["sharpe"]           if s else None,
                "profit_factor":s["profit_factor"]    if s else None,
                "win_rate":     s["win_rate"]         if s else None,
                "trades":       s["total_trades"]     if s else 0,
                "wins":         s.get("wins",0)       if s else 0,
                "losses":       s.get("losses",0)     if s else 0,
                "avg_win":      s["avg_win"]          if s else None,
                "avg_loss":     s["avg_loss"]         if s else None,
                "kelly_full":   s["kelly_full"]       if s else None,
                "total_fees":   s.get("total_fees",0) if s else None,
                "computed_at":  datetime.now(timezone.utc).isoformat(),
            }
            save_buf.append(row)
            if s: saved+=1
            else: errors+=1
            completed+=1

        # Save every 200
        if len(save_buf)>=200:
            save_batch(save_buf)
            save_buf=[]

        # Telegram every 10 min
        if time.time()-last_tg>600:
            elapsed=time.time()-start_time
            rate=completed/elapsed if elapsed>0 else 0
            eta=(len(pending)-completed)/rate if rate>0 else 0
            pct=completed/len(pending)*100
            cur=pending[i][0] if i<len(pending) else ""
            tg(f"⏳ <b>Matrix Progress</b>\n"
               f"Done: {completed:,}/{len(pending):,} ({pct:.1f}%)\n"
               f"Saved: {saved:,} | Errors: {errors:,}\n"
               f"Rate: {rate:.1f}/s | ETA: {eta/3600:.1f}h\n"
               f"Current: {cur}")
            update_status("compute","running",completed,len(pending),cur)
            last_tg=time.time()

    # Final save
    if save_buf:
        save_batch(save_buf)

    elapsed=time.time()-start_time
    update_status("compute","done",TOTAL_COMBOS,TOTAL_COMBOS,"")
    tg(f"""✅ <b>Matrix Complete</b>
Combos: {TOTAL_COMBOS:,}
Saved: {saved:,} | Errors: {errors:,}
Time: {elapsed/3600:.2f}h
Download from Runner tab.""")
    print(f"Done. Saved {saved:,} in {elapsed/3600:.2f}h")

def main_prefetch():
    """Prefetch candles via API."""
    import ccxt
    tg("📥 <b>Prefetch Started</b>")
    ex = ccxt.kucoin({"enableRateLimit":True})
    start_ms=int(datetime.strptime(PERIOD_START,"%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
    end_ms  =int(datetime.strptime(PERIOD_END,  "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000)
    save_h  ={**HEADERS,"Prefer":"return=minimal,resolution=ignore-duplicates"}
    done=0
    for sym,tf in itertools.product(PAIRS,TIMEFRAMES):
        print(f"Fetching {sym} {tf}...")
        candles=[]; since=start_ms; empty=0
        while since<end_ms:
            try:
                batch=ex.fetch_ohlcv(sym,tf,since=since,limit=1000)
                if not batch: empty+=1;
                if empty>=3: break
                filtered=[c for c in batch if c[0]<end_ms]
                candles+=filtered
                if batch[-1][0]>=end_ms: break
                since=batch[-1][0]+1
                time.sleep(0.3)
            except Exception as e:
                print(f"Error: {e}"); time.sleep(5)
        if candles:
            for i in range(0,len(candles),500):
                batch=candles[i:i+500]
                rows=[{"symbol":sym,"timeframe":tf,"ts":c[0],"open":c[1],
                       "high":c[2],"low":c[3],"close":c[4],"volume":c[5] if len(c)>5 else 0}
                      for c in batch]
                httpx.post(f"{SUPABASE_URL}/rest/v1/candles",json=rows,headers=save_h,timeout=30)
            print(f"  Saved {len(candles)} candles")
        done+=1
    tg(f"✅ <b>Prefetch Done</b> — {done} pair+TF combinations cached")

def main():
    main_compute()

if __name__=="__main__":
    import sys
    if len(sys.argv)>1 and sys.argv[1]=="prefetch":
        main_prefetch()
    else:
        main_compute()
