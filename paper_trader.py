# ============================================================
# FIB PAPER TRADER v5
# 3 Core Strategies + 1M Speed Test pairs
# ============================================================

import ccxt
import numpy as np
import time
import os
import httpx
from datetime import datetime, timezone, timedelta

BYBIT_API_KEY    = os.environ.get("BYBIT_API_KEY", "")
BYBIT_SECRET     = os.environ.get("BYBIT_SECRET", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SUPABASE_URL     = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY     = os.environ.get("SUPABASE_KEY", "")

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}

SCAN_INTERVALS = {
    "1m":15,"5m":300,"15m":900,
    "1h":3600,"4h":14400,"1d":86400,
}
SIGNAL_COOLDOWNS = {
    "1m":60,"5m":300,"15m":900,
    "1h":3600,"4h":14400,"1d":86400,
}

START_BALANCE = 100.0
RISK_PCT      = 0.02
FIB_LEVEL     = 0.618

# ── WATCHLIST ─────────────────────────────────────────────
WATCHLIST = [
    {"symbol":"SOL/USDT","timeframe":"1m","pivot_n":3,"rr":1.5,"label":"⚡ SOL 1M"},
    {"symbol":"BTC/USDT","timeframe":"1m","pivot_n":3,"rr":2.0,"label":"⚡ BTC 1M"},
    {"symbol":"XRP/USDT","timeframe":"1m","pivot_n":5,"rr":1.5,"label":"⚡ XRP 1M"},
]

# ── SUPABASE ──────────────────────────────────────────────
def get_account():
    try:
        res = httpx.get(f"{SUPABASE_URL}/rest/v1/paper_account?id=eq.1&select=*", headers=SUPABASE_HEADERS, timeout=10)
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
        httpx.post(f"{SUPABASE_URL}/rest/v1/paper_account", json=row, headers=SUPABASE_HEADERS, timeout=10)
        return row
    except Exception as e:
        print(f"Init account error: {e}")
        return {"balance":START_BALANCE,"total_trades":0,"wins":0,"losses":0,"total_pnl":0.0,"peak_balance":START_BALANCE,"max_drawdown":0.0}

def update_account(balance, won, pnl):
    try:
        acc = get_account()
        if not acc: acc = init_account()
        peak   = max(acc["peak_balance"], balance)
        dd     = round((peak-balance)/peak*100, 2)
        max_dd = max(acc["max_drawdown"], dd)
        updates = {
            "balance":       round(balance,4),
            "total_trades":  acc["total_trades"]+1,
            "wins":          acc["wins"]+(1 if won else 0),
            "losses":        acc["losses"]+(0 if won else 1),
            "total_pnl":     round(acc["total_pnl"]+pnl,4),
            "peak_balance":  round(peak,4),
            "max_drawdown":  max_dd,
        }
        httpx.patch(f"{SUPABASE_URL}/rest/v1/paper_account?id=eq.1", json=updates, headers=SUPABASE_HEADERS, timeout=10)
        return {**acc, **updates}
    except Exception as e:
        print(f"Update account error: {e}")
        return None

def log_trade(trade_data):
    try:
        httpx.post(f"{SUPABASE_URL}/rest/v1/paper_trades", json=trade_data,
                   headers={**SUPABASE_HEADERS,"Prefer":"return=minimal"}, timeout=10)
    except Exception as e: print(f"Log trade error: {e}")

def get_today_trades():
    try:
        since = (datetime.now(timezone.utc)-timedelta(hours=24)).isoformat()
        res = httpx.get(f"{SUPABASE_URL}/rest/v1/paper_trades?created_at=gte.{since}&select=*&order=created_at.desc", headers=SUPABASE_HEADERS, timeout=10)
        if res.status_code==200: return res.json()
    except Exception as e: print(f"Get trades error: {e}")
    return []

# ── TELEGRAM ──────────────────────────────────────────────
def send_telegram(message):
    try:
        httpx.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                   json={"chat_id":TELEGRAM_CHAT_ID,"text":message,"parse_mode":"HTML"}, timeout=10)
    except Exception as e: print(f"Telegram error: {e}")

def send_entry(symbol, timeframe, signal, label, acc):
    balance   = acc["balance"]
    risk_amt  = round(balance*RISK_PCT, 4)
    risk_pp   = abs(signal["entry"]-signal["sl"])
    pos_size  = round(risk_amt/risk_pp, 6) if risk_pp>0 else 0
    pos_value = round(pos_size*signal["entry"], 2)
    sl_loss   = round(balance-risk_amt, 2)
    tp_gain   = round(balance+risk_amt*signal["rr"], 2)
    direction = signal["direction"]
    emoji     = "🟢" if direction=="LONG" else "🔴"
    arrow     = "📈" if direction=="LONG" else "📉"
    now_str   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total_ret = round((balance-START_BALANCE)/START_BALANCE*100, 2)
    wr        = round(acc["wins"]/acc["total_trades"]*100,1) if acc["total_trades"]>0 else 0

    send_telegram(f"""{emoji} <b>TRADE ENTERED — {symbol} {timeframe.upper()}</b> {arrow}
<b>{label}</b>

<b>Direction:</b> {direction}
<b>Structure:</b> {"LH→LL→LH" if signal["structure"]=="bear" else "HL→HH→HL"}

<b>Entry:</b>  ${signal["entry"]}
<b>SL:</b>     ${signal["sl"]} (-{abs(signal["entry"]-signal["sl"])/signal["entry"]*100:.2f}%)
<b>TP:</b>     ${signal["tp"]} (+{abs(signal["tp"]-signal["entry"])/signal["entry"]*100:.2f}%)
<b>RR:</b>     1:{signal["rr"]}R

<b>Account:</b>   ${balance:.2f}
<b>Risk:</b>      {RISK_PCT*100:.0f}% = ${risk_amt:.2f}
<b>Position:</b>  ${pos_value:.2f} worth of {symbol.split('/')[0]}
<b>If SL hit:</b> Balance → ${sl_loss:.2f} (-${risk_amt:.2f})
<b>If TP hit:</b> Balance → ${tp_gain:.2f} (+${round(risk_amt*signal["rr"],2):.2f})

<b>Stats:</b> {acc["total_trades"]} trades · {acc["wins"]}W {acc["losses"]}L · {wr}% WR
<b>Total return:</b> {"+'" if total_ret>=0 else ""}{total_ret}%
⏰ {now_str}
📊 <i>Paper trade</i>""")

def send_exit(symbol, timeframe, signal, exit_price, won, pnl, acc):
    emoji   = "✅" if won else "❌"
    result  = "TAKE PROFIT" if won else "STOP LOSS"
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total_ret = round((acc["balance"]-START_BALANCE)/START_BALANCE*100, 2)
    wr = round(acc["wins"]/acc["total_trades"]*100,1) if acc["total_trades"]>0 else 0

    send_telegram(f"""{emoji} <b>TRADE CLOSED — {symbol} {timeframe.upper()}</b>

<b>Result:</b> {result} {"✅" if won else "❌"}
<b>Direction:</b> {signal["direction"]}

<b>Entry:</b>  ${signal["entry"]}
<b>Exit:</b>   ${exit_price:.6f}
<b>P&L:</b>    {"+'" if pnl>=0 else ""}${pnl:.4f}
<b>RR:</b>     {signal["rr"]}R {"achieved ✅" if won else "missed ❌"}

<b>Previous balance:</b> ${round(acc["balance"]-pnl,2):.2f}
<b>Current balance:</b>  ${acc["balance"]:.2f}
<b>Total return:</b>     {"+'" if total_ret>=0 else ""}{total_ret}%
<b>Max drawdown:</b>     -{acc["max_drawdown"]:.2f}%

<b>All time:</b> {acc["total_trades"]} trades · {acc["wins"]}W {acc["losses"]}L · {wr}% WR
⏰ {now_str}
📊 <i>Paper trade</i>""")

def send_daily_summary(open_signals):
    try:
        acc    = get_account()
        trades = get_today_trades()
        if not acc: return
        today_pnl  = sum(t.get("pnl",0) for t in trades)
        today_wins = sum(1 for t in trades if t.get("won"))
        today_loss = sum(1 for t in trades if not t.get("won"))
        total_ret  = round((acc["balance"]-START_BALANCE)/START_BALANCE*100, 2)
        wr         = round(acc["wins"]/acc["total_trades"]*100,1) if acc["total_trades"]>0 else 0
        now_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        open_str   = ""
        if open_signals:
            open_str = "\n\n<b>Open positions:</b>"
            for key,sig in open_signals.items():
                open_str += f"\n• {sig.get('symbol','?')} {sig.get('timeframe','?').upper()} {sig['direction']} @ ${sig['entry']}"
        send_telegram(f"""📊 <b>DAILY REPORT — {now_str}</b>

<b>Today:</b> {len(trades)} trades · {today_wins}W {today_loss}L
<b>P&L today:</b> {"+'" if today_pnl>=0 else ""}${today_pnl:.4f}

<b>Account balance:</b> ${acc["balance"]:.2f}
<b>Total return:</b>    {"+'" if total_ret>=0 else ""}{total_ret}%
<b>Max drawdown:</b>    -{acc["max_drawdown"]:.2f}%

<b>All time:</b> {acc["total_trades"]} trades · {acc["wins"]}W {acc["losses"]}L · {wr}% WR{open_str}""")
    except Exception as e: print(f"Daily summary error: {e}")

# ── EXCHANGE ──────────────────────────────────────────────
def get_exchange():
    return ccxt.kucoin({"enableRateLimit":True})

def fetch_candles(exchange, symbol, timeframe, limit=300):
    return exchange.fetch_ohlcv(symbol, timeframe, limit=limit)

def find_pivots(candles, N):
    """Pivot detection — N candles left and right."""
    highs = np.array([c[2] for c in candles])
    lows  = np.array([c[3] for c in candles])
    closes= np.array([c[4] for c in candles])
    pivots = []
    for i in range(N, len(candles)-N):
        if highs[i] == max(highs[i-N:i+N+1]):
            pivots.append({"idx":i,"type":"H","price":float(highs[i]),"close":float(closes[i])})
        elif lows[i] == min(lows[i-N:i+N+1]):
            pivots.append({"idx":i,"type":"L","price":float(lows[i]),"close":float(closes[i])})
    deduped = []
    for p in pivots:
        if not deduped: deduped.append(p); continue
        last = deduped[-1]
        if last["type"]==p["type"]:
            if p["type"]=="H" and p["price"]>last["price"]: deduped[-1]=p
            elif p["type"]=="L" and p["price"]<last["price"]: deduped[-1]=p
        else: deduped.append(p)
    return deduped


def detect_signal(candles, pivots, rr):
    """
    BOS-based Fibonacci pullback signal detection.

    LONG:
      P1 = most recent confirmed pivot HIGH
      BOS = current candle close > P1 high (P3)
      P3 - P1 >= N_MIN candles (duration filter)
      P2 = min(close[P1:P3]) — lowest close between P1 and P3
      Range filter: (P3_close - P2) / P2 > 0.003
      Entry: current candle LOW <= fib618 AND close > fib618
      Enter next candle open
      SL = P2, TP = entry + (entry - SL) * RR

    SHORT: mirror logic
    """
    if len(candles) < 20 or len(pivots) < 1: return None

    highs  = np.array([c[2] for c in candles])
    lows   = np.array([c[3] for c in candles])
    closes = np.array([c[4] for c in candles])
    n      = len(candles)

    # BOS confirmation: candles[-2] (prev closed candle)
    bos_close = closes[-2]
    # Entry trigger: candles[-1] (current candle)
    c_high  = highs[-1]
    c_low   = lows[-1]
    c_close = closes[-1]
    N_MIN   = 3
    MIN_RANGE = 0.003

    for direction in ["bull", "bear"]:
        # Find most recent P1
        p1_candidates = [p for p in pivots if
                        (direction=="bull" and p["type"]=="H") or
                        (direction=="bear" and p["type"]=="L")]
        if not p1_candidates: continue
        p1 = p1_candidates[-1]
        p1_idx = p1["idx"]

        if direction == "bull":
            p1_price = p1["price"]
            # BOS: previous candle already closed above P1
            if bos_close <= p1_price: continue
            # Duration filter — BOS candle is n-2
            bos_idx = n - 2
            if bos_idx - p1_idx < N_MIN: continue
            # P2 = min close between P1 and BOS
            p2 = float(min(closes[p1_idx:bos_idx+1]))
            rng = bos_close - p2
            if rng <= 0 or rng/p2 < MIN_RANGE: continue
            fib618 = p2 + rng * FIB_LEVEL
            sl     = p2
            rpp    = abs(fib618 - sl)
            if rpp <= 0: continue
            tp     = fib618 + rpp * rr
            # Entry trigger: current candle wicks to fib618 and closes above
            if c_low <= fib618 and c_close > fib618:
                return {
                    "structure":"bull","direction":"LONG",
                    "entry":round(fib618,6),"sl":round(sl,6),"tp":round(tp,6),
                    "p1":round(p1_price,6),"p2":round(p2,6),
                    "current":round(c_close,6),"rr":rr
                }

        else:  # bear
            p1_price = p1["price"]
            # BOS: previous candle already closed below P1
            if bos_close >= p1_price: continue
            bos_idx = n - 2
            if bos_idx - p1_idx < N_MIN: continue
            # P2 = max close between P1 and BOS
            p2 = float(max(closes[p1_idx:bos_idx+1]))
            rng = p2 - bos_close
            if rng <= 0 or rng/p2 < MIN_RANGE: continue
            fib618 = p2 - rng * FIB_LEVEL
            sl     = p2
            rpp    = abs(fib618 - sl)
            if rpp <= 0: continue
            tp     = fib618 - rpp * rr
            # Entry trigger: current candle wicks to fib618 and closes below
            if c_high >= fib618 and c_close < fib618:
                return {
                    "structure":"bear","direction":"SHORT",
                    "entry":round(fib618,6),"sl":round(sl,6),"tp":round(tp,6),
                    "p1":round(p1_price,6),"p2":round(p2,6),
                    "current":round(c_close,6),"rr":rr
                }

    return None

# ── MAIN LOOP ─────────────────────────────────────────────
open_signals = {}  # key: symbol_timeframe (one per pair+tf combo)
pair_bias    = {}  # key: symbol_timeframe → "bull" | "bear" | None
tp_anchors   = {}  # key: symbol_timeframe → N=1 anchor tracking after TP

def run():
    global open_signals, pair_bias, tp_anchors
    print("🤖 Fib Paper Trader v5 starting...")
    acc = init_account()
    watchlist_str = "\n".join([f"• {w['symbol']} {w['timeframe'].upper()} N={w['pivot_n']} {w['rr']}R — {w['label']}" for w in WATCHLIST])
    send_telegram(f"""🤖 <b>Fib Paper Trader v5 LIVE</b>

<b>Account:</b> ${acc["balance"]:.2f}
<b>Risk per trade:</b> {RISK_PCT*100:.0f}%

<b>Watchlist:</b>
{watchlist_str}

📊 Paper trading — full journal active""")

    exchange    = get_exchange()
    last_signal = {}
    last_scan   = {}
    last_daily  = 0
    last_heartbeat = 0

    while True:
        try:
            now     = time.time()
            now_utc = datetime.now(timezone.utc)
            now_str = now_utc.strftime("%H:%M:%S")

            # Daily summary at 8AM UTC
            if now_utc.hour==8 and now_utc.minute<1 and now-last_daily>3600:
                send_daily_summary(open_signals)
                last_daily = now

            # Hourly heartbeat
            if now-last_heartbeat>3600:
                acc = init_account()
                total_ret = round((acc["balance"]-START_BALANCE)/START_BALANCE*100,2)
                open_str = f"\nOpen positions: {len(open_signals)}" + ("" if not open_signals else "\n"+"\n".join([f"• {s.get('symbol','?')} {s.get('timeframe','?').upper()} {s['direction']}" for s in open_signals.values()]))
                send_telegram(f"💓 <b>Bot Alive</b> — {now_utc.strftime('%H:%M UTC')}\nBalance: ${acc['balance']:.2f} ({'+' if total_ret>=0 else ''}{total_ret}%)\nScanning {len(WATCHLIST)} pairs{open_str}")
                last_heartbeat = now

            # Scan watchlist
            for watch in WATCHLIST:
                symbol    = watch["symbol"]
                timeframe = watch["timeframe"]
                pivot_n   = watch["pivot_n"]
                rr        = watch["rr"]
                label     = watch["label"]
                key            = f"{symbol}_{timeframe}"  # one per pair+timeframe
                scan_interval  = SCAN_INTERVALS.get(timeframe, 60)
                sig_cooldown   = SIGNAL_COOLDOWNS.get(timeframe, 1800)

                if now-last_scan.get(key,0)<scan_interval: continue
                last_scan[key]=now

                try:
                    candles = fetch_candles(exchange, symbol, timeframe, limit=300)
                    if not candles or len(candles)<50: continue

                    pivots = find_pivots(candles, pivot_n)
                    signal = detect_signal(candles, pivots, rr)

                    # N=1 anchor tracking after TP — use candle high/low
                    if key in tp_anchors and key not in open_signals:
                        anchor = tp_anchors[key]
                        try:
                            ohlcv_anchor = exchange.fetch_ohlcv(symbol, timeframe, limit=2)
                            if ohlcv_anchor:
                                c = ohlcv_anchor[-1]
                                c_h = c[2]; c_l = c[3]
                                anchor["candles_since"] = anchor.get("candles_since", 0) + 1
                                if anchor["direction"] == "bull":
                                    if c_h > anchor["candidate"]:
                                        anchor["candidate"] = c_h  # new HH candidate
                                    else:
                                        # N=1 confirmed — HH locked
                                        new_p3  = anchor["candidate"]
                                        new_p2  = anchor["from_price"]
                                        rng     = new_p3 - new_p2
                                        MIN_R   = 0.003
                                        if rng > 0 and rng / max(new_p2, 1) >= MIN_R:
                                            new_fib = new_p2 + rng * FIB_LEVEL
                                            # Store as pending fib for next detect_signal
                                            tp_anchors[key]["confirmed"] = True
                                            tp_anchors[key]["fib618"]    = new_fib
                                            tp_anchors[key]["sl"]        = new_p2
                                            print(f"[{now_str}] N=1 HH confirmed @ ${new_p3:.4f} fib618=${new_fib:.4f}")
                                        del tp_anchors[key]
                                else:
                                    if c_l < anchor["candidate"]:
                                        anchor["candidate"] = c_l  # new LL candidate
                                    else:
                                        # N=1 confirmed — LL locked
                                        new_p3  = anchor["candidate"]
                                        new_p2  = anchor["from_price"]
                                        rng     = new_p2 - new_p3
                                        MIN_R   = 0.003
                                        if rng > 0 and rng / max(new_p2, 1) >= MIN_R:
                                            new_fib = new_p2 - rng * FIB_LEVEL
                                            tp_anchors[key]["confirmed"] = True
                                            tp_anchors[key]["fib618"]    = new_fib
                                            tp_anchors[key]["sl"]        = new_p2
                                            print(f"[{now_str}] N=1 LL confirmed @ ${new_p3:.4f} fib618=${new_fib:.4f}")
                                        del tp_anchors[key]
                                if anchor.get("candles_since", 0) >= 50:
                                    if key in tp_anchors: del tp_anchors[key]
                        except Exception as e:
                            print(f"[{now_str}] Anchor error {key}: {e}")

                    # Bias filter — skip if direction conflicts with current bias
                    current_bias = pair_bias.get(key)
                    signal_dir   = signal["structure"] if signal else None
                    if signal and current_bias and current_bias != signal_dir:
                        print(f"[{now_str}] Bias skip: {symbol} {timeframe} signal={signal_dir} bias={current_bias}")
                        signal = None

                    if signal and key not in open_signals:
                        if now-last_signal.get(key,0)>sig_cooldown:
                            acc = init_account()
                            send_entry(symbol, timeframe, signal, label, acc)
                            last_signal[key]=now
                            open_signals[key]={**signal,"symbol":symbol,"timeframe":timeframe,"label":label,"entry_time":now,"entry_balance":acc["balance"]}
                            print(f"[{now_str}] ✅ ENTRY: {symbol} {timeframe} {signal['direction']} {label}")
                    else:
                        print(f"[{now_str}] No signal: {symbol} {timeframe} N={pivot_n} {rr}R")

                    time.sleep(0.3)

                except Exception as e:
                    print(f"[{now_str}] Error {symbol} {timeframe}: {e}")
                    time.sleep(2)

            # Check SL/TP on open signals — using candle high/low (catches wicks)
            closed=[]
            for key,sig in open_signals.items():
                try:
                    ohlcv     = exchange.fetch_ohlcv(sig["symbol"], sig["timeframe"], limit=2)
                    if not ohlcv: continue
                    candle    = ohlcv[-1]
                    c_high    = candle[2]
                    c_low     = candle[3]
                    direction = sig["direction"]
                    won=False; hit=False; exit_price=None

                    if direction=="LONG":
                        if c_low<=sig["sl"]:   won=False; hit=True; exit_price=sig["sl"]
                        elif c_high>=sig["tp"]: won=True;  hit=True; exit_price=sig["tp"]
                    else:
                        if c_high>=sig["sl"]:  won=False; hit=True; exit_price=sig["sl"]
                        elif c_low<=sig["tp"]:  won=True;  hit=True; exit_price=sig["tp"]

                    if hit:
                        acc          = init_account()
                        entry_bal    = sig.get("entry_balance", acc["balance"])
                        risk_amt     = entry_bal * RISK_PCT
                        risk_pp      = abs(sig["entry"]-sig["sl"])
                        pos_size     = risk_amt/risk_pp if risk_pp>0 else 0
                        pnl          = round((sig["entry"]-exit_price)*pos_size if direction=="SHORT" else (exit_price-sig["entry"])*pos_size, 4)
                        new_bal      = round(acc["balance"]+pnl, 4)
                        acc          = update_account(new_bal, won, pnl)

                        log_trade({
                            "symbol":sig["symbol"],"timeframe":sig["timeframe"],
                            "direction":direction,"entry":sig["entry"],
                            "exit_price":exit_price,"sl":sig["sl"],"tp":sig["tp"],
                            "rr":sig["rr"],"pnl":pnl,"won":won,"balance":new_bal,
                            "label":sig["label"],
                            "created_at":datetime.now(timezone.utc).isoformat(),
                        })

                        send_exit(sig["symbol"],sig["timeframe"],sig,exit_price,won,pnl,acc)
                        closed.append(key)
                        print(f"[{now_str}] {'✅TP' if won else '❌SL'}: {sig['symbol']} {sig['timeframe']} PnL=${pnl}")

                        # After TP: set N=1 anchor state so next scan finds next HH/LL
                        if won:
                            pair_bias[key] = None  # reset bias — stay same direction
                            # Use P2 of the completed trade as anchor base
                            # For bull: track new HH from TP level upward
                            # For bear: track new LL from TP level downward
                            anchor_price = exit_price
                            tp_anchors[key] = {
                                "direction": sig["structure"],  # bull or bear
                                "from_price": sig["p2"] if "p2" in sig else anchor_price,
                                "candidate":  anchor_price,
                                "candles_since": 0
                            }
                            print(f"[{now_str}] TP hit — watching for next N=1 anchor from ${anchor_price:.4f}: {sig['symbol']} {sig['timeframe']}")
                        else:
                            # SL hit: flip bias
                            flipped = "bull" if sig["structure"]=="bear" else "bear"
                            pair_bias[key] = flipped
                            if key in tp_anchors: del tp_anchors[key]
                            print(f"[{now_str}] SL hit — bias flipped to {flipped}: {sig['symbol']} {sig['timeframe']}")

                except Exception as e:
                    print(f"Check error {key}: {e}")

            for key in closed:
                del open_signals[key]

            time.sleep(30)

        except KeyboardInterrupt:
            print("Bot stopped")
            break
        except Exception as e:
            print(f"Main loop error: {e}")
            time.sleep(60)

if __name__=="__main__":
    run()
