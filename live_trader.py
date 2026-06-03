# ============================================================
# FIB LIVE TRADER v1
# Real execution on Bybit — based on Paper Trader v5
# KuCoin for data, Bybit for execution
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
    "1m":60,"5m":300,"15m":900,
    "1h":3600,"4h":14400,"1d":86400,
}

RISK_PCT  = 0.02   # 2% risk per trade
FIB_LEVEL = 0.618
MODE      = "LIVE"  # change to "PAPER" to revert to paper mode

# ── WATCHLIST — Single primary strategy ────
WATCHLIST = [
    {"symbol":"SOL/USDT","timeframe":"1m","pivot_n":3,"rr":1.5,"label":"⚡ SOL 1M — Primary"},
]

# ── SUPABASE ──────────────────────────────────────────────
def get_account():
    try:
        res = httpx.get(f"{SUPABASE_URL}/rest/v1/live_account?id=eq.1&select=*", headers=SUPABASE_HEADERS, timeout=10)
        if res.status_code==200 and res.json(): return res.json()[0]
    except Exception as e: print(f"Get account error: {e}")
    return None

def init_account(start_balance):
    try:
        existing = get_account()
        if existing: return existing
        row = {"id":1,"balance":start_balance,"total_trades":0,"wins":0,"losses":0,
               "total_pnl":0.0,"peak_balance":start_balance,"max_drawdown":0.0,
               "start_balance":start_balance,
               "created_at":datetime.now(timezone.utc).isoformat()}
        httpx.post(f"{SUPABASE_URL}/rest/v1/live_account", json=row, headers=SUPABASE_HEADERS, timeout=10)
        return row
    except Exception as e:
        print(f"Init account error: {e}")
        return {"balance":start_balance,"total_trades":0,"wins":0,"losses":0,
                "total_pnl":0.0,"peak_balance":start_balance,"max_drawdown":0.0,"start_balance":start_balance}

def update_account(balance, won, pnl):
    try:
        acc = get_account()
        if not acc: return None
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
        httpx.patch(f"{SUPABASE_URL}/rest/v1/live_account?id=eq.1", json=updates, headers=SUPABASE_HEADERS, timeout=10)
        return {**acc, **updates}
    except Exception as e:
        print(f"Update account error: {e}")
        return None

def log_trade(trade_data):
    try:
        httpx.post(f"{SUPABASE_URL}/rest/v1/live_trades", json=trade_data,
                   headers={**SUPABASE_HEADERS,"Prefer":"return=minimal"}, timeout=10)
    except Exception as e: print(f"Log trade error: {e}")

def get_today_trades():
    try:
        since = (datetime.now(timezone.utc)-timedelta(hours=24)).isoformat()
        res = httpx.get(f"{SUPABASE_URL}/rest/v1/live_trades?created_at=gte.{since}&select=*&order=created_at.desc", headers=SUPABASE_HEADERS, timeout=10)
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
    start_bal = acc.get("start_balance", balance)
    total_ret = round((balance-start_bal)/start_bal*100, 2) if start_bal>0 else 0
    wr        = round(acc["wins"]/acc["total_trades"]*100,1) if acc["total_trades"]>0 else 0

    send_telegram(f"""{emoji} <b>🔴 LIVE ORDER PLACED — {symbol} {timeframe.upper()}</b> {arrow}
<b>{label}</b>

<b>Direction:</b> {direction}
<b>Structure:</b> {"LH→LL→LH" if signal["structure"]=="bear" else "HL→HH→HL"}

<b>Entry:</b>  ${signal["entry"]}
<b>SL:</b>     ${signal["sl"]} (-{abs(signal["entry"]-signal["sl"])/signal["entry"]*100:.2f}%)
<b>TP:</b>     ${signal["tp"]} (+{abs(signal["tp"]-signal["entry"])/signal["entry"]*100:.2f}%)
<b>RR:</b>     1:{signal["rr"]}R

<b>Account:</b>   ${balance:.2f}
<b>Risk:</b>      {RISK_PCT*100:.0f}% = ${risk_amt:.2f}
<b>Position:</b>  {pos_size} {symbol.split('/')[0]} (${pos_value:.2f})
<b>If SL hit:</b> Balance → ${sl_loss:.2f} (-${risk_amt:.2f})
<b>If TP hit:</b> Balance → ${tp_gain:.2f} (+${round(risk_amt*signal["rr"],2):.2f})

<b>Stats:</b> {acc["total_trades"]} trades · {acc["wins"]}W {acc["losses"]}L · {wr}% WR
<b>Total return:</b> {"+'" if total_ret>=0 else ""}{total_ret}%
⏰ {now_str}
💰 <b>LIVE TRADE — Real money</b>""")

def send_exit(symbol, timeframe, signal, exit_price, won, pnl, acc):
    emoji   = "✅" if won else "❌"
    result  = "TAKE PROFIT" if won else "STOP LOSS"
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    start_bal = acc.get("start_balance", acc["balance"])
    total_ret = round((acc["balance"]-start_bal)/start_bal*100, 2) if start_bal>0 else 0
    wr = round(acc["wins"]/acc["total_trades"]*100,1) if acc["total_trades"]>0 else 0

    send_telegram(f"""{emoji} <b>🔴 LIVE TRADE CLOSED — {symbol} {timeframe.upper()}</b>

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
💰 <b>LIVE TRADE — Real money</b>""")

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
def get_data_exchange():
    return ccxt.kucoin({"enableRateLimit": True})

def get_bybit():
    return ccxt.bybit({
        "apiKey":    BYBIT_API_KEY,
        "secret":    BYBIT_SECRET,
        "enableRateLimit": True,
        "options":   {"defaultType": "spot"},
    })

def fetch_candles(exchange, symbol, timeframe, limit=300):
    return exchange.fetch_ohlcv(symbol, timeframe, limit=limit)

def get_bybit_balance(bybit):
    """Get real USDT balance from Bybit"""
    try:
        bal = bybit.fetch_balance()
        return float(bal["USDT"]["free"])
    except Exception as e:
        print(f"Balance fetch error: {e}")
        return None

def place_limit_order(bybit, symbol, direction, entry, sl, tp, risk_pct, balance):
    """Place limit order on Bybit with SL and TP"""
    try:
        risk_amt = balance * risk_pct
        risk_pp  = abs(entry - sl)
        if risk_pp <= 0: return None
        qty = round(risk_amt / risk_pp, 6)

        side = "buy" if direction == "LONG" else "sell"

        # Place limit entry order
        order = bybit.create_order(
            symbol=symbol,
            type="limit",
            side=side,
            amount=qty,
            price=entry,
            params={
                "stopLoss":   {"triggerPrice": sl,  "type": "limit", "price": sl},
                "takeProfit": {"triggerPrice": tp,  "type": "limit", "price": tp},
            }
        )
        print(f"Order placed: {direction} {symbol} qty={qty} entry={entry} sl={sl} tp={tp}")
        return order
    except Exception as e:
        print(f"Order placement error: {e}")
        send_telegram(f"⚠️ <b>ORDER FAILED</b> — {symbol} {direction}\nError: {e}")
        return None

def cancel_order(bybit, symbol, order_id):
    """Cancel an open limit order"""
    try:
        bybit.cancel_order(order_id, symbol)
        print(f"Order cancelled: {order_id}")
    except Exception as e:
        print(f"Cancel error: {e}")

def check_order_status(bybit, symbol, order_id):
    """Check if limit order filled, cancelled, or still open"""
    try:
        order = bybit.fetch_order(order_id, symbol)
        return order["status"]  # 'open', 'closed', 'canceled'
    except Exception as e:
        print(f"Order status error: {e}")
        return None

def emergency_stop(bybit, open_signals):
    """Cancel all open orders and alert on Bybit auth failure"""
    send_telegram("🚨 <b>EMERGENCY STOP</b> — Bybit auth error detected\nCancelling all open orders...")
    for key, sig in open_signals.items():
        if sig.get("order_id"):
            cancel_order(bybit, sig["symbol"], sig["order_id"])
    open_signals.clear()

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
            # BOS: current close > P1 high
            if c_close <= p1_price: continue
            # Duration filter
            bos_idx = n - 1
            if bos_idx - p1_idx < N_MIN: continue
            # P2 = min close between P1 and BOS
            p2 = float(min(closes[p1_idx:bos_idx+1]))
            rng = c_close - p2
            if rng <= 0 or rng/p2 < MIN_RANGE: continue
            fib618 = p2 + rng * FIB_LEVEL
            sl     = p2
            rpp    = abs(fib618 - sl)
            if rpp <= 0: continue
            tp     = fib618 + rpp * rr
            # Entry trigger: wick touches fib618, closes above
            if c_low <= fib618 and c_close > fib618:
                return {
                    "structure":"bull","direction":"LONG",
                    "entry":round(fib618,6),"sl":round(sl,6),"tp":round(tp,6),
                    "p1":round(p1_price,6),"p2":round(p2,6),
                    "current":round(c_close,6),"rr":rr
                }

        else:  # bear
            p1_price = p1["price"]
            # BOS: current close < P1 low
            if c_close >= p1_price: continue
            bos_idx = n - 1
            if bos_idx - p1_idx < N_MIN: continue
            # P2 = max close between P1 and BOS
            p2 = float(max(closes[p1_idx:bos_idx+1]))
            rng = p2 - c_close
            if rng <= 0 or rng/p2 < MIN_RANGE: continue
            fib618 = p2 - rng * FIB_LEVEL
            sl     = p2
            rpp    = abs(fib618 - sl)
            if rpp <= 0: continue
            tp     = fib618 - rpp * rr
            # Entry trigger: wick touches fib618, closes below
            if c_high >= fib618 and c_close < fib618:
                return {
                    "structure":"bear","direction":"SHORT",
                    "entry":round(fib618,6),"sl":round(sl,6),"tp":round(tp,6),
                    "p1":round(p1_price,6),"p2":round(p2,6),
                    "current":round(c_close,6),"rr":rr
                }

    return None
    p1,p2,p3=pivots[-3],pivots[-2],pivots[-1]
    current=candles[-1][4]
    n=len(candles)

    structure=None
    if p1["type"]=="H" and p2["type"]=="L" and p3["type"]=="H" and p3["price"]<p1["price"]: structure="bear"
    elif p1["type"]=="L" and p2["type"]=="H" and p3["type"]=="L" and p3["price"]>p1["price"]: structure="bull"
    if not structure: return None

    # Recency check — p3 must be within last 50 candles
    if n-p3["idx"]>50: return None

    fh=p1["price"] if structure=="bear" else p2["price"]
    fl=p2["price"] if structure=="bear" else p1["price"]
    rng=fh-fl
    if rng<=0: return None

    fib618=fl+rng*FIB_LEVEL if structure=="bear" else fh-rng*FIB_LEVEL
    sl=fh+rng*0.02 if structure=="bear" else fl-rng*0.02
    rpp=abs(fib618-sl)
    if rpp<=0: return None
    tp=fib618-rpp*rr if structure=="bear" else fib618+rpp*rr

    # Structure invalidation check
    if structure=="bear" and current>fh: return None
    if structure=="bull" and current<fl: return None

    zone_pct=abs(current-fib618)/fib618*100
    if zone_pct<=0.5:
        return {
            "structure":structure,"direction":"SHORT" if structure=="bear" else "LONG",
            "entry":round(fib618,6),"sl":round(sl,6),"tp":round(tp,6),
            "current":round(current,6),"rr":rr,"zone_pct":round(zone_pct,3)
        }
    return None

# ── MAIN LOOP ─────────────────────────────────────────────
open_signals = {}  # key: symbol_timeframe
pair_bias    = {}  # key: symbol_timeframe → "bull" | "bear" | None
tp_anchors   = {}  # key: symbol_timeframe → N=1 anchor tracking after TP

def run():
    global open_signals, pair_bias, tp_anchors
    print("💰 Fib Live Trader v1 starting...")

    data_exchange = get_data_exchange()
    bybit         = get_bybit()

    # Get real balance from Bybit
    real_balance = get_bybit_balance(bybit)
    if not real_balance:
        send_telegram("🚨 <b>STARTUP FAILED</b> — Cannot fetch Bybit balance. Check API keys.")
        return

    print(f"Bybit balance: ${real_balance:.2f} USDT")

    acc = init_account(real_balance)

    watchlist_str = "\n".join([f"• {w['symbol']} {w['timeframe'].upper()} N={w['pivot_n']} {w['rr']}R — {w['label']}" for w in WATCHLIST])
    send_telegram(f"""💰 <b>Fib Live Trader v1 STARTED</b>

<b>Bybit Balance:</b> ${real_balance:.2f} USDT
<b>Risk per trade:</b> {RISK_PCT*100:.0f}%

<b>Watchlist:</b>
{watchlist_str}

🔴 <b>LIVE MODE — Real money</b>""")

    last_signal    = {}
    last_scan      = {}
    last_daily     = 0
    last_heartbeat = 0
    bybit_fail_count = 0

    while True:
        try:
            now     = time.time()
            now_utc = datetime.now(timezone.utc)
            now_str = now_utc.strftime("%H:%M:%S")

            # Daily summary at 8AM UTC
            if now_utc.hour==8 and now_utc.minute<1 and now-last_daily>3600:
                send_daily_summary(open_signals)
                last_daily = now

            # Hourly heartbeat — sync balance from Bybit
            if now-last_heartbeat>3600:
                real_balance = get_bybit_balance(bybit)
                if real_balance:
                    bybit_fail_count = 0
                    acc = get_account() or acc
                    total_ret = round((acc["balance"]-acc.get("start_balance",acc["balance"]))/acc.get("start_balance",acc["balance"])*100,2)
                    open_str = f"\nOpen positions: {len(open_signals)}" + ("" if not open_signals else "\n"+"\n".join([f"• {s.get('symbol','?')} {s.get('timeframe','?').upper()} {s['direction']} @ ${s['entry']}" for s in open_signals.values()]))
                    send_telegram(f"💓 <b>Bot Alive — LIVE</b> — {now_utc.strftime('%H:%M UTC')}\nBybit Balance: ${real_balance:.2f} USDT\nBot Tracking: ${acc['balance']:.2f} ({'+' if total_ret>=0 else ''}{total_ret}%)\nScanning {len(WATCHLIST)} pairs{open_str}")
                else:
                    bybit_fail_count += 1
                    if bybit_fail_count >= 3:
                        emergency_stop(bybit, open_signals)
                        return
                last_heartbeat = now

            # Scan watchlist for new signals
            for watch in WATCHLIST:
                symbol    = watch["symbol"]
                timeframe = watch["timeframe"]
                pivot_n   = watch["pivot_n"]
                rr        = watch["rr"]
                label     = watch["label"]
                key       = f"{symbol}_{timeframe}"
                interval  = SCAN_INTERVALS.get(timeframe, 1800)

                if now-last_scan.get(key,0)<interval: continue
                last_scan[key]=now

                try:
                    candles = fetch_candles(data_exchange, symbol, timeframe, limit=300)
                    if not candles or len(candles)<50: continue

                    pivots = find_pivots(candles, pivot_n)
                    signal = detect_signal(candles, pivots, rr)

                    # N=1 anchor tracking after TP — use candle high/low
                    if key in tp_anchors and key not in open_signals:
                        anchor = tp_anchors[key]
                        try:
                            ohlcv_anchor = data_exchange.fetch_ohlcv(symbol, timeframe, limit=2)
                            if ohlcv_anchor:
                                c = ohlcv_anchor[-1]
                                c_h = c[2]; c_l = c[3]
                                anchor["candles_since"] = anchor.get("candles_since", 0) + 1
                                if anchor["direction"] == "bull":
                                    if c_h > anchor["candidate"]:
                                        anchor["candidate"] = c_h
                                    else:
                                        new_p3 = anchor["candidate"]
                                        new_p2 = anchor["from_price"]
                                        rng    = new_p3 - new_p2
                                        if rng > 0 and rng/max(new_p2,1) >= 0.003:
                                            print(f"[{now_str}] N=1 HH confirmed @ ${new_p3:.4f}")
                                        del tp_anchors[key]
                                else:
                                    if c_l < anchor["candidate"]:
                                        anchor["candidate"] = c_l
                                    else:
                                        new_p3 = anchor["candidate"]
                                        new_p2 = anchor["from_price"]
                                        rng    = new_p2 - new_p3
                                        if rng > 0 and rng/max(new_p2,1) >= 0.003:
                                            print(f"[{now_str}] N=1 LL confirmed @ ${new_p3:.4f}")
                                        del tp_anchors[key]
                                if anchor.get("candles_since", 0) >= 50:
                                    if key in tp_anchors: del tp_anchors[key]
                        except Exception as e:
                            print(f"[{now_str}] Anchor error {key}: {e}")

                    # Bias filter
                    current_bias = pair_bias.get(key)
                    signal_dir   = signal["structure"] if signal else None
                    if signal and current_bias and current_bias != signal_dir:
                        print(f"[{now_str}] Bias skip: {symbol} {timeframe} signal={signal_dir} bias={current_bias}")
                        signal = None

                    if signal and key not in open_signals:
                        if now-last_signal.get(key,0)>interval:
                            # Sync real balance before placing order
                            real_balance = get_bybit_balance(bybit)
                            if not real_balance:
                                print(f"Cannot get balance, skipping {symbol}")
                                continue

                            # Place real order on Bybit
                            order = place_limit_order(
                                bybit, symbol, signal["direction"],
                                signal["entry"], signal["sl"], signal["tp"],
                                RISK_PCT, real_balance
                            )

                            if order:
                                acc = get_account() or acc
                                send_entry(symbol, timeframe, signal, label, acc)
                                last_signal[key] = now
                                open_signals[key] = {
                                    **signal,
                                    "symbol":     symbol,
                                    "timeframe":  timeframe,
                                    "label":      label,
                                    "entry_time": now,
                                    "order_id":   order["id"],
                                    "order_status": "open",
                                    "balance_at_entry": real_balance,
                                }
                                print(f"[{now_str}] ✅ LIVE ORDER: {symbol} {timeframe} {signal['direction']} @ {signal['entry']}")
                    else:
                        print(f"[{now_str}] No signal: {symbol} {timeframe} N={pivot_n} {rr}R")

                    time.sleep(0.3)

                except Exception as e:
                    print(f"[{now_str}] Scan error {symbol} {timeframe}: {e}")
                    time.sleep(2)

            # Monitor open positions
            closed = []
            for key, sig in open_signals.items():
                try:
                    order_id = sig.get("order_id")
                    symbol   = sig["symbol"]

                    # Check if limit order filled
                    if sig.get("order_status") == "open" and order_id:
                        status = check_order_status(bybit, symbol, order_id)
                        if status == "canceled":
                            # Order expired or cancelled — remove from tracking
                            print(f"[{now_str}] Order cancelled/expired: {symbol}")
                            send_telegram(f"⚠️ <b>ORDER EXPIRED</b> — {symbol} {sig['timeframe'].upper()}\nLimit order at ${sig['entry']} never filled.")
                            closed.append(key)
                            continue
                        elif status == "closed":
                            sig["order_status"] = "filled"
                            print(f"[{now_str}] Order filled: {symbol} @ {sig['entry']}")

                    # If filled — monitor SL/TP via ticker
                    if sig.get("order_status") == "filled":
                        ticker    = bybit.fetch_ticker(symbol)
                        price     = ticker["last"]
                        direction = sig["direction"]
                        won=False; hit=False; exit_price=price

                        if direction=="LONG":
                            if price<=sig["sl"]: won=False;hit=True;exit_price=sig["sl"]
                            elif price>=sig["tp"]: won=True;hit=True;exit_price=sig["tp"]
                        else:
                            if price>=sig["sl"]: won=False;hit=True;exit_price=sig["sl"]
                            elif price<=sig["tp"]: won=True;hit=True;exit_price=sig["tp"]

                        if hit:
                            # Sync real balance after close
                            real_balance = get_bybit_balance(bybit)
                            bal_at_entry = sig.get("balance_at_entry", real_balance)
                            risk_amt     = bal_at_entry * RISK_PCT
                            risk_pp      = abs(sig["entry"]-sig["sl"])
                            pos_size     = risk_amt/risk_pp if risk_pp>0 else 0
                            pnl          = round((sig["entry"]-exit_price)*pos_size if direction=="SHORT" else (exit_price-sig["entry"])*pos_size, 4)
                            new_bal      = real_balance if real_balance else round((acc["balance"] or bal_at_entry)+pnl, 4)

                            acc = update_account(new_bal, won, pnl)

                            log_trade({
                                "symbol":    sig["symbol"],
                                "timeframe": sig["timeframe"],
                                "direction": direction,
                                "entry":     sig["entry"],
                                "exit_price":exit_price,
                                "sl":        sig["sl"],
                                "tp":        sig["tp"],
                                "rr":        sig["rr"],
                                "pnl":       pnl,
                                "won":       won,
                                "balance":   new_bal,
                                "label":     sig["label"],
                                "created_at":datetime.now(timezone.utc).isoformat(),
                            })

                            send_exit(sig["symbol"], sig["timeframe"], sig, exit_price, won, pnl, acc)
                            closed.append(key)
                            print(f"[{now_str}] {'✅TP' if won else '❌SL'}: {symbol} PnL=${pnl}")

                            if won:
                                pair_bias[key] = None
                                anchor_price = exit_price
                                tp_anchors[key] = {
                                    "direction": sig["structure"],
                                    "from_price": sig["p2"] if "p2" in sig else anchor_price,
                                    "candidate":  anchor_price,
                                    "candles_since": 0
                                }
                                print(f"[{now_str}] TP hit — watching for next N=1 anchor from ${anchor_price:.4f}: {symbol} {timeframe}")
                            else:
                                flipped = "bull" if sig["structure"]=="bear" else "bear"
                                pair_bias[key] = flipped
                                if key in tp_anchors: del tp_anchors[key]
                                print(f"[{now_str}] SL hit — bias flipped to {flipped}: {symbol} {timeframe}")

                except Exception as e:
                    print(f"Monitor error {key}: {e}")

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
