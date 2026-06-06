# ============================================================
# FIB PAPER TRADER v6
# Changes from v5:
#   - Engine: structure (BOS state machine) replaces original triplet
#   - Pairs: XLM, DOGE, ADA, TRX, ARB, XRP (drop ETH, BTC)
#   - Removed N=1 anchor tracking (unvalidated, not in backtest)
#   - SL/TP: checked on candle high/low (wick-based, last CLOSED candle)
#   - Bias flip on loss, reset on win (unchanged)
#   - Circuit breaker at 10 consecutive losses (unchanged)
# ============================================================

import ccxt
import numpy as np
import time
import os
import httpx
from datetime import datetime, timezone, timedelta

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
    "1m":15, "5m":300, "15m":900,
    "1h":3600, "4h":14400, "1d":86400,
}
SIGNAL_COOLDOWNS = {
    "1m":60, "5m":300, "15m":900,
    "1h":3600, "4h":14400, "1d":86400,
}

START_BALANCE     = 100.0
RISK_PCT          = 0.02
MAX_CONSEC_LOSSES = 10

# ── WATCHLIST ─────────────────────────────────────────────────
WATCHLIST = [
    {"symbol":"XLM/USDT",  "timeframe":"1m", "pivot_n":8, "rr":2.0, "fib_level":0.5,   "label":"⭐ XLM 1M"},
    {"symbol":"DOGE/USDT", "timeframe":"1m", "pivot_n":8, "rr":2.0, "fib_level":0.5,   "label":"🐶 DOGE 1M"},
    {"symbol":"ADA/USDT",  "timeframe":"1m", "pivot_n":5, "rr":1.5, "fib_level":0.5,   "label":"🔵 ADA 1M"},
    {"symbol":"TRX/USDT",  "timeframe":"1m", "pivot_n":8, "rr":2.0, "fib_level":0.5,   "label":"🔺 TRX 1M"},
    {"symbol":"ARB/USDT",  "timeframe":"1m", "pivot_n":8, "rr":2.0, "fib_level":0.618, "label":"⚙️ ARB 1M"},
    {"symbol":"XRP/USDT",  "timeframe":"1m", "pivot_n":5, "rr":4.0, "fib_level":0.618, "label":"💧 XRP 1M"},
]

# ── SUPABASE ──────────────────────────────────────────────────
def get_account():
    try:
        res = httpx.get(f"{SUPABASE_URL}/rest/v1/paper_account?id=eq.1&select=*",
                        headers=SUPABASE_HEADERS, timeout=10)
        if res.status_code == 200 and res.json():
            return res.json()[0]
    except Exception as e:
        print(f"Get account error: {e}")
    return None

def init_account():
    try:
        existing = get_account()
        if existing: return existing
        row = {
            "id": 1, "balance": START_BALANCE, "total_trades": 0,
            "wins": 0, "losses": 0, "total_pnl": 0.0,
            "peak_balance": START_BALANCE, "max_drawdown": 0.0,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        httpx.post(f"{SUPABASE_URL}/rest/v1/paper_account",
                   json=row, headers=SUPABASE_HEADERS, timeout=10)
        return row
    except Exception as e:
        print(f"Init account error: {e}")
        return {"balance": START_BALANCE, "total_trades": 0, "wins": 0, "losses": 0,
                "total_pnl": 0.0, "peak_balance": START_BALANCE, "max_drawdown": 0.0}

def update_account(balance, won, pnl):
    try:
        acc    = get_account()
        if not acc: acc = init_account()
        peak   = max(acc["peak_balance"], balance)
        dd     = round((peak - balance) / peak * 100, 2)
        max_dd = max(acc["max_drawdown"], dd)
        updates = {
            "balance":      round(balance, 4),
            "total_trades": acc["total_trades"] + 1,
            "wins":         acc["wins"] + (1 if won else 0),
            "losses":       acc["losses"] + (0 if won else 1),
            "total_pnl":    round(acc["total_pnl"] + pnl, 4),
            "peak_balance": round(peak, 4),
            "max_drawdown": max_dd,
        }
        httpx.patch(f"{SUPABASE_URL}/rest/v1/paper_account?id=eq.1",
                    json=updates, headers=SUPABASE_HEADERS, timeout=10)
        return {**acc, **updates}
    except Exception as e:
        print(f"Update account error: {e}")
        return None

def log_trade(trade_data):
    try:
        httpx.post(f"{SUPABASE_URL}/rest/v1/paper_trades", json=trade_data,
                   headers={**SUPABASE_HEADERS, "Prefer": "return=minimal"}, timeout=10)
    except Exception as e:
        print(f"Log trade error: {e}")

def get_today_trades():
    try:
        since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        res   = httpx.get(
            f"{SUPABASE_URL}/rest/v1/paper_trades?created_at=gte.{since}&select=*&order=created_at.desc",
            headers=SUPABASE_HEADERS, timeout=10)
        if res.status_code == 200: return res.json()
    except Exception as e:
        print(f"Get trades error: {e}")
    return []

# ── TELEGRAM ──────────────────────────────────────────────────
def send_telegram(message):
    try:
        httpx.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10)
    except Exception as e:
        print(f"Telegram error: {e}")

def send_entry(symbol, timeframe, signal, label, acc):
    balance   = acc["balance"]
    risk_amt  = round(balance * RISK_PCT, 4)
    risk_pp   = abs(signal["entry"] - signal["sl"])
    pos_size  = round(risk_amt / risk_pp, 6) if risk_pp > 0 else 0
    pos_value = round(pos_size * signal["entry"], 2)
    sl_loss   = round(balance - risk_amt, 2)
    tp_gain   = round(balance + risk_amt * signal["rr"], 2)
    direction = signal["direction"]
    emoji     = "🟢" if direction == "LONG" else "🔴"
    arrow     = "📈" if direction == "LONG" else "📉"
    now_str   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total_ret = round((balance - START_BALANCE) / START_BALANCE * 100, 2)
    wr        = round(acc["wins"] / acc["total_trades"] * 100, 1) if acc["total_trades"] > 0 else 0

    send_telegram(f"""{emoji} <b>TRADE ENTERED — {symbol} {timeframe.upper()}</b> {arrow}
<b>{label}</b>

<b>Direction:</b> {direction}
<b>Structure:</b> {"BOS Bear" if signal["structure"]=="bear" else "BOS Bull"}

<b>Entry:</b>  ${signal["entry"]}
<b>SL:</b>     ${signal["sl"]} (-{abs(signal["entry"]-signal["sl"])/signal["entry"]*100:.2f}%)
<b>TP:</b>     ${signal["tp"]} (+{abs(signal["tp"]-signal["entry"])/signal["entry"]*100:.2f}%)
<b>RR:</b>     1:{signal["rr"]}R

<b>Account:</b>   ${balance:.2f}
<b>Risk:</b>      {RISK_PCT*100:.0f}% = ${risk_amt:.2f}
<b>Position:</b>  ${pos_value:.2f} worth of {symbol.split('/')[0]}
<b>If SL hit:</b> ${sl_loss:.2f} (-${risk_amt:.2f})
<b>If TP hit:</b> ${tp_gain:.2f} (+${round(risk_amt*signal["rr"],2):.2f})

<b>Stats:</b> {acc["total_trades"]} trades · {acc["wins"]}W {acc["losses"]}L · {wr}% WR
<b>Total return:</b> {"+'" if total_ret>=0 else ""}{total_ret}%
⏰ {now_str}
📊 <i>Paper trade v6</i>""")

def send_exit(symbol, timeframe, signal, exit_price, won, pnl, acc):
    emoji     = "✅" if won else "❌"
    result    = "TAKE PROFIT" if won else "STOP LOSS"
    now_str   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total_ret = round((acc["balance"] - START_BALANCE) / START_BALANCE * 100, 2)
    wr        = round(acc["wins"] / acc["total_trades"] * 100, 1) if acc["total_trades"] > 0 else 0

    send_telegram(f"""{emoji} <b>TRADE CLOSED — {symbol} {timeframe.upper()}</b>

<b>Result:</b>    {result}
<b>Direction:</b> {signal["direction"]}

<b>Entry:</b>   ${signal["entry"]}
<b>Exit:</b>    ${exit_price:.6f}
<b>P&L:</b>     {"+'" if pnl>=0 else ""}${pnl:.4f}
<b>RR:</b>      {signal["rr"]}R {"achieved ✅" if won else "missed ❌"}

<b>Previous balance:</b> ${round(acc["balance"]-pnl, 2):.2f}
<b>Current balance:</b>  ${acc["balance"]:.2f}
<b>Total return:</b>     {"+'" if total_ret>=0 else ""}{total_ret}%
<b>Max drawdown:</b>     -{acc["max_drawdown"]:.2f}%

<b>All time:</b> {acc["total_trades"]} trades · {acc["wins"]}W {acc["losses"]}L · {wr}% WR
⏰ {now_str}
📊 <i>Paper trade v6</i>""")

def send_daily_summary(open_signals):
    try:
        acc    = get_account()
        trades = get_today_trades()
        if not acc: return
        today_pnl  = sum(t.get("pnl", 0) for t in trades)
        today_wins = sum(1 for t in trades if t.get("won"))
        today_loss = sum(1 for t in trades if not t.get("won"))
        total_ret  = round((acc["balance"] - START_BALANCE) / START_BALANCE * 100, 2)
        wr         = round(acc["wins"] / acc["total_trades"] * 100, 1) if acc["total_trades"] > 0 else 0
        now_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        open_str   = ""
        if open_signals:
            open_str = "\n\n<b>Open positions:</b>"
            for key, sig in open_signals.items():
                open_str += f"\n• {sig.get('symbol','?')} {sig['direction']} @ ${sig['entry']}"
        send_telegram(f"""📊 <b>DAILY REPORT — {now_str}</b>

<b>Today:</b> {len(trades)} trades · {today_wins}W {today_loss}L
<b>P&L today:</b> {"+'" if today_pnl>=0 else ""}${today_pnl:.4f}

<b>Account balance:</b> ${acc["balance"]:.2f}
<b>Total return:</b>    {"+'" if total_ret>=0 else ""}{total_ret}%
<b>Max drawdown:</b>    -{acc["max_drawdown"]:.2f}%

<b>All time:</b> {acc["total_trades"]} trades · {acc["wins"]}W {acc["losses"]}L · {wr}% WR{open_str}""")
    except Exception as e:
        print(f"Daily summary error: {e}")

# ── EXCHANGE ──────────────────────────────────────────────────
def get_exchange():
    return ccxt.kucoin({"enableRateLimit": True})

def fetch_candles(exchange, symbol, timeframe, limit=300):
    return exchange.fetch_ohlcv(symbol, timeframe, limit=limit)

# ── PIVOT DETECTION ───────────────────────────────────────────
def find_pivots(candles, N):
    highs  = np.array([c[2] for c in candles])
    lows   = np.array([c[3] for c in candles])
    closes = np.array([c[4] for c in candles])
    n      = len(candles) - 1  # exclude live candle[-1]
    pivots = []
    for i in range(N, n - N):
        if highs[i] == max(highs[i-N:i+N+1]):
            pivots.append({"idx": i, "type": "H", "price": float(highs[i]), "close": float(closes[i])})
        elif lows[i] == min(lows[i-N:i+N+1]):
            pivots.append({"idx": i, "type": "L", "price": float(lows[i]),  "close": float(closes[i])})
    deduped = []
    for p in pivots:
        if not deduped:
            deduped.append(p); continue
        last = deduped[-1]
        if last["type"] == p["type"]:
            if p["type"] == "H" and p["price"] > last["price"]: deduped[-1] = p
            elif p["type"] == "L" and p["price"] < last["price"]: deduped[-1] = p
        else:
            deduped.append(p)
    return deduped

# ── STRUCTURE ENGINE SIGNAL DETECTION ────────────────────────
def detect_signal(candles, pivots, rr, fib_level=0.5, min_swing_pct=0.002, stop_buffer_pct=0.001):
    """
    Structure engine — matches backtest structure engine.
    Scans for BOS (Break of Structure) then waits for fib pullback.

    BULL: new pivot HIGH > previous pivot HIGH → BOS up
          Fib drawn from P2 (swing low) up to P3 (new high)
          Entry when last closed candle LOW <= fib level

    BEAR: new pivot LOW < previous pivot LOW → BOS down
          Fib drawn from P2 (swing high) down to P3 (new low)
          Entry when last closed candle HIGH >= fib level

    Uses candles[-2] as last closed candle.
    """
    if len(candles) < 50 or len(pivots) < 4: return None

    highs = np.array([c[2] for c in candles])
    lows  = np.array([c[3] for c in candles])

    c_high = highs[-2]
    c_low  = lows[-2]

    # Separate pivot lists by type — alternating dedup means we need to
    # compare H to previous H and L to previous L by skipping intervening pivots
    pivot_highs = [p for p in pivots if p["type"] == "H"]
    pivot_lows  = [p for p in pivots if p["type"] == "L"]

    best_bull = None
    best_bear = None

    # Bull BOS: find most recent pivot HIGH that exceeds the one before it
    # Between two consecutive HIGHs, the LOW between them is P2 (swing low)
    for i in range(1, len(pivot_highs)):
        p_prev_h = pivot_highs[i - 1]
        p_curr_h = pivot_highs[i]

        if p_curr_h["price"] > p_prev_h["price"]:
            # P2 = lowest low between these two highs
            p2  = float(min(lows[p_prev_h["idx"]:p_curr_h["idx"] + 1]))
            p3  = p_curr_h["price"]
            rng = p3 - p2
            if rng > 0 and rng / max(p2, 1) >= min_swing_pct:
                fe = p3 - rng * fib_level
                sl = p2 - p2 * stop_buffer_pct
                # Structure valid: price hasn't dropped below SL
                if c_low >= sl:
                    best_bull = {
                        "p2": p2, "p3": p3, "fib_entry": fe, "sl": sl,
                        "p_idx": p_curr_h["idx"]
                    }

    # Bear BOS: find most recent pivot LOW that breaks below the one before it
    # Between two consecutive LOWs, the HIGH between them is P2 (swing high)
    for i in range(1, len(pivot_lows)):
        p_prev_l = pivot_lows[i - 1]
        p_curr_l = pivot_lows[i]

        if p_curr_l["price"] < p_prev_l["price"]:
            # P2 = highest high between these two lows
            p2  = float(max(highs[p_prev_l["idx"]:p_curr_l["idx"] + 1]))
            p3  = p_curr_l["price"]
            rng = p2 - p3
            if rng > 0 and rng / max(p2, 1) >= min_swing_pct:
                fe = p3 + rng * fib_level
                sl = p2 + p2 * stop_buffer_pct
                # Structure valid: price hasn't broken above SL
                if c_high <= sl:
                    best_bear = {
                        "p2": p2, "p3": p3, "fib_entry": fe, "sl": sl,
                        "p_idx": p_curr_l["idx"]
                    }

    # Bull — last closed candle LOW touched fib → LONG entry
    if best_bull:
        fe  = best_bull["fib_entry"]
        sl  = best_bull["sl"]
        rpp = abs(fe - sl)
        if rpp > 0 and c_low <= fe:
            tp = fe + rpp * rr
            return {
                "structure": "bull", "direction": "LONG",
                "entry": round(fe, 6), "sl": round(sl, 6), "tp": round(tp, 6),
                "p2": round(best_bull["p2"], 6), "p3": round(best_bull["p3"], 6),
                "current": round(c_low, 6), "rr": rr
            }

    # Bear — last closed candle HIGH touched fib → SHORT entry
    if best_bear:
        fe  = best_bear["fib_entry"]
        sl  = best_bear["sl"]
        rpp = abs(fe - sl)
        if rpp > 0 and c_high >= fe:
            tp = fe - rpp * rr
            return {
                "structure": "bear", "direction": "SHORT",
                "entry": round(fe, 6), "sl": round(sl, 6), "tp": round(tp, 6),
                "p2": round(best_bear["p2"], 6), "p3": round(best_bear["p3"], 6),
                "current": round(c_high, 6), "rr": rr
            }

    return None

# ── MAIN LOOP ─────────────────────────────────────────────────
def run():
    open_signals   = {}
    pair_bias      = {}
    last_signal    = {}
    last_scan      = {}
    last_daily     = 0
    last_heartbeat = 0
    consec_losses  = 0

    print("🤖 Fib Paper Trader v6 starting...")
    acc = init_account()

    watchlist_str = "\n".join([
        f"• {w['symbol']} {w['timeframe'].upper()} N={w['pivot_n']} {w['rr']}R fib={w['fib_level']} — {w['label']}"
        for w in WATCHLIST
    ])
    send_telegram(f"""🤖 <b>Fib Paper Trader v6 LIVE</b>

<b>Engine:</b> Structure (BOS)
<b>Account:</b> ${acc["balance"]:.2f}
<b>Risk per trade:</b> {RISK_PCT*100:.0f}%

<b>Watchlist:</b>
{watchlist_str}

📊 Paper trading active""")

    exchange = get_exchange()

    while True:
        try:
            now     = time.time()
            now_utc = datetime.now(timezone.utc)
            now_str = now_utc.strftime("%H:%M:%S")

            # Circuit breaker
            if consec_losses >= MAX_CONSEC_LOSSES:
                send_telegram(
                    f"🛑 <b>CIRCUIT BREAKER</b> — {consec_losses} consecutive losses.\n"
                    f"Bot stopped. Manual restart required."
                )
                print(f"[{now_str}] 🛑 Circuit breaker. Bot stopped.")
                break

            # Daily summary 8AM UTC
            if now_utc.hour == 8 and now_utc.minute < 1 and now - last_daily > 3600:
                send_daily_summary(open_signals)
                last_daily = now

            # Hourly heartbeat
            if now - last_heartbeat > 3600:
                acc       = init_account()
                total_ret = round((acc["balance"] - START_BALANCE) / START_BALANCE * 100, 2)
                open_str  = f"\nOpen: {len(open_signals)}"
                if open_signals:
                    open_str += "\n" + "\n".join(
                        [f"• {s.get('symbol','?')} {s['direction']} @ ${s['entry']}"
                         for s in open_signals.values()]
                    )
                send_telegram(
                    f"💓 <b>Bot Alive</b> — {now_utc.strftime('%H:%M UTC')}\n"
                    f"Balance: ${acc['balance']:.2f} ({'+' if total_ret>=0 else ''}{total_ret}%)\n"
                    f"Scanning {len(WATCHLIST)} pairs{open_str}"
                )
                last_heartbeat = now

            # Scan watchlist
            for watch in WATCHLIST:
                symbol    = watch["symbol"]
                timeframe = watch["timeframe"]
                pivot_n   = watch["pivot_n"]
                rr        = watch["rr"]
                fib_level = watch.get("fib_level", 0.5)
                label     = watch["label"]
                key           = f"{symbol}_{timeframe}"
                scan_interval = SCAN_INTERVALS.get(timeframe, 60)
                sig_cooldown  = SIGNAL_COOLDOWNS.get(timeframe, 60)

                if now - last_scan.get(key, 0) < scan_interval: continue
                last_scan[key] = now

                try:
                    candles = fetch_candles(exchange, symbol, timeframe, limit=300)
                    if not candles or len(candles) < 50: continue

                    pivots = find_pivots(candles, pivot_n)
                    signal = detect_signal(candles, pivots, rr, fib_level)

                    # Bias filter
                    current_bias = pair_bias.get(key)
                    if signal and current_bias and current_bias != signal["structure"]:
                        print(f"[{now_str}] Bias skip: {symbol} signal={signal['structure']} bias={current_bias}")
                        signal = None

                    if signal and key not in open_signals:
                        if now - last_signal.get(key, 0) > sig_cooldown:
                            acc = init_account()
                            send_entry(symbol, timeframe, signal, label, acc)
                            last_signal[key] = now
                            open_signals[key] = {
                                **signal,
                                "symbol":        symbol,
                                "timeframe":     timeframe,
                                "label":         label,
                                "entry_time":    now,
                                "entry_balance": acc["balance"],
                            }
                            print(f"[{now_str}] ✅ ENTRY: {symbol} {signal['direction']} @ {signal['entry']}")
                    else:
                        print(f"[{now_str}] No signal: {symbol} {timeframe} N={pivot_n}")

                    time.sleep(0.3)

                except Exception as e:
                    print(f"[{now_str}] Scan error {symbol}: {e}")
                    time.sleep(2)

            # Check SL/TP on open signals — wick-based on last closed candle
            closed = []
            for key, sig in open_signals.items():
                try:
                    ohlcv = exchange.fetch_ohlcv(sig["symbol"], sig["timeframe"], limit=2)
                    if not ohlcv: continue
                    candle    = ohlcv[-2]  # last CLOSED candle, ignore ohlcv[-1] (live)
                    c_high    = candle[2]
                    c_low     = candle[3]
                    direction = sig["direction"]
                    won = False; hit = False; exit_price = None

                    if direction == "LONG":
                        if c_low  <= sig["sl"]:  won = False; hit = True; exit_price = sig["sl"]
                        elif c_high >= sig["tp"]: won = True;  hit = True; exit_price = sig["tp"]
                    else:
                        if c_high >= sig["sl"]:  won = False; hit = True; exit_price = sig["sl"]
                        elif c_low  <= sig["tp"]: won = True;  hit = True; exit_price = sig["tp"]

                    if hit:
                        acc       = init_account()
                        entry_bal = sig.get("entry_balance", acc["balance"])
                        risk_amt  = entry_bal * RISK_PCT
                        risk_pp   = abs(sig["entry"] - sig["sl"])
                        pos_size  = risk_amt / risk_pp if risk_pp > 0 else 0
                        pnl       = round(
                            (sig["entry"] - exit_price) * pos_size if direction == "SHORT"
                            else (exit_price - sig["entry"]) * pos_size, 4
                        )
                        new_bal = round(acc["balance"] + pnl, 4)
                        acc     = update_account(new_bal, won, pnl)

                        log_trade({
                            "symbol":     sig["symbol"],
                            "timeframe":  sig["timeframe"],
                            "direction":  direction,
                            "entry":      sig["entry"],
                            "exit_price": exit_price,
                            "sl":         sig["sl"],
                            "tp":         sig["tp"],
                            "rr":         sig["rr"],
                            "pnl":        pnl,
                            "won":        won,
                            "balance":    new_bal,
                            "label":      sig["label"],
                            "created_at": datetime.now(timezone.utc).isoformat(),
                        })

                        send_exit(sig["symbol"], sig["timeframe"], sig, exit_price, won, pnl, acc)
                        closed.append(key)
                        print(f"[{now_str}] {'✅TP' if won else '❌SL'}: {sig['symbol']} PnL=${pnl}")

                        if won:
                            pair_bias[key] = None
                            consec_losses  = 0
                        else:
                            flipped        = "bull" if sig["structure"] == "bear" else "bear"
                            pair_bias[key] = flipped
                            consec_losses += 1
                            print(f"[{now_str}] SL — bias → {flipped} ({consec_losses}/{MAX_CONSEC_LOSSES})")
                            if consec_losses >= MAX_CONSEC_LOSSES:
                                send_telegram(
                                    f"🚨 <b>WARNING</b> — {consec_losses} consecutive losses. "
                                    f"Circuit breaker triggers next loop."
                                )

                except Exception as e:
                    print(f"[{now_str}] Monitor error {key}: {e}")

            for key in closed:
                del open_signals[key]

            time.sleep(15)

        except KeyboardInterrupt:
            print("Bot stopped")
            break
        except Exception as e:
            print(f"Main loop error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    run()
