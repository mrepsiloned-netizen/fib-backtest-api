# ============================================================
# FIB PAPER TRADER — Runs on Railway 24/7
# Checks for signals every 4H candle close
# Sends Telegram alerts when Fib structure detected
# ============================================================

import ccxt
import numpy as np
import time
import os
import httpx
from datetime import datetime, timezone

# ── CONFIG ────────────────────────────────────────────────
BYBIT_API_KEY = os.environ.get("BYBIT_API_KEY", "")
BYBIT_SECRET  = os.environ.get("BYBIT_SECRET", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Strategy settings — based on our best backtest results
WATCHLIST = [
    {"symbol": "ETH/USDT", "timeframe": "4h", "pivot_n": 5, "rr": 2.0},
    {"symbol": "BTC/USDT", "timeframe": "4h", "pivot_n": 5, "rr": 2.0},
    {"symbol": "INJ/USDT", "timeframe": "4h", "pivot_n": 5, "rr": 2.0},
    {"symbol": "SOL/USDT", "timeframe": "4h", "pivot_n": 5, "rr": 2.0},
    {"symbol": "INJ/USDT", "timeframe": "1d", "pivot_n": 8, "rr": 2.0},
    {"symbol": "SOL/USDT", "timeframe": "1d", "pivot_n": 8, "rr": 1.5},
]

FIB_LEVEL = 0.618
MAX_BARS  = 200
RISK_PCT  = 0.02

# ── TELEGRAM ──────────────────────────────────────────────
def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        httpx.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
        print(f"Telegram sent: {message[:50]}...")
    except Exception as e:
        print(f"Telegram error: {e}")

# ── EXCHANGE ──────────────────────────────────────────────
def get_exchange():
    return ccxt.bybit({
        "apiKey": BYBIT_API_KEY,
        "secret": BYBIT_SECRET,
        "enableRateLimit": True,
    })

# ── FETCH CANDLES ─────────────────────────────────────────
def fetch_recent_candles(exchange, symbol, timeframe, limit=200):
    candles = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
    return candles

# ── PIVOTS ────────────────────────────────────────────────
def find_pivots(candles, N):
    highs = np.array([c[2] for c in candles])
    lows  = np.array([c[3] for c in candles])
    pivots = []
    for i in range(N, len(candles) - N):
        if highs[i] == max(highs[i-N:i+N+1]):
            pivots.append({"idx":i,"type":"H","price":float(highs[i]),"time":candles[i][0]})
        elif lows[i] == min(lows[i-N:i+N+1]):
            pivots.append({"idx":i,"type":"L","price":float(lows[i]),"time":candles[i][0]})
    deduped = []
    for p in pivots:
        if not deduped: deduped.append(p); continue
        last = deduped[-1]
        if last["type"]==p["type"]:
            if p["type"]=="H" and p["price"]>last["price"]: deduped[-1]=p
            elif p["type"]=="L" and p["price"]<last["price"]: deduped[-1]=p
        else: deduped.append(p)
    return deduped

# ── SIGNAL DETECTION ──────────────────────────────────────
def detect_signal(candles, pivots, rr):
    if len(pivots) < 3:
        return None

    highs = np.array([c[2] for c in candles])
    lows  = np.array([c[3] for c in candles])
    current_price = candles[-1][4]  # latest close

    # Check last 3 pivots
    p1, p2, p3 = pivots[-3], pivots[-2], pivots[-1]

    structure = None
    if p1["type"]=="H" and p2["type"]=="L" and p3["type"]=="H" and p3["price"]<p1["price"]:
        structure = "bear"
    elif p1["type"]=="L" and p2["type"]=="H" and p3["type"]=="L" and p3["price"]>p1["price"]:
        structure = "bull"

    if not structure:
        return None

    # Calculate Fib levels
    fib_high = p1["price"] if structure=="bear" else p2["price"]
    fib_low  = p2["price"] if structure=="bear" else p1["price"]
    rng      = fib_high - fib_low
    if rng <= 0: return None

    fib618 = fib_low + rng * FIB_LEVEL if structure=="bear" else fib_high - rng * FIB_LEVEL
    sl     = fib_high + rng * 0.02     if structure=="bear" else fib_low  - rng * 0.02
    rpp    = abs(fib618 - sl)
    if rpp <= 0: return None
    tp     = fib618 - rpp * rr         if structure=="bear" else fib618 + rpp * rr

    # Check if current price is near 61.8% entry zone (within 0.5%)
    entry_zone_pct = abs(current_price - fib618) / fib618 * 100

    if entry_zone_pct <= 0.5:
        return {
            "structure": structure,
            "direction": "SHORT" if structure=="bear" else "LONG",
            "entry":     round(fib618, 4),
            "sl":        round(sl, 4),
            "tp":        round(tp, 4),
            "current":   round(current_price, 4),
            "fib_high":  round(fib_high, 4),
            "fib_low":   round(fib_low, 4),
            "rr":        rr,
            "entry_zone_pct": round(entry_zone_pct, 3),
        }

    return None

# ── FORMAT SIGNAL MESSAGE ─────────────────────────────────
def format_signal(symbol, timeframe, signal, pivot_n):
    direction = signal["direction"]
    emoji = "🟢" if direction == "LONG" else "🔴"
    arrow = "📈" if direction == "LONG" else "📉"

    sl_pct  = abs(signal["entry"] - signal["sl"])  / signal["entry"] * 100
    tp_pct  = abs(signal["tp"]   - signal["entry"]) / signal["entry"] * 100
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    return f"""
{emoji} <b>FIB SIGNAL — {symbol} {timeframe.upper()}</b> {arrow}

<b>Direction:</b> {direction}
<b>Structure:</b> {"LH→LL→LH" if signal["structure"]=="bear" else "HL→HH→HL"}

<b>Entry:</b>  ${signal["entry"]}
<b>SL:</b>     ${signal["sl"]} (-{sl_pct:.2f}%)
<b>TP:</b>     ${signal["tp"]} (+{tp_pct:.2f}%)
<b>RR:</b>     1:{signal["rr"]}R

<b>Current:</b> ${signal["current"]}
<b>Fib Range:</b> ${signal["fib_low"]} → ${signal["fib_high"]}
<b>Pivot N:</b> {pivot_n}

⏰ {now_str}
📊 <i>Paper trade only — do not execute with real money</i>
""".strip()

# ── TRACK OPEN SIGNALS ────────────────────────────────────
open_signals = {}  # key: symbol+timeframe, value: signal dict

def check_open_signals(exchange):
    closed = []
    for key, sig in open_signals.items():
        symbol, tf = key.split("_")
        try:
            ticker = exchange.fetch_ticker(symbol)
            price  = ticker["last"]
            direction = sig["direction"]

            if direction == "LONG":
                if price <= sig["sl"]:
                    send_telegram(f"🔴 SL HIT — {symbol} {tf.upper()} LONG\nEntry: ${sig['entry']} → Exit: ${price:.4f}\n❌ Stop Loss")
                    closed.append(key)
                elif price >= sig["tp"]:
                    send_telegram(f"🟢 TP HIT — {symbol} {tf.upper()} LONG\nEntry: ${sig['entry']} → Exit: ${price:.4f}\n✅ Take Profit {sig['rr']}R")
                    closed.append(key)
            else:
                if price >= sig["sl"]:
                    send_telegram(f"🔴 SL HIT — {symbol} {tf.upper()} SHORT\nEntry: ${sig['entry']} → Exit: ${price:.4f}\n❌ Stop Loss")
                    closed.append(key)
                elif price <= sig["tp"]:
                    send_telegram(f"🟢 TP HIT — {symbol} {tf.upper()} SHORT\nEntry: ${sig['entry']} → Exit: ${price:.4f}\n✅ Take Profit {sig['rr']}R")
                    closed.append(key)
        except Exception as e:
            print(f"Error checking {key}: {e}")

    for key in closed:
        del open_signals[key]

# ── MAIN LOOP ─────────────────────────────────────────────
def run():
    print("🤖 Fib Paper Trader starting...")
    send_telegram("🤖 <b>Fib Paper Trader is LIVE</b>\n\nWatching:\n" +
                  "\n".join([f"• {w['symbol']} {w['timeframe'].upper()} N={w['pivot_n']} {w['rr']}R" for w in WATCHLIST]) +
                  "\n\n📊 Paper trading only — signals only, no real orders")

    exchange = get_exchange()
    last_check = {}

    while True:
        try:
            now = datetime.now(timezone.utc)
            print(f"\n[{now.strftime('%H:%M:%S')}] Scanning {len(WATCHLIST)} pairs...")

            # Check open signals for SL/TP
            if open_signals:
                check_open_signals(exchange)

            for watch in WATCHLIST:
                symbol   = watch["symbol"]
                timeframe = watch["timeframe"]
                pivot_n  = watch["pivot_n"]
                rr       = watch["rr"]
                key      = f"{symbol}_{timeframe}"

                try:
                    candles = fetch_recent_candles(exchange, symbol, timeframe, limit=300)
                    if not candles or len(candles) < 50:
                        continue

                    pivots = find_pivots(candles, pivot_n)
                    signal = detect_signal(candles, pivots, rr)

                    if signal:
                        # Don't resend same signal within 4H
                        last = last_check.get(key, 0)
                        if time.time() - last > 3600 * 4:
                            msg = format_signal(symbol, timeframe, signal, pivot_n)
                            send_telegram(msg)
                            last_check[key] = time.time()
                            open_signals[key] = signal
                            print(f"✅ Signal: {symbol} {timeframe} {signal['direction']}")
                        else:
                            print(f"⏭ Skipped (too soon): {symbol} {timeframe}")
                    else:
                        print(f"  No signal: {symbol} {timeframe}")

                    time.sleep(0.5)  # Rate limit

                except Exception as e:
                    print(f"Error scanning {symbol} {timeframe}: {e}")
                    time.sleep(2)

            # Send hourly heartbeat
            if now.minute < 5:
                print("💓 Heartbeat — bot is alive")

            # Sleep until next check (every 30 minutes)
            print(f"Sleeping 30 minutes...")
            time.sleep(60 * 30)

        except KeyboardInterrupt:
            print("Bot stopped")
            break
        except Exception as e:
            print(f"Main loop error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    run()
