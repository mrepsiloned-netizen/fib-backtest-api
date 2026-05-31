# ============================================================
# FIB PAPER TRADER v2 — Runs on Railway 24/7
# Supports 1M, 5M, 15M, 1H, 4H, 1D timeframes
# ============================================================

import ccxt
import numpy as np
import time
import os
import httpx
from datetime import datetime, timezone

BYBIT_API_KEY    = os.environ.get("BYBIT_API_KEY", "")
BYBIT_SECRET     = os.environ.get("BYBIT_SECRET", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Scan intervals per timeframe (seconds)
SCAN_INTERVALS = {
    "1m":  60,
    "5m":  300,
    "15m": 900,
    "1h":  3600,
    "4h":  14400,
    "1d":  86400,
}

# Watchlist — add/remove pairs as needed
WATCHLIST = [
    # Short timeframes
    {"symbol":"ETH/USDT", "timeframe":"5m",  "pivot_n":3, "rr":2.0},
    {"symbol":"BTC/USDT", "timeframe":"5m",  "pivot_n":3, "rr":2.0},
    {"symbol":"ETH/USDT", "timeframe":"15m", "pivot_n":3, "rr":2.0},
    {"symbol":"BTC/USDT", "timeframe":"15m", "pivot_n":3, "rr":2.0},
    # Medium timeframes
    {"symbol":"ETH/USDT", "timeframe":"4h",  "pivot_n":5, "rr":2.0},
    {"symbol":"BTC/USDT", "timeframe":"4h",  "pivot_n":5, "rr":2.0},
    {"symbol":"INJ/USDT", "timeframe":"4h",  "pivot_n":5, "rr":2.0},
    {"symbol":"SOL/USDT", "timeframe":"4h",  "pivot_n":5, "rr":2.0},
    # Daily
    {"symbol":"INJ/USDT", "timeframe":"1d",  "pivot_n":8, "rr":2.0},
    {"symbol":"SOL/USDT", "timeframe":"1d",  "pivot_n":8, "rr":1.5},
]

FIB_LEVEL = 0.618

def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        httpx.post(url, json={"chat_id":TELEGRAM_CHAT_ID,"text":message,"parse_mode":"HTML"}, timeout=10)
    except Exception as e:
        print(f"Telegram error: {e}")

def get_exchange():
    return ccxt.bybit({"apiKey":BYBIT_API_KEY,"secret":BYBIT_SECRET,"enableRateLimit":True})

def fetch_candles(exchange, symbol, timeframe, limit=300):
    return exchange.fetch_ohlcv(symbol, timeframe, limit=limit)

def find_pivots(candles, N):
    highs = np.array([c[2] for c in candles])
    lows  = np.array([c[3] for c in candles])
    pivots = []
    for i in range(N, len(candles)-N):
        if highs[i] == max(highs[i-N:i+N+1]):
            pivots.append({"idx":i,"type":"H","price":float(highs[i])})
        elif lows[i] == min(lows[i-N:i+N+1]):
            pivots.append({"idx":i,"type":"L","price":float(lows[i])})
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
    if len(pivots) < 3: return None
    p1,p2,p3 = pivots[-3],pivots[-2],pivots[-1]
    current = candles[-1][4]

    structure = None
    if p1["type"]=="H" and p2["type"]=="L" and p3["type"]=="H" and p3["price"]<p1["price"]: structure="bear"
    elif p1["type"]=="L" and p2["type"]=="H" and p3["type"]=="L" and p3["price"]>p1["price"]: structure="bull"
    if not structure: return None

    fh = p1["price"] if structure=="bear" else p2["price"]
    fl = p2["price"] if structure=="bear" else p1["price"]
    rng = fh-fl
    if rng<=0: return None

    fib618 = fl+rng*FIB_LEVEL if structure=="bear" else fh-rng*FIB_LEVEL
    sl     = fh+rng*0.02       if structure=="bear" else fl-rng*0.02
    rpp    = abs(fib618-sl)
    if rpp<=0: return None
    tp     = fib618-rpp*rr     if structure=="bear" else fib618+rpp*rr

    zone_pct = abs(current-fib618)/fib618*100
    if zone_pct <= 0.5:
        return {
            "structure":structure,"direction":"SHORT" if structure=="bear" else "LONG",
            "entry":round(fib618,6),"sl":round(sl,6),"tp":round(tp,6),
            "current":round(current,6),"rr":rr,"zone_pct":round(zone_pct,3)
        }
    return None

def format_signal(symbol, timeframe, signal):
    emoji = "🟢" if signal["direction"]=="LONG" else "🔴"
    arrow = "📈" if signal["direction"]=="LONG" else "📉"
    sl_pct = abs(signal["entry"]-signal["sl"])/signal["entry"]*100
    tp_pct = abs(signal["tp"]-signal["entry"])/signal["entry"]*100
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return f"""{emoji} <b>FIB SIGNAL — {symbol} {timeframe.upper()}</b> {arrow}

<b>Direction:</b> {signal["direction"]}
<b>Structure:</b> {"LH→LL→LH" if signal["structure"]=="bear" else "HL→HH→HL"}

<b>Entry:</b>  ${signal["entry"]}
<b>SL:</b>     ${signal["sl"]} (-{sl_pct:.2f}%)
<b>TP:</b>     ${signal["tp"]} (+{tp_pct:.2f}%)
<b>RR:</b>     1:{signal["rr"]}R

<b>Current:</b> ${signal["current"]}
⏰ {now_str}
📊 <i>Paper trade only</i>""".strip()

def run():
    print("🤖 Fib Paper Trader v2 starting...")
    watchlist_str = "\n".join([f"• {w['symbol']} {w['timeframe'].upper()} N={w['pivot_n']} {w['rr']}R" for w in WATCHLIST])
    send_telegram(f"🤖 <b>Fib Paper Trader v2 LIVE</b>\n\nWatching:\n{watchlist_str}\n\n📊 Paper trading only")

    exchange = get_exchange()
    last_signal = {}
    last_scan   = {}
    open_signals = {}

    while True:
        try:
            now = time.time()
            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")

            for watch in WATCHLIST:
                symbol    = watch["symbol"]
                timeframe = watch["timeframe"]
                pivot_n   = watch["pivot_n"]
                rr        = watch["rr"]
                key       = f"{symbol}_{timeframe}"
                interval  = SCAN_INTERVALS.get(timeframe, 1800)

                # Only scan at appropriate interval
                if now - last_scan.get(key, 0) < interval:
                    continue

                last_scan[key] = now

                try:
                    candles = fetch_candles(exchange, symbol, timeframe, limit=300)
                    if not candles or len(candles) < 50: continue

                    pivots = find_pivots(candles, pivot_n)
                    signal = detect_signal(candles, pivots, rr)

                    if signal:
                        # Don't resend same signal within one interval
                        if now - last_signal.get(key, 0) > interval:
                            msg = format_signal(symbol, timeframe, signal)
                            send_telegram(msg)
                            last_signal[key] = now
                            open_signals[key] = {**signal, "symbol":symbol, "timeframe":timeframe}
                            print(f"[{now_str}] ✅ Signal: {symbol} {timeframe} {signal['direction']}")
                        else:
                            print(f"[{now_str}] ⏭ Duplicate: {symbol} {timeframe}")
                    else:
                        print(f"[{now_str}] No signal: {symbol} {timeframe}")

                    time.sleep(0.3)

                except Exception as e:
                    print(f"[{now_str}] Error {symbol} {timeframe}: {e}")
                    time.sleep(2)

            # Check open signals for SL/TP hits
            closed = []
            for key, sig in open_signals.items():
                try:
                    ticker = exchange.fetch_ticker(sig["symbol"])
                    price  = ticker["last"]
                    if sig["direction"]=="LONG":
                        if price <= sig["sl"]:
                            send_telegram(f"🔴 SL HIT — {sig['symbol']} {sig['timeframe'].upper()} LONG\nEntry: ${sig['entry']} → Exit: ${price:.6f}\n❌ Stop Loss")
                            closed.append(key)
                        elif price >= sig["tp"]:
                            send_telegram(f"🟢 TP HIT — {sig['symbol']} {sig['timeframe'].upper()} LONG\nEntry: ${sig['entry']} → Exit: ${price:.6f}\n✅ Take Profit {sig['rr']}R")
                            closed.append(key)
                    else:
                        if price >= sig["sl"]:
                            send_telegram(f"🔴 SL HIT — {sig['symbol']} {sig['timeframe'].upper()} SHORT\nEntry: ${sig['entry']} → Exit: ${price:.6f}\n❌ Stop Loss")
                            closed.append(key)
                        elif price <= sig["tp"]:
                            send_telegram(f"🟢 TP HIT — {sig['symbol']} {sig['timeframe'].upper()} SHORT\nEntry: ${sig['entry']} → Exit: ${price:.6f}\n✅ Take Profit {sig['rr']}R")
                            closed.append(key)
                except Exception as e:
                    print(f"Error checking {key}: {e}")

            for key in closed:
                del open_signals[key]

            time.sleep(30)

        except KeyboardInterrupt:
            print("Bot stopped")
            break
        except Exception as e:
            print(f"Main loop error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    run()
