# ============================================================
# FIB BACKTEST API — FastAPI Backend v2.0 — clean rebuild
# Single-backtest tab removed (unused). Diagnostic one-offs
# removed. Kept: matrix runner trigger/status/results,
# paper trade journal, candle prefetch, candle export.
# ============================================================

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os, httpx, threading, importlib.util

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=ignore-duplicates",
}

_matrix_thread = None
_matrix_running = False

# ── ROUTES ────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "ok", "service": "fib-backtest-api v2.0"}


@app.api_route("/run-matrix", methods=["GET","POST"])
def run_matrix(engine: str = "bos", stage: str = "sweep"):
    """
    Trigger a matrix run.
    engine: bos | ema | div | all
    stage:  sweep | stability | monthly
    """
    global _matrix_thread, _matrix_running
    if _matrix_running:
        return {"success": False, "message": "Already running"}

    def _run_compute():
        global _matrix_running
        _matrix_running = True
        try:
            spec = importlib.util.spec_from_file_location(
                "matrix_runner",
                os.path.join(os.path.dirname(__file__), "matrix_runner.py")
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            mod.main_compute(engine, stage)
        except Exception as e:
            print(f"Compute error: {e}")
        finally:
            _matrix_running = False

    _matrix_thread = threading.Thread(target=_run_compute, daemon=True)
    _matrix_thread.start()
    return {"success": True, "message": f"Started engine={engine} stage={stage} — check Telegram for progress"}


@app.post("/prefetch-candles")
def prefetch_candles():
    global _matrix_thread, _matrix_running
    if _matrix_running:
        return {"success": False, "message": "A job is already running"}

    def _run_prefetch():
        global _matrix_running
        _matrix_running = True
        try:
            spec = importlib.util.spec_from_file_location(
                "matrix_runner",
                os.path.join(os.path.dirname(__file__), "matrix_runner.py")
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            mod.main_prefetch()
        except Exception as e:
            print(f"Prefetch error: {e}")
        finally:
            _matrix_running = False

    _matrix_thread = threading.Thread(target=_run_prefetch, daemon=True)
    _matrix_thread.start()
    return {"success": True, "message": "Prefetch started — check Telegram for progress"}


@app.get("/matrix-status")
def matrix_status():
    try:
        res = httpx.get(f"{SUPABASE_URL}/rest/v1/matrix_status?id=eq.1&select=*",
                         headers=HEADERS, timeout=10)
        if res.status_code == 200 and res.json():
            row = res.json()[0]
            return {"success": True, "status": row.get("status",""),
                    "completed": row.get("completed",0), "total": row.get("total",0),
                    "phase": row.get("detail",""), "is_running": _matrix_running}
        return {"success": False, "status": "not_started", "completed": 0,
                "total": 0, "phase": "", "is_running": _matrix_running}
    except Exception as e:
        return {"success": False, "error": str(e), "is_running": _matrix_running}


@app.get("/matrix-results")
def matrix_results(engine: str = None, stage: str = None, passed_only: bool = True, limit: int = 1000):
    """
    Query results from the new clean schema.
    Defaults to only passed_filter=true rows — pass passed_only=false to see everything.
    """
    try:
        q = "select=*&order=sharpe.desc"
        if engine: q += f"&engine=eq.{engine}"
        if stage: q += f"&stage=eq.{stage}"
        if passed_only: q += "&passed_filter=eq.true"
        q += f"&limit={limit}"
        res = httpx.get(f"{SUPABASE_URL}/rest/v1/matrix_results?{q}", headers=HEADERS, timeout=30)
        if res.status_code == 200:
            return {"success": True, "count": len(res.json()), "results": res.json()}
        return {"success": False, "error": res.text}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/matrix-results/export")
def matrix_results_export(engine: str = None, stage: str = None, passed_only: bool = False):
    """Export results as CSV. Set passed_only=true to only get validated combos."""
    from fastapi.responses import Response
    import io, csv as csv_mod

    q = "select=*&order=pair.asc,timeframe.asc"
    if engine: q += f"&engine=eq.{engine}"
    if stage: q += f"&stage=eq.{stage}"
    if passed_only: q += "&passed_filter=eq.true"

    all_rows = []; offset = 0
    while True:
        page_q = q + f"&limit=1000&offset={offset}"
        res = httpx.get(f"{SUPABASE_URL}/rest/v1/matrix_results?{page_q}", headers=HEADERS, timeout=60)
        if res.status_code != 200: break
        batch = res.json()
        if not batch: break
        all_rows += batch
        if len(batch) < 1000: break
        offset += len(batch)

    fields = ["pair","timeframe","engine","stage","period_label","period_start","period_end",
              "params","return_pct","cagr","max_dd","sharpe","profit_factor","win_rate",
              "trades","wins","losses","avg_win","avg_loss","kelly_full","total_fees",
              "passed_filter","computed_at"]

    buf = io.StringIO()
    writer = csv_mod.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for row in all_rows:
        writer.writerow(row)

    return Response(content=buf.getvalue(), media_type="text/csv",
                     headers={"Content-Disposition": "attachment; filename=matrix_results.csv"})


@app.post("/send-matrix-csv")
def send_matrix_csv(engine: str = None, stage: str = None, passed_only: bool = False):
    """Send the results CSV directly to Telegram instead of downloading via browser."""
    import io, csv as csv_mod

    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN","")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID","")

    q = "select=*&order=pair.asc,timeframe.asc"
    if engine: q += f"&engine=eq.{engine}"
    if stage: q += f"&stage=eq.{stage}"
    if passed_only: q += "&passed_filter=eq.true"

    all_rows = []; offset = 0
    while True:
        page_q = q + f"&limit=1000&offset={offset}"
        res = httpx.get(f"{SUPABASE_URL}/rest/v1/matrix_results?{page_q}", headers=HEADERS, timeout=60)
        if res.status_code != 200: break
        batch = res.json()
        if not batch: break
        all_rows += batch
        if len(batch) < 1000: break
        offset += len(batch)

    fields = ["pair","timeframe","engine","stage","period_label","period_start","period_end",
              "params","return_pct","cagr","max_dd","sharpe","profit_factor","win_rate",
              "trades","wins","losses","avg_win","avg_loss","kelly_full","total_fees",
              "passed_filter","computed_at"]

    buf = io.StringIO()
    writer = csv_mod.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for row in all_rows:
        writer.writerow(row)

    try:
        files = {"document": ("matrix_results.csv", buf.getvalue(), "text/csv")}
        data = {"chat_id": TELEGRAM_CHAT_ID, "caption": f"✅ Matrix CSV — {len(all_rows)} rows"}
        httpx.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
                   data=data, files=files, timeout=30)
        return {"success": True, "rows_sent": len(all_rows)}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/journal")
def journal():
    """Paper trading account + recent trades."""
    try:
        acc_res = httpx.get(f"{SUPABASE_URL}/rest/v1/paper_account?id=eq.1&select=*",
                             headers=HEADERS, timeout=10)
        trades_res = httpx.get(f"{SUPABASE_URL}/rest/v1/paper_trades?select=*&order=created_at.desc&limit=50",
                                headers=HEADERS, timeout=10)
        account = acc_res.json()[0] if acc_res.status_code==200 and acc_res.json() else None
        trades = trades_res.json() if trades_res.status_code==200 else []
        return {"success": True, "account": account, "trades": trades}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/journal/by-label")
def journal_by_label():
    """Per-config performance breakdown — the post-mortem view."""
    try:
        res = httpx.get(f"{SUPABASE_URL}/rest/v1/paper_trades?select=*", headers=HEADERS, timeout=30)
        if res.status_code != 200:
            return {"success": False, "error": res.text}
        trades = res.json()
        by_label = {}
        for t in trades:
            label = t.get("label","unknown")
            if label not in by_label:
                by_label[label] = {"engine": t.get("engine",""), "trades":0, "wins":0, "losses":0, "total_pnl":0.0}
            by_label[label]["trades"] += 1
            by_label[label]["wins"] += 1 if t.get("won") else 0
            by_label[label]["losses"] += 0 if t.get("won") else 1
            by_label[label]["total_pnl"] += t.get("pnl",0) or 0
        for label in by_label:
            d = by_label[label]
            d["win_rate"] = round(d["wins"]/d["trades"]*100,1) if d["trades"]>0 else 0
            d["total_pnl"] = round(d["total_pnl"],4)
        return {"success": True, "by_label": by_label}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/candles")
def get_candles_endpoint(symbol: str, timeframe: str, limit: int = 500):
    try:
        all_rows = []; offset = 0; page = 1000
        while len(all_rows) < limit:
            query = (f"symbol=eq.{symbol}&timeframe=eq.{timeframe}"
                     f"&order=ts.desc&limit={page}&offset={offset}&select=ts,open,high,low,close,volume")
            res = httpx.get(f"{SUPABASE_URL}/rest/v1/candles?{query}", headers=HEADERS, timeout=30)
            if res.status_code != 200: break
            rows = res.json()
            if not rows: break
            all_rows += rows
            if len(rows) < page: break
            offset += page
        return {"success": True, "candles": all_rows[:limit], "count": len(all_rows[:limit])}
    except Exception as e:
        return {"success": False, "error": str(e), "candles": []}
