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


@app.get("/matrix-results/count")
def matrix_results_count(engine: str = None, stage: str = None, passed_only: bool = False):
    """Quick diagnostic — just returns a row count, no CSV generation, to verify data exists."""
    q = "select=id"
    if engine: q += f"&engine=eq.{engine}"
    if stage: q += f"&stage=eq.{stage}"
    if passed_only: q += "&passed_filter=eq.true"
    try:
        res = httpx.get(f"{SUPABASE_URL}/rest/v1/matrix_results?{q}",
                         headers={**HEADERS, "Prefer": "count=exact"}, timeout=30)
        count = res.headers.get("content-range", "?/0").split("/")[-1]
        return {"success": res.status_code==200, "status_code": res.status_code,
                "count": count, "query": q, "body_preview": res.text[:200]}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/matrix-results/test-thread")
def matrix_results_test_thread():
    """
    Diagnostic: runs the exact same batch write, but from INSIDE a background
    thread (matching how the real sweep executes), to isolate whether threading
    context itself is the problem.
    """
    import threading, time as time_mod

    result = {"done": False, "write_status": None, "write_body": None, "exception": None}

    def _threaded_write():
        try:
            rows = [{
                "pair": "THREADTEST", "timeframe": "15m", "engine": "diagnostic_thread", "stage": "test",
                "period_label": None, "period_start": "2025-01-01", "period_end": "2026-01-01",
                "params": {"i": i},
                "return_pct": 5.0, "cagr": 1.0, "max_dd": 5.0, "sharpe": 1.2,
                "profit_factor": 1.2, "win_rate": 50.0, "trades": 30, "wins": 15, "losses": 15,
                "avg_win": 1.0, "avg_loss": 1.0, "kelly_full": 1.0, "total_fees": 0.1,
                "passed_filter": True,
            } for i in range(50)]

            headers_runner = {
                "apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal,resolution=ignore-duplicates",
            }
            write_res = httpx.post(f"{SUPABASE_URL}/rest/v1/matrix_results", json=rows,
                                    headers=headers_runner, timeout=30)
            result["write_status"] = write_res.status_code
            result["write_body"] = write_res.text[:300]
        except Exception as e:
            result["exception"] = str(e)
        finally:
            result["done"] = True

    t = threading.Thread(target=_threaded_write, daemon=True)
    t.start()
    t.join(timeout=15)  # wait for the thread to finish (request stays open)

    time_mod.sleep(1)
    read_res = httpx.get(f"{SUPABASE_URL}/rest/v1/matrix_results?engine=eq.diagnostic_thread&select=id",
                          headers={**HEADERS,"Prefer":"count=exact"}, timeout=30)
    count = read_res.headers.get("content-range","?/0").split("/")[-1]

    try:
        httpx.delete(f"{SUPABASE_URL}/rest/v1/matrix_results?engine=eq.diagnostic_thread", headers=HEADERS, timeout=30)
    except: pass

    return {
        "thread_completed": result["done"],
        "write_status": result["write_status"],
        "write_body": result["write_body"],
        "exception": result["exception"],
        "rows_actually_persisted": count,
    }


@app.get("/matrix-results/test-batch")
def matrix_results_test_batch():
    """
    Diagnostic: writes a BATCH of 300 rows at once (matching the real sweep's
    batch size) using matrix_runner.py's exact headers, to see if batch writes
    behave differently than single-row writes.
    """
    import random
    rows = []
    for i in range(300):
        rows.append({
            "pair": "BATCHTEST", "timeframe": "15m", "engine": "diagnostic_batch", "stage": "test",
            "period_label": None, "period_start": "2025-01-01", "period_end": "2026-01-01",
            "params": {"i": i, "rr": round(random.uniform(1.0,3.0),2)},
            "return_pct": round(random.uniform(-10,50),2), "cagr": 1.0, "max_dd": round(random.uniform(0,30),2),
            "sharpe": round(random.uniform(0.5,2.5),2),
            "profit_factor": 1.2, "win_rate": 50.0, "trades": 30+i, "wins": 15, "losses": 15,
            "avg_win": 1.0, "avg_loss": 1.0, "kelly_full": 1.0, "total_fees": 0.1,
            "passed_filter": True,
        })

    headers_runner = {
        "apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=ignore-duplicates",
    }
    write_res = httpx.post(f"{SUPABASE_URL}/rest/v1/matrix_results", json=rows,
                            headers=headers_runner, timeout=30)

    read_res = httpx.get(f"{SUPABASE_URL}/rest/v1/matrix_results?engine=eq.diagnostic_batch&select=id",
                          headers={**HEADERS,"Prefer":"count=exact"}, timeout=30)
    count = read_res.headers.get("content-range","?/0").split("/")[-1]

    try:
        httpx.delete(f"{SUPABASE_URL}/rest/v1/matrix_results?engine=eq.diagnostic_batch", headers=HEADERS, timeout=30)
    except: pass

    return {
        "rows_sent": len(rows),
        "write_status": write_res.status_code,
        "write_body": write_res.text[:500],
        "rows_actually_persisted": count,
    }


@app.get("/matrix-results/test-write")
def matrix_results_test_write():
    """
    Diagnostic: writes ONE test row using matrix_runner.py's EXACT headers
    (return=minimal,resolution=ignore-duplicates), then reads it back.
    Isolates whether the Prefer header combo is the actual culprit.
    """
    test_row = {
        "pair": "TESTPAIR", "timeframe": "15m", "engine": "diagnostic_test", "stage": "test",
        "period_label": None, "period_start": "2025-01-01", "period_end": "2026-01-01",
        "params": {"test": True, "n": 1},
        "return_pct": 1.0, "cagr": 1.0, "max_dd": 1.0, "sharpe": 1.0,
        "profit_factor": 1.0, "win_rate": 50.0, "trades": 30, "wins": 15, "losses": 15,
        "avg_win": 1.0, "avg_loss": 1.0, "kelly_full": 1.0, "total_fees": 0.1,
        "passed_filter": True,
    }

    # Test A: main.py's working headers (return=representation override)
    headers_a = {**HEADERS, "Prefer": "return=representation"}
    write_a = httpx.post(f"{SUPABASE_URL}/rest/v1/matrix_results", json=[test_row],
                          headers=headers_a, timeout=30)

    # Test B: matrix_runner.py's EXACT headers (the ones actually used in the real sweep)
    headers_b = {
        "apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=ignore-duplicates",
    }
    test_row_b = dict(test_row); test_row_b["engine"] = "diagnostic_test_b"
    write_b = httpx.post(f"{SUPABASE_URL}/rest/v1/matrix_results", json=[test_row_b],
                          headers=headers_b, timeout=30)

    read_a = httpx.get(f"{SUPABASE_URL}/rest/v1/matrix_results?engine=eq.diagnostic_test&select=*", headers=HEADERS, timeout=30)
    read_b = httpx.get(f"{SUPABASE_URL}/rest/v1/matrix_results?engine=eq.diagnostic_test_b&select=*", headers=HEADERS, timeout=30)

    # Cleanup
    try:
        httpx.delete(f"{SUPABASE_URL}/rest/v1/matrix_results?engine=eq.diagnostic_test", headers=HEADERS, timeout=30)
        httpx.delete(f"{SUPABASE_URL}/rest/v1/matrix_results?engine=eq.diagnostic_test_b", headers=HEADERS, timeout=30)
    except: pass

    return {
        "test_A_return_representation": {
            "write_status": write_a.status_code, "write_body": write_a.text[:300],
            "read_status": read_a.status_code, "persisted": read_a.status_code==200 and len(read_a.json())>0,
        },
        "test_B_return_minimal_matrix_runner_headers": {
            "write_status": write_b.status_code, "write_body": write_b.text[:300],
            "read_status": read_b.status_code, "persisted": read_b.status_code==200 and len(read_b.json())>0,
        },
    }


@app.get("/matrix-results/export")
def matrix_results_export(engine: str = None, stage: str = None, passed_only: bool = False):
    """Export results as CSV. Set passed_only=true to only get validated combos."""
    from fastapi.responses import Response, JSONResponse
    import io, csv as csv_mod

    q = "select=*&order=pair.asc,timeframe.asc"
    if engine: q += f"&engine=eq.{engine}"
    if stage: q += f"&stage=eq.{stage}"
    if passed_only: q += "&passed_filter=eq.true"

    all_rows = []; offset = 0
    last_error = None
    while True:
        page_q = q + f"&limit=1000&offset={offset}"
        res = httpx.get(f"{SUPABASE_URL}/rest/v1/matrix_results?{page_q}", headers=HEADERS, timeout=60)
        if res.status_code != 200:
            last_error = f"Supabase returned {res.status_code}: {res.text[:500]}"
            break
        batch = res.json()
        if not batch: break
        all_rows += batch
        if len(batch) < 1000: break
        offset += len(batch)

    if not all_rows:
        return JSONResponse({
            "success": False,
            "message": "No rows found or query failed",
            "error": last_error,
            "query_used": q,
        })

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

    all_rows = []; offset = 0; last_error = None
    while True:
        page_q = q + f"&limit=1000&offset={offset}"
        res = httpx.get(f"{SUPABASE_URL}/rest/v1/matrix_results?{page_q}", headers=HEADERS, timeout=60)
        if res.status_code != 200:
            last_error = f"Supabase {res.status_code}: {res.text[:300]}"
            break
        batch = res.json()
        if not batch: break
        all_rows += batch
        if len(batch) < 1000: break
        offset += len(batch)

    if not all_rows:
        return {"success": False, "message": "No rows found", "error": last_error, "query_used": q}

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
