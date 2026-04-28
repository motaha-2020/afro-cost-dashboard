"""
api.py — FastAPI backend for Afro Cost scraping pipeline
=========================================================

Endpoints
---------
    GET  /               — serve the HTML control page
    GET  /health         — liveness check
    POST /scrape         — trigger a new scrape run (runs in background thread)
    GET  /scrape/status  — current scrape state + last DB sync info
    GET  /data/summary   — quick stats (row count, last sync) for the HTML page

Run locally:
    uvicorn api:app --reload --port 8000

Environment variables
---------------------
    AFRO_USER    ERP username       (default: motaha)
    AFRO_PASS    ERP password
    AFRO_ACCESS  ERP access code
"""

import logging
import os
import threading
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

app = FastAPI(title="Afro Cost API", version="1.0")


# ── Scrape state (in-memory, single instance) ─────────────────────────────────

_state: dict = {
    "running":    False,
    "started_at": None,
    "message":    "Idle — no scrape has run yet.",
    "error":      None,
}
_lock = threading.Lock()


# ── Request schema ────────────────────────────────────────────────────────────

class ScrapeRequest(BaseModel):
    username:    str = os.environ.get("AFRO_USER", "motaha")
    password:    str = os.environ.get("AFRO_PASS", "")
    access_code: str = os.environ.get("AFRO_ACCESS", "")
    date_from:   str = "01/01/2026"
    date_to:     str = "02/28/2026"


# ── Background worker ─────────────────────────────────────────────────────────

def _do_scrape(req: ScrapeRequest) -> None:
    """Runs in a daemon thread so the HTTP response returns immediately."""
    sync_id = db.log_sync_start(req.date_from, req.date_to)

    def _set(msg: str, error: str = None, done: bool = False):
        with _lock:
            _state["message"] = msg
            _state["error"]   = error
            if done:
                _state["running"] = False

    try:
        _set("Step 1/3 — Logging into ERP …")
        from scraper import scrape_cost_data

        _set("Step 2/3 — Scraping project data (this takes ~10 min) …")
        raw = scrape_cost_data(
            date_from   = req.date_from,
            date_to     = req.date_to,
            username    = req.username,
            password    = req.password,
            access_code = req.access_code,
        )

        _set("Step 3/3 — Cleaning & saving to database …")
        from cleaner import clean_cost_data
        cleaned = clean_cost_data(raw)
        rows    = db.save_data(cleaned)
        db.log_sync_end(sync_id, rows)

        ts  = datetime.now().strftime("%Y-%m-%d %H:%M")
        _set(f"Done ✓ — {rows:,} new rows saved at {ts}.", done=True)
        log.info("Scrape complete: %d new rows", rows)

    except Exception as exc:
        db.log_sync_end(sync_id, 0, str(exc))
        _set(f"Failed — {exc}", error=str(exc), done=True)
        log.error("Scrape failed: %s", exc)


# ── Startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
def _startup():
    db.init_db()
    log.info("API ready — DB at %s", db.DB_PATH)


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "db_rows": db.row_count()}


@app.post("/scrape")
def start_scrape(req: ScrapeRequest):
    """Trigger a scrape run. Returns 409 if one is already running."""
    if not req.password:
        raise HTTPException(400, "password is required")
    if not req.access_code:
        raise HTTPException(400, "access_code is required")

    with _lock:
        if _state["running"]:
            raise HTTPException(409, "A scrape is already in progress.")
        _state["running"]    = True
        _state["started_at"] = datetime.now().isoformat()
        _state["message"]    = "Starting …"
        _state["error"]      = None

    t = threading.Thread(target=_do_scrape, args=(req,), daemon=True)
    t.start()
    return {"status": "started", "date_from": req.date_from, "date_to": req.date_to}


@app.get("/scrape/status")
def scrape_status():
    """Current in-memory scrape state + last DB sync record."""
    with _lock:
        state = dict(_state)
    state["last_sync"] = db.get_last_sync()
    return state


@app.get("/data/summary")
def data_summary():
    """Quick DB stats for the HTML control page."""
    return db.get_last_sync()


# ── Static files & HTML page ──────────────────────────────────────────────────

_static_dir = Path(__file__).parent / "static"
_static_dir.mkdir(exist_ok=True)

app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")


@app.get("/")
def index():
    html = _static_dir / "index.html"
    if not html.exists():
        return {"error": "index.html not found — check the static/ folder."}
    return FileResponse(str(html))
