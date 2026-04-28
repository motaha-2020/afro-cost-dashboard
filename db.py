"""
db.py — SQLite persistence layer for Afro Cost Analysis
========================================================

Public API
----------
    init_db()                 — create tables if they don't exist
    save_data(df)             — upsert a cleaned DataFrame, return new-row count
    load_data()               — return full cost_data as a DataFrame
    get_last_sync()           — info about the most recent sync run
    log_sync_start()          — open a sync_log entry, return its id
    log_sync_end(id, n, err)  — close a sync_log entry
"""

import logging
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "data" / "afro_cost.db"

# Canonical mapping: cleaned DataFrame column → DB column
_COL_MAP = {
    "JE Date":     "je_date",
    "JE No.":      "je_no",
    "Project":     "project",
    "Account":     "account",
    "item":        "item",
    "Category":    "category",
    "Debit":       "debit",
    "Credit":      "credit",
    "Cost amount": "cost_amount",
    "Year":        "year",
    "month":       "month",
    "Quarter":     "quarter",
    "%":           "pct",
}
_COL_MAP_INV = {v: k for k, v in _COL_MAP.items()}


# ── Connection ────────────────────────────────────────────────────────────────

def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")   # allow concurrent reads from dashboard
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ── Schema ────────────────────────────────────────────────────────────────────

def init_db() -> None:
    """Create tables if they don't exist. Safe to call repeatedly."""
    with _connect() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS cost_data (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                je_date     TEXT,
                je_no       TEXT,
                project     TEXT,
                account     TEXT,
                item        TEXT,
                category    TEXT,
                debit       REAL,
                credit      REAL,
                cost_amount REAL,
                year        INTEGER,
                month       TEXT,
                quarter     TEXT,
                pct         TEXT,
                scrape_date TEXT,
                UNIQUE(je_no, project, je_date, debit, credit)
            );

            CREATE INDEX IF NOT EXISTS idx_cost_account  ON cost_data(account);
            CREATE INDEX IF NOT EXISTS idx_cost_category ON cost_data(category);
            CREATE INDEX IF NOT EXISTS idx_cost_date     ON cost_data(je_date);

            CREATE TABLE IF NOT EXISTS sync_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at  TEXT,
                finished_at TEXT,
                status      TEXT,
                rows_added  INTEGER,
                date_from   TEXT,
                date_to     TEXT,
                error_msg   TEXT
            );
        """)
    log.info("DB initialised at %s", DB_PATH)


# ── Write ─────────────────────────────────────────────────────────────────────

def save_data(df: pd.DataFrame) -> int:
    """
    Persist a cleaned DataFrame.  Only inserts rows whose
    (je_no, project, je_date, debit, credit) key does not already exist.

    Returns the number of newly inserted rows.
    """
    if df.empty:
        return 0

    # Rename to DB columns and keep only known columns
    out = df.rename(columns=_COL_MAP).copy()
    out = out[[c for c in _COL_MAP.values() if c in out.columns]]

    # Normalise je_date to ISO text
    if "je_date" in out.columns:
        out["je_date"] = pd.to_datetime(out["je_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Normalise year: pandas Int64 → plain Python int (sqlite3 can't store pd.NA)
    if "year" in out.columns:
        out["year"] = out["year"].astype(object).where(out["year"].notna(), None)

    out["scrape_date"] = datetime.now().isoformat()

    # Load existing keys to avoid hitting the UNIQUE constraint
    with _connect() as conn:
        existing_rows = conn.execute(
            "SELECT je_no, project, je_date, debit, credit FROM cost_data"
        ).fetchall()

    existing = set(existing_rows)

    def _is_new(row) -> bool:
        key = (
            row.get("je_no"),
            row.get("project"),
            str(row.get("je_date") or ""),
            row.get("debit"),
            row.get("credit"),
        )
        return key not in existing

    new_rows = out[out.apply(_is_new, axis=1)]

    if new_rows.empty:
        log.info("DB save: 0 new rows (all already present)")
        return 0

    with _connect() as conn:
        new_rows.to_sql("cost_data", conn, if_exists="append", index=False)

    log.info("DB save: %d new rows inserted", len(new_rows))
    return len(new_rows)


# ── Read ──────────────────────────────────────────────────────────────────────

def load_data() -> pd.DataFrame:
    """
    Return all cost_data rows as a cleaned DataFrame with the same column
    names used by cleaner.py so the dashboard works without modification.
    """
    with _connect() as conn:
        df = pd.read_sql(
            "SELECT * FROM cost_data ORDER BY je_date, je_no",
            conn,
        )

    if df.empty:
        return df

    df = df.drop(columns=["id", "scrape_date"], errors="ignore")
    df = df.rename(columns=_COL_MAP_INV)
    df["JE Date"] = pd.to_datetime(df["JE Date"], errors="coerce")

    # Restore Int64 for Year
    if "Year" in df.columns:
        df["Year"] = pd.array(df["Year"], dtype="Int64")

    return df


def row_count() -> int:
    """Quick count of rows in cost_data."""
    with _connect() as conn:
        return conn.execute("SELECT COUNT(*) FROM cost_data").fetchone()[0]


# ── Sync log ──────────────────────────────────────────────────────────────────

def log_sync_start(date_from: str = "", date_to: str = "") -> int:
    """Open a sync_log entry, return its id."""
    with _connect() as conn:
        cur = conn.execute(
            "INSERT INTO sync_log (started_at, status, date_from, date_to) VALUES (?,?,?,?)",
            (datetime.now().isoformat(), "running", date_from, date_to),
        )
        return cur.lastrowid


def log_sync_end(sync_id: int, rows_added: int, error: str = None) -> None:
    """Close a sync_log entry."""
    with _connect() as conn:
        conn.execute(
            """UPDATE sync_log
               SET finished_at=?, status=?, rows_added=?, error_msg=?
               WHERE id=?""",
            (
                datetime.now().isoformat(),
                "error" if error else "success",
                rows_added,
                error,
                sync_id,
            ),
        )


def get_last_sync() -> dict:
    """Return a dict describing the most recent sync run."""
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM sync_log ORDER BY id DESC LIMIT 1"
        ).fetchone()
        total = conn.execute("SELECT COUNT(*) FROM cost_data").fetchone()[0]

    if row is None:
        return {"status": "never", "total_rows": total}

    cols = ["id", "started_at", "finished_at", "status",
            "rows_added", "date_from", "date_to", "error_msg"]
    return {**dict(zip(cols, row)), "total_rows": total}
