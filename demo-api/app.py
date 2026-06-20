"""
SPaaS MDP harness — demo API.

Reads the live Iceberg tables THROUGH TRINO (the swappable external query engine).
Pointing this at real Snowflake later is a connection change here, not a processor
change. Endpoints:

  GET /            -> service info + endpoint list
  GET /health      -> Trino connectivity check
  GET /latest      -> recent flattened measurements
  GET /freshness   -> latest freshness percentiles (from `make freshness`) + live lag
  GET /quarantine  -> recent rejected records
  GET /audit       -> recent run audit records

Run: make demo   (http://localhost:8000, docs at /docs)
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query
import trino

TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")
SCHEMA = os.environ.get("TRINO_SCHEMA", "mdp")
FRESHNESS_FILE = Path(os.environ.get("FRESHNESS_FILE", str(Path(__file__).with_name(".freshness.json"))))

app = FastAPI(title="SPaaS MDP Harness Demo API", version="0.1.0")


def connect():
    return trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT, user="demo-api",
        catalog=CATALOG, schema=SCHEMA,
    )


def query(sql: str, params: Optional[tuple] = None) -> list[dict[str, Any]]:
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or None)
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _table_exists(name: str) -> bool:
    rows = query(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = ? AND table_name = ?",
        (SCHEMA, name),
    )
    return bool(rows)


def _safe_select(table: str, order_col: str, limit: int) -> list[dict]:
    if not _table_exists(table):
        return []
    # order_col/table are server-controlled identifiers, not user input.
    return query(f"SELECT * FROM {CATALOG}.{SCHEMA}.{table} ORDER BY {order_col} DESC LIMIT {int(limit)}")


@app.get("/")
def root():
    return {
        "service": "SPaaS MDP flattener harness — demo API",
        "query_engine": f"Trino @ {TRINO_HOST}:{TRINO_PORT} (catalog={CATALOG}, schema={SCHEMA})",
        "note": "Trino stands in for Snowflake; same Iceberg tables via the same REST catalog.",
        "endpoints": ["/health", "/latest", "/freshness", "/quarantine", "/audit", "/docs"],
    }


@app.get("/health")
def health():
    try:
        query("SELECT 1")
        return {"status": "ok", "trino": f"{TRINO_HOST}:{TRINO_PORT}"}
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"trino unreachable: {e}")


@app.get("/latest")
def latest(limit: int = Query(20, ge=1, le=500)):
    rows = _safe_select("flattened_measurements", "processed_at", limit)
    return {"count": len(rows), "rows": json.loads(json.dumps(rows, default=str))}


@app.get("/quarantine")
def quarantine(limit: int = Query(20, ge=1, le=500)):
    rows = _safe_select("quarantine", "ingested_at", limit)
    return {"count": len(rows), "rows": json.loads(json.dumps(rows, default=str))}


@app.get("/audit")
def audit(limit: int = Query(20, ge=1, le=200)):
    rows = _safe_select("audit", "commit_timestamp", limit)
    return {"count": len(rows), "rows": json.loads(json.dumps(rows, default=str))}


@app.get("/freshness")
def freshness():
    # Latest measured percentiles, if `make freshness` has been run.
    measured = None
    if FRESHNESS_FILE.exists():
        try:
            measured = json.loads(FRESHNESS_FILE.read_text())
        except Exception:  # noqa: BLE001
            measured = None
    # Live lag: how stale is the newest row right now (seconds).
    live_lag = None
    if _table_exists("flattened_measurements"):
        rows = query(
            f"SELECT to_unixtime(current_timestamp) - to_unixtime(max(processed_at)) AS lag_s "
            f"FROM {CATALOG}.{SCHEMA}.flattened_measurements"
        )
        if rows and rows[0].get("lag_s") is not None:
            live_lag = round(float(rows[0]["lag_s"]), 3)
    return {
        "measured": measured or "run `make freshness` to populate latency percentiles",
        "live_lag_seconds": live_lag,
    }
