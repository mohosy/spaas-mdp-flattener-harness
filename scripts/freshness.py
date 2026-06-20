#!/usr/bin/env python3
"""
End-to-end freshness probe: Kafka -> Flink -> Iceberg -> queryable-in-Trino.

Produces a burst of uniquely-tagged events, records each event's produce time,
then polls Trino until each event is visible in mdp.flattened_measurements,
recording first-visible wall-clock time. Latency = visible_time - produced_time.
Reports P50/P95/P99 and writes demo-api/.freshness.json for GET /freshness.

Requires the stack up (make up) and the flattener job running (make submit-job).
Usage: make freshness N=300
"""
from __future__ import annotations

import argparse
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import trino
from confluent_kafka import Producer


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def percentile(sorted_vals, pct):
    if not sorted_vals:
        return None
    k = (len(sorted_vals) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = k - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:29092")
    ap.add_argument("--topic", default="mdp.mfg.raw")
    ap.add_argument("--trino-host", default="localhost")
    ap.add_argument("--trino-port", type=int, default=8080)
    ap.add_argument("--count", type=int, default=300)
    ap.add_argument("--timeout", type=float, default=180.0)
    args = ap.parse_args()

    run_id = uuid.uuid4().hex[:10]
    marker = f"fresh-{run_id}"
    print(f"[freshness] run={marker} count={args.count}")

    # --- produce a tagged burst, recording produce time per event ---
    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 5, "acks": "all"})
    produced_at: dict[str, float] = {}
    for i in range(args.count):
        event_id = f"{marker}-{i:05d}"
        now = time.time()
        envelope = {
            "messageId": str(uuid.uuid4()),
            "schemaVersion": "mdp.mfg.v1",
            "producedAt": iso(now),
            "events": [{
                "eventId": event_id,
                "testStationId": "EFT-07", "productModel": "RX-5",
                "productSerial": f"SN-{i:06d}", "testProgram": "Freshness",
                "testSessionId": marker, "operatorId": "op-freshness",
                "measurementName": "freshness_probe", "measurementValue": float(i),
                "measurementUnit": "ms", "lowerLimit": 0.0, "upperLimit": 1e9,
                "result": "PASS", "measuredAt": iso(now),
            }],
        }
        producer.produce(args.topic, key=envelope["messageId"].encode(),
                         value=json.dumps(envelope).encode())
        produced_at[event_id] = now
        if i % 500 == 0:
            producer.poll(0)
    producer.flush(30)
    print(f"[freshness] produced {len(produced_at)} events, polling Trino...")

    # --- poll Trino until all are visible, recording first-seen time ---
    conn = trino.dbapi.connect(host=args.trino_host, port=args.trino_port,
                               user="freshness", catalog="iceberg", schema="mdp")
    visible_at: dict[str, float] = {}
    deadline = time.time() + args.timeout
    while len(visible_at) < args.count and time.time() < deadline:
        cur = conn.cursor()
        cur.execute(
            "SELECT event_id FROM iceberg.mdp.flattened_measurements WHERE test_session_id = ?",
            (marker,),
        )
        seen_now = time.time()
        for (event_id,) in cur.fetchall():
            if event_id not in visible_at and event_id in produced_at:
                visible_at[event_id] = seen_now
        if len(visible_at) < args.count:
            time.sleep(0.5)

    latencies = sorted(visible_at[e] - produced_at[e] for e in visible_at)
    result = {
        "run_id": marker,
        "produced": len(produced_at),
        "visible": len(visible_at),
        "timed_out": len(visible_at) < args.count,
        "latency_seconds": {
            "p50": round(percentile(latencies, 50), 3) if latencies else None,
            "p95": round(percentile(latencies, 95), 3) if latencies else None,
            "p99": round(percentile(latencies, 99), 3) if latencies else None,
            "min": round(latencies[0], 3) if latencies else None,
            "max": round(latencies[-1], 3) if latencies else None,
            "mean": round(sum(latencies) / len(latencies), 3) if latencies else None,
        },
        "measured_at": iso(time.time()),
    }

    out_file = Path(__file__).resolve().parents[1] / "demo-api" / ".freshness.json"
    out_file.write_text(json.dumps(result, indent=2))

    print("\n===== FRESHNESS (Kafka -> queryable) =====")
    print(f"  visible:  {result['visible']}/{result['produced']}"
          + ("  (TIMED OUT)" if result["timed_out"] else ""))
    s = result["latency_seconds"]
    print(f"  P50: {s['p50']}s   P95: {s['p95']}s   P99: {s['p99']}s")
    print(f"  min: {s['min']}s   max: {s['max']}s   mean: {s['mean']}s")
    print(f"  written: {out_file}")
    return 0 if latencies and not result["timed_out"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
