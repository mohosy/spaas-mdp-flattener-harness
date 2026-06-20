#!/usr/bin/env python3
"""
Synthetic manufacturing-test event generator for the SPaaS MDP flattener harness.

Publishes envelope messages to a Kafka topic. A message is an ENVELOPE that may
carry one or many events; the Flink flattener emits one output row per event.

Supports:
  * sustained rate (--rate msgs/sec) or burst (--rate 0)
  * malformed-record injection (--malformed-pct): non-JSON garbage OR a valid
    envelope whose event is missing a required field -> both must be quarantined
  * duplicate-event injection (--dup-pct): repeats an event within a single
    message -> exercises INTRA-MESSAGE dedup (by canonical_row_hash)

=============================================================================
 PLACEHOLDER SCHEMA -- REPLACE WITH REAL SCHEMA
 The event shape below is the synthetic `mdp.mfg.v1` placeholder. When the real
 `testMeasurementEvent` is gathered, change the shape HERE and the field mapping
 in config/field-mapping.yaml + processor MdpFieldMapping. No pipeline code in
 the Flink job needs to change -- it is driven by that mapping.
=============================================================================
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

# ----------------------------------------------------------------------------
# Synthetic value pools (PLACEHOLDER -- REPLACE WITH REAL SCHEMA)
# ----------------------------------------------------------------------------
STATIONS = ["EFT-07", "EFT-08", "FCT-01", "FCT-02", "BURNIN-03"]
MODELS = ["RX-5", "RX-5", "RX-9", "MX-2"]
PROGRAMS = ["FinalFunctional", "InCircuit", "BurnIn", "Calibration"]
MEASUREMENTS = [
    ("motor_torque_axis1", "Nm", 10.0, 15.0),
    ("motor_torque_axis2", "Nm", 9.0, 14.0),
    ("supply_voltage", "V", 23.5, 24.5),
    ("leak_rate", "sccm", 0.0, 2.5),
    ("encoder_offset", "deg", -0.5, 0.5),
]
OPERATORS = [f"op-{n}" for n in range(40, 50)]
SCHEMA_VERSION = "mdp.mfg.v1"

# Required-for-valid fields (mirror processor config; keep in sync with
# config/job.yaml -> transform.required_fields).
REQUIRED_EVENT_FIELDS = [
    "eventId",
    "productSerial",
    "measurementName",
    "measurementValue",
    "measuredAt",
]


def _iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def make_event(rng: random.Random, serial_seq: int, tag: str = None) -> dict:
    name, unit, lo, hi = rng.choice(MEASUREMENTS)
    # Mostly in-spec, occasionally out -> drives PASS/FAIL + realistic spread.
    span = hi - lo
    value = round(rng.uniform(lo - 0.15 * span, hi + 0.15 * span), 4)
    result = "PASS" if lo <= value <= hi else "FAIL"
    measured = datetime.now(timezone.utc)
    return {
        "eventId": str(uuid.uuid4()),
        "testStationId": rng.choice(STATIONS),
        "productModel": rng.choice(MODELS),
        "productSerial": f"SN-{serial_seq:06d}",
        "testProgram": rng.choice(PROGRAMS),
        "testSessionId": tag if tag else f"sess-{rng.randrange(16**6):06x}",
        "operatorId": rng.choice(OPERATORS),
        "measurementName": name,
        "measurementValue": value,
        "measurementUnit": unit,
        "lowerLimit": lo,
        "upperLimit": hi,
        "result": result,
        "measuredAt": _iso(measured),
    }


def make_envelope(rng: random.Random, events_min: int, events_max: int,
                  serial_base: int, dup_pct: float, tag: str = None):
    """Returns (envelope, dup_injected_count)."""
    n = rng.randint(events_min, events_max)
    events = [make_event(rng, serial_base + i, tag) for i in range(n)]
    # INTRA-MESSAGE duplicate injection: repeat an existing event in THIS message.
    dup_injected = 0
    if events and rng.random() < dup_pct / 100.0:
        events.append(json.loads(json.dumps(rng.choice(events))))
        dup_injected = 1
    envelope = {
        "messageId": str(uuid.uuid4()),
        "schemaVersion": SCHEMA_VERSION,
        "producedAt": _iso(datetime.now(timezone.utc)),
        "events": events,
    }
    return envelope, dup_injected


def corrupt(rng: random.Random, envelope: dict) -> bytes:
    """Return malformed payload bytes. Half: non-JSON garbage. Half: valid JSON
    but an event missing a required field. Both must land in quarantine."""
    if rng.random() < 0.5:
        return b'{"messageId": "' + str(uuid.uuid4()).encode() + b'", "events": [ THIS IS NOT JSON %%%'
    bad = envelope
    if bad["events"]:
        victim = rng.choice(bad["events"])
        victim.pop(rng.choice(REQUIRED_EVENT_FIELDS), None)
    return json.dumps(bad).encode("utf-8")


def build_producer(bootstrap: str):
    from confluent_kafka import Producer  # lazy import so --dry-run needs no deps
    return Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "spaas-mdp-generator",
        "linger.ms": 20,
        "acks": "all",
        "enable.idempotence": True,
    })


def main() -> int:
    ap = argparse.ArgumentParser(description="Synthetic MDP event generator")
    ap.add_argument("--bootstrap", default=os.environ.get("BOOTSTRAP", "localhost:29092"))
    ap.add_argument("--topic", default="mdp.mfg.raw")
    ap.add_argument("--count", type=int, default=100, help="number of messages")
    ap.add_argument("--rate", type=float, default=50.0, help="msgs/sec; 0 = burst")
    ap.add_argument("--events-min", type=int, default=1)
    ap.add_argument("--events-max", type=int, default=4)
    ap.add_argument("--malformed-pct", type=float, default=0.0)
    ap.add_argument("--dup-pct", type=float, default=0.0)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--tag", default=None,
                    help="fixed testSessionId for all events (for replay/freshness tests)")
    ap.add_argument("--dry-run", action="store_true", help="print messages, do not send")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    produced = malformed = dup_msgs = 0
    serial_base = rng.randrange(900000)
    producer = None if args.dry_run else build_producer(args.bootstrap)
    interval = 0.0 if args.rate <= 0 else 1.0 / args.rate

    print(f"[generator] topic={args.topic} count={args.count} rate={args.rate}/s "
          f"malformed={args.malformed_pct}% dup={args.dup_pct}% dry_run={args.dry_run}",
          file=sys.stderr)

    start = time.time()
    for i in range(args.count):
        env, dup_added = make_envelope(rng, args.events_min, args.events_max,
                                       serial_base + i * 10, args.dup_pct, args.tag)
        dup_msgs += dup_added
        if rng.random() < args.malformed_pct / 100.0:
            payload = corrupt(rng, env)
            malformed += 1
        else:
            payload = json.dumps(env).encode("utf-8")
        key = env["messageId"].encode()

        if args.dry_run:
            sys.stdout.write(payload.decode("utf-8", errors="replace") + "\n")
        else:
            producer.produce(args.topic, key=key, value=payload)
            if i % 500 == 0:
                producer.poll(0)
        produced += 1
        if interval:
            time.sleep(interval)

    if producer is not None:
        producer.flush(30)
    elapsed = time.time() - start
    print(f"[generator] done: produced={produced} malformed={malformed} "
          f"dup_messages={dup_msgs} elapsed={elapsed:.2f}s "
          f"({produced/elapsed:.0f} msg/s)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
