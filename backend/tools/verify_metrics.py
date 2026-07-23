#!/usr/bin/env python3
import json
import sys
import os
from pathlib import Path

# Ensure repo root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from backend.app.audio import metrics_engine as me


def main():
    if len(sys.argv) < 4:
        print("Usage: verify_metrics.py <trace.json> <metrics.json> <correlation_id>")
        return 2
    trace_path = Path(sys.argv[1])
    metrics_path = Path(sys.argv[2])
    corr_id = sys.argv[3]

    trace = me.load_trace(trace_path)
    # build by-corr using engine sorting
    events = me._sort_events(trace.get("events", []))
    from collections import defaultdict
    by_corr = defaultdict(list)
    for ev in events:
        c = ev.get("correlation_id") or ""
        if c:
            by_corr[c].append(ev)

    evs = by_corr.get(corr_id, [])
    if not evs:
        print(f"No events for correlation {corr_id}")
        return 1

    computed = me.compute_correlation_metrics(evs)
    reported_doc = json.load(open(metrics_path, "r", encoding="utf-8"))
    reported = reported_doc.get("per_correlation", {}).get(corr_id, {}).get("metrics")

    print("=== Computed metrics ===")
    print(json.dumps(computed, indent=2))
    print("\n=== Reported metrics ===")
    print(json.dumps(reported, indent=2))

    print("\n=== Differences (computed vs reported) ===")
    diffs = {}
    for k in set(list((computed or {}).keys()) + list((reported or {}).keys())):
        v1 = computed.get(k) if computed else None
        v2 = reported.get(k) if reported else None
        diffs[k] = {"computed": v1, "reported": v2}
    print(json.dumps(diffs, indent=2))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
