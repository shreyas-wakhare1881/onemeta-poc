#!/usr/bin/env python3
import json
import sys
import os
import math
from pathlib import Path

# Ensure repo root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from backend.app.audio import metrics_engine as me


def _is_number(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _equal(a, b, rel_tol=1e-6, abs_tol=1e-6):
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if _is_number(a) and _is_number(b):
        try:
            return math.isclose(float(a), float(b), rel_tol=rel_tol, abs_tol=abs_tol)
        except Exception:
            return False
    return a == b


def main():
    if len(sys.argv) < 3:
        print("Usage: verify_all_metrics.py <session_trace.json> <metrics.json> [out_report.json]")
        return 2

    trace_path = Path(sys.argv[1])
    metrics_path = Path(sys.argv[2])
    out_path = Path(sys.argv[3]) if len(sys.argv) > 3 else metrics_path.parent / "metrics_verification_report.json"

    if not trace_path.exists():
        print(f"Trace not found: {trace_path}")
        return 2
    if not metrics_path.exists():
        print(f"Metrics not found: {metrics_path}")
        return 2

    trace = me.load_trace(trace_path)
    reported = json.load(open(metrics_path, "r", encoding="utf-8"))

    # Recompute session metrics using the engine's public API
    recomputed = me.compute_session_metrics(trace)

    reported_per = reported.get("per_correlation", {}) or {}
    recomputed_per = recomputed.get("per_correlation", {}) or {}

    all_corrs = sorted(set(list(reported_per.keys()) + list(recomputed_per.keys())))

    details = {}
    matched = 0
    mismatched = 0

    for cid in all_corrs:
        comp_metrics = recomputed_per.get(cid, {}).get("metrics") if cid in recomputed_per else None
        rep_metrics = reported_per.get(cid, {}).get("metrics") if cid in reported_per else None

        keys = set()
        if comp_metrics:
            keys.update(comp_metrics.keys())
        if rep_metrics:
            keys.update(rep_metrics.keys())

        diffs = {}
        for k in sorted(keys):
            v_comp = comp_metrics.get(k) if comp_metrics else None
            v_rep = rep_metrics.get(k) if rep_metrics else None
            if not _equal(v_comp, v_rep):
                diffs[k] = {"computed": v_comp, "reported": v_rep}

        ok = len(diffs) == 0
        details[cid] = {"matched": ok, "diffs": diffs}
        if ok:
            matched += 1
        else:
            mismatched += 1

    report = {
        "total_correlations_reported": len(reported_per),
        "total_correlations_computed": len(recomputed_per),
        "total_checked_correlations": len(all_corrs),
        "matched": matched,
        "mismatched": mismatched,
        "percent_matched": float(matched) / max(1, len(all_corrs)) * 100.0,
        "details": details,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"Verification complete — matched={matched}, mismatched={mismatched}, report={out_path}")
    return (0 if mismatched == 0 else 1)


if __name__ == "__main__":
    raise SystemExit(main())
