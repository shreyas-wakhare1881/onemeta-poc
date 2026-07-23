Developer utilities for validating traces and metrics.

Files:

- `inspect_trace.py`
  - Inspect incomplete correlations and summarize per-correlation events.

- `verify_metrics.py`
  - Verify metrics for a single correlation comparing `session_trace.json` vs `metrics.json`.

- `verify_all_metrics.py`
  - Verify metrics for all correlations and produce `metrics_verification_report.json`.

Usage examples:

From repository root (with virtualenv active):

```powershell
python backend/tools/verify_all_metrics.py \
  output/<session_dir>/session_trace.json \
  output/<session_dir>/metrics.json
```
