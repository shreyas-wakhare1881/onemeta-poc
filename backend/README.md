Backend (FastAPI)

Run locally:

```bash
python -m pip install -r backend/requirements.txt
uvicorn app.main:app --reload --app-dir backend --port 8000
```

Environment variables are read from the environment; see `.env.example`.

Telemetry contract (Phase 2.1):
- The backend participates in producing `session_trace.json` (canonical telemetry) and `trace_validation.json` (validation report).
- Once `session_trace.json` has been finalized (merged with frontend events), it is immutable — downstream tools must read it but not modify it. Any enrichment or derived outputs are produced as separate artifacts (e.g. `session_trace.enriched.json`).
