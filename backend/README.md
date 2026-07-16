Backend (FastAPI)

Run locally:

```bash
python -m pip install -r backend/requirements.txt
uvicorn app.main:app --reload --app-dir backend --port 8000
```

Environment variables are read from the environment; see `.env.example`.
