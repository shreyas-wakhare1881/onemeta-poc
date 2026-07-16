from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import config

app = FastAPI(title="OneMeta Backend")

# Allow CORS for development frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[config.BACKEND_CORS_ORIGINS],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "OneMeta Backend",
        "version": "0.1.0",
        "environment": config.ENVIRONMENT,
    }
