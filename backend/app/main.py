import os
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import config
from .routes import livekit, audio
from .audio.agent import stop_all_agents

# Configure application-wide logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

app = FastAPI(title="OneMeta Backend")

# Allow CORS for development frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[config.BACKEND_CORS_ORIGINS],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routes
app.include_router(livekit.router)
app.include_router(audio.router)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "OneMeta Backend",
        "version": "0.1.0",
        "environment": config.ENVIRONMENT,
    }


@app.on_event("shutdown")
async def shutdown_event():
    logging.info("Application shutdown triggered: Tearing down all active agents and pipelines...")
    await stop_all_agents()
    logging.info("Graceful application shutdown completed.")
