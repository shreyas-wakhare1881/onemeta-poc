import logging
import asyncio
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field
from ..audio.agent import start_audio_agent, stop_audio_agent

logger = logging.getLogger("onemeta.routes.audio")

router = APIRouter(prefix="/api/audio", tags=["audio"])

class AgentStartRequest(BaseModel):
    room_name: str = Field(..., min_length=1, max_length=128, example="test-room")
    loopback: bool = Field(False)
    source_participant_identity: str = Field("User-A")

class AgentStopRequest(BaseModel):
    room_name: str = Field(..., min_length=1, max_length=128, example="test-room")

@router.post("/agent/start")
async def start_agent(request: AgentStartRequest):
    """
    Temporary development endpoint to trigger the backend audio agent connection.
    Will be replaced by an event-driven lifecycle in a future milestone.
    """
    try:
        await start_audio_agent(
            request.room_name,
            loopback=request.loopback,
            source_participant_identity=request.source_participant_identity
        )
        return {
            "status": "success", 
            "message": f"Dispatched background agent for room: {request.room_name} (Temporary Endpoint)"
        }
    except Exception as e:
        logger.exception(f"Failed to start agent for room '{request.room_name}': {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start agent: {str(e)}"
        )

@router.post("/agent/stop")
async def stop_agent(request: AgentStopRequest):
    """
    Temporary development endpoint to stop the backend audio agent connection.
    Will be replaced by an event-driven lifecycle in a future milestone.
    """
    try:
        await stop_audio_agent(request.room_name)
        return {
            "status": "success", 
            "message": f"Stopped background agent for room: {request.room_name} (Temporary Endpoint)"
        }
    except Exception as e:
        logger.exception(f"Failed to stop agent for room '{request.room_name}': {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to stop agent: {str(e)}"
        )

class UploadArtifactsRequest(BaseModel):
    room_name: str
    frontend_trace: dict = None
    console_logs: list = None

@router.post("/session/upload_artifacts")
async def upload_artifacts(request: UploadArtifactsRequest):
    import json
    from pathlib import Path
    from ..audio.agent import get_active_session_folder
    
    session_folder = get_active_session_folder(request.room_name)
    if not session_folder:
        # Fall back to searching for the latest directory under output ending with _room_name
        output_dir = Path(__file__).resolve().parents[3] / "output"
        if output_dir.exists():
            matching_dirs = [d for d in output_dir.iterdir() if d.is_dir() and d.name.endswith(f"_{request.room_name}")]
            if matching_dirs:
                matching_dirs.sort(key=lambda d: d.stat().st_mtime)
                session_folder = matching_dirs[-1].name
                
    if not session_folder:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Session folder not found for this room"
        )
        
    session_dir = Path(__file__).resolve().parents[3] / "output" / session_folder
    session_dir.mkdir(parents=True, exist_ok=True)
    
    # Save frontend.log
    if request.frontend_trace:
        frontend_path = session_dir / "frontend.log"
        try:
            with open(frontend_path, "w", encoding="utf-8") as f:
                json.dump(request.frontend_trace, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to write frontend.log: {e}")
            
    # Save console.log
    if request.console_logs:
        console_path = session_dir / "console.log"
        try:
            with open(console_path, "w", encoding="utf-8") as f:
                for log in request.console_logs:
                    f.write(f"{log}\n")
        except Exception as e:
            logger.error(f"Failed to write console.log: {e}")
            
    # Finalize combined trace and validation report (non-blocking dispatch)
    try:
        from ..audio.session_finalizer import finalize_session_trace
        logger.info(f"[Routes] SESSION_END_RECEIVED — session_dir={session_dir} — module_file={__file__}")
        logger.info(f"[Routes] FINALIZER_DISPATCHED — session_dir={session_dir}")
        # Run finalizer in a thread to avoid blocking the FastAPI event loop.
        task = asyncio.create_task(asyncio.to_thread(finalize_session_trace, session_dir))
        def _finalizer_done_callback(t):
            try:
                t.result()
                logger.info(f"[Routes] FINALIZER_COMPLETED_ASYNC — session_dir={session_dir}")
            except Exception as e:
                logger.exception(f"[Routes] FINALIZER_FAILED_ASYNC — session_dir={session_dir} — error={e}")
        task.add_done_callback(_finalizer_done_callback)
    except Exception as fe:
        logger.exception(f"[Routes] Failed to dispatch session finalization in upload_artifacts route: {fe}")
    return {"status": "success", "session_folder": session_folder}

