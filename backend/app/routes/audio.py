import logging
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
