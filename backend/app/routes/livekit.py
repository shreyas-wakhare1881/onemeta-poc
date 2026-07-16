from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field
from .. import config
from ..services.livekit_token import generate_token

router = APIRouter(prefix="/api/livekit", tags=["livekit"])

class TokenRequest(BaseModel):
    room_name: str = Field(
        ...,
        min_length=1,
        max_length=128,
        example="onemeta-demo",
        description="The name of the room to join"
    )
    identity: str = Field(
        ...,
        min_length=1,
        max_length=128,
        example="User-A",
        description="The unique identity for the participant"
    )

class TokenResponse(BaseModel):
    token: str = Field(..., description="Signed JWT Access Token for LiveKit connection")
    url: str = Field(..., description="LiveKit Server URL")

@router.post("/token", response_model=TokenResponse)
async def get_token(request: TokenRequest):
    try:
        token_str = generate_token(request.room_name, request.identity)
        return TokenResponse(token=token_str, url=config.LIVEKIT_URL)
    except ValueError as ve:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(ve)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate token: {str(e)}"
        )
