import logging
from livekit import api
from .. import config

logger = logging.getLogger("onemeta.livekit_token")

def generate_token(room_name: str, identity: str) -> str:
    """
    Generates a LiveKit JWT token for a specific room and participant identity.
    
    Args:
        room_name: The name of the room to join.
        identity: The unique identifier for the participant.
        
    Returns:
        str: The signed JWT access token.
        
    Raises:
        ValueError: If LiveKit credentials are not configured or arguments are invalid.
        Exception: If token generation fails.
    """
    if not config.LIVEKIT_API_KEY or not config.LIVEKIT_API_SECRET:
        logger.error("LiveKit API Key or Secret is not configured.")
        raise ValueError("LiveKit credentials are not configured on the backend.")

    if not room_name or not room_name.strip():
        raise ValueError("Room name cannot be empty.")
        
    if not identity or not identity.strip():
        raise ValueError("Participant identity cannot be empty.")

    try:
        # Create token with video grants allowing room connection
        token = (
            api.AccessToken(config.LIVEKIT_API_KEY, config.LIVEKIT_API_SECRET)
            .with_identity(identity)
            .with_name(identity)
            .with_grants(
                api.VideoGrants(
                    room_join=True,
                    room=room_name,
                )
            )
            .to_jwt()
        )
        return token
    except Exception as e:
        logger.exception(f"Error generating LiveKit token for room '{room_name}' and identity '{identity}': {e}")
        raise
