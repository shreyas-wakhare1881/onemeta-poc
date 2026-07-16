import asyncio
import logging
from livekit import rtc
from .. import config
from ..services.livekit_token import generate_token
from .config import AudioConfig
from .processor import StreamingSpeechProcessor
from .sink import LoggingChunkSink, MultiChunkSink
from .telemetry import AudioTelemetry
from .registry import pipeline_registry

logger = logging.getLogger("onemeta.audio_agent")

# Holds active agent connection tasks to prevent duplicate agent processes per room
_active_agents = {}

async def start_audio_agent(room_name: str):
    """
    Launches an agent background task for a given room if not already running.
    """
    if room_name in _active_agents:
        logger.info(f"Audio agent already active or starting for room: {room_name}")
        return
        
    task = asyncio.create_task(_run_agent(room_name))
    _active_agents[room_name] = task
    logger.info(f"Dispatched background agent process for room: {room_name}")

async def stop_audio_agent(room_name: str):
    """
    Cancels and removes the agent background task for a given room.
    """
    task = _active_agents.pop(room_name, None)
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        logger.info(f"Terminated background agent process for room: {room_name}")

async def stop_all_agents():
    """
    Cleans up all active agent tasks on backend shutdown.
    """
    rooms = list(_active_agents.keys())
    for room_name in rooms:
        await stop_audio_agent(room_name)
    await pipeline_registry.shutdown_all()

async def _run_agent(room_name: str):
    logger.info(f"Connecting audio agent to room: {room_name}")
    
    # 1. Initialize config, telemetry, and speech sinks
    audio_config = AudioConfig()
    telemetry = AudioTelemetry()
    
    # Configure direct logging sink wrapped in MultiChunkSink for future plug-and-play expansions
    logging_sink = LoggingChunkSink()
    multi_sink = MultiChunkSink([logging_sink])
    
    # Instantiate StreamingSpeechProcessor (injecting config, room name, sink, and telemetry)
    processor = StreamingSpeechProcessor(audio_config, room_name, multi_sink, telemetry)
    
    # 2. Build and register pipeline using dependency injection (await async registry)
    pipeline = await pipeline_registry.create(room_name, audio_config, processor, telemetry)
    await pipeline.start()
    
    # 3. Generate token for agent participant
    agent_identity = f"agent-bot-{room_name}"
    try:
        token = generate_token(room_name, agent_identity)
    except Exception as e:
        logger.error(f"Failed to generate agent token for room '{room_name}': {e}")
        await pipeline_registry.remove(room_name)
        await pipeline.cleanup()
        _active_agents.pop(room_name, None)
        return
        
    room = rtc.Room()
    active_subscriptions = []
    
    @room.on("track_subscribed")
    def on_track_subscribed(track: rtc.Track, publication, participant):
        if track.kind == rtc.TrackKind.KIND_AUDIO:
            logger.info(f"Agent subscribed to remote audio track {track.sid} from participant {participant.identity}")
            # Bind track stream to pipeline ingestion task, passing both identity and session ID (sid)
            t = asyncio.create_task(_ingest_track(track, pipeline, participant.identity, participant.sid))
            active_subscriptions.append(t)
            
    @room.on("track_unsubscribed")
    def on_track_unsubscribed(track: rtc.Track, publication, participant):
        if track.kind == rtc.TrackKind.KIND_AUDIO:
            logger.info(f"Agent unsubscribed from audio track {track.sid}")

    try:
        # Connect to room
        await room.connect(config.LIVEKIT_URL, token)
        logger.info(f"Agent successfully joined LiveKit room: {room_name}")
        
        # Keep agent connection loop alive
        while True:
            await asyncio.sleep(1)
            
    except asyncio.CancelledError:
        logger.info(f"Agent connection task for room {room_name} was cancelled.")
    except Exception as e:
        logger.error(f"Error occurred in audio agent loop for room {room_name}: {e}")
    finally:
        # Cancel track ingestion streams
        for sub_task in active_subscriptions:
            sub_task.cancel()
            try:
                await sub_task
            except asyncio.CancelledError:
                pass
                
        # Perform clean pipeline and room teardowns
        await pipeline_registry.remove(room_name)
        await pipeline.cleanup()
        
        try:
            await room.disconnect()
        except Exception as e:
            logger.error(f"Error during room disconnect in agent: {e}")
            
        _active_agents.pop(room_name, None)
        logger.info(f"Agent connection for room {room_name} cleaned up completely.")

async def _ingest_track(track: rtc.Track, pipeline, participant_identity: str, participant_session_id: str):
    """
    Pulls audio frames from rtc.AudioStream and forwards them to Pipeline.ingest_pcm.
    """
    try:
        # Standardize stream to 16kHz mono PCM16 at decoders level
        audio_stream = rtc.AudioStream(track, sample_rate=16000, num_channels=1)
        
        async for event in audio_stream:
            lk_frame = event.frame
            pcm_bytes = bytes(lk_frame.data)
            
            # Forward raw bytes directly to pipeline ingestion layer
            pipeline.ingest_pcm(
                pcm_bytes=pcm_bytes,
                timestamp=lk_frame.timestamp,
                participant_identity=participant_identity,
                participant_session_id=participant_session_id
            )
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Exception during track ingestion in agent: {e}")
