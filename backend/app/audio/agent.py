import asyncio
import logging
import uuid
import json
import time
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

def _is_transient_error(exc: Exception) -> bool:
    """
    Identify if the exception represents a transient network or transport failure.
    Whitelists socket, connection, and LiveKit transport exceptions.
    """
    import asyncio
    # Whitelisted transient exceptions
    transient_exceptions = (
        ConnectionError, TimeoutError, OSError,
        asyncio.TimeoutError
    )
    exc_name = exc.__class__.__name__
    if isinstance(exc, transient_exceptions):
        return True
    if "Publish" in exc_name or "Transport" in exc_name or "LiveKit" in exc_name:
        return True
    return False

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
    
    # Bounded publish queue and telemetry counters
    PACKET_PROTOCOL_VERSION = 1
    publish_queue = asyncio.Queue(maxsize=audio_config.publisher_queue_size)
    
    published_packets = 0
    retried_packets = 0
    dropped_packets = 0
    queue_evictions = 0
    
    room = rtc.Room()
    active_subscriptions = []
    
    async def publisher_worker():
        nonlocal published_packets, retried_packets, dropped_packets
        while True:
            try:
                packet_data = await publish_queue.get()
                retries = 3
                retry_delays = [0.1, 0.2, 0.4]  # 100ms, 200ms, 400ms
                
                for attempt in range(retries):
                    if not room.isconnected or not room.local_participant:
                        logger.warning("LiveKit room is not connected. Dropping packet.")
                        dropped_packets += 1
                        break
                        
                    try:
                        await room.local_participant.publish_data(packet_data)
                        published_packets += 1
                        break
                    except Exception as pe:
                        # Verify if the exception is transient
                        if not _is_transient_error(pe):
                            logger.error(f"Non-transient error in packet publishing: {pe}. Dropping packet immediately.")
                            dropped_packets += 1
                            break
                            
                        retried_packets += 1
                        if attempt == retries - 1:
                            logger.error(f"Failed to publish packet after 3 retries: {pe}")
                            dropped_packets += 1
                        else:
                            delay = retry_delays[attempt]
                            logger.warning(f"Publish failed: {pe}. Retrying in {delay}s...")
                            await asyncio.sleep(delay)
                            
                publish_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in publisher worker: {e}")

    publisher_task = asyncio.create_task(publisher_worker())
    
    # Configure direct logging sink wrapped in MultiChunkSink for future plug-and-play expansions
    logging_sink = LoggingChunkSink()
    
    # Initialize the AI Pipeline (InferenceSink -> Queue -> AIEngine)
    from ..ai import AIConfig, AITelemetry, AIEngine, InferenceSink
    
    ai_config = AIConfig()
    ai_telemetry = AITelemetry()
    ai_engine = AIEngine(ai_config, ai_telemetry)
    
    # Register an event listener to log and broadcast streaming translation events
    def log_ai_event(event):
        nonlocal queue_evictions
        logger.info(f"[AI Event] {event.__class__.__name__} for chunk {event.chunk_id}: {event}")
        if room.isconnected and room.local_participant:
            packet = {
                "id": str(uuid.uuid4()),
                "version": PACKET_PROTOCOL_VERSION,
                "type": event.__class__.__name__,
                "timestamp": time.time(),
                "payload": {
                    "chunk_id": event.chunk_id,
                    "sequence_number": event.sequence_number,
                }
            }
            if hasattr(event, "text_delta"):
                packet["payload"]["text_delta"] = event.text_delta
                packet["payload"]["cumulative_text"] = event.cumulative_text
            elif hasattr(event, "full_text"):
                packet["payload"]["full_text"] = event.full_text
                packet["payload"]["duration_ms"] = event.duration_ms
            elif hasattr(event, "error_message"):
                packet["payload"]["error_message"] = event.error_message
                
            if hasattr(event, "metrics") and event.metrics:
                from dataclasses import asdict
                packet["metrics"] = asdict(event.metrics)
                
            try:
                data_bytes = json.dumps(packet).encode("utf-8")
                try:
                    publish_queue.put_nowait(data_bytes)
                except asyncio.QueueFull:
                    logger.warning("Publisher queue is full! Evicting oldest pending packet.")
                    try:
                        publish_queue.get_nowait()
                        publish_queue.task_done()
                        queue_evictions += 1
                    except asyncio.QueueEmpty:
                        pass
                    publish_queue.put_nowait(data_bytes)
            except Exception as pe:
                logger.error(f"Failed to queue AI event packet: {pe}")

    ai_engine.register_listener(log_ai_event)
    
    inference_sink = InferenceSink(ai_engine)
    
    # Combine logging sink and inference sink in the MultiChunkSink
    multi_sink = MultiChunkSink([logging_sink, inference_sink])
    
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
        publisher_task.cancel()
        return
        
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
            
            # Broadcast throttled telemetry updates (maximum 1 second refresh rate)
            if room.isconnected and room.local_participant:
                try:
                    audio_report = telemetry.get_report(
                        current_queue_size=processor.queue.qsize() if hasattr(processor, "queue") else 0,
                        queue_maxsize=audio_config.max_queue_size if hasattr(audio_config, "max_queue_size") else 50
                    )
                    ai_report = ai_engine.telemetry.get_report(
                        current_queue_depth=ai_engine.queue.qsize()
                    )
                    
                    payload = {
                        "audio": audio_report,
                        "ai": ai_report,
                        "publisher": {
                            "published_packets": published_packets,
                            "retried_packets": retried_packets,
                            "dropped_packets": dropped_packets,
                            "queue_evictions": queue_evictions,
                            "queue_depth": publish_queue.qsize()
                        },
                        "timestamp": time.time()
                    }
                    
                    packet = {
                        "id": str(uuid.uuid4()),
                        "version": PACKET_PROTOCOL_VERSION,
                        "type": "TelemetryUpdate",
                        "timestamp": time.time(),
                        "payload": payload
                    }
                    
                    data_bytes = json.dumps(packet).encode("utf-8")
                    try:
                        publish_queue.put_nowait(data_bytes)
                    except asyncio.QueueFull:
                        try:
                            publish_queue.get_nowait()
                            publish_queue.task_done()
                            queue_evictions += 1
                        except asyncio.QueueEmpty:
                            pass
                        publish_queue.put_nowait(data_bytes)
                except Exception as te:
                    logger.error(f"Failed to queue periodic telemetry update: {te}")
            
    except asyncio.CancelledError:
        logger.info(f"Agent connection task for room {room_name} was cancelled.")
    except Exception as e:
        logger.error(f"Error occurred in audio agent loop for room {room_name}: {e}")
    finally:
        # Cancel publisher worker task
        publisher_task.cancel()
        try:
            await publisher_task
        except asyncio.CancelledError:
            pass
            
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
