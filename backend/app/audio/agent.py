import asyncio
import logging
import uuid
import json
import time
import base64
from livekit import rtc
from .. import config
from ..services.livekit_token import generate_token
from .config import AudioConfig
from .processor import StreamingSpeechProcessor
from .telemetry import AudioTelemetry
from .registry import pipeline_registry
from .tracing_events import PipelineEvent
from .tracer import PipelineEventTracer

logger = logging.getLogger("onemeta.audio_agent")

# Holds active agent connection tasks to prevent duplicate agent processes per room
_active_agents = {}
_active_session_folders = {}

def get_active_session_folder(room_name: str) -> str:
    return _active_session_folders.get(room_name, "")

# Experiment: Global counters for audio packets sent to LiveKit data channel and incoming audio frames
_audio_packets_sent_count = 0
_audio_frames_received_count = 0
_input_packets_sent_count = 0

def _is_transient_error(exc: Exception) -> bool:
    """
    Identify if the exception represents a transient network or transport failure.
    Whitelists socket, connection, and LiveKit transport exceptions.
    """
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


async def start_audio_agent(room_name: str, loopback: bool = False, source_participant_identity: str = "User-A"):
    """
    Launches an agent background task for a given room if not already running.
    """
    if room_name in _active_agents:
        logger.info(f"Audio agent already active or starting for room: {room_name}")
        return

    task = asyncio.create_task(_run_agent(room_name, loopback, source_participant_identity))
    _active_agents[room_name] = task
    logger.info(f"Dispatched background agent process for room: {room_name} (loopback={loopback}, source_participant={source_participant_identity})")


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


async def _run_agent(room_name: str, loopback: bool = False, source_participant_identity: str = "User-A"):
    logger.info(f"Connecting audio agent to room: {room_name} (loopback={loopback}, source={source_participant_identity})")

    import datetime
    from pathlib import Path
    
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    session_folder = f"{timestamp}_{room_name}"
    _active_session_folders[room_name] = session_folder

    # Create session folder immediately
    session_dir = Path(__file__).resolve().parents[3] / "output" / session_folder
    session_dir.mkdir(parents=True, exist_ok=True)

    # 1. Initialize config, audio telemetry and pipeline event tracer
    audio_config = AudioConfig()
    telemetry = AudioTelemetry()
    tracer = PipelineEventTracer(room_name, session_folder=session_folder)
    tracer.log_event(PipelineEvent.SESSION_STARTED)

    # Log Audio Configuration details (Suggestion 4)
    logger.info("=== AUDIO CONFIGURATION VERIFICATION ===")
    logger.info(f"Sample Rate: {audio_config.sample_rate} Hz")
    logger.info(f"Channels: {audio_config.channels}")
    logger.info(f"Bits per Sample: {audio_config.bytes_per_sample * 8} (PCM16)")
    logger.info(f"Frame Duration: {audio_config.frame_duration_sec * 1000:.1f} ms")
    logger.info(f"Samples per Frame: {audio_config.samples_per_frame}")
    logger.info(f"Bytes per Frame: {audio_config.bytes_per_frame} bytes")
    logger.info("=========================================")

    # Bounded publish queue
    PACKET_PROTOCOL_VERSION = 1
    publish_queue = asyncio.Queue(maxsize=audio_config.publisher_queue_size)

    published_packets = 0
    retried_packets = 0
    dropped_packets = 0
    queue_evictions = 0

    global _audio_packets_sent_count, _audio_frames_received_count, _input_packets_sent_count
    _audio_packets_sent_count = 0
    _audio_frames_received_count = 0
    _input_packets_sent_count = 0

    room = rtc.Room()
    active_subscriptions = []

    async def publisher_worker():
        nonlocal published_packets, retried_packets, dropped_packets
        while True:
            try:
                packet_item = await publish_queue.get()
                if isinstance(packet_item, tuple):
                    packet_data, destination_identities = packet_item
                else:
                    packet_data = packet_item
                    destination_identities = None

                retries = 3
                retry_delays = [0.1, 0.2, 0.4]

                for attempt in range(retries):
                    if not room.isconnected or not room.local_participant:
                        logger.warning("LiveKit room is not connected. Dropping packet.")
                        dropped_packets += 1
                        break

                    try:
                        parsed_packet = None
                        corr_id = ""
                        pkt_type = None
                        try:
                            parsed_packet = json.loads(packet_data.decode('utf-8'))
                            corr_id = parsed_packet.get("payload", {}).get("correlation_id", "")
                            pkt_type = parsed_packet.get('type')
                        except Exception:
                            pass

                        if destination_identities:
                            await room.local_participant.publish_data(
                                packet_data,
                                destination_identities=destination_identities
                            )
                        else:
                            await room.local_participant.publish_data(packet_data)
                        
                        if tracer.enabled and parsed_packet:
                            import base64
                            dest_str = ", ".join(destination_identities) if destination_identities else "broadcast"
                            if pkt_type == 'StreamingPartialTranslationEvent':
                                tracer.log_event(
                                    PipelineEvent.TEXT_PUBLISHED,
                                    correlation_id=corr_id,
                                    metadata={
                                        "destination": dest_str,
                                        "payload_size": len(packet_data)
                                    }
                                )
                            elif pkt_type == 'StreamingTranslationAudioEvent':
                                try:
                                    audio_b64 = parsed_packet.get("payload", {}).get("audio_data", "")
                                    pcm_bytes_len = len(base64.b64decode(audio_b64))
                                    duration_sec = pcm_bytes_len / 48000.0
                                except Exception:
                                    pcm_bytes_len = 0
                                    duration_sec = 0.0

                                tracer.log_event(
                                    PipelineEvent.AUDIO_PUBLISHED,
                                    correlation_id=corr_id,
                                    metadata={
                                        "destination": dest_str,
                                        "frame_size": pcm_bytes_len,
                                        "sample_rate": 24000,
                                        "duration": duration_sec
                                    }
                                )

                        published_packets += 1
                        # Attempt to decode packet type for richer logging
                        try:
                            parsed_packet = json.loads(packet_data.decode('utf-8'))
                            pkt_type = parsed_packet.get('type')
                        except Exception:
                            pkt_type = None

                        if pkt_type == 'StreamingTranslationAudioEvent' or (isinstance(packet_data, (bytes, bytearray)) and b'"type": "StreamingTranslationAudioEvent"' in packet_data):
                            logger.info(f"EXPERIMENT TIMING: Successfully published StreamingTranslationAudioEvent packet #{published_packets} to LiveKit data channel at {time.time():.4f} (targeted to identities: {destination_identities})")
                        else:
                            logger.info(f"Published data packet #{published_packets} type={pkt_type} to LiveKit data channel at {time.time():.4f} (targeted to identities: {destination_identities})")
                        break
                    except Exception as pe:
                        if tracer.enabled:
                            tracer.log_event(
                                PipelineEvent.PIPELINE_ERROR,
                                correlation_id=corr_id,
                                metadata={
                                    "stage": "publish",
                                    "exception": pe.__class__.__name__,
                                    "message": str(pe)
                                }
                            )
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

    # 2. Build streaming AI engine — provider resolved via create_streaming_runtime()
    from ..ai import AIConfig, AIEngine
    from ..ai.runtimes import create_streaming_runtime
    from ..ai.events import StreamingSpeechStartedEvent, StreamingSpeechEndedEvent, StreamingTranslationAudioEvent

    ai_config = AIConfig()
    ai_engine = AIEngine(ai_config)

    # Events that should be published to the LiveKit data channel (all participants receive these).
    # Internal frame-received/state-change telemetry events are intentionally excluded to avoid
    # flooding the data channel with high-frequency diagnostic traffic.
    from ..ai.events import (
        StreamingPartialTranslationEvent,
        StreamingTranslationCompletedEvent,
        StreamingTranslationAudioEvent,
        StreamingRuntimeErrorEvent,
        StreamingInputTranscriptionEvent,
        StreamingInputTranscriptionCompletedEvent,
    )
    _PUBLISHABLE_EVENT_TYPES = (
        StreamingPartialTranslationEvent,
        StreamingTranslationCompletedEvent,
        StreamingTranslationAudioEvent,
        StreamingRuntimeErrorEvent,
        StreamingInputTranscriptionEvent,
        StreamingInputTranscriptionCompletedEvent,
    )

    # Register an event listener to broadcast streaming translation events over LiveKit data channel
    def log_ai_event(event):
        nonlocal queue_evictions
        c_id = getattr(event, "session_id", getattr(event, "correlation_id", "stream"))
        seq = getattr(event, "event_seq", 0)

        # Only emit meaningful log lines for translation events to reduce noise
        if isinstance(event, _PUBLISHABLE_EVENT_TYPES):
            logger.info(f"[AI Event] {event.__class__.__name__}: {event}")
        else:
            logger.debug(f"[AI Event internal] {event.__class__.__name__}")

        # Only publish translation-relevant events over data channel.
        # High-frequency internal events (StreamingAudioFrameReceivedEvent, StreamingStateChangedEvent,
        # StreamingBackpressureEvent, etc.) are suppressed here; aggregate telemetry is published
        # via the periodic TelemetryUpdate packet below.
        if not isinstance(event, _PUBLISHABLE_EVENT_TYPES):
            return

        if room.isconnected and room.local_participant:
            packet = {
                "id": str(uuid.uuid4()),
                "version": PACKET_PROTOCOL_VERSION,
                "type": event.__class__.__name__,
                "timestamp": time.time(),
                "payload": {
                    "session_id": c_id,
                    "event_seq": seq,
                    "participant_identity": getattr(event, "participant_identity", ""),
                    "correlation_id": getattr(event, "correlation_id", "")
                }
            }
            if hasattr(event, "text_delta"):
                packet["payload"]["text_delta"] = event.text_delta
                packet["payload"]["cumulative_text"] = event.cumulative_text
            elif hasattr(event, "full_text"):
                packet["payload"]["full_text"] = event.full_text
            elif hasattr(event, "audio_data"):
                # NOTE: Base64 encoding audio bytes and publishing them over the LiveKit data channel
                # is purely a POC transport mechanism for UI transcript/audio synchronization.
                # In production, this output stream should ideally publish to a dedicated
                # LiveKit audio track (e.g., rtc.LocalAudioTrack) for optimal network efficiency
                # and direct WebRTC playback.
                packet["payload"]["audio_data"] = base64.b64encode(event.audio_data).decode("utf-8")
                packet["payload"]["mime_type"] = event.mime_type
                
                global _audio_packets_sent_count
                _audio_packets_sent_count += 1
                logger.info(f"EXPERIMENT METRIC: Total Audio Packets Sent: {_audio_packets_sent_count}")
                logger.info(f"EXPERIMENT TIMING: Enqueuing StreamingTranslationAudioEvent packet #{_audio_packets_sent_count} at {time.time():.4f}")
            elif hasattr(event, "error_message"):
                packet["payload"]["error_message"] = event.error_message

            # Resolve destination identities
            dest_identities = None
            if room.isconnected:
                if loopback:
                    for p in room.remote_participants.values():
                        if p.identity == source_participant_identity:
                            dest_identities = [p.identity]
                            break
                else:
                    for p in room.remote_participants.values():
                        if "User-B" in p.identity:
                            dest_identities = [p.identity]
                            break

            try:
                data_bytes = json.dumps(packet).encode("utf-8")
                try:
                    publish_queue.put_nowait((data_bytes, dest_identities))
                except asyncio.QueueFull:
                    logger.warning("Publisher queue is full! Evicting oldest pending packet.")
                    try:
                        publish_queue.get_nowait()
                        publish_queue.task_done()
                        queue_evictions += 1
                    except asyncio.QueueEmpty:
                        pass
                    publish_queue.put_nowait((data_bytes, dest_identities))
            except Exception as pe:
                logger.error(f"Failed to queue AI event packet: {pe}")

    await ai_engine.start()

    # 3. Initialize streaming runtime and session
    live_runtime = create_streaming_runtime(ai_config)
    await live_runtime.initialize()

    session = await ai_engine.start_streaming_session(
        session_id=room_name,
        runtime=live_runtime,
        source_lang=ai_config.default_source_lang,
        target_lang=ai_config.target_language,
        metadata={"tracer": tracer, "session_folder": session_folder}
    )
    session.register_listener(log_ai_event)

    # 4. Build StreamingSpeechProcessor (no chunk sink — audio routes via packet listener)
    processor = StreamingSpeechProcessor(audio_config, room_name, telemetry, tracer=tracer)

    async def forward_audio_packet(packet):
        global _input_packets_sent_count
        _input_packets_sent_count += 1
        if _input_packets_sent_count % 50 == 0:
            logger.info(f"EXPERIMENT METRIC: Total Audio Packets forwarded to AI Engine: {_input_packets_sent_count}")
        await ai_engine.process_audio_packet(room_name, packet)

    def forward_vad_event(ev):
        if isinstance(ev, StreamingSpeechStartedEvent):
            session.record_speech_start()
        elif isinstance(ev, StreamingSpeechEndedEvent):
            session.record_speech_end()

    processor.register_packet_listener(forward_audio_packet)
    processor.register_listener(forward_vad_event)

    # 5. Build and register pipeline
    pipeline = await pipeline_registry.create(room_name, audio_config, processor, telemetry)
    await pipeline.start()

    # 6. Generate token for agent participant
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
            # Ignore agent's own published tracks; subscribe to all remote participant audio.
            if participant.identity == agent_identity:
                logger.info(f"Agent ignoring local audio track {track.sid} from agent participant {participant.identity}")
                return

            # Backend Safeguard (Suggestion 3):
            # Only ingest audio from the primary source participant (User-A or the first speaker)
            # to prevent acoustic echo loops from the target participant (User-B) or other listeners.
            if "agent-bot" in participant.identity:
                return

            # If there's a target participant like User-B, we ignore their track for this session
            if "User-B" in participant.identity or "Spanish" in participant.identity:
                logger.info(f"Agent backend safeguard: ignoring audio track from target/non-source participant: {participant.identity}")
                return

            logger.info(f"Agent subscribed to remote audio track {track.sid} from participant {participant.identity}")
            t = asyncio.create_task(_ingest_track(track, pipeline, participant.identity, participant.sid, tracer))
            active_subscriptions.append(t)

    @room.on("track_unsubscribed")
    def on_track_unsubscribed(track: rtc.Track, publication, participant):
        if track.kind == rtc.TrackKind.KIND_AUDIO:
            logger.info(f"Agent unsubscribed from audio track {track.sid}")

    try:
        await room.connect(config.LIVEKIT_URL, token)
        logger.info(f"Agent successfully joined LiveKit room: {room_name}")

        while True:
            await asyncio.sleep(1)

            if room.isconnected and room.local_participant:
                try:
                    audio_report = telemetry.get_report(
                        current_queue_size=processor.vad._frame_count if hasattr(processor.vad, "_frame_count") else 0,
                        queue_maxsize=50
                    )
                    payload = {
                        "audio": audio_report,
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
        # Log SESSION_ENDED and save trace file
        try:
            tracer.log_event(PipelineEvent.SESSION_ENDED)
            tracer.save()
        except Exception as e:
            logger.error(f"Failed to log/save session trace: {e}")

        publisher_task.cancel()
        try:
            await publisher_task
        except asyncio.CancelledError:
            pass

        for sub_task in active_subscriptions:
            sub_task.cancel()
            try:
                await sub_task
            except asyncio.CancelledError:
                pass

        await pipeline_registry.remove(room_name)
        await pipeline.cleanup()

        try:
            await ai_engine.stop_streaming_session(room_name)
            await live_runtime.shutdown()
            await ai_engine.shutdown()
        except Exception as e:
            logger.error(f"Error during AI engine shutdown in agent: {e}")

        try:
            await room.disconnect()
        except Exception as e:
            logger.error(f"Error during room disconnect in agent: {e}")

        _active_agents.pop(room_name, None)
        logger.info(f"Agent connection for room {room_name} cleaned up completely.")


async def _ingest_track(track: rtc.Track, pipeline, participant_identity: str, participant_session_id: str, tracer=None):
    """
    Pulls audio frames from rtc.AudioStream and forwards them to Pipeline.ingest_pcm.
    """
    try:
        audio_stream = rtc.AudioStream(track, sample_rate=16000, num_channels=1)

        async for event in audio_stream:
            lk_frame = event.frame
            pcm_bytes = bytes(lk_frame.data)


            global _audio_frames_received_count
            _audio_frames_received_count += 1
            if _audio_frames_received_count % 50 == 0:
                logger.info(f"EXPERIMENT METRIC: Total Mic Frames Ingested from LiveKit: {_audio_frames_received_count}")

            pipeline.ingest_pcm(
                pcm_bytes=pcm_bytes,
                timestamp=time.time(),
                participant_identity=participant_identity,
                participant_session_id=participant_session_id
            )
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Exception during track ingestion in agent: {e}")
