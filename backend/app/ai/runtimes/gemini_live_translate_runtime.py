import logging
import asyncio
import time
import json
import os
import base64
from pathlib import Path
from typing import Any, Optional, Callable, List

from google import genai
from google.genai import types

from ..streaming import BaseStreamingTransport, BaseStreamingRuntime
from ..events import (
    StreamingPartialTranslationEvent,
    StreamingTranslationCompletedEvent,
    StreamingRuntimeErrorEvent,
    StreamingTranslationAudioEvent,
    StreamingInputTranscriptionEvent,
    StreamingInputTranscriptionCompletedEvent
)
from ...transport.packet import StreamingAudioPacket

logger = logging.getLogger("onemeta.ai.runtimes.gemini_live_translate")

# Opt-in debug logging for Gemini SDK interactions. Enable by setting env var:
#   E2E_DEBUG_GEMINI=1
DEBUG_GEMINI = True
try:
    DEBUG_LOG_PATH = Path(__file__).resolve().parents[4] / "output" / "gemini_debug.log"
    DEBUG_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Gemini debug logging enabled -> {DEBUG_LOG_PATH}")
except Exception:
    DEBUG_GEMINI = False

class GeminiLiveTranslateTransport(BaseStreamingTransport):
    """
    Manages a stateful WebSocket connection to the Gemini Live Translation API
    using the official google-genai SDK.
    """
    def __init__(
        self,
        session_id: str,
        sdk_session: Any,
        sdk_ctx: Any,
        source_language: str,
        target_language: str,
        model_name: str,
        modalities: List[str],
        voice_name: str,
        tracer: Any = None,
        session_folder: str = None
    ):
        self.session_id = session_id
        self.sdk_session = sdk_session
        self.sdk_ctx = sdk_ctx
        self.source_language = source_language
        self.target_language = target_language
        self.model_name = model_name
        self.modalities = modalities
        self.voice_name = voice_name
        self.closed = False
        self.session_folder = session_folder
        import collections
        self._pending_events = collections.deque()
        self._last_transcript = ""
        
        # Initialize WAV writers for audio verification (WAV Capture)
        from pathlib import Path
        from ...audio.wav_writer import WavWriter
        
        if not self.session_folder:
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            self.session_folder = f"{timestamp}_{session_id}"
            
        session_dir = Path(__file__).resolve().parents[4] / "output" / self.session_folder
        session_dir.mkdir(parents=True, exist_ok=True)
        self._input_wav = WavWriter(session_dir / "input.wav", sample_rate=16000, channels=1)
        self._output_wav = WavWriter(session_dir / "output.wav", sample_rate=24000, channels=1)
        self.debug_log_path = session_dir / "gemini.log"
        
        self._current_correlation_id = ""
        self._receive_iterator = self.sdk_session.receive()
        self._input_packets_sent_count = 0
        self._received_audio_count = 0
        self._ws_frame_recv_count = 0
        self._text_chunk_count = 0
        self._cumulative_text_len = 0
        self.tracer = tracer

    async def send_packet(self, packet: StreamingAudioPacket) -> None:
        if self.closed:
            return

        corr_id = ""
        frame_id = ""
        if packet.metadata:
            corr_id = packet.metadata.correlation_id
            frame_id = packet.metadata.frame_id
            if corr_id:
                self._current_correlation_id = corr_id

        self._input_packets_sent_count += 1
        # Convert memoryview to bytes
        pcm_bytes = bytes(packet.pcm_data)
        self._input_wav.write(pcm_bytes)
        
        # Log timestamp of forwarded mic frame to Gemini
        logger.info(f"EXPERIMENT TIMING: Forwarded Mic Packet #{self._input_packets_sent_count} (corr={corr_id}) to Gemini at {time.time():.4f}")
        
        if self.tracer:
            from ...audio.tracing_events import PipelineEvent
            self.tracer.log_event(
                PipelineEvent.AUDIO_SENT_TO_GEMINI,
                correlation_id=corr_id,
                metadata={
                    "frame_id": frame_id,
                    "packet_size": len(pcm_bytes),
                    "sample_rate": packet.sample_rate
                }
            )
        
        # Optional debug: log outgoing packet metadata and a small prefix of audio
        if DEBUG_GEMINI:
            try:
                prefix_b64 = base64.b64encode(pcm_bytes[:64]).decode("ascii")
                with open(self.debug_log_path, "a", encoding="utf-8") as _f:
                    _f.write(json.dumps({
                        "time": time.time(),
                        "session": self.session_id,
                        "action": "send_packet",
                        "sample_rate": packet.sample_rate,
                        "length": len(pcm_bytes),
                        "corr": self._current_correlation_id,
                        "prefix_b64": prefix_b64
                    }) + "\n")
            except Exception:
                logger.debug("Failed to write Gemini send_packet debug log", exc_info=True)

        # Send raw audio input to the translation model
        await self.sdk_session.send_realtime_input(
            media=types.Blob(
                data=pcm_bytes,
                mime_type=f"audio/pcm;rate={packet.sample_rate}"
            )
        )

    async def receive_event(self) -> Any:
        if self.closed:
            return None

        # Return queued events first to prevent message drops
        if self._pending_events:
            return self._pending_events.popleft()

        while not self.closed:
            try:
                response = await self._receive_iterator.__anext__()
                if response:
                    # Parse WebSocket frame details for metadata
                    response_type = "unknown"
                    contains_text = False
                    contains_audio = False
                    finish_reason = None
                    
                    server_content = getattr(response, "server_content", None)
                    if server_content is not None:
                        response_type = "server_content"
                        if getattr(server_content, "output_transcription", None) and getattr(server_content.output_transcription, "text", None):
                            contains_text = True
                        if getattr(server_content, "input_transcription", None) and getattr(server_content.input_transcription, "text", None):
                            contains_text = True
                        model_turn = getattr(server_content, "model_turn", None)
                        if model_turn is not None:
                            for part in getattr(model_turn, "parts", []):
                                if getattr(part, "inline_data", None):
                                    contains_audio = True
                                if getattr(part, "text", None):
                                    contains_text = True
                        if getattr(server_content, "turn_complete", False):
                            finish_reason = "turn_complete"
                    elif getattr(response, "tool_call", None) is not None:
                        response_type = "tool_call"

                    self._ws_frame_recv_count += 1
                    
                    if self.tracer:
                        from ...audio.tracing_events import PipelineEvent
                        self.tracer.log_event(
                            PipelineEvent.GEMINI_WS_FRAME_RECEIVED,
                            correlation_id=self._current_correlation_id,
                            metadata={
                                "response_type": response_type,
                                "contains_text": contains_text,
                                "contains_audio": contains_audio,
                                "finish_reason": finish_reason,
                                "chunk_index": self._ws_frame_recv_count
                            }
                        )

                    # Optional debug: log full response repr and a small server_content summary
                    if DEBUG_GEMINI:
                        try:
                            with open(self.debug_log_path, "a", encoding="utf-8") as _f:
                                _f.write(json.dumps({"time": time.time(), "session": self.session_id, "action": "recv_response", "repr": repr(response)}) + "\n")
                                sc = getattr(response, "server_content", None)
                                if sc is not None:
                                    try:
                                        summary = {}
                                        if getattr(sc, "input_transcription", None):
                                            summary["input_transcription"] = getattr(sc.input_transcription, "text", None)
                                        if getattr(sc, "output_transcription", None):
                                            summary["output_transcription"] = getattr(sc.output_transcription, "text", None)
                                        if getattr(sc, "model_turn", None) and getattr(sc.model_turn, "parts", None):
                                            parts = []
                                            for part in sc.model_turn.parts:
                                                parts.append({
                                                    "text": getattr(part, "text", None),
                                                    "inline_len": len(getattr(part, "inline_data").data) if getattr(part, "inline_data", None) else None
                                                })
                                            summary["model_turn_parts"] = parts
                                        _f.write(json.dumps({"time": time.time(), "session": self.session_id, "server_content_summary": summary}) + "\n")
                                    except Exception:
                                        pass
                        except Exception:
                            logger.debug("Failed to write Gemini recv_response debug log", exc_info=True)

                    content = getattr(response, "server_content", None)
                    if content is None:
                        # If there's no server_content, continue to next message
                        logger.debug("GeminiLiveTranslateTransport: received non-server-content response; continuing")
                        continue

                    events_extracted = []

                    # Handle input transcription text (English source audio ASR deltas)
                    if getattr(content, "input_transcription", None) and getattr(content.input_transcription, "text", None):
                        input_delta = content.input_transcription.text
                        if input_delta:
                            events_extracted.append(
                                StreamingInputTranscriptionEvent(
                                    session_id=self.session_id,
                                    event_seq=0,
                                    wall_timestamp=time.time(),
                                    session_time_ms=0.0,
                                    text_delta=input_delta,
                                    cumulative_text="",
                                    correlation_id=self._current_correlation_id
                                )
                            )
                            logger.info(f"GeminiLiveTranslateTransport: Extracted input_transcription delta: {input_delta!r} (corr={self._current_correlation_id})")

                    # A. Handle output transcription text (Spanish translation deltas)
                    if getattr(content, "output_transcription", None) and getattr(content.output_transcription, "text", None):
                        delta = content.output_transcription.text
                        if delta:
                            self._text_chunk_count += 1
                            self._cumulative_text_len += len(delta)
                            if self.tracer:
                                from ...audio.tracing_events import PipelineEvent
                                self.tracer.log_event(
                                    PipelineEvent.TRANSLATED_TEXT_RECEIVED,
                                    correlation_id=self._current_correlation_id,
                                    metadata={
                                        "text_length": len(delta),
                                        "cumulative_text_length": self._cumulative_text_len,
                                        "chunk_index": self._text_chunk_count,
                                        "text_delta": delta
                                    }
                                )
                            events_extracted.append(
                                StreamingPartialTranslationEvent(
                                    session_id=self.session_id,
                                    event_seq=0,
                                    wall_timestamp=time.time(),
                                    session_time_ms=0.0,
                                    text_delta=delta,
                                    cumulative_text="",  # Formatted dynamically by StreamingSession
                                    correlation_id=self._current_correlation_id
                                )
                            )
                            logger.info(f"GeminiLiveTranslateTransport: Extracted output_transcription delta: {delta!r} (corr={self._current_correlation_id})")
                    
                    # B. Handle model turn parts (which may contain translated audio bytes)
                    if content.model_turn:
                        audio_data = b""
                        text_delta = ""
                        for part in content.model_turn.parts:
                            if part.inline_data:
                                audio_data += part.inline_data.data
                            if part.text:
                                text_delta += part.text
                                
                        # If we received translated audio chunks, emit a StreamingTranslationAudioEvent
                        if audio_data:
                            self._received_audio_count += 1
                            self._output_wav.write(audio_data)
                            logger.info(f"EXPERIMENT TIMING: Received Gemini Audio Chunk #{self._received_audio_count} at {time.time():.4f}")
                            if self.tracer:
                                from ...audio.tracing_events import PipelineEvent
                                duration_sec = len(audio_data) / 48000.0
                                self.tracer.log_event(
                                    PipelineEvent.TRANSLATED_AUDIO_RECEIVED,
                                    correlation_id=self._current_correlation_id,
                                    metadata={
                                        "pcm_bytes": len(audio_data),
                                        "sample_rate": 24000,
                                        "duration": duration_sec,
                                        "chunk_index": self._received_audio_count
                                    }
                                )
                            events_extracted.append(
                                StreamingTranslationAudioEvent(
                                    session_id=self.session_id,
                                    event_seq=0,
                                    wall_timestamp=time.time(),
                                    session_time_ms=0.0,
                                    audio_data=audio_data,
                                    mime_type="audio/pcm",
                                    correlation_id=self._current_correlation_id
                                )
                            )
                            
                        # Fallback for text turn parts
                        if text_delta:
                            self._text_chunk_count += 1
                            self._cumulative_text_len += len(text_delta)
                            if self.tracer:
                                from ...audio.tracing_events import PipelineEvent
                                self.tracer.log_event(
                                    PipelineEvent.TRANSLATED_TEXT_RECEIVED,
                                    correlation_id=self._current_correlation_id,
                                    metadata={
                                        "text_length": len(text_delta),
                                        "cumulative_text_length": self._cumulative_text_len,
                                        "chunk_index": self._text_chunk_count,
                                        "text_delta": text_delta,
                                        "is_fallback": True
                                    }
                                )
                            events_extracted.append(
                                StreamingPartialTranslationEvent(
                                    session_id=self.session_id,
                                    event_seq=0,
                                    wall_timestamp=time.time(),
                                    session_time_ms=0.0,
                                    text_delta=text_delta,
                                    cumulative_text="",
                                    correlation_id=self._current_correlation_id
                                )
                            )
                            logger.info(f"GeminiLiveTranslateTransport: Extracted model_turn text parts: {text_delta!r} (corr={self._current_correlation_id})")

                    # C. Turn complete boundary
                    if content.turn_complete:
                        self._last_transcript = ""
                        events_extracted.append(
                            StreamingTranslationCompletedEvent(
                                session_id=self.session_id,
                                event_seq=0,
                                wall_timestamp=time.time(),
                                session_time_ms=0.0,
                                full_text="",
                                correlation_id=self._current_correlation_id
                            )
                        )
                        events_extracted.append(
                            StreamingInputTranscriptionCompletedEvent(
                                session_id=self.session_id,
                                event_seq=0,
                                wall_timestamp=time.time(),
                                session_time_ms=0.0,
                                full_text="",
                                correlation_id=self._current_correlation_id
                            )
                        )
                        # Clear active correlation ID since turn is completed
                        self._current_correlation_id = ""

                    # D. Interruption boundary
                    if content.interrupted:
                        self._last_transcript = ""
                        events_extracted.append(
                            StreamingTranslationCompletedEvent(
                                session_id=self.session_id,
                                event_seq=0,
                                wall_timestamp=time.time(),
                                session_time_ms=0.0,
                                full_text="[Interrupted]",
                                correlation_id=self._current_correlation_id
                            )
                        )

                    if events_extracted:
                        self._pending_events.extend(events_extracted)
                        return self._pending_events.popleft()

                # If we received a message but it doesn't match our event types, continue
                logger.debug(f"GeminiLiveTranslateTransport {self.session_id}: Ignored non-translation server message.")
                continue

            except StopAsyncIteration:
                logger.info(f"GeminiLiveTranslateTransport {self.session_id}: Receive iterator exhausted.")
                return None
            except Exception as e:
                if self.closed:
                    return None

                err_str = str(e)
                # 1008 GoAway = server-initiated session lifetime limit.
                # This is recoverable: signal the reader loop to reconnect instead of
                # emitting a fatal error event.
                if "1008" in err_str or "GoAway" in err_str or "go_away" in err_str.lower():
                    logger.warning(
                        f"GeminiLiveTranslateTransport {self.session_id}: Received GoAway (session lifetime limit). "
                        "Closing transport and signalling reconnect..."
                    )
                    self.closed = True
                    return None  # Reader loop will call reconnect() on None

                logger.error(f"Error in GeminiLiveTranslateTransport {self.session_id} receive: {e}", exc_info=True)
                return StreamingRuntimeErrorEvent(
                    session_id=self.session_id,
                    event_seq=0,
                    wall_timestamp=time.time(),
                    session_time_ms=0.0,
                    error_message=str(e)
                )
        return None

    async def end_user_turn(self) -> None:
        if self.closed:
            return
        await self.sdk_session.send_realtime_input(audio_stream_end=True)
        logger.info(f"GeminiLiveTranslateTransport {self.session_id}: Sent audio_stream_end to SDK.")


    async def cancel_generation(self) -> None:
        if self.closed:
            return

        cancel_msg = {
            "clientContent": {
                "turnComplete": False,
                "interrupted": True
            }
        }
        try:
            await self.sdk_session._ws.send(json.dumps(cancel_msg))
            logger.info(f"GeminiLiveTranslateTransport {self.session_id}: Sent interruption frame.")
        except Exception as e:
            logger.warning(f"Failed to send interruption frame to websocket: {e}")

    async def close(self) -> None:
        self.closed = True
        try:
            self._input_wav.close()
            self._output_wav.close()
        except Exception:
            pass
        try:
            await self.sdk_ctx.__aexit__(None, None, None)
        except Exception as e:
            logger.warning(f"Error closing SDK session: {e}")
        logger.info(f"GeminiLiveTranslateTransport {self.session_id} connection closed.")


class GeminiLiveTranslateRuntime(BaseStreamingRuntime):
    """
    Gemini Live Translation connection runtime leveraging the official google-genai SDK.
    Uses target language translation config for continuous speech-to-speech translation.
    """
    def __init__(self, config):
        self.config = config
        self._client: Optional[genai.Client] = None
        self._initialized = False

    async def initialize(self) -> None:
        api_key = self.config.google_api_key or self.config.gemini_live_api_key
        if not api_key:
            raise ValueError("Google API Key missing. Configure GOOGLE_API_KEY or GEMINI_LIVE_API_KEY.")
            
        # Initialize client targeting v1alpha endpoint for Live Connect / Translation features
        self._client = genai.Client(api_key=api_key, http_options={'api_version': 'v1alpha'})
        self._initialized = True
        logger.info("GeminiLiveTranslateRuntime initialized with GenAI client.")

    async def is_ready(self) -> bool:
        return self._initialized and self._client is not None

    async def connect(
        self,
        session_id: str,
        source_language: str,
        target_language: str,
        on_event: Optional[Callable[[Any], Any]] = None,
        metadata: dict = None
    ) -> BaseStreamingTransport:
        if not self._initialized or not self._client:
            await self.initialize()

        # Normalize modalities and defensively ensure both TEXT and AUDIO are requested.
        modalities_list = [m.strip().upper() for m in self.config.gemini_live_translate_modalities.split(",") if m.strip()]
        if 'TEXT' not in modalities_list:
            modalities_list.append('TEXT')
        if 'AUDIO' not in modalities_list:
            modalities_list.append('AUDIO')
        
        # Configure dedicated translation config (auto source detection, target language ES/etc)
        translation_config = types.TranslationConfig(
            target_language_code=self.config.target_language,
            echo_target_language=self.config.gemini_live_translate_echo
        )
        
        # Log Gemini configuration on startup (Suggestion 6)
        sdk_version = getattr(genai, "__version__", "unknown")
        logger.info("=== GEMINI TRANSLATION CONFIGURATION ===")
        logger.info(f"SDK Version: {sdk_version}")
        logger.info(f"Model: {self.config.gemini_live_translate_model}")
        logger.info(f"Target Language Code: {self.config.target_language}")
        logger.info(f"Echo Target Language: {self.config.gemini_live_translate_echo}")
        logger.info(f"Response Modalities: {modalities_list}")
        logger.info(f"Voice Name: {self.config.gemini_live_voice_name}")
        logger.info("=========================================")

        # Build SDK configuration object for live translation model
        sdk_config = types.LiveConnectConfig(
            response_modalities=modalities_list,
            translation_config=translation_config,
            input_audio_transcription=types.AudioTranscriptionConfig(),
            output_audio_transcription=types.AudioTranscriptionConfig(),
            speech_config=types.SpeechConfig(
                voice_config=types.VoiceConfig(
                    prebuilt_voice_config=types.PrebuiltVoiceConfig(
                        voice_name=self.config.gemini_live_voice_name
                    )
                )
            )
        )
        if DEBUG_GEMINI:
            try:
                session_folder = metadata.get("session_folder") if metadata else None
                if session_folder:
                    session_debug_log_path = Path(__file__).resolve().parents[4] / "output" / session_folder / "gemini.log"
                else:
                    session_debug_log_path = DEBUG_LOG_PATH
                session_debug_log_path.parent.mkdir(parents=True, exist_ok=True)
                with open(session_debug_log_path, "a", encoding="utf-8") as _f:
                    _f.write(json.dumps({
                        "time": time.time(),
                        "session": session_id,
                        "action": "sdk_config",
                        "response_modalities": modalities_list,
                        "translation_config": {"target_language": self.config.target_language, "echo": self.config.gemini_live_translate_echo},
                        "model": self.config.gemini_live_translate_model,
                        "voice_name": self.config.gemini_live_voice_name
                    }) + "\n")
            except Exception:
                logger.debug("Failed to write gemini sdk_config debug log", exc_info=True)
        
        ctx = self._client.aio.live.connect(
            model=self.config.gemini_live_translate_model,
            config=sdk_config
        )
        sdk_session = await ctx.__aenter__()
        
        tracer = metadata.get("tracer") if metadata else None
        session_folder = metadata.get("session_folder") if metadata else None
        transport = GeminiLiveTranslateTransport(
            session_id=session_id,
            sdk_session=sdk_session,
            sdk_ctx=ctx,
            source_language=source_language,
            target_language=target_language,
            model_name=self.config.gemini_live_translate_model,
            modalities=modalities_list,
            voice_name=self.config.gemini_live_voice_name,
            tracer=tracer,
            session_folder=session_folder
        )
        return transport

    async def shutdown(self) -> None:
        self._client = None
        self._initialized = False
        logger.info("GeminiLiveTranslateRuntime shut down.")
