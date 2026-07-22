import logging
import asyncio
import time
import json
import base64
import traceback
from typing import Any, Optional, Callable, List

from google import genai
from google.genai import types

import os
from pathlib import Path
from ..streaming import BaseStreamingTransport, BaseStreamingRuntime
from ..events import (
    StreamingPartialTranslationEvent,
    StreamingTranslationCompletedEvent,
    StreamingRuntimeErrorEvent,
    StreamingTranslationAudioEvent
)
from ...transport.packet import StreamingAudioPacket

DEBUG_GEMINI = True
try:
    DEBUG_LOG_PATH = Path(__file__).resolve().parents[4] / "output" / "gemini_debug.log"
    DEBUG_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Gemini debug logging enabled -> {DEBUG_LOG_PATH}")
except Exception:
    DEBUG_GEMINI = False

logger = logging.getLogger("onemeta.ai.runtimes.gemini_live")


# Helper utilities for diagnostic dumps
def _serialize_for_json(obj: Any):
    try:
        if obj is None:
            return None
        if isinstance(obj, (bytes, bytearray)):
            return {"__bytes_base64__": base64.b64encode(obj).decode("ascii")}
        if isinstance(obj, (str, int, float, bool)):
            return obj
        if isinstance(obj, (list, tuple)):
            return [_serialize_for_json(i) for i in obj]
        if isinstance(obj, dict):
            return {str(k): _serialize_for_json(v) for k, v in obj.items()}

        # Try common object -> dict conversions used by SDKs
        for method in ("model_dump", "dict", "to_dict"):
            if hasattr(obj, method) and callable(getattr(obj, method)):
                try:
                    return _serialize_for_json(getattr(obj, method)())
                except Exception:
                    pass

        if hasattr(obj, "__dict__"):
            return {"__class__": obj.__class__.__name__, "__dict__": {k: _serialize_for_json(v) for k, v in vars(obj).items()}}

        return repr(obj)
    except Exception as e:
        return f"<serialization_error: {repr(e)}>"


def _append_json_file(path: Path, data: Any):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
            f.write("\n")
    except Exception:
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(repr(data) + "\n")
        except Exception:
            pass

class GeminiLiveTransport(BaseStreamingTransport):
    """
    Manages a stateful WebSocket connection to the Gemini Multimodal Live API
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
        voice_name: str
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
        import collections
        self._pending_events = collections.deque()
        self._last_transcript = ""
        
        # Initialize WAV writers for audio verification (WAV Capture)
        from pathlib import Path
        output_dir = Path(__file__).resolve().parents[4] / "output"
        from ...audio.wav_writer import WavWriter
        self._input_wav = WavWriter(output_dir / f"{session_id}_input.wav", sample_rate=16000, channels=1)
        self._output_wav = WavWriter(output_dir / f"{session_id}_output.wav", sample_rate=24000, channels=1)
        
        self._current_correlation_id = ""
        # Keep a single active async generator for receiving messages
        self._receive_iterator = self.sdk_session.receive()
        self._input_packets_sent_count = 0
        self._received_audio_count = 0

    async def send_setup(self) -> None:
        # The google-genai SDK connects and performs setup handshake in a single call,
        # so this is a no-op to satisfy the BaseStreamingTransport interface.
        pass

    async def send_packet(self, packet: StreamingAudioPacket) -> None:
        if self.closed:
            return

        if packet.metadata:
            self._current_correlation_id = packet.metadata.correlation_id

        self._input_packets_sent_count += 1
        # Convert the zero-copy memoryview packet to bytes for transmission
        pcm_bytes = bytes(packet.pcm_data)
        self._input_wav.write(pcm_bytes)
        
        # Log timestamp of forwarded mic frame to Gemini
        logger.info(f"EXPERIMENT TIMING: Forwarded Mic Packet #{self._input_packets_sent_count} (corr={self._current_correlation_id}) to Gemini at {time.time():.4f}")
        
        # Logging websocket send (Suggestion 1)
        if DEBUG_GEMINI:
            try:
                prefix_b64 = base64.b64encode(pcm_bytes[:64]).decode("ascii") if "base64" in globals() else ""
                with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as _f:
                    _f.write(json.dumps({
                        "time": time.time(),
                        "session": self.session_id,
                        "action": "send_packet",
                        "sample_rate": packet.sample_rate,
                        "length": len(pcm_bytes),
                        "corr": self._current_correlation_id
                    }) + "\n")
            except Exception:
                pass

        # Stream the audio buffer via the SDK's realtime input interface
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
                # Advance the async generator to read the next LiveServerMessage
                response = await self._receive_iterator.__anext__()

                # DIAGNOSTIC: Dump the complete response object using multiple fallbacks
                if DEBUG_GEMINI:
                    try:
                        out_dir = DEBUG_LOG_PATH.parent if 'DEBUG_LOG_PATH' in globals() else Path(__file__).resolve().parents[4] / 'output'
                        full_dump = {"time": time.time(), "session": self.session_id}
                        # Try model_dump / dict / to_dict in order
                        for method in ("model_dump", "dict", "to_dict"):
                            if hasattr(response, method) and callable(getattr(response, method)):
                                try:
                                    full_dump[method] = _serialize_for_json(getattr(response, method)())
                                except Exception:
                                    full_dump[method] = {"error": traceback.format_exc()}

                        # Vars / dir / repr as fallbacks
                        try:
                            full_dump['vars'] = _serialize_for_json(vars(response))
                        except Exception:
                            full_dump['vars'] = {"error": traceback.format_exc()}
                        full_dump['dir'] = dir(response)
                        full_dump['repr'] = repr(response)
                        _append_json_file(out_dir / 'gemini_full_response.json', full_dump)
                    except Exception:
                        pass

                if response and response.server_content:
                    content = response.server_content
                    
                    # Logging websocket receive (Suggestion 1) and detailed dumps
                    if DEBUG_GEMINI:
                        try:
                            out_dir = DEBUG_LOG_PATH.parent if 'DEBUG_LOG_PATH' in globals() else Path(__file__).resolve().parents[4] / 'output'
                            # keep legacy concise rep
                            with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as _f:
                                _f.write(json.dumps({"time": time.time(), "session": self.session_id, "action": "recv_response", "repr": repr(response)}) + "\n")

                            # Server content deep-dump
                            sc = response.server_content
                            sc_dump = {"time": time.time(), "session": self.session_id}
                            for method in ("model_dump", "dict", "to_dict"):
                                if hasattr(sc, method) and callable(getattr(sc, method)):
                                    try:
                                        sc_dump[method] = _serialize_for_json(getattr(sc, method)())
                                    except Exception:
                                        sc_dump[method] = {"error": traceback.format_exc()}
                            try:
                                sc_dump['vars'] = _serialize_for_json(vars(sc))
                            except Exception:
                                sc_dump['vars'] = {"error": traceback.format_exc()}
                            sc_dump['dir'] = dir(sc)
                            _append_json_file(out_dir / 'server_content_dump.json', sc_dump)

                            # Model turn dump + parts
                            if getattr(sc, 'model_turn', None):
                                mt = sc.model_turn
                                mt_dump = {"time": time.time(), "session": self.session_id}
                                for method in ("model_dump", "dict", "to_dict"):
                                    if hasattr(mt, method) and callable(getattr(mt, method)):
                                        try:
                                            mt_dump[method] = _serialize_for_json(getattr(mt, method)())
                                        except Exception:
                                            mt_dump[method] = {"error": traceback.format_exc()}
                                try:
                                    mt_dump['vars'] = _serialize_for_json(vars(mt))
                                except Exception:
                                    mt_dump['vars'] = {"error": traceback.format_exc()}
                                mt_dump['dir'] = dir(mt)
                                _append_json_file(out_dir / 'model_turn_dump.json', mt_dump)

                                parts_list = []
                                try:
                                    for idx, part in enumerate(getattr(mt, 'parts', [])):
                                        pd = {"index": idx, "type": str(type(part))}
                                        pd['dir'] = dir(part)
                                        for method in ("model_dump", "dict", "to_dict"):
                                            if hasattr(part, method) and callable(getattr(part, method)):
                                                try:
                                                    pd[method] = _serialize_for_json(getattr(part, method)())
                                                except Exception:
                                                    pd[method] = {"error": traceback.format_exc()}

                                        # Explicitly include requested fields even if None
                                        for field in ('text', 'inline_data', 'file_data', 'function_call', 'function_response', 'executable_code', 'code_execution_result'):
                                            try:
                                                pd[field] = _serialize_for_json(getattr(part, field, None))
                                            except Exception:
                                                pd[field] = {"error": traceback.format_exc()}

                                        # Summarize inline_data if present
                                        inline = getattr(part, 'inline_data', None)
                                        if inline:
                                            try:
                                                b = getattr(inline, 'data', None)
                                                mime = getattr(inline, 'mime_type', getattr(inline, 'mimeType', None))
                                                pd['inline_data_summary'] = {
                                                    'mime_type': mime,
                                                    'size': len(b) if isinstance(b, (bytes, bytearray)) else None,
                                                    'first_20_bytes_b64': base64.b64encode(b[:20]).decode('ascii') if isinstance(b, (bytes, bytearray)) else None
                                                }
                                            except Exception:
                                                pd['inline_data_summary'] = {"error": traceback.format_exc()}

                                        parts_list.append(pd)
                                    _append_json_file(out_dir / 'part_dump.json', {"time": time.time(), "session": self.session_id, "parts": parts_list})
                                except Exception:
                                    _append_json_file(out_dir / 'part_dump.json', {"time": time.time(), "session": self.session_id, "error": traceback.format_exc()})

                            # Output transcription dump
                            ot = getattr(sc, 'output_transcription', None)
                            if ot is not None:
                                ot_dump = {"time": time.time(), "session": self.session_id}
                                for method in ("model_dump", "dict", "to_dict"):
                                    if hasattr(ot, method) and callable(getattr(ot, method)):
                                        try:
                                            ot_dump[method] = _serialize_for_json(getattr(ot, method)())
                                        except Exception:
                                            ot_dump[method] = {"error": traceback.format_exc()}
                                try:
                                    ot_dump['vars'] = _serialize_for_json(vars(ot))
                                except Exception:
                                    ot_dump['vars'] = {"error": traceback.format_exc()}
                                ot_dump['dir'] = dir(ot)
                                _append_json_file(out_dir / 'output_transcription_dump.json', ot_dump)

                        except Exception:
                            pass

                    events_extracted = []

                    # A. Output transcription (Spanish translation deltas)
                    if content.output_transcription and content.output_transcription.text:
                        delta = content.output_transcription.text
                        if delta:
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
                    
                    # B. Text delta (optional model turn text fallback)
                    if content.model_turn:
                        text_delta = ""
                        audio_data = b""
                        for part in content.model_turn.parts:
                            if part.inline_data:
                                audio_data += part.inline_data.data
                            if part.text:
                                text_delta += part.text

                        if audio_data:
                            self._received_audio_count += 1
                            self._output_wav.write(audio_data)
                            # Try to extract inline_data mime type from parts (if provided)
                            mime_type = None
                            try:
                                for p in content.model_turn.parts:
                                    if getattr(p, 'inline_data', None):
                                        mime_type = getattr(p.inline_data, 'mime_type', getattr(p.inline_data, 'mimeType', None))
                                        if mime_type:
                                            break
                            except Exception:
                                mime_type = None

                            logger.info(f"EXPERIMENT TIMING: Received Gemini Audio Chunk #{self._received_audio_count} at {time.time():.4f} (mime={mime_type}, bytes={len(audio_data)})")
                            events_extracted.append(
                                StreamingTranslationAudioEvent(
                                    session_id=self.session_id,
                                    event_seq=0,
                                    wall_timestamp=time.time(),
                                    session_time_ms=0.0,
                                    audio_data=audio_data,
                                    mime_type=mime_type or "audio/pcm",
                                    correlation_id=self._current_correlation_id
                                )
                            )

                        if text_delta:
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

                # Ignore other system frames
                logger.debug(f"GeminiLiveTransport {self.session_id}: Ignored non-translation server message.")
                continue

            except StopAsyncIteration:
                logger.info(f"GeminiLiveTransport {self.session_id}: Receive iterator exhausted.")
                return None
            except Exception as e:
                if not self.closed:
                    logger.error(f"Error in GeminiLiveTransport {self.session_id} receive: {e}", exc_info=True)
                    return StreamingRuntimeErrorEvent(
                        session_id=self.session_id,
                        event_seq=0,
                        wall_timestamp=time.time(),
                        session_time_ms=0.0,
                        error_message=str(e)
                    )
                return None
        return None

    async def end_user_turn(self) -> None:
        if self.closed:
            return
        await self.sdk_session.send_realtime_input(audio_stream_end=True)
        logger.info(f"GeminiLiveTransport {self.session_id}: Sent audio_stream_end to SDK.")

    async def cancel_generation(self) -> None:
        if self.closed:
            return

        # Build raw clientContent interruption message
        cancel_msg = {
            "clientContent": {
                "turnComplete": False,
                "interrupted": True
            }
        }
        try:
            # Bypass SDK send methods to issue direct interruption signal to underlying WS
            await self.sdk_session._ws.send(json.dumps(cancel_msg))
            logger.info(f"GeminiLiveTransport {self.session_id}: Sent interruption client frame.")
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
            # Safely exit the SDK's async context manager to release socket connections
            await self.sdk_ctx.__aexit__(None, None, None)
        except Exception as e:
            logger.warning(f"Error closing SDK session: {e}")
        logger.info(f"GeminiLiveTransport {self.session_id} connection closed.")

class GeminiLiveRuntime(BaseStreamingRuntime):
    """
    Gemini Multimodal Live connection runtime leveraging the official google-genai SDK.
    """
    def __init__(self, config):
        self.config = config
        self._client: Optional[genai.Client] = None
        self._initialized = False

    async def initialize(self) -> None:
        api_key = self.config.gemini_live_api_key or self.config.google_api_key
        if not api_key:
            raise ValueError("Google API Key missing. Configure GEMINI_LIVE_API_KEY or GOOGLE_API_KEY.")
            
        # Initialize client targeting v1alpha endpoint for Live Connect features
        self._client = genai.Client(api_key=api_key, http_options={'api_version': 'v1alpha'})
        self._initialized = True
        logger.info("GeminiLiveRuntime initialized with GenAI client.")

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

        # Parse modalities list from config (e.g. "AUDIO"). Normalize to upper-case.
        modalities_list = [m.strip().upper() for m in self.config.gemini_live_modalities.split(",") if m.strip()]
        if DEBUG_GEMINI:
            try:
                out_dir = DEBUG_LOG_PATH.parent if 'DEBUG_LOG_PATH' in globals() else Path(__file__).resolve().parents[4] / 'output'
                _append_json_file(out_dir / 'sdk_config_dump.json', {"time": time.time(), "session": session_id, "modalities_list": modalities_list, "sdk_version": getattr(genai, '__version__', 'unknown'), "model": self.config.gemini_live_model})
            except Exception:
                pass
        
        # Log Gemini configuration on startup (Suggestion 6)
        sdk_version = getattr(genai, "__version__", "unknown")
        logger.info("=== GEMINI LIVE CONFIGURATION ===")
        logger.info(f"SDK Version: {sdk_version}")
        logger.info(f"Model: {self.config.gemini_live_model}")
        logger.info(f"System Instruction: Translate {source_language} speech to {target_language}. Speak clearly only in {target_language}.")
        logger.info(f"Response Modalities: {modalities_list}")
        logger.info(f"Voice Name: {self.config.gemini_live_voice_name}")
        logger.info("=========================================")

        # Build SDK configuration object
        sdk_config = types.LiveConnectConfig(
            response_modalities=modalities_list,
            system_instruction=types.Content(
                parts=[types.Part(text=f"Translate {source_language} speech to {target_language}. Speak clearly only in {target_language}.")]
            ),
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
                out_dir = DEBUG_LOG_PATH.parent if 'DEBUG_LOG_PATH' in globals() else Path(__file__).resolve().parents[4] / 'output'
                _append_json_file(out_dir / 'sdk_config_dump.json', {"time": time.time(), "session": session_id, "sdk_config": _serialize_for_json(sdk_config)})
            except Exception:
                pass
        
        # Retrieve context manager
        ctx = self._client.aio.live.connect(
            model=self.config.gemini_live_model,
            config=sdk_config
        )
        # Manually enter the async context manager to retain persistent session lifecycle control
        sdk_session = await ctx.__aenter__()
        
        transport = GeminiLiveTransport(
            session_id=session_id,
            sdk_session=sdk_session,
            sdk_ctx=ctx,
            source_language=source_language,
            target_language=target_language,
            model_name=self.config.gemini_live_model,
            modalities=modalities_list,
            voice_name=self.config.gemini_live_voice_name
        )
        return transport

    async def shutdown(self) -> None:
        self._client = None
        self._initialized = False
        logger.info("GeminiLiveRuntime shut down.")
