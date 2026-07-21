import logging
import asyncio
import time
import json
from typing import Any, Optional, Callable, List

from google import genai
from google.genai import types

from ..streaming import BaseStreamingTransport, BaseStreamingRuntime
from ..events import (
    StreamingPartialTranslationEvent,
    StreamingTranslationCompletedEvent,
    StreamingRuntimeErrorEvent
)
from ...transport.packet import StreamingAudioPacket

logger = logging.getLogger("onemeta.ai.runtimes.gemini_live")

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
        
        self._current_correlation_id = ""
        # Keep a single active async generator for receiving messages
        self._receive_iterator = self.sdk_session.receive()

    async def send_setup(self) -> None:
        # The google-genai SDK connects and performs setup handshake in a single call,
        # so this is a no-op to satisfy the BaseStreamingTransport interface.
        pass

    async def send_packet(self, packet: StreamingAudioPacket) -> None:
        if self.closed:
            return

        if packet.metadata and packet.metadata.correlation_id:
            self._current_correlation_id = packet.metadata.correlation_id

        # Convert the zero-copy memoryview packet to bytes for transmission
        pcm_bytes = bytes(packet.pcm_data)
        
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

        while not self.closed:
            try:
                # Advance the async generator to read the next LiveServerMessage
                response = await self._receive_iterator.__anext__()
                if response and response.server_content:
                    content = response.server_content
                    
                    # A. Output transcription (Spanish translation deltas)
                    if content.output_transcription and content.output_transcription.text:
                        return StreamingPartialTranslationEvent(
                            session_id=self.session_id,
                            event_seq=0,
                            wall_timestamp=time.time(),
                            session_time_ms=0.0,
                            text_delta=content.output_transcription.text,
                            cumulative_text="",  # Formatted dynamically by StreamingSession
                            correlation_id=self._current_correlation_id
                        )
                    
                    # B. Text delta (optional model turn text fallback)
                    if content.model_turn:
                        text_delta = ""
                        for part in content.model_turn.parts:
                            if part.text:
                                text_delta += part.text
                        if text_delta:
                            return StreamingPartialTranslationEvent(
                                session_id=self.session_id,
                                event_seq=0,
                                wall_timestamp=time.time(),
                                session_time_ms=0.0,
                                text_delta=text_delta,
                                cumulative_text="",
                                correlation_id=self._current_correlation_id
                            )

                    # C. Turn complete boundary
                    if content.turn_complete:
                        return StreamingTranslationCompletedEvent(
                            session_id=self.session_id,
                            event_seq=0,
                            wall_timestamp=time.time(),
                            session_time_ms=0.0,
                            full_text="",
                            correlation_id=self._current_correlation_id
                        )

                    # D. Interruption boundary
                    if content.interrupted:
                        return StreamingTranslationCompletedEvent(
                            session_id=self.session_id,
                            event_seq=0,
                            wall_timestamp=time.time(),
                            session_time_ms=0.0,
                            full_text="[Interrupted]",
                            correlation_id=self._current_correlation_id
                        )
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

        # Parse modalities list from config (e.g. "AUDIO")
        modalities_list = [m.strip() for m in self.config.gemini_live_modalities.split(",")]
        
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
