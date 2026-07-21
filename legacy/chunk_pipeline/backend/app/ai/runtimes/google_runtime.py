import os
import sys
import time
import logging
from datetime import datetime
from typing import AsyncIterator
import asyncio
from threading import Thread

from .base import BaseRuntime
from ..types import RuntimeRequest, TranslationResult, TranslationMetrics

logger = logging.getLogger("onemeta.ai.runtimes.google")

class GoogleGeminiRuntime(BaseRuntime):
    """
    Google Gemini cloud API runtime for real-time speech translation.
    Routes generation requests to Google AI Studio.
    """
    def __init__(self, config):
        super().__init__(config)
        self._initialized = False
        self._client = None

    async def initialize(self) -> None:
        """
        Validates API key and initializes the Google GenAI Client.
        """
        if self._initialized:
            return

        logger.info(f"Initializing GoogleGeminiRuntime (Model: {self.config.google_model})...")

        # Load API Key
        api_key = self.config.google_api_key
        if not api_key:
            api_key = os.getenv("GOOGLE_API_KEY")

        if not api_key:
            logger.error("GOOGLE_API_KEY not configured. Cannot initialize GoogleGeminiRuntime.")
            raise ValueError("GOOGLE_API_KEY not configured in backend/.env or AIConfig.")

        # Import GenAI SDK
        try:
            from google import genai
        except ImportError as e:
            logger.error("google-genai SDK is not installed in the virtual environment.")
            raise RuntimeError("Missing google-genai library. Run: pip install google-genai") from e

        # Initialize Client
        try:
            self._client = genai.Client(api_key=api_key)
        except Exception as e:
            logger.error(f"Failed to initialize Google GenAI Client: {e}")
            raise RuntimeError(f"Failed to initialize Google GenAI Client: {e}") from e

        self._initialized = True
        logger.info("GoogleGeminiRuntime initialized successfully.")

    async def is_ready(self) -> bool:
        """
        Lightweight operational health check.
        """
        return self._initialized

    async def stream_generate(self, request: RuntimeRequest) -> AsyncIterator[TranslationResult]:
        """
        Streams generated translation chunks from Gemini using a non-blocking queue.
        """
        if not self._initialized:
            raise RuntimeError("GoogleGeminiRuntime has not been initialized.")

        queue = asyncio.Queue()
        loop = asyncio.get_running_loop()

        def worker():
            import io
            import wave
            from google.genai import types
            from google.genai import errors

            try:
                # 1. If input is raw PCM16, wrap it in a valid WAV header container
                audio_payload = request.audio_bytes
                if request.audio_format == "pcm16":
                    wav_io = io.BytesIO()
                    with wave.open(wav_io, "wb") as w:
                        w.setnchannels(1)
                        w.setsampwidth(2)  # 16-bit PCM
                        w.setframerate(request.sample_rate)
                        w.writeframes(audio_payload)
                    audio_payload = wav_io.getvalue()

                payload_size = len(audio_payload)
                part = types.Part.from_bytes(data=audio_payload, mime_type="audio/wav")

                # 2. Translate Prompt
                system_prompt = (
                    "You are a real-time speech translation engine.\n"
                    "Translate the provided English speech into Spanish.\n"
                    "Return ONLY the translated Spanish text.\n"
                    "Do not explain.\n"
                    "Do not summarize.\n"
                    "Do not add additional words.\n"
                    "Do not return the original English."
                )

                t_start = time.perf_counter()
                first_token_time = None
                cumulative_text = ""
                started_dt = datetime.now()

                # 3. Call streaming API
                response_stream = self._client.models.generate_content_stream(
                    model=self.config.google_model,
                    contents=[part, system_prompt],
                    config=types.GenerateContentConfig(
                        temperature=self.config.temperature,
                        top_p=self.config.top_p
                    )
                )

                for chunk in response_stream:
                    token = chunk.text or ""
                    if token and first_token_time is None:
                        first_token_time = time.perf_counter()

                    cumulative_text += token
                    ttft_ms = (first_token_time - t_start) * 1000.0 if first_token_time else 0.0
                    elapsed_ms = (time.perf_counter() - t_start) * 1000.0

                    result = TranslationResult(
                        chunk_id=request.chunk_id,
                        sequence_number=request.sequence_number,
                        translated_text=token,
                        source_language=request.source_language,
                        target_language=request.target_language,
                        finished=False,
                        ready_for_tts=True,
                        metrics=TranslationMetrics(
                            ttft_ms=ttft_ms,
                            total_response_time_ms=elapsed_ms,
                            payload_size_bytes=payload_size,
                            audio_duration_ms=len(request.audio_bytes) / 32.0,  # 16kHz PCM16 Mono: 32 bytes/ms
                            translation_length_chars=len(cumulative_text),
                            chunk_number=request.sequence_number
                        )
                    )
                    loop.call_soon_threadsafe(queue.put_nowait, result)

                # Send final completed item
                t_end = time.perf_counter()
                finished_dt = datetime.now()
                latency_ms = int((t_end - t_start) * 1000)

                # 4. Logging Requirement
                logger.info(
                    f"\n==============================\n"
                    f"Google Gemini Runtime\n"
                    f"==============================\n"
                    f"Request Id: {request.chunk_id}\n"
                    f"Chunk Id: {request.chunk_id}\n"
                    f"Model: {self.config.google_model}\n"
                    f"Started: {started_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}\n"
                    f"Finished: {finished_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}\n"
                    f"Latency: {latency_ms} ms\n"
                    f"Translation: {cumulative_text}\n"
                    f"=============================="
                )

                final_result = TranslationResult(
                    chunk_id=request.chunk_id,
                    sequence_number=request.sequence_number,
                    translated_text="",
                    source_language=request.source_language,
                    target_language=request.target_language,
                    finished=True,
                    ready_for_tts=True,
                    metrics=TranslationMetrics(
                        ttft_ms=(first_token_time - t_start) * 1000.0 if first_token_time else latency_ms,
                        total_response_time_ms=latency_ms,
                        payload_size_bytes=payload_size,
                        audio_duration_ms=len(request.audio_bytes) / 32.0,
                        translation_length_chars=len(cumulative_text),
                        chunk_number=request.sequence_number
                    )
                )
                loop.call_soon_threadsafe(queue.put_nowait, final_result)

            except errors.APIError as e:
                logger.error(f"Google Gemini API error during streaming translation: {e}")
                loop.call_soon_threadsafe(queue.put_nowait, e)
            except Exception as e:
                logger.error(f"Unexpected error in GoogleGeminiRuntime streaming thread: {e}")
                loop.call_soon_threadsafe(queue.put_nowait, e)

        # Launch requests streaming in a background daemon thread
        thread = Thread(target=worker, daemon=True)
        thread.start()

        # Ingest values from the queue asynchronously
        while True:
            item = await queue.get()
            if isinstance(item, Exception):
                # Return structured error translation result instead of crashing
                yield TranslationResult(
                    chunk_id=request.chunk_id,
                    sequence_number=request.sequence_number,
                    translated_text="[Translation Error]",
                    source_language=request.source_language,
                    target_language=request.target_language,
                    finished=True,
                    ready_for_tts=False,
                    metadata={"error": str(item)}
                )
                break
            yield item
            if item.finished:
                break

    async def shutdown(self) -> None:
        """
        Shuts down connections.
        """
        logger.info("Shutting down GoogleGeminiRuntime...")
        self._initialized = False
        self._client = None
        logger.info("GoogleGeminiRuntime shutdown complete.")
