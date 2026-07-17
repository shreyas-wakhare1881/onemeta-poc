import asyncio
import logging
import base64
import json
import requests
from typing import AsyncIterator
from threading import Thread

from .base import BaseRuntime
from ..types import RuntimeRequest, TranslationResult, TranslationMetrics
from ..prompts import PromptBuilder

logger = logging.getLogger("onemeta.ai.runtimes.ollama")

class OllamaGemmaRuntime(BaseRuntime):
    """
    Local Ollama server API runtime.
    Routes generation requests to an active Ollama host.
    """
    def __init__(self, config):
        super().__init__(config)
        self._initialized = False

    async def initialize(self) -> None:
        """
        Pings the Ollama host, checks if the model is loaded, and executes warmup.
        Fails loudly if the server is unreachable.
        """
        if self._initialized:
            return

        logger.info(f"Initializing OllamaGemmaRuntime (Host: {self.config.ollama_host}, Model: {self.config.ollama_model})...")
        
        # 1. Ping the server health endpoint
        try:
            # We run the synchronous request in a thread to keep initialize async-friendly
            def ping():
                return requests.get(f"{self.config.ollama_host}/api/tags", timeout=5)
            
            response = await asyncio.to_thread(ping)
            if response.status_code != 200:
                raise RuntimeError(f"Ollama health check returned status code {response.status_code}")
        except Exception as e:
            logger.error(f"Failed to connect to Ollama server at {self.config.ollama_host}: {e}")
            raise RuntimeError(f"Ollama server unreachable at {self.config.ollama_host}: {e}") from e

        # 2. Check if the model is pulled
        try:
            models_data = response.json()
            available_models = [m.get("name", "") for m in models_data.get("models", [])]
            
            # Match model names (e.g. gemma4:12b or google/gemma-4-12B-it)
            model_found = any(self.config.ollama_model in name or name in self.config.ollama_model for name in available_models)
            if not model_found:
                raise RuntimeError(f"Model '{self.config.ollama_model}' is not installed. Run: ollama pull {self.config.ollama_model}")
        except Exception as e:
            if isinstance(e, RuntimeError):
                raise e
            logger.error(f"Could not verify model availability in Ollama registry: {e}")
            raise RuntimeError(f"Could not verify model availability in Ollama registry: {e}") from e

        # 3. Warmup the model with a simple prompt
        try:
            def warmup():
                payload = {
                    "model": self.config.ollama_model,
                    "messages": [
                        {
                            "role": "user",
                            "content": "Respond with OK."
                        }
                    ],
                    "stream": False
                }
                return requests.post(f"{self.config.ollama_host}/api/chat", json=payload, timeout=10)
            
            await asyncio.to_thread(warmup)
            logger.info("OllamaGemmaRuntime model warmup completed.")
        except Exception as e:
            logger.warning(f"Ollama model warmup failed: {e}")

        self._initialized = True
        logger.info("OllamaGemmaRuntime initialized successfully.")

    async def is_ready(self) -> bool:
        """
        Performs a lightweight ping check to verify server availability.
        """
        if not self._initialized:
            return False
            
        try:
            def ping():
                return requests.get(self.config.ollama_host, timeout=2)
            response = await asyncio.to_thread(ping)
            return response.status_code == 200
        except Exception:
            return False

    async def stream_generate(self, request: RuntimeRequest) -> AsyncIterator[TranslationResult]:
        """
        Streams generated translation tokens from Ollama using a non-blocking queue.
        """
        if not self._initialized:
            raise RuntimeError("OllamaGemmaRuntime has not been initialized.")

        queue = asyncio.Queue()
        loop = asyncio.get_running_loop()

        def worker():
            import time
            import io
            import wave
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
                
                # 2. Base64 encode the WAV byte stream
                audio_base64 = base64.b64encode(audio_payload).decode("utf-8")
                payload_size = len(audio_base64)
                
                # 3. Build dynamic prompt and payload structure for Ollama chat API
                prompt_text = PromptBuilder.build_translation_prompt(request.source_language, request.target_language)
                
                payload = {
                    "model": self.config.ollama_model,
                    "messages": [
                        {
                            "role": "user",
                            "content": prompt_text,
                            "images": [audio_base64]
                        }
                    ],
                    "stream": True,
                    "options": {
                        "temperature": self.config.temperature,
                        "top_p": self.config.top_p
                    }
                }
                
                url = f"{self.config.ollama_host}/api/chat"
                
                start_time = time.perf_counter()
                first_token_time = None
                cumulative_text = ""
                
                retries = self.config.max_retries
                while retries > 0:
                    try:
                        response = requests.post(
                            url, 
                            json=payload, 
                            stream=True, 
                            timeout=self.config.request_timeout
                        )
                        response.raise_for_status()
                        
                        for line in response.iter_lines():
                            if line:
                                data = json.loads(line.decode("utf-8"))
                                message = data.get("message", {})
                                token = message.get("content", "")
                                done = data.get("done", False)
                                
                                if token and first_token_time is None:
                                    first_token_time = time.perf_counter()
                                
                                cumulative_text += token
                                ttft_ms = (first_token_time - start_time) * 1000.0 if first_token_time else 0.0
                                elapsed_ms = (time.perf_counter() - start_time) * 1000.0
                                
                                result = TranslationResult(
                                    chunk_id=request.chunk_id,
                                    sequence_number=request.sequence_number,
                                    translated_text=token,
                                    source_language=request.source_language,
                                    target_language=request.target_language,
                                    finished=done,
                                    ready_for_tts=True,
                                    metrics=TranslationMetrics(
                                        ttft_ms=ttft_ms,
                                        total_response_time_ms=elapsed_ms,
                                        payload_size_bytes=payload_size,
                                        audio_duration_ms=audio_duration_ms,
                                        translation_length_chars=len(cumulative_text),
                                        chunk_number=request.sequence_number
                                    )
                                )
                                
                                # Dispatch TranslationResult safely to the event loop queue
                                loop.call_soon_threadsafe(queue.put_nowait, result)
                                if done:
                                    break
                        break # Success, break retry loop
                    except Exception as e:
                        retries -= 1
                        if retries == 0:
                            raise e
                        logger.warning(f"Ollama stream API chat request failed: {e}. Retries remaining: {retries}")
                        time.sleep(1.0)
            except Exception as e:
                loop.call_soon_threadsafe(queue.put_nowait, e)

        # Launch requests streaming in a background daemon thread
        thread = Thread(target=worker, daemon=True)
        thread.start()

        # Ingest values from the queue asynchronously
        while True:
            item = await queue.get()
            if isinstance(item, Exception):
                raise item
            yield item
            if item.finished:
                break

    async def shutdown(self) -> None:
        """
        Shuts down connections and releases handles.
        """
        logger.info("Shutting down OllamaGemmaRuntime...")
        self._initialized = False
        logger.info("OllamaGemmaRuntime shutdown complete.")
