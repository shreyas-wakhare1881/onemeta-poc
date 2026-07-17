import asyncio
import logging
from typing import AsyncIterator
import numpy as np

import time
from .base import BaseRuntime
from ..types import RuntimeRequest, TranslationResult, TranslationMetrics
from ..prompts import PromptBuilder

logger = logging.getLogger("onemeta.ai.runtimes.transformers")

class TransformersGemmaRuntime(BaseRuntime):
    """
    Native PyTorch / Hugging Face Transformers local execution runtime.
    Processes audio waveforms directly using GPU weights.
    """
    def __init__(self, config):
        super().__init__(config)
        self.model = None
        self.processor = None
        self._initialized = False
        self.device = None
        self._active_threads = []

    async def initialize(self) -> None:
        """
        Initializes and warms up the local Gemma 4 Audio-Native model.
        Fails loudly if dependencies, drivers, or model weights are missing.
        """
        if self._initialized:
            return

        logger.info(f"Initializing TransformersGemmaRuntime (Model: {self.config.model_path}, Device: {self.config.device})...")
        
        try:
            import torch
            from transformers import AutoProcessor, AutoModelForMultimodalLM
        except ImportError as e:
            logger.error("Failed to import PyTorch or HuggingFace Transformers. Please verify your environment installation.")
            raise RuntimeError("Missing required AI dependencies (torch, transformers).") from e

        self.device = self.config.device
        if self.device == "cuda" and not torch.cuda.is_available():
            raise RuntimeError("CUDA device requested but CUDA is not available on this system.")
            
        try:
            # Load processor and model for audio-native gemma
            self.processor = AutoProcessor.from_pretrained(self.config.model_path)
            
            # Load audio-native model weights using multimodal class
            self.model = AutoModelForMultimodalLM.from_pretrained(
                self.config.model_path,
                torch_dtype=torch.float16 if self.device == "cuda" else torch.float32,
                device_map="auto" if self.device == "cuda" else None,
                trust_remote_code=True
            )
        except Exception as e:
            logger.error(f"Failed to load Gemma model from '{self.config.model_path}': {e}")
            raise RuntimeError(f"Gemma model loading failure: {e}") from e
        
        self._initialized = True
        logger.info("TransformersGemmaRuntime initialized successfully.")

    async def is_ready(self) -> bool:
        """
        Lightweight operational health check.
        """
        return self._initialized

    async def stream_generate(self, request: RuntimeRequest) -> AsyncIterator[TranslationResult]:
        """
        Accepts RuntimeRequest and streams token increments using TextIteratorStreamer.
        """
        if not self._initialized:
            raise RuntimeError("TransformersGemmaRuntime has not been initialized.")

        import torch
        from transformers import TextIteratorStreamer
        from threading import Thread

        # Clean up completed background threads
        self._active_threads = [t for t in self._active_threads if t.is_alive()]

        # Parse audio format and convert
        if request.audio_format == "pcm16":
            audio_array = np.frombuffer(request.audio_bytes, dtype=np.int16).astype(np.float32) / 32768.0
        elif request.audio_format == "float32":
            audio_array = np.frombuffer(request.audio_bytes, dtype=np.float32)
        else:
            raise ValueError(f"Unsupported audio format in request: {request.audio_format}")

        try:
            # Construct standard user conversation using the official multimodal message format
            messages = [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "audio", 
                            "audio_url": "input_audio.wav"  # Required placeholder for template parser
                        },
                        {
                            "type": "text", 
                            "text": PromptBuilder.build_translation_prompt(request.source_language, request.target_language)
                        }
                    ]
                }
            ]
            
            # Format the text prompt using the official chat template processor
            prompt_text = self.processor.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=True
            )
            
            # Combine prompt text and raw audio array for the processor inputs
            inputs = self.processor(
                text=prompt_text,
                audios=audio_array,
                sampling_rate=request.sample_rate,
                return_tensors="pt"
            )
            
            # Determine correct device safely to avoid device_map="auto" conflicts
            device = getattr(self.model, "device", None)
            if device is None:
                try:
                    device = next(self.model.parameters()).device
                except StopIteration:
                    device = self.device
            
            # Move inputs dictionary/tensors to target device safely
            if hasattr(inputs, "to"):
                inputs = inputs.to(device)
            else:
                inputs = {k: v.to(device) if hasattr(v, "to") else v for k, v in inputs.items()}
            
            # Setup TextIteratorStreamer using the processor's tokenizer
            tokenizer = self.processor.tokenizer if hasattr(self.processor, "tokenizer") else self.processor
            streamer = TextIteratorStreamer(tokenizer, skip_prompt=True, skip_special_tokens=True)
            
            generation_kwargs = dict(
                **inputs,
                streamer=streamer,
                max_new_tokens=128,
                temperature=self.config.temperature,
                do_sample=self.config.temperature > 0.0
            )
            
            # Calculate input audio duration in milliseconds
            audio_samples = len(audio_array)
            audio_duration_ms = (audio_samples / request.sample_rate) * 1000.0
            
            # Start timer for metrics tracking
            start_time = time.perf_counter()
            first_token_time = None
            cumulative_text = ""
            
            # Run generation in a background thread to prevent blocking the async loop
            thread = Thread(target=self.model.generate, kwargs=generation_kwargs, daemon=True)
            thread.start()
            self._active_threads.append(thread)

            for token in streamer:
                if first_token_time is None:
                    first_token_time = time.perf_counter()
                
                cumulative_text += token
                ttft_ms = (first_token_time - start_time) * 1000.0 if first_token_time else 0.0
                elapsed_ms = (time.perf_counter() - start_time) * 1000.0
                
                yield TranslationResult(
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
                        audio_duration_ms=audio_duration_ms,
                        translation_length_chars=len(cumulative_text),
                        chunk_number=request.sequence_number
                    )
                )
                # Yield control to the event loop
                await asyncio.sleep(0.001)

            elapsed_ms = (time.perf_counter() - start_time) * 1000.0
            yield TranslationResult(
                chunk_id=request.chunk_id,
                sequence_number=request.sequence_number,
                translated_text="",
                source_language=request.source_language,
                target_language=request.target_language,
                finished=True,
                ready_for_tts=True,
                metrics=TranslationMetrics(
                    ttft_ms=ttft_ms if first_token_time else elapsed_ms,
                    total_response_time_ms=elapsed_ms,
                    audio_duration_ms=audio_duration_ms,
                    translation_length_chars=len(cumulative_text),
                    chunk_number=request.sequence_number
                )
            )

        except Exception as e:
            logger.exception(f"TransformersGemmaRuntime inference execution failed: {e}")
            raise RuntimeError(f"Gemma inference execution failed: {e}") from e

    async def shutdown(self) -> None:
        """
        Releases GPU resources and clears model cache from memory, ensuring active threads are joined.
        """
        logger.info("Shutting down TransformersGemmaRuntime...")
        
        # Clean up and join active threads
        logger.info("TransformersGemmaRuntime: joining active generation threads...")
        for thread in self._active_threads:
            if thread.is_alive():
                thread.join(timeout=1.0)
        self._active_threads.clear()

        self.model = None
        self.processor = None
        self._initialized = False
        logger.info("TransformersGemmaRuntime shutdown complete.")
