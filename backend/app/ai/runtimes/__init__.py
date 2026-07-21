from typing import Any
from ..streaming import BaseStreamingRuntime
from .gemini_live_runtime import GeminiLiveRuntime
from .gemini_live_translate_runtime import GeminiLiveTranslateRuntime

# ---------------------------------------------------------------------------
# Streaming Runtime Registry
# ---------------------------------------------------------------------------
# Maps config key → runtime class. Add new providers here.
# Chunk runtimes (ollama, transformers, google) removed in Phase 4C.
# See legacy/chunk_pipeline/ for historical reference.
# ---------------------------------------------------------------------------
STREAMING_RUNTIMES = {
    "gemini_live": GeminiLiveRuntime,
    "gemini_live_translate": GeminiLiveTranslateRuntime,
    # future: "gemma_live": GemmaLiveRuntime,
}

def create_streaming_runtime(config) -> BaseStreamingRuntime:
    """
    Factory function to instantiate the configured streaming runtime.
    Provider-agnostic: callers receive a BaseStreamingRuntime interface.
    """
    cls = STREAMING_RUNTIMES.get(config.streaming_runtime)
    if not cls:
        raise ValueError(
            f"Unknown streaming runtime: {config.streaming_runtime!r}. "
            f"Available: {list(STREAMING_RUNTIMES.keys())}"
        )
    return cls(config)
