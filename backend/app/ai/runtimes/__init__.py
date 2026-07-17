from .base import BaseRuntime
from .transformers_runtime import TransformersGemmaRuntime
from .ollama_runtime import OllamaGemmaRuntime

# Registry mapping runtime type string keys to their adapter classes
RUNTIMES = {
    "ollama": OllamaGemmaRuntime,
    "transformers": TransformersGemmaRuntime
}

def create_runtime(config) -> BaseRuntime:
    """
    Factory function to instantiate the correct AI inference runtime based on configuration parameters.
    """
    runtime_cls = RUNTIMES.get(config.runtime_type)
    if not runtime_cls:
        raise ValueError(f"Unsupported runtime type: {config.runtime_type}")
    return runtime_cls(config)
