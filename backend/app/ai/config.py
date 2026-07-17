import os
from dataclasses import dataclass
from enum import Enum

class QueueDropPolicy(Enum):
    DROP_OLDEST = "DROP_OLDEST"
    DROP_NEWEST = "DROP_NEWEST"

@dataclass
class AIConfig:
    """
    Configuration parameters for the AI processing engine.
    """
    model_path: str = os.getenv("GEMMA_MODEL_PATH", "google/gemma-4-12B-it")
    device: str = os.getenv("GEMMA_DEVICE", "cuda")
    queue_maxsize: int = int(os.getenv("AI_QUEUE_MAXSIZE", "3"))
    queue_drop_policy: QueueDropPolicy = QueueDropPolicy(
        os.getenv("AI_QUEUE_DROP_POLICY", "DROP_OLDEST")
    )
    default_source_lang: str = os.getenv("AI_SOURCE_LANG", "English")
    default_target_lang: str = os.getenv("AI_TARGET_LANG", "Spanish")
    
    # Selection: "ollama" or "transformers"
    runtime_type: str = os.getenv("AI_RUNTIME_TYPE", "ollama")
    
    # Ollama Specific configs
    ollama_host: str = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    ollama_model: str = os.getenv("OLLAMA_MODEL", "gemma4:12b")
    request_timeout: int = int(os.getenv("OLLAMA_REQUEST_TIMEOUT", "30"))
    max_retries: int = int(os.getenv("OLLAMA_MAX_RETRIES", "3"))
    
    # Model Generation configs
    temperature: float = float(os.getenv("AI_TEMPERATURE", "0.1"))
    top_p: float = float(os.getenv("AI_TOP_P", "0.9"))
