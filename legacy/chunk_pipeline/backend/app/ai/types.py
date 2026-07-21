from dataclasses import dataclass, field
from typing import Dict, Any

@dataclass
class TranslationMetrics:
    """
    Strongly-typed metadata tracking performance metrics for a single translation chunk.
    """
    ttft_ms: float = 0.0
    total_response_time_ms: float = 0.0
    payload_size_bytes: int = 0
    audio_duration_ms: float = 0.0
    translation_length_chars: int = 0
    translation_length_tokens: int = 0
    chunk_number: int = 0

@dataclass
class RuntimeRequest:
    """
    Standardized payload for local model runtime generation requests.
    Decoupled from audio processing types (e.g. SpeechChunk).
    """
    audio_bytes: bytes  # Contains transport-ready audio. Each runtime interprets it according to backend requirements.
    audio_format: str  # "pcm16", "wav", "float32"
    sample_rate: int
    source_language: str
    target_language: str
    chunk_id: str
    sequence_number: int
    conversation_id: str = ""
    stream: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class TranslationResult:
    """
    Standardized streaming output yielded incrementally by runtimes.
    """
    chunk_id: str
    sequence_number: int
    translated_text: str
    source_language: str
    target_language: str
    finished: bool = False
    ready_for_tts: bool = False
    metrics: TranslationMetrics = field(default_factory=TranslationMetrics)
    metadata: Dict[str, Any] = field(default_factory=dict)

# Alias for backward compatibility with existing tests and structures
RuntimeResponse = TranslationResult
