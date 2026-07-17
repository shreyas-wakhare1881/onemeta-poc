from dataclasses import dataclass, field

@dataclass(frozen=True)
class EventMetrics:
    start_timestamp: float = 0.0
    end_timestamp: float = 0.0
    queue_wait_ms: float = 0.0
    processing_time_ms: float = 0.0
    chunk_duration_ms: float = 0.0
    ttft_ms: float = 0.0
    gemma_latency_ms: float = 0.0
    total_ai_latency_ms: float = 0.0
    audio_duration_ms: float = 0.0

@dataclass(frozen=True)
class AIStartedEvent:
    chunk_id: str
    sequence_number: int
    timestamp: float
    metrics: EventMetrics = field(default_factory=EventMetrics)

@dataclass(frozen=True)
class AIPartialEvent:
    chunk_id: str
    sequence_number: int
    text_delta: str
    cumulative_text: str
    timestamp: float
    metrics: EventMetrics = field(default_factory=EventMetrics)

@dataclass(frozen=True)
class AICompletedEvent:
    chunk_id: str
    sequence_number: int
    full_text: str
    duration_ms: float
    timestamp: float
    metrics: EventMetrics = field(default_factory=EventMetrics)

@dataclass(frozen=True)
class AIErrorEvent:
    chunk_id: str
    sequence_number: int
    error_message: str
    timestamp: float
    metrics: EventMetrics = field(default_factory=EventMetrics)

@dataclass(frozen=True)
class TranslationFailedEvent:
    chunk_id: str
    sequence_number: int
    error_message: str
    timestamp: float
    metrics: EventMetrics = field(default_factory=EventMetrics)
