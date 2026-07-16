import os
import math
from dataclasses import dataclass, field

@dataclass(frozen=True)
class AudioConfig:
    """
    Centralized configuration settings for audio processing.
    
    Defaults are 16kHz, mono, PCM16, 20ms frame duration, and 200 bounded queue size.
    Includes VAD start/stop hysteresis, silence timeout, max chunk duration, and sink queue settings.
    """
    sample_rate: int = int(os.getenv("AUDIO_SAMPLE_RATE", "16000"))
    channels: int = int(os.getenv("AUDIO_CHANNELS", "1"))
    frame_duration_sec: float = float(os.getenv("AUDIO_FRAME_DURATION_SEC", "0.02"))
    bytes_per_sample: int = 2  # 16-bit linear PCM
    queue_maxsize: int = int(os.getenv("AUDIO_QUEUE_MAXSIZE", "200"))

    # VAD & Chunking Configuration (Hysteresis Thresholds)
    vad_start_threshold_energy: float = float(os.getenv("VAD_START_THRESHOLD_ENERGY", "550.0"))
    vad_stop_threshold_energy: float = float(os.getenv("VAD_STOP_THRESHOLD_ENERGY", "400.0"))
    silence_timeout_sec: float = float(os.getenv("SILENCE_TIMEOUT_SEC", "0.2"))
    max_chunk_duration_sec: float = float(os.getenv("MAX_CHUNK_DURATION_SEC", "0.3"))

    # Chunk Sink Bounded Queue Configuration
    chunk_sink_queue_maxsize: int = int(os.getenv("CHUNK_SINK_QUEUE_MAXSIZE", "100"))
    chunk_sink_timeout_sec: float = float(os.getenv("CHUNK_SINK_TIMEOUT_SEC", "0.5"))

    # Precomputed Frame Thresholds (as fields initialized in __post_init__)
    max_silence_frames: int = field(init=False)
    max_chunk_duration_frames: int = field(init=False)

    def __post_init__(self):
        # Precompute integer thresholds on instantiation to eliminate runtime overhead
        object.__setattr__(self, "max_silence_frames", math.ceil(self.silence_timeout_sec / self.frame_duration_sec))
        object.__setattr__(self, "max_chunk_duration_frames", math.ceil(self.max_chunk_duration_sec / self.frame_duration_sec))

    @property
    def samples_per_frame(self) -> int:
        """
        Calculates the number of samples per processing frame block.
        """
        return int(self.sample_rate * self.frame_duration_sec)

    @property
    def bytes_per_frame(self) -> int:
        """
        Calculates the total byte length per processing frame block.
        """
        return self.samples_per_frame * self.bytes_per_sample * self.channels
