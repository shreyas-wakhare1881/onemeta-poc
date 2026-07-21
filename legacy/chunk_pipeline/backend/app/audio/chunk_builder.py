from collections import deque
import logging
from typing import Deque, Tuple, Optional
from ..types.audio import AudioFrame
from ..types.speech import FlushReason
from .config import AudioConfig

logger = logging.getLogger("onemeta.chunk_builder")

class AdaptiveChunkBuilder:
    """
    Accumulates continuous speech frames in an O(1) collections.deque buffer.
    
    Evaluates precomputed frame count thresholds to eliminate runtime float arithmetic.
    Enforces silence frame isolation.
    """
    def __init__(self, config: AudioConfig):
        self.config = config
        self.speech_frames: Deque[AudioFrame] = deque()
        self.silence_counter = 0

    def add_frame(self, frame: AudioFrame, is_speech: bool) -> Optional[Tuple[Deque[AudioFrame], FlushReason, int]]:
        """
        Processes a frame using precomputed frame limits. Returns (frames, reason, silence_count) on flush.
        """
        if is_speech:
            self.speech_frames.append(frame)
            self.silence_counter = 0

            # Max duration threshold check using precomputed frame counts
            if len(self.speech_frames) >= self.config.max_chunk_duration_frames:
                logger.debug("Max duration limit reached. Flushing speech chunk...")
                return self.flush(FlushReason.MAX_DURATION)
        else:
            # Silence frames are isolated and never appended to self.speech_frames
            if self.speech_frames:
                self.silence_counter += 1

                # Silence timeout threshold check using precomputed frame counts
                if self.silence_counter >= self.config.max_silence_frames:
                    logger.debug("Silence timeout threshold reached. Flushing speech chunk...")
                    return self.flush(FlushReason.SILENCE)

        return None

    def flush(self, reason: FlushReason = FlushReason.END_OF_STREAM) -> Optional[Tuple[Deque[AudioFrame], FlushReason, int]]:
        """
        Flushes and returns all accumulated speech frames, along with the silence frame count.
        """
        if not self.speech_frames:
            return None

        frames = self.speech_frames
        self.speech_frames = deque()
        
        silence_count = self.silence_counter
        self.silence_counter = 0
        
        return frames, reason, silence_count
