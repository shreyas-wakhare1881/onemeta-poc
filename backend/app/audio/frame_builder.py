import time
import logging
from ..types.audio import AudioFrame

logger = logging.getLogger("onemeta.frame_builder")

class AudioFrameBuilder:
    """
    Accumulates incoming raw PCM bytes and slices them into deterministic processing blocks.
    
    Uses sliding window memoryviews for zero-copy slicing internally, but casts slices to
    immutable bytes when constructing AudioFrame objects (immutable bytes at public boundary)
    to ensure thread-safety and prevent downstream BufferErrors during queue/worker processing.
    """
    def __init__(self, target_samples: int = 320, sample_rate: int = 16000, channels: int = 1):
        self.target_samples = target_samples
        self.sample_rate = sample_rate
        self.channels = channels
        self.bytes_per_sample = 2  # 16-bit linear PCM
        self.target_bytes = target_samples * self.bytes_per_sample * channels
        self.buffer = bytearray()
        self.read_idx = 0
        self._sequence_counter = 0

    def append(
        self, 
        pcm_bytes: bytes, 
        base_timestamp_ns: int, 
        participant_identity: str,
        participant_session_id: str
    ) -> list[AudioFrame]:
        """
        Appends incoming raw bytes, slices complete frames, and builds AudioFrame objects.
        """
        # Slide buffer window to keep bounds small, allocating new bytearray to prevent BufferError on active views
        if self.read_idx > 64000:  # ~2 seconds of audio
            self.buffer = bytearray(self.buffer[self.read_idx:])
            self.read_idx = 0

        self.buffer.extend(pcm_bytes)
        frames = []
        
        sample_duration_ns = 1_000_000_000 // self.sample_rate
        
        # Zero-copy memoryview slice on active byte buffer
        view = memoryview(self.buffer)
        buffer_len = len(self.buffer)
        
        while (buffer_len - self.read_idx) >= self.target_bytes:
            # Slice a zero-copy memoryview slice internally
            frame_view = view[self.read_idx : self.read_idx + self.target_bytes]
            self.read_idx += self.target_bytes
            
            # Cast the slice to immutable bytes to ensure thread-safety for public contract
            frame_bytes = bytes(frame_view)
            
            seq = self._sequence_counter
            self._sequence_counter += 1
            frame_id = f"{participant_session_id}-{seq}"

            # Standardized immutable AudioFrame containing stable bytes copy
            frames.append(AudioFrame(
                frame_id=frame_id,
                sequence_number=seq,
                participant_identity=participant_identity,
                participant_session_id=participant_session_id,
                capture_timestamp_ns=base_timestamp_ns,
                sample_rate=self.sample_rate,
                channels=self.channels,
                frame_duration=self.target_samples / self.sample_rate,
                pcm_data=frame_bytes
            ))
            
            # Increment relative timestamp
            base_timestamp_ns += self.target_samples * sample_duration_ns
            
        return frames

    def clear(self):
        """
        Drains buffered audio and resets the sequence counter.
        """
        self.buffer.clear()
        self.read_idx = 0
        self._sequence_counter = 0
        logger.info("AudioFrameBuilder state reset completely.")
