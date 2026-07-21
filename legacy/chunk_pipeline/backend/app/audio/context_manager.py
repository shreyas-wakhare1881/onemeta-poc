from typing import Iterable, List
import logging
from ..types.speech import SpeechChunk, SpeechChunkMetadata, FlushReason
from ..types.audio import AudioFrame
from .config import AudioConfig

logger = logging.getLogger("onemeta.context_manager")

class StreamingContextManager:
    """
    A pure transformation layer that assembles an Iterable of frames into a SpeechChunk.
    
    This component is stateless and side-effect free. It does not perform VAD logic,
    latency calculations, logging, frame mutations, or sink writes.
    """
    def __init__(self, room_name: str, config: AudioConfig):
        self.room_name = room_name
        self.config = config
        self._sequence_counter = 0

    def build_chunk(
        self, 
        frames_iterable: Iterable[AudioFrame], 
        metadata: SpeechChunkMetadata
    ) -> SpeechChunk:
        """
        Purely maps an Iterable of AudioFrames to a SpeechChunk.
        """
        frames: List[AudioFrame] = list(frames_iterable)
        if not frames:
            raise ValueError("Cannot assemble SpeechChunk from an empty frames sequence.")

        # Combine PCM bytes. Reuse the already buffered PCM data to avoid copying.
        pcm_data = b"".join(f.pcm_data for f in frames)

        # Retrieve capture intervals (converted from nanoseconds performance counters to seconds)
        start_ts = frames[0].capture_timestamp_ns / 1_000_000_000.0
        total_duration = len(frames) * frames[0].frame_duration
        end_ts = start_ts + total_duration
        duration_ms = total_duration * 1000.0

        # Sequence-based chunk ID (e.g. {session_id}-C{sequence})
        session_id = frames[0].participant_session_id
        chunk_id = f"{session_id}-C{self._sequence_counter}"
        
        seq = self._sequence_counter
        self._sequence_counter += 1

        is_final = (metadata.flush_reason == FlushReason.SILENCE or metadata.flush_reason == FlushReason.END_OF_STREAM)

        return SpeechChunk(
            chunk_id=chunk_id,
            sequence_number=seq,
            participant_identity=frames[0].participant_identity,
            participant_session_id=session_id,
            room_name=self.room_name,
            start_timestamp=start_ts,
            end_timestamp=end_ts,
            duration_ms=duration_ms,
            frame_count=len(frames),
            pcm_data=pcm_data,
            is_final=is_final,
            metadata=metadata
        )
