import numpy as np
import logging
from typing import Tuple
from ..types.audio import AudioFrame
from .config import AudioConfig

logger = logging.getLogger("onemeta.vad")

class StreamingVADProcessor:
    """
    A stateful Voice Activity Detector (VAD) utilizing dual-threshold hysteresis.
    
    Prevents high-frequency speech/silence flickering on natural voice fluctuations.
    Uses float32 array calculations to optimize CPU throughput.
    """
    def __init__(self, config: AudioConfig):
        self.config = config
        self.is_speech_active = False

    def is_speech(self, frame: AudioFrame) -> Tuple[bool, float]:
        """
        Calculates float32 RMS energy, updates hysteresis state, and returns (is_speech, rms).
        """
        if not frame.pcm_data:
            return False, 0.0

        # Load raw bytes as 16-bit linear PCM samples and convert to float32
        samples = np.frombuffer(frame.pcm_data, dtype=np.int16).astype(np.float32, copy=False)
        if len(samples) == 0:
            return False, 0.0

        # Compute Root Mean Square (RMS) energy in float32
        rms = float(np.sqrt(np.mean(samples ** 2)))

        # Stateful Hysteresis Transition Logic
        if self.is_speech_active:
            # Transition to silence only if energy drops below stop threshold
            if rms < self.config.vad_stop_threshold_energy:
                self.is_speech_active = False
                logger.debug(f"VAD Hysteresis: Speech -> Silence (RMS={rms:.1f} < stop={self.config.vad_stop_threshold_energy})")
        else:
            # Transition to speech only if energy rises above start threshold
            if rms >= self.config.vad_start_threshold_energy:
                self.is_speech_active = True
                logger.debug(f"VAD Hysteresis: Silence -> Speech (RMS={rms:.1f} >= start={self.config.vad_start_threshold_energy})")

        return self.is_speech_active, rms
