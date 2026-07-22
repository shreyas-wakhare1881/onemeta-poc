import wave
import logging
from pathlib import Path

logger = logging.getLogger("onemeta.audio.wav_writer")

class WavWriter:
    """
    Utility class to write raw PCM audio bytes into a standard WAV file.
    """
    def __init__(self, filepath: str | Path, sample_rate: int = 16000, channels: int = 1):
        self.filepath = Path(filepath)
        self.sample_rate = sample_rate
        self.channels = channels
        self._wav = None

    def write(self, data: bytes):
        if not data:
            return
        try:
            if not self._wav:
                # Ensure parent directories exist
                self.filepath.parent.mkdir(parents=True, exist_ok=True)
                self._wav = wave.open(str(self.filepath), "wb")
                self._wav.setnchannels(self.channels)
                self._wav.setsampwidth(2)  # 16-bit PCM (2 bytes)
                self._wav.setframerate(self.sample_rate)
            self._wav.writeframes(data)
        except Exception as e:
            logger.error(f"Failed to write audio to WAV file {self.filepath}: {e}")

    def close(self):
        if self._wav:
            try:
                self._wav.close()
            except Exception as e:
                logger.error(f"Failed to close WAV file {self.filepath}: {e}")
            self._wav = None
