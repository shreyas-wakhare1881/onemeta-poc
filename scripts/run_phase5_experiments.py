import os
import sys
import wave
import time
import asyncio
import numpy as np
from pathlib import Path
from dotenv import load_dotenv

# Set paths
workspace_root = Path(__file__).resolve().parents[1]
env_path = workspace_root / "backend" / ".env"
load_dotenv(dotenv_path=env_path)

backend_dir = workspace_root / "backend"
sys.path.insert(0, str(backend_dir))

from app.ai.config import AIConfig
from app.ai.engine import AIEngine
from app.ai.runtimes.gemini_live_runtime import GeminiLiveRuntime
from app.transport.packet import StreamingAudioPacket, StreamingPacketMetadata
from app.ai.events import (
    StreamingPartialTranslationEvent,
    StreamingTranslationCompletedEvent,
    StreamingRuntimeErrorEvent,
    StreamingStateChangedEvent,
    StreamingSessionClosedEvent,
    StreamingSessionStartedEvent
)
from google.genai import types

def load_and_resample_wav(filepath: str) -> bytes:
    with wave.open(filepath, 'rb') as w:
        nchannels = w.getnchannels()
        sampwidth = w.getsampwidth()
        framerate = w.getframerate()
        nframes = w.getnframes()
        raw_bytes = w.readframes(nframes)
        data = np.frombuffer(raw_bytes, dtype=np.int16)
        if nchannels == 2:
            left = data[0::2]
            right = data[1::2]
            mono = (left.astype(np.float32) + right.astype(np.float32)) / 2
            data = mono.astype(np.int16)
        if framerate == 48000:
            length = (len(data) // 3) * 3
            data = data[:length]
            reshaped = data.reshape(-1, 3).astype(np.float32)
            data = np.mean(reshaped, axis=1)
        peak = np.max(np.abs(data))
        if peak > 0:
            data = data * (20000.0 / peak)
        data = data.astype(np.int16)
        return data.tobytes()

class CustomGeminiTransport(BaseException):
    # Dummy placeholder so we can import absolute from backend later
    pass

from app.ai.runtimes.gemini_live_runtime import GeminiLiveTransport

class ExperimentalGeminiTransport(GeminiLiveTransport):
    """
    Experimental transport that sends ActivityStart / ActivityEnd signals
    when server VAD is disabled.
    """
    def __init__(self, *args, disable_server_vad: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self.disable_server_vad = disable_server_vad
        self._speech_started = False

    async def send_packet(self, packet: StreamingAudioPacket) -> None:
        if self.closed:
            return

        # Handle ActivityStart on speech onset
        if self.disable_server_vad and packet.is_speech and not self._speech_started:
            print(f"[{time.perf_counter():.3f}] SENDING: ActivityStart to Gemini Live")
            await self.sdk_session.send_realtime_input(activity_start=types.ActivityStart())
            self._speech_started = True

        await super().send_packet(packet)

    async def end_user_turn(self) -> None:
        if self.closed:
            return

        if self.disable_server_vad:
            print(f"[{time.perf_counter():.3f}] SENDING: ActivityEnd to Gemini Live")
            await self.sdk_session.send_realtime_input(activity_end=types.ActivityEnd())
        else:
            print(f"[{time.perf_counter():.3f}] SENDING: audio_stream_end=True to Gemini Live")
            await self.sdk_session.send_realtime_input(audio_stream_end=True)
            
        self._speech_started = False

class CustomGeminiRuntime(GeminiLiveRuntime):
    def __init__(self, config, disable_server_vad: bool = False):
        super().__init__(config)
        self.disable_server_vad = disable_server_vad

    async def connect(
        self,
        session_id: str,
        source_language: str,
        target_language: str,
        on_event = None,
        metadata: dict = None
    ):
        if not self._initialized or not self._client:
            await self.initialize()

        modalities_list = [m.strip() for m in self.config.gemini_live_modalities.split(",")]
        
        # Configure AAD settings
        aad = types.AutomaticActivityDetection(disabled=self.disable_server_vad)
        
        sdk_config = types.LiveConnectConfig(
            response_modalities=modalities_list,
            system_instruction=types.Content(
                parts=[types.Part(text=f"Translate {source_language} speech to {target_language}. Speak clearly only in {target_language}.")]
            ),
            output_audio_transcription=types.AudioTranscriptionConfig(),
            realtime_input_config=types.RealtimeInputConfig(
                automatic_activity_detection=aad
            ),
            speech_config=types.SpeechConfig(
                voice_config=types.VoiceConfig(
                    prebuilt_voice_config=types.PrebuiltVoiceConfig(
                        voice_name=self.config.gemini_live_voice_name
                    )
                )
            )
        )
        
        ctx = self._client.aio.live.connect(
            model=self.config.gemini_live_model,
            config=sdk_config
        )
        sdk_session = await ctx.__aenter__()
        
        transport = ExperimentalGeminiTransport(
            session_id=session_id,
            sdk_session=sdk_session,
            sdk_ctx=ctx,
            source_language=source_language,
            target_language=target_language,
            model_name=self.config.gemini_live_model,
            modalities=modalities_list,
            voice_name=self.config.gemini_live_voice_name,
            disable_server_vad=self.disable_server_vad
        )
        return transport

async def run_experiment(pcm_bytes: bytes, ai_config: AIConfig, disable_server_vad: bool):
    label = "MANUAL TURN DETECTION (Server VAD Disabled)" if disable_server_vad else "AUTOMATIC TURN DETECTION (Server VAD Enabled)"
    print("\n" + "="*70)
    print(f"RUNNING EXPERIMENT: {label}")
    print("="*70)
    
    ai_engine = AIEngine(ai_config)
    await ai_engine.start()
    
    runtime = CustomGeminiRuntime(ai_config, disable_server_vad=disable_server_vad)
    await runtime.initialize()
    
    session_id = f"exp-{'manual' if disable_server_vad else 'auto'}-session"
    session = await ai_engine.start_streaming_session(
        session_id=session_id,
        runtime=runtime,
        source_lang="English",
        target_lang="Spanish"
    )
    
    t_start = time.perf_counter()
    
    def handle_event(event):
        elapsed = (time.perf_counter() - t_start) * 1000.0
        if event.__class__.__name__ == "StreamingAudioFrameReceivedEvent":
            return
        
        if isinstance(event, StreamingPartialTranslationEvent):
            print(f"[{elapsed:.1f}ms] PARTIAL: '{event.text_delta}'")
        elif isinstance(event, StreamingTranslationCompletedEvent):
            print(f"[{elapsed:.1f}ms] COMPLETED: '{event.full_text}'")
        elif isinstance(event, StreamingRuntimeErrorEvent):
            print(f"[{elapsed:.1f}ms] ERROR: {event.error_message}")
        else:
            print(f"[{elapsed:.1f}ms] EVENT: {event.__class__.__name__}")
            
    session.register_listener(handle_event)
    session.record_speech_start()
    
    frame_size = 640
    num_frames = len(pcm_bytes) // frame_size
    frames_to_stream = min(500, num_frames)  # 10 seconds of audio
    
    print(f"Streaming {frames_to_stream} frames (10s of audio) in real-time...")
    for i in range(frames_to_stream):
        offset = i * frame_size
        chunk_data = pcm_bytes[offset : offset + frame_size]
        packet = StreamingAudioPacket(
            pcm_data=memoryview(chunk_data),
            sample_rate=16000,
            channels=1,
            capture_timestamp_ns=time.perf_counter_ns(),
            sequence_number=i + 1,
            is_speech=True,
            metadata=StreamingPacketMetadata(
                frame_id=f"frame-{i}",
                participant_identity="tester",
                participant_session_id="tester-sid",
                rms=1000.0,
                correlation_id="exp-corr"
            )
        )
        await ai_engine.process_audio_packet(session_id, packet)
        await asyncio.sleep(0.02)
        if (i + 1) % 100 == 0:
            print(f"-> Streamed {i+1} frames ({(i+1)*20}ms)...")
            
    print("Streaming complete. Waiting 3 seconds (No turn signal yet)...")
    await asyncio.sleep(3.0)
    
    print("Calling record_speech_end now to explicitly end the turn...")
    session.record_speech_end()
    
    print("Waiting 6 seconds for any final response...")
    await asyncio.sleep(6.0)
    
    print("Closing session...")
    await ai_engine.stop_streaming_session(session_id)
    await runtime.shutdown()
    await ai_engine.shutdown()

async def main():
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("ERROR: GOOGLE_API_KEY not set!")
        return

    sample_path = workspace_root / "samples" / "english_sample.wav"
    pcm_bytes = load_and_resample_wav(str(sample_path))
    
    ai_config = AIConfig(
        streaming_runtime="gemini_live",
        google_api_key=api_key,
        gemini_live_modalities="AUDIO"
    )
    
    # 1. Run with automatic turn detection (Default server VAD)
    await run_experiment(pcm_bytes, ai_config, disable_server_vad=False)
    
    # 2. Run with manual turn detection (Explicit turn signaling)
    await run_experiment(pcm_bytes, ai_config, disable_server_vad=True)

if __name__ == "__main__":
    asyncio.run(main())
