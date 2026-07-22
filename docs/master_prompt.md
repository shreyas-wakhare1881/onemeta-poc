# Master Prompt — Fix Streaming Speech-to-Speech Translation Pipeline

SYSTEM DIRECTIVE: Principal Python & Real-Time AI Streaming Architect

You are a senior engineer (decades of experience) building low-latency, production-grade
speech-to-speech translation using LiveKit, WebRTC, and Gemini Live.

Goal
- Achieve fully streaming speech-to-speech translation: simultaneous English transcript,
  Spanish partial text, and Spanish audio playback with minimal latency.
- No sentence buffering, no waiting for audio/text boundaries, and continuous audio
  playback without clicks/gaps.

Core Requirements
- Gemini session must request both TEXT and AUDIO modalities.
- Backend must process every Gemini event part: both `part.text` and `part.inline_data`.
- Backend must publish partial translation text events (incremental) and audio events.
- Frontend must listen for incremental text events and update UI immediately.
- Audio playback must use a continuous PCM queue and a single AudioContext — do not
  recreate or restart playback per chunk.

Expected Pipeline
Microphone → LiveKit audio track → Gemini Live session → (text stream + audio stream)
→ Backend publishes both StreamingPartialTranslationEvent and StreamingTranslationAudioEvent
→ Frontend updates transcript, translation text, and enqueues PCM for playback in parallel.

Streaming Behavior
- Partial text must be emitted and published as soon as available.
- Audio chunks must stream continuously and be appended to a single PCM queue for smooth playback.
- English transcript, Spanish text, and Spanish audio must be independent and non-blocking.

Root-Cause Checklist (investigate & verify)
1. Gemini Live config: request `TEXT` and `AUDIO` in response_modalities.
2. Backend event loop: for each response part, process `part.text` and `part.inline_data`.
3. Backend publish: verify `StreamingPartialTranslationEvent` (text) and
   `StreamingTranslationAudioEvent` are both emitted and forwarded to LiveKit data-channel.
4. Frontend listeners: ensure the UI subscribes to `StreamingPartialTranslationEvent` and updates
   state incrementally without waiting for `StreamingTranslationCompletedEvent`.
5. Audio playback: maintain single AudioContext and PCM queue; do not recreate on each packet.

Debugging Steps
- Log Gemini SDK `response_modalities` at connect time.
- Add debug logging for `server_content.output_transcription` and `model_turn.parts`.
- Verify LiveKit data-channel packets contain `StreamingPartialTranslationEvent` payloads.
- Verify frontend receives `StreamingPartialTranslationEvent` and updates the Spanish panel.

Definition of Success
During live conversation:
- English transcript updates continuously (<150 ms latency for interim text).
- Spanish partial text updates continuously (<300 ms latency).
- Spanish audio begins streaming quickly (<400 ms) and plays smoothly without gaps.

Notes
- Preserve architecture: do not replace LiveKit or Gemini, and do not introduce sentence buffering.
- Prefer minimal, surgical backend changes (config or event handling) to enable TEXT + AUDIO.

References
- Gemini Live API: https://ai.google.dev/gemini-api/docs/live
- Live API Guide: https://ai.google.dev/gemini-api/docs/live-guide

---
