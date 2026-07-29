from enum import Enum

class PipelineEvent(str, Enum):
    # ── Session lifecycle ─────────────────────────────────────────────────────
    SESSION_STARTED = "SESSION_STARTED"
    SESSION_ENDED   = "SESSION_ENDED"

    # ── Microphone / ingestion ────────────────────────────────────────────────
    MIC_FRAME_RECEIVED = "MIC_FRAME_RECEIVED"

    # ── VAD control-plane ─────────────────────────────────────────────────────
    # Emitted on every VAD state transition (speech↔silence)
    VAD_DECISION = "VAD_DECISION"
    # Emitted once when VAD first confirms a new speech segment (first is_speech frame)
    SPEECH_SEGMENT_STARTED = "SPEECH_SEGMENT_STARTED"
    # Emitted once when VAD confirms sustained silence (debounce elapsed)
    # carries: speech_duration_ms, silence_frames_elapsed, total_speech_frames
    SPEECH_SEGMENT_ENDED = "SPEECH_SEGMENT_ENDED"
    # Emitted when end_user_turn() is actually called on the transport
    # This is the turn-commit anchor used for TTFT/TTFA calculations
    # carries: speech_duration_ms, debounce_frames, correlation_id
    AUDIO_STREAM_END_SENT = "AUDIO_STREAM_END_SENT"
    # Emitted when a speech segment is cancelled before turn-commit
    # (e.g., barge-in, session close during speech)
    VAD_FALSE_START = "VAD_FALSE_START"

    # ── Backend pipeline ──────────────────────────────────────────────────────
    AUDIO_SENT_TO_GEMINI = "AUDIO_SENT_TO_GEMINI"
    TEXT_PUBLISHED       = "TEXT_PUBLISHED"
    AUDIO_PUBLISHED      = "AUDIO_PUBLISHED"
    PIPELINE_ERROR       = "PIPELINE_ERROR"

    # ── Gemini runtime phases ─────────────────────────────────────────────────
    # Raw WebSocket frame received (every frame from Gemini)
    GEMINI_WS_FRAME_RECEIVED  = "GEMINI_WS_FRAME_RECEIVED"
    # Full translated text token arrived from Gemini
    TRANSLATED_TEXT_RECEIVED  = "TRANSLATED_TEXT_RECEIVED"
    # First audio chunk arrived from Gemini after AUDIO_STREAM_END_SENT
    # This is the "Gemini first token" — most important Gemini latency point
    GEMINI_FIRST_TOKEN        = "GEMINI_FIRST_TOKEN"
    # First audio chunk produced by Gemini for this turn
    GEMINI_FIRST_AUDIO        = "GEMINI_FIRST_AUDIO"
    # turn_complete signal received from Gemini
    GEMINI_TURN_COMPLETE      = "GEMINI_TURN_COMPLETE"
    # Raw translated audio bytes received (one event per audio chunk)
    TRANSLATED_AUDIO_RECEIVED = "TRANSLATED_AUDIO_RECEIVED"

    # ── Frontend telemetry ────────────────────────────────────────────────────
    # Emitted when a source transcript is painted to the UI (paint-anchored)
    SOURCE_TRANSCRIPT_RENDERED = "SOURCE_TRANSCRIPT_RENDERED"
    # Emitted when a target (translated) transcript is painted to the UI (paint-anchored)
    TARGET_TRANSCRIPT_RENDERED = "TARGET_TRANSCRIPT_RENDERED"
    # Emitted when audio output becomes audible (first sample above threshold)
    AUDIO_FIRST_AUDIBLE = "AUDIO_FIRST_AUDIBLE"

    # ── Client telemetry / server pacing ─────────────────────────────────────
    CLIENT_TELEMETRY_RECEIVED = "CLIENT_TELEMETRY_RECEIVED"
    SERVER_PACING_APPLIED     = "SERVER_PACING_APPLIED"
    SERVER_PACING_RELEASED    = "SERVER_PACING_RELEASED"
    SERVER_PACING_DROPPED     = "SERVER_PACING_DROPPED"
