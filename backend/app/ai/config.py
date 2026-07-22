import os
from dataclasses import dataclass

@dataclass
class AIConfig:
    """
    Configuration for the streaming AI pipeline.
    Provider-agnostic: runtime is selected via streaming_runtime key.
    Chunk-pipeline configs (Ollama, Transformers, runtime_type) have been
    removed in Phase 4C. See legacy/chunk_pipeline/ for historical reference.
    """
    # Language defaults
    default_source_lang: str = os.getenv("AI_SOURCE_LANG", "English")
    default_target_lang: str = os.getenv("AI_TARGET_LANG", "Spanish")

    # Streaming runtime selection — extensible for future providers
    streaming_runtime: str = os.getenv("STREAMING_RUNTIME", "gemini_live_translate")

    # Gemini Live configs
    gemini_live_model: str = os.getenv("GEMINI_LIVE_MODEL", "models/gemini-2.5-flash-native-audio-latest")
    gemini_live_api_key: str = os.getenv("GEMINI_LIVE_API_KEY", "")
    gemini_live_url: str = os.getenv(
        "GEMINI_LIVE_URL",
        "wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent"
    )
    gemini_live_timeout: float = float(os.getenv("GEMINI_LIVE_TIMEOUT", "10.0"))
    gemini_live_reconnect_delay: float = float(os.getenv("GEMINI_LIVE_RECONNECT_DELAY", "2.0"))
    # Modalities: allow explicit per-runtime env override. Prefer the
    # translate-specific env var when present to avoid accidental AUDIO-only
    # sessions when users set only the translate variable.
    gemini_live_modalities: str = os.getenv("GEMINI_LIVE_MODALITIES", os.getenv("GEMINI_LIVE_TRANSLATE_MODALITIES", "AUDIO"))
    gemini_live_voice_name: str = os.getenv("GEMINI_LIVE_VOICE_NAME", "Aoede")

    # Gemini Live Translation configs
    gemini_live_translate_model: str = os.getenv("GEMINI_LIVE_TRANSLATE_MODEL", "models/gemini-3.5-live-translate-preview")
    target_language: str = os.getenv("TARGET_LANGUAGE", "es")
    publish_source_transcript: bool = os.getenv("PUBLISH_SOURCE_TRANSCRIPT", "false").lower() == "true"
    gemini_live_translate_echo: bool = os.getenv("GEMINI_LIVE_TRANSLATE_ECHO", "false").lower() == "true"
    # Request both TEXT and AUDIO by default to enable parallel streaming of
    # translated text and audio. Can be overridden via env var.
    gemini_live_translate_modalities: str = os.getenv("GEMINI_LIVE_TRANSLATE_MODALITIES", os.getenv("GEMINI_LIVE_MODALITIES", "TEXT,AUDIO"))

    # Generic LLM generation parameters — applicable across streaming providers
    google_api_key: str = os.getenv("GOOGLE_API_KEY", "")
    temperature: float = float(os.getenv("AI_TEMPERATURE", "0.1"))
    top_p: float = float(os.getenv("AI_TOP_P", "0.9"))
