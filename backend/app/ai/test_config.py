def test_default_translate_modalities_include_text_and_audio():
    from .config import AIConfig
    cfg = AIConfig()
    modalities = cfg.gemini_live_translate_modalities or ""
    upper = modalities.upper()
    assert 'TEXT' in upper, f"Expected 'TEXT' in modalities, got: {modalities}"
    assert 'AUDIO' in upper, f"Expected 'AUDIO' in modalities, got: {modalities}"
