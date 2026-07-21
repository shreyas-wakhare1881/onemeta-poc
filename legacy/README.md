# Legacy Archive

This directory contains the chunk-based inference pipeline, preserved for research reference.

Archived during **Phase 4C** (streaming-only repository cleanup).

## Contents

```
chunk_pipeline/
    backend/app/ai/
        pipeline_chunk.py       ← Stateless chunk inference pipeline
        types.py                ← RuntimeRequest, TranslationResult DTOs
        runtime.py              ← Deprecated LocalGemmaRuntime shim
        prompts.py              ← PromptBuilder
        sink.py                 ← InferenceSink (SpeechChunk → AI queue)
        test_ai_pipeline.py     ← Chunk pipeline unit tests
        test_live_gemini.py     ← Stale stub
        runtimes/
            base.py             ← BaseRuntime (chunk runtime ABC)
            ollama_runtime.py   ← Ollama/Gemma chunk inference
            transformers_runtime.py ← HuggingFace chunk inference
            google_runtime.py   ← Gemini REST API chunk inference
    backend/app/audio/
        chunk_builder.py        ← AdaptiveChunkBuilder
        context_manager.py      ← StreamingContextManager (SpeechChunk assembly)
    backend/app/types/
        speech.py               ← SpeechChunk, FlushReason, SpeechChunkMetadata
    scripts/
        benchmark_*.py          ← Stage 1 + Stage 2 latency benchmarks
        profile_ollama_*.py     ← Ollama profiling tools
        validate_*.py           ← Ollama validation tools
        test_gemini*.py         ← Non-live Gemini API experiments
        test_gemma*.py          ← Gemma model experiments
        debug_ws.py             ← Raw WebSocket debugging
        list_models.py          ← Ollama model enumeration
        test_sdk.py             ← SDK protocol exploration
        verify_gemma_models.py  ← Gemma model verification

## WARNING

Do NOT import from these files in production code.
The production codebase uses exclusively the streaming pipeline.
```
