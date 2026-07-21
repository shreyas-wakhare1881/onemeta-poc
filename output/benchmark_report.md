# Gemini 3.5 Live Translate Benchmark Report

## Configuration & Environment
- **SDK Version:** google-genai 2.12.1
- **Model Used:** models/gemini-3.5-live-translate-preview
- **Target Language:** Spanish (es)
- **Response Modality:** AUDIO

## Latency Metrics
- **Connection Handshake Time:** 429.54 ms
- **Time to First Transcript Token (TTFT):** 5669.71 ms
- **Time to First Audio Chunk (TTFA):** 5693.21 ms
- **First Translation Offset (Speech Start -> First Transcript):** 5669.71 ms
- **First Audio Packet Sent -> First Transcript:** 5669.66 ms (Actual processing overhead)
- **Average Audio Chunk Interval:** 245.77 ms
- **Total Session Streaming Runtime:** 24.81 seconds

## Throughput Statistics
- **Total Audio Chunks Received:** 78
- **Total Audio Bytes Received:** 936000 bytes
- **Total Transcript Length:** 135 characters

## Status Summary
- **Connection Status:** PASS
- **Translation Status:** PASS
- **Audio Output Status:** PASS
- **Translation Accuracy (Manual Observation):** PASS

---
*Note on Latency Metrics: Since the benchmark streams audio in real-time (20ms frames), the TTFT and TTFA latency metrics include the time spent uploading the audio stream (e.g. streaming 3 seconds of audio before a complete translatable phrase is voiced by the speaker) and do not represent pure inference latency.*

---
*Report generated automatically on 2026-07-21 18:04:11.*
