# Latency Tracing and Observability Specification (V1)

This document serves as the implementation specification and guide for the end-to-end latency tracing system in the OneMeta Speech-to-Speech translation POC. 
Use this specification to build the **Phase 2 Latency Analyzer** and parse the trace logs correctly.

---

## 1. Trace Log Schema V1

Both backend and frontend produce a JSON log file using the following structure:

```json
{
  "trace_version": 1,
  "session": {
    "session_id": "string",
    "start_time_epoch_ms": number,
    "end_time_epoch_ms": number
  },
  "events": [
    {
      "seq": number,
      "event": "string (PipelineEvent Enum)",
      "component": "string (session | backend | gemini | frontend | pcm)",
      "correlation_id": "string (VAD correlation ID / Turn identifier)",
      "timestamp_epoch_ms": number (float for sub-millisecond precision),
      "timestamp_monotonic_ns": number (integer nanoseconds),
      "metadata": {}
    }
  ]
}
```

---

## 2. Event Definitions and Life Cycles

Here are the definitions of all events captured in the trace.

### Backend Events

| Event Name | Component | Description |
|---|---|---|
| `SESSION_STARTED` | `session` | Emitted when the audio agent session is spawned and initialization begins. |
| `SESSION_ENDED` | `session` | Emitted when the audio agent loop finishes and the trace is saved. |
| `MIC_FRAME_RECEIVED` | `backend` | Emitted for each 20ms block frame sliced by the raw audio input loop. Metadata contains `"frame_id"` (e.g. `onemeta-demo-0`) and VAD decision `"is_speech"`. |
| `VAD_DECISION` | `backend` | Emitted when the Voice Activity Detection state transitions (speech start / end). Metadata includes `"is_speech"`, `"frame_id"`, and `"rms"`. |
| `AUDIO_SENT_TO_GEMINI` | `gemini` | Emitted when a chunk of audio is uploaded to Gemini's WebSocket session. Includes `"frame_id"` and `"correlation_id"` mapping. |
| `GEMINI_WS_FRAME_RECEIVED`| `gemini` | Emitted when a WebSocket message is received back from the Gemini Live Server. **Note**: This represents low-level WebSocket frame arrival. A single logical response turn may arrive in multiple frames. |
| `TRANSLATED_TEXT_RECEIVED`| `gemini` | Emitted when Spanish translation text is parsed from a server response frame. Metadata includes `"text_length"`, `"cumulative_text_length"`, and `"chunk_index"`. |
| `TRANSLATED_AUDIO_RECEIVED`| `gemini` | Emitted when Spanish translation PCM bytes are parsed from a server response frame. Metadata includes `"pcm_bytes"`, `"sample_rate"`, `"duration"`, and `"chunk_index"`. |
| `TEXT_PUBLISHED` | `backend` | Emitted when a translated text packet is successfully published to the LiveKit data channel. Metadata includes `"destination"` and `"payload_size"`. |
| `AUDIO_PUBLISHED` | `backend` | Emitted when a translated audio packet is successfully published to the LiveKit data channel. Metadata includes `"destination"`, `"frame_size"`, `"sample_rate"`, and `"duration"`. |
| `PIPELINE_ERROR` | `backend` | Emitted when a recoverable error occurs in the pipeline. Metadata includes `"stage"`, `"exception"`, and `"message"`. |

### Frontend Events

| Event Name | Component | Description |
|---|---|---|
| `SESSION_STARTED` | `session` | Emitted when the browser connects to the LiveKit room and begins the session. |
| `SESSION_ENDED` | `session` | Emitted when the browser disconnects and triggers the trace download. |
| `TEXT_PACKET_RECEIVED` | `frontend` | Emitted when a Spanish translation text packet is read from the LiveKit data channel. |
| `AUDIO_PACKET_RECEIVED` | `frontend` | Emitted when a Spanish translation PCM packet is read from the LiveKit data channel. |
| `REACT_RENDER_COMPLETED` | `frontend` | Emitted inside a `requestAnimationFrame` block immediately after updating the translation text in React state. |
| `PCM_DECODE_STARTED` | `pcm` | Emitted before initiating base64 to Float32 conversion for Web Audio. |
| `PCM_DECODE_COMPLETED` | `pcm` | Emitted after base64 to Float32 conversion completes successfully. |
| `AUDIO_SCHEDULED` | `pcm` | Emitted when a sound source is scheduled onto the Web Audio timeline. |
| `AUDIO_PLAYBACK_SCHEDULED`| `pcm` | Records the target timestamp where the sound chunk is scheduled to begin hardware playback based on the Web Audio scheduling clock. **Note**: This is not a hardware callback. It represents the mathematically-precise target playback start time. |

---

## 3. Clock Sources and Limitations

> [!WARNING]
> **CRITICAL CLOCK RULE**:
> * The Backend monotonic counter uses `time.perf_counter_ns()`.
> * The Frontend monotonic counter uses `performance.now()`.
> * **These monotonic clocks MUST NOT be directly compared across devices.**
> * Monotonic clocks are only valid for computing **relative offsets (deltas)** on the *same device*.
> * To calculate cross-device metrics (e.g. Network Latency: Frontend Receive - Backend Publish), the analyzer must either align the clocks using the difference between `start_time_epoch_ms` or rely on epoch timestamps (`timestamp_epoch_ms`) which are synchronized via NTP.

---

## 4. Latency Analysis Reference (Phase 2 Formulas)

Here is how the Phase 2 Latency Analyzer should calculate the primary performance metrics:

### A. End-to-End Latency
Measure from the moment the user's voice frame was ingested at the mic until the translated audio is scheduled to play:
$$\Delta_{e2e} = \text{AUDIO\_PLAYBACK\_SCHEDULED.epoch} - \text{MIC\_FRAME\_RECEIVED.epoch}$$
*(Note: Match events using the unique `correlation_id` of the speech turn).*

### B. Gemini Processing Latency
Measure the processing speed of Gemini Live translation server:
$$\Delta_{gemini} = \text{TRANSLATED\_AUDIO\_RECEIVED.monotonic} - \text{AUDIO\_SENT\_TO\_GEMINI.monotonic}$$

### C. Backend Processing Overhead
Measure internal backend pipeline queue and parsing latency:
$$\Delta_{backend} = \text{AUDIO\_PUBLISHED.monotonic} - \text{GEMINI\_WS\_FRAME\_RECEIVED.monotonic}$$

### E. LiveKit Network Latency
Measure network delay from Backend publisher to Frontend receiver:
$$\Delta_{network} = \text{AUDIO\_PACKET\_RECEIVED.epoch} - \text{AUDIO\_PUBLISHED.epoch}$$

### F. Frontend Render Latency
Measure text presentation lag in the React DOM:
$$\Delta_{render} = \text{REACT\_RENDER\_COMPLETED.monotonic} - \text{TEXT\_PACKET\_RECEIVED.monotonic}$$

### G. PCM Decode Latency
Measure audio format parsing time in JavaScript:
$$\Delta_{decode} = \text{PCM\_DECODE\_COMPLETED.monotonic} - \text{PCM\_DECODE\_STARTED.monotonic}$$

### H. Playback Scheduling Overhead
Measure Web Audio queue overhead before playback begins:
$$\Delta_{schedule} = \text{AUDIO\_PLAYBACK\_SCHEDULED.epoch} - \text{AUDIO\_PACKET\_RECEIVED.epoch}$$
