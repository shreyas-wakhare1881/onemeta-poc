# OneMeta Speech-to-Speech Translation POC

## Overview

This repository contains the Proof of Concept (POC) for a low-latency, real-time Speech-to-Speech (S2S) translation system for OneMeta.

The objective of this POC is to validate whether an audio-native translation pipeline can significantly reduce end-to-end latency compared to the traditional Speech-to-Text → Machine Translation → Text-to-Speech (STT → MT → TTS) cascade.

Unlike the conventional sequential pipeline, this POC focuses on streaming translation using semantic chunks to deliver translated text and translated speech simultaneously with minimal delay.

---

# Problem Statement

The existing Speech-to-Speech pipeline follows a sequential execution model:

Speech → ASR → Translation → TTS → Audio Output

Each stage waits for the previous stage to complete, resulting in cumulative latency and delayed responses.

Our goal is to build a streaming architecture where translation begins as early as possible while preserving conversational context.

---

# POC Objective

The primary objective of this POC is to validate the feasibility of a real-time multilingual communication platform capable of:

- Streaming live audio between two users.
- Translating conversations in real time.
- Displaying translated subtitles instantly.
- Playing translated speech with minimal delay.
- Preserving conversational context using semantic chunk streaming.

---

# Target Performance

| Metric | Target |
|----------|---------|
| Time to First Translated Text (TTFT) | < 500 ms |
| Time to First Translated Audio (TTFA) | < 500 ms |
| Streaming | Continuous |
| Translation | Context Aware |

---

# High-Level Architecture

```text
                User A (Source Language)
                         │
                   Live Audio Stream
                         │
                         ▼
                LiveKit + WebRTC
                         │
                         ▼
              LiveKit Python Agent
                         │
                         ▼
                  Semantic VAD
           (Detect Active Speech)
                         │
                         ▼
          Streaming Context Manager
      (Semantic Chunk Generation)
                         │
                         ▼
               Gemma 4 (Local GPU)
        (Translation Processing Engine)
               /                  \
              /                    \
             ▼                      ▼
 Streaming Translated Text   Streaming Translated Audio
             │                      │
             └──────────┬───────────┘
                        ▼
           LiveKit Streaming Publisher
                        │
                        ▼
             User B (Target Language)
```

---

# Architecture Components

## User A

Initiates the conversation by speaking in the source language.

**Responsibility**

- Capture live speech.
- Stream audio continuously.
- Act as the conversation initiator.

---

## LiveKit + WebRTC

Responsible for establishing the real-time communication channel.

**Responsibility**

- Stream audio between participants.
- Maintain low-latency communication.
- Manage real-time media transport.

---

## LiveKit Python Agent

Acts as the central processing engine of the pipeline.

**Responsibility**

- Receive live audio frames.
- Control the processing workflow.
- Forward audio to downstream components.

---

## Semantic VAD

Detects meaningful speech activity.

**Responsibility**

- Detect speech start and end.
- Remove silence.
- Forward only speech segments.

---

## Streaming Context Manager

Builds semantic chunks while preserving conversational meaning.

**Responsibility**

- Group speech into meaningful chunks.
- Preserve conversational context.
- Decide the optimal point to trigger translation.

---

## Gemma 4 (Local GPU)

Core AI engine responsible for translation.

**Responsibility**

- Process incoming semantic chunks.
- Generate translated text.
- Generate translated speech.

---

## Streaming Translated Text

Continuously displays translated subtitles.

**Responsibility**

- Stream translated text.
- Keep subtitles synchronized.
- Update the UI incrementally.

---

## Streaming Translated Audio

Continuously generates translated speech.

**Responsibility**

- Generate streaming audio.
- Synchronize with translated text.
- Deliver low-latency speech output.

---

## LiveKit Streaming Publisher

Publishes translated content back to the listener.

**Responsibility**

- Publish translated audio.
- Deliver translated stream.
- Maintain real-time playback.

---

## User B

Receives translated content.

**Responsibility**

- Listen to translated speech.
- View translated subtitles.
- Continue the conversation naturally.

---

# Technology Stack

| Layer | Technology |
|--------|------------|
| Frontend | Next.js + React + Tailwind CSS |
| Backend | FastAPI (Python) |
| Real-Time Communication | LiveKit + WebRTC |
| AI Translation Engine | Gemma 4 (Local GPU) |
| Speech Detection | Semantic VAD |
| GPU Runtime | PyTorch + CUDA |
| Communication | WebSockets |

---

# Implementation Phases

## Phase 1

Project Initialization

- Repository setup
- Folder structure
- Development environment

---

## Phase 2

Communication Layer

- LiveKit setup
- WebRTC integration
- Video & audio communication

---

## Phase 3

Speech Processing

- Semantic VAD
- Streaming Context Manager
- Semantic chunk generation

---

## Phase 4

AI Translation

- Gemma 4 integration
- Streaming translation
- Streaming audio generation

---

## Phase 5

Real-Time Delivery

- Streaming subtitles
- Streaming translated speech
- UI synchronization

---

## Phase 6

Performance Validation

- Latency measurement
- TTFT validation
- TTFA validation
- Pipeline optimization

---

# Project Scope

This repository focuses only on the Proof of Concept (POC).

Included:

- Live audio communication
- Real-time translation
- Streaming subtitles
- Streaming translated speech
- Semantic chunk streaming
- Local GPU inference

Excluded:

- Production deployment
- Authentication
- User management
- Database
- Monitoring
- Scalability
- Multi-region deployment

---

# Future Improvements

Potential enhancements after successful POC validation:

- Multi-language support
- Dynamic model orchestration
- Adaptive buffering
- Intelligent routing
- GPU scheduling optimization
- Enterprise deployment
- Production monitoring

---

# Repository Status

Current Status

Project Initialization

Version

v0.1.0 (POC)

---

# License

This project is intended for research and Proof of Concept purposes only.