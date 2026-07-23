import os
import time
import json
import uuid
import logging
from pathlib import Path
from threading import Lock
from datetime import datetime
from typing import Dict, Any, List

from .tracing_events import PipelineEvent

logger = logging.getLogger("onemeta.pipeline_tracer")

class PipelineEventTracer:
    def __init__(self, session_id: str, session_folder: str = None):
        self.session_id = session_id
        self.session_folder = session_folder
        self.enabled = os.getenv("ENABLE_PIPELINE_TRACE", "false").lower() == "true"
        self.trace_level = os.getenv("PIPELINE_TRACE_LEVEL", "basic").lower()
        self.events: List[Dict[str, Any]] = []
        self._lock = Lock()
        self._seq = 0
        self._start_time_epoch_ms = 0
        
        if self.enabled:
            # Create output directory for traces (parents[3] is onemeta-poc base directory)
            if self.session_folder:
                self.output_dir = Path(__file__).resolve().parents[3] / "output" / self.session_folder
            else:
                self.output_dir = Path(__file__).resolve().parents[3] / "output" / "traces" / "backend"
            try:
                self.output_dir.mkdir(parents=True, exist_ok=True)
                logger.info(f"Initialized PipelineEventTracer for session: {self.session_id} (Output: {self.output_dir})")
            except Exception as e:
                logger.error(f"Failed to create backend traces directory {self.output_dir}: {e}")

    def _determine_component(self, event: PipelineEvent) -> str:
        if event in (PipelineEvent.SESSION_STARTED, PipelineEvent.SESSION_ENDED):
            return "session"
        elif event in (PipelineEvent.MIC_FRAME_RECEIVED, PipelineEvent.VAD_DECISION, PipelineEvent.TEXT_PUBLISHED, PipelineEvent.AUDIO_PUBLISHED, PipelineEvent.PIPELINE_ERROR):
            return "backend"
        elif event in (PipelineEvent.AUDIO_SENT_TO_GEMINI, PipelineEvent.GEMINI_WS_FRAME_RECEIVED, PipelineEvent.TRANSLATED_TEXT_RECEIVED, PipelineEvent.TRANSLATED_AUDIO_RECEIVED):
            return "gemini"
        return "unknown"

    def log_event(self, event: PipelineEvent, correlation_id: str = "", metadata: Dict[str, Any] = None) -> None:
        if not self.enabled:
            return
            
        epoch_ms = time.time() * 1000
        mono_ns = time.perf_counter_ns()
        component = self._determine_component(event)
        
        with self._lock:
            self._seq += 1
            if event == PipelineEvent.SESSION_STARTED:
                self._start_time_epoch_ms = int(epoch_ms)
                
            # Stable, globally-unique event identifier
            ev_id = f"evt_{uuid.uuid4().hex}"

            self.events.append({
                "seq": self._seq,
                "event": event.value,
                "component": component,
                "correlation_id": correlation_id,
                "timestamp_epoch_ms": epoch_ms,
                "timestamp_monotonic_ns": mono_ns,
                "event_id": ev_id,
                "metadata": metadata or {}
            })
            
    def _validate_trace(self, trace_data: dict) -> bool:
        errors = []
        if trace_data.get("trace_version") != 1:
            errors.append("Missing or invalid trace_version")
        session = trace_data.get("session")
        if not isinstance(session, dict):
            errors.append("session is missing or not a dictionary")
        else:
            if not session.get("session_id"):
                errors.append("session_id is missing or empty")
            if not session.get("start_time_epoch_ms"):
                errors.append("start_time_epoch_ms is missing")
            if not session.get("end_time_epoch_ms"):
                errors.append("end_time_epoch_ms is missing")
                
        events = trace_data.get("events")
        if not isinstance(events, list):
            errors.append("events is missing or not a list")
        else:
            last_seq = 0
            valid_event_names = {e.value for e in PipelineEvent}
            for idx, ev in enumerate(events):
                if not isinstance(ev, dict):
                    errors.append(f"Event at index {idx} is not a dictionary")
                    continue
                # Ensure event_id exists
                if ev.get("event_id") is None:
                    errors.append(f"Event at index {idx} is missing event_id")
                seq = ev.get("seq")
                if seq is None:
                    errors.append(f"Event at index {idx} is missing sequence number (seq)")
                else:
                    if seq <= last_seq:
                        errors.append(f"Event at index {idx} sequence number is not strictly increasing (seq={seq}, last={last_seq})")
                    last_seq = seq
                if not ev.get("event"):
                    errors.append(f"Event at index {idx} is missing event name")
                elif ev.get("event") not in valid_event_names:
                    errors.append(f"Event at index {idx} has invalid event name: {ev.get('event')}")
                if ev.get("timestamp_epoch_ms") is None:
                    errors.append(f"Event at index {idx} is missing timestamp_epoch_ms")
                if ev.get("timestamp_monotonic_ns") is None:
                    errors.append(f"Event at index {idx} is missing timestamp_monotonic_ns")
                if not ev.get("component"):
                    errors.append(f"Event at index {idx} is missing component name")
                
                # Validate metadata fields for each pipeline event
                event_name = ev.get("event")
                metadata = ev.get("metadata", {})
                if not isinstance(metadata, dict):
                    errors.append(f"Event at index {idx} has invalid metadata structure (not a dictionary)")
                    continue
                
                if event_name == PipelineEvent.MIC_FRAME_RECEIVED.value:
                    for field in ["frame_id", "packet_size", "sample_rate"]:
                        if field not in metadata:
                            errors.append(f"Event at index {idx} ({event_name}) is missing required metadata field: {field}")
                elif event_name == PipelineEvent.VAD_DECISION.value:
                    for field in ["is_speech", "frame_id"]:
                        if field not in metadata:
                            errors.append(f"Event at index {idx} ({event_name}) is missing required metadata field: {field}")
                elif event_name == PipelineEvent.AUDIO_SENT_TO_GEMINI.value:
                    for field in ["frame_id", "packet_size", "sample_rate"]:
                        if field not in metadata:
                            errors.append(f"Event at index {idx} ({event_name}) is missing required metadata field: {field}")
                elif event_name == PipelineEvent.GEMINI_WS_FRAME_RECEIVED.value:
                    for field in ["response_type", "chunk_index"]:
                        if field not in metadata:
                            errors.append(f"Event at index {idx} ({event_name}) is missing required metadata field: {field}")
                elif event_name == PipelineEvent.TRANSLATED_TEXT_RECEIVED.value:
                    for field in ["text_length", "cumulative_text_length", "chunk_index"]:
                        if field not in metadata:
                            errors.append(f"Event at index {idx} ({event_name}) is missing required metadata field: {field}")
                elif event_name == PipelineEvent.TRANSLATED_AUDIO_RECEIVED.value:
                    for field in ["pcm_bytes", "sample_rate", "duration"]:
                        if field not in metadata:
                            errors.append(f"Event at index {idx} ({event_name}) is missing required metadata field: {field}")
                elif event_name == PipelineEvent.TEXT_PUBLISHED.value:
                    for field in ["destination", "payload_size"]:
                        if field not in metadata:
                            errors.append(f"Event at index {idx} ({event_name}) is missing required metadata field: {field}")
                elif event_name == PipelineEvent.AUDIO_PUBLISHED.value:
                    for field in ["destination", "frame_size", "sample_rate", "duration"]:
                        if field not in metadata:
                            errors.append(f"Event at index {idx} ({event_name}) is missing required metadata field: {field}")
                elif event_name == PipelineEvent.PIPELINE_ERROR.value:
                    for field in ["stage", "exception", "message"]:
                        if field not in metadata:
                            errors.append(f"Event at index {idx} ({event_name}) is missing required metadata field: {field}")
                    
        if errors:
            logger.error(f"Trace validation failed with {len(errors)} errors:\n" + "\n".join(errors))
            return False
        return True

    def save(self) -> None:
        if not self.enabled or not self.events:
            return
            
        end_epoch_ms = time.time() * 1000
        if self.events:
            end_epoch_ms = self.events[-1]["timestamp_epoch_ms"]
            
        # Format the trace according to V1 schema
        trace_data = {
            "trace_version": 1,
            "session": {
                "session_id": self.session_id,
                "start_time_epoch_ms": self._start_time_epoch_ms,
                "end_time_epoch_ms": int(end_epoch_ms)
            },
            "events": self.events
        }
        
        # Validate trace before saving (errors are logged but saving is still attempted)
        self._validate_trace(trace_data)
        
        if self.session_folder:
            from .session_finalizer import get_session_lock
            session_lock = get_session_lock(self.output_dir)
            with session_lock:
                # 1. Save standard trace JSON as session_trace.json
                json_filepath = self.output_dir / "session_trace.json"
                try:
                    with open(json_filepath, "w", encoding="utf-8") as f:
                        json.dump(trace_data, f, indent=2)
                    logger.info(f"Successfully saved session trace JSON to: {json_filepath}")
                except Exception as e:
                    logger.error(f"Failed to save session trace JSON: {e}")
                    
                # 2. Save readable backend event log as backend.log
                backend_log_path = self.output_dir / "backend.log"
                try:
                    with open(backend_log_path, "w", encoding="utf-8") as f:
                        for ev in self.events:
                            f.write(f"[{ev['component'].upper()}] {ev['event']} | seq={ev['seq']} | timestamp={ev['timestamp_epoch_ms']} | metadata={json.dumps(ev['metadata'])}\n")
                    logger.info(f"Successfully saved backend.log event trace to: {backend_log_path}")
                except Exception as e:
                    logger.error(f"Failed to save backend.log event trace: {e}")
        else:
            # Legacy fallback for backwards compatibility when no active session folder is configured
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"session_{self.session_id}_{timestamp_str}.json"
            filepath = self.output_dir / filename
            try:
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(trace_data, f, indent=2)
                logger.info(f"Successfully saved backend trace file to: {filepath}")
            except Exception as e:
                logger.error(f"Failed to save backend trace file: {e}")
