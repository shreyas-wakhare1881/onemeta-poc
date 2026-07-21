import time
import logging
from typing import Dict, Any

logger = logging.getLogger("onemeta.ai.telemetry")

class TelemetryCollector:
    """
    Pluggable telemetry interface for the streaming AI pipeline.

    Tracks session-level events and latencies. Designed for extension:
    future providers (GPU metrics, network metrics, provider-specific stats)
    can plug into this interface without modifying the engine layer.

    Replaces the chunk-specific telemetry class removed in Phase 4C.
    """
    def __init__(self):
        self.reset()

    def reset(self) -> None:
        self.sessions_started: int = 0
        self.sessions_closed: int = 0
        self.partial_events: int = 0
        self.errors: int = 0
        self.total_session_duration_ms: float = 0.0
        self.start_time: float = time.perf_counter()

    def record_session_started(self, session_id: str) -> None:
        self.sessions_started += 1
        logger.debug(f"[Telemetry] Session started: {session_id}")

    def record_session_closed(self, session_id: str, duration_ms: float) -> None:
        self.sessions_closed += 1
        self.total_session_duration_ms += duration_ms
        logger.debug(f"[Telemetry] Session closed: {session_id} duration={duration_ms:.1f}ms")

    def record_partial_event(self) -> None:
        self.partial_events += 1

    def record_error(self) -> None:
        self.errors += 1

    def get_report(self) -> Dict[str, Any]:
        elapsed_sec = time.perf_counter() - self.start_time
        avg_duration = (
            self.total_session_duration_ms / self.sessions_closed
            if self.sessions_closed > 0 else 0.0
        )
        return {
            "elapsed_seconds": elapsed_sec,
            "sessions_started": self.sessions_started,
            "sessions_closed": self.sessions_closed,
            "partial_events": self.partial_events,
            "errors": self.errors,
            "avg_session_duration_ms": avg_duration,
        }

    def log_report(self) -> None:
        r = self.get_report()
        logger.info(
            f"[Telemetry] Elapsed: {r['elapsed_seconds']:.1f}s | "
            f"Sessions: {r['sessions_started']} started / {r['sessions_closed']} closed | "
            f"Partials: {r['partial_events']} | Errors: {r['errors']} | "
            f"Avg Session Duration: {r['avg_session_duration_ms']:.1f}ms"
        )
