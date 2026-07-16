import time
import logging
from typing import Iterable, List
from ..types.audio import AudioFrame
from ..types.speech import SpeechChunkMetadata, FlushReason

logger = logging.getLogger("onemeta.telemetry")

class AudioTelemetry:
    """
    Passive metrics observer tracking pipeline latency, queue sizes, and frame drops.
    
    Uses time.perf_counter_ns() for monotonic nanosecond time capture.
    """
    def __init__(self):
        self.reset()

    def reset(self):
        self.frames_received = 0
        self.frames_processed = 0
        self.dropped_frames = 0
        self.total_bytes_processed = 0
        
        self.start_time_ns = time.perf_counter_ns()
        self.last_log_time_ns = time.perf_counter_ns()
        
        # Latency statistics in relative nanoseconds
        self.total_queue_delay_ns = 0
        self.max_queue_delay_ns = 0
        self.total_processing_duration_ns = 0
        self.max_processing_duration_ns = 0
        self.total_frame_age_ns = 0
        self.max_frame_age_ns = 0

    def record_received(self):
        self.frames_received += 1

    def record_queued(self, capture_time_ns: int):
        delay = time.perf_counter_ns() - capture_time_ns
        self.total_queue_delay_ns += delay
        if delay > self.max_queue_delay_ns:
            self.max_queue_delay_ns = delay

    def record_processed(self, frame: AudioFrame, t_start_ns: int, t_end_ns: int):
        self.frames_processed += 1
        self.total_bytes_processed += len(frame.pcm_data)
        
        # Calculate queue wait duration
        queue_wait = t_start_ns - frame.queue_timestamp_ns if frame.queue_timestamp_ns > 0 else 0
        self.total_queue_delay_ns += queue_wait
        if queue_wait > self.max_queue_delay_ns:
            self.max_queue_delay_ns = queue_wait

        # Calculate worker processing duration
        proc_time = t_end_ns - t_start_ns
        self.total_processing_duration_ns += proc_time
        if proc_time > self.max_processing_duration_ns:
            self.max_processing_duration_ns = proc_time

        # Calculate end-to-end frame age (capture to worker completion)
        age = t_end_ns - frame.capture_timestamp_ns if frame.capture_timestamp_ns > 0 else 0
        self.total_frame_age_ns += age
        if age > self.max_frame_age_ns:
            self.max_frame_age_ns = age

    def record_dropped(self):
        self.dropped_frames += 1

    def create_chunk_metadata(
        self, 
        frames_iterable: Iterable[AudioFrame], 
        reason: FlushReason, 
        t_start_ns: int,
        silence_count: int,
        average_rms: float,
        peak_rms: float
    ) -> SpeechChunkMetadata:
        """
        Pure latency and statistical calculator creating strongly typed SpeechChunkMetadata.
        
        Single-pass loop over frame collections to avoid intermediate list allocations.
        """
        now_ns = time.perf_counter_ns()
        frames: List[AudioFrame] = list(frames_iterable)

        total_queue_wait_ns = 0.0
        valid_queue_count = 0
        total_frame_age_ns = 0.0

        for f in frames:
            if f.processing_timestamp_ns > 0 and f.queue_timestamp_ns > 0:
                total_queue_wait_ns += (f.processing_timestamp_ns - f.queue_timestamp_ns)
                valid_queue_count += 1
            total_frame_age_ns += (now_ns - f.capture_timestamp_ns)

        avg_queue_wait_ms = (total_queue_wait_ns / valid_queue_count / 1_000_000.0) if valid_queue_count > 0 else 0.0
        processing_time_ms = (now_ns - t_start_ns) / 1_000_000.0
        avg_frame_age_ms = (total_frame_age_ns / len(frames) / 1_000_000.0) if frames else 0.0

        # Calculate speech ratio in window
        speech_frames = len(frames)
        total_window_frames = speech_frames + silence_count
        speech_ratio = (speech_frames / total_window_frames) if total_window_frames > 0 else 1.0

        return SpeechChunkMetadata(
            queue_wait_ms=avg_queue_wait_ms,
            processing_time_ms=processing_time_ms,
            end_to_end_age_ms=avg_frame_age_ms,
            flush_reason=reason,
            average_rms=average_rms,
            peak_rms=peak_rms,
            speech_ratio=speech_ratio
        )

    def get_report(self, current_queue_size: int, queue_maxsize: int) -> dict:
        now_ns = time.perf_counter_ns()
        elapsed = (now_ns - self.start_time_ns) / 1_000_000_000.0
        fps = self.frames_processed / elapsed if elapsed > 0 else 0.0
        throughput_bps = self.total_bytes_processed / elapsed if elapsed > 0 else 0.0
        
        avg_queue_wait_ms = (self.total_queue_delay_ns / self.frames_processed / 1_000_000.0) if self.frames_processed > 0 else 0.0
        avg_proc_time_ms = (self.total_processing_duration_ns / self.frames_processed / 1_000_000.0) if self.frames_processed > 0 else 0.0
        avg_frame_age_ms = (self.total_frame_age_ns / self.frames_processed / 1_000_000.0) if self.frames_processed > 0 else 0.0

        # Queue capacity metrics
        queue_utilization = (current_queue_size / queue_maxsize * 100.0) if queue_maxsize > 0 else 0.0
        worker_busy = (self.total_processing_duration_ns / (now_ns - self.start_time_ns) * 100.0) if elapsed > 0 else 0.0
        worker_busy = min(worker_busy, 100.0)

        # Drop rates
        total_attempts = self.frames_received
        drop_rate = (self.dropped_frames / total_attempts * 100.0) if total_attempts > 0 else 0.0

        return {
            "elapsed_seconds": elapsed,
            "frames_received": self.frames_received,
            "frames_processed": self.frames_processed,
            "dropped_frames": self.dropped_frames,
            "drop_rate_pct": drop_rate,
            "processed_fps": fps,
            "throughput_bytes_per_sec": throughput_bps,
            "queue_depth": current_queue_size,
            "queue_utilization_pct": queue_utilization,
            "worker_busy_pct": worker_busy,
            # Latency statistics (estimated, not measured)
            "avg_queue_wait_ms_est": avg_queue_wait_ms,
            "max_queue_wait_ms_est": self.max_queue_delay_ns / 1_000_000.0,
            "avg_processing_time_ms_est": avg_proc_time_ms,
            "max_processing_time_ms_est": self.max_processing_duration_ns / 1_000_000.0,
            "avg_frame_age_ms_est": avg_frame_age_ms,
            "max_frame_age_ms_est": self.max_frame_age_ns / 1_000_000.0,
        }

    def log_report(self, current_queue_size: int, queue_maxsize: int):
        now_ns = time.perf_counter_ns()
        # Log report once every 5 seconds
        if (now_ns - self.last_log_time_ns) >= 5_000_000_000:
            report = self.get_report(current_queue_size, queue_maxsize)
            
            # Dual alert logic for worker busy percentage (Warning at 80%, Critical at 95%)
            busy_pct = report["worker_busy_pct"]
            if busy_pct > 95.0:
                logger.critical(
                    f"CRITICAL WORKER BUSY ALERT: Processing threads are {busy_pct:.1f}% busy! "
                    f"Pipeline capacity limit reached, frames will be dropped!"
                )
            elif busy_pct > 80.0:
                logger.warning(
                    f"HIGH WORKER BUSY WARNING: Processing threads are {busy_pct:.1f}% busy. "
                    f"Check for downstream sink queue blockages!"
                )

            logger.info(
                f"[Audio Telemetry Observer] "
                f"FPS: {report['processed_fps']:.1f} | "
                f"Queue depth: {report['queue_depth']}/{queue_maxsize} ({report['queue_utilization_pct']:.1f}%) | "
                f"Worker busy: {report['worker_busy_pct']:.1f}% | "
                f"Drops: {report['dropped_frames']} ({report['drop_rate_pct']:.2f}%) | "
                f"Avg Queue Delay (Est): {report['avg_queue_wait_ms_est']:.2f}ms | "
                f"Avg Proc Time (Est): {report['avg_processing_time_ms_est']:.2f}ms | "
                f"Avg Frame Age (Est): {report['avg_frame_age_ms_est']:.2f}ms"
            )
            self.last_log_time_ns = now_ns
