import time
import logging
from typing import Dict, Any

logger = logging.getLogger("onemeta.ai.telemetry")

class AITelemetry:
    """
    Tracks and reports real-time latency and performance metrics for the AI pipeline.
    """
    def __init__(self):
        self.reset()

    def reset(self):
        self.successful_requests = 0
        self.failed_requests = 0
        self.dropped_chunks = 0
        
        # Accumulators for latency tracking (in milliseconds)
        self.total_queue_wait_ms = 0.0
        self.max_queue_wait_ms = 0.0
        
        self.total_first_token_latency_ms = 0.0
        self.max_first_token_latency_ms = 0.0
        
        self.total_gemma_latency_ms = 0.0
        self.max_gemma_latency_ms = 0.0
        
        self.total_ai_latency_ms = 0.0
        self.max_ai_latency_ms = 0.0
        
        self.total_tokens = 0
        self.start_time = time.perf_counter()

    def record_success(
        self,
        queue_wait_ms: float,
        first_token_latency_ms: float,
        gemma_latency_ms: float,
        total_ai_latency_ms: float,
        token_count: int
    ):
        self.successful_requests += 1
        
        self.total_queue_wait_ms += queue_wait_ms
        self.max_queue_wait_ms = max(self.max_queue_wait_ms, queue_wait_ms)
        
        self.total_first_token_latency_ms += first_token_latency_ms
        self.max_first_token_latency_ms = max(self.max_first_token_latency_ms, first_token_latency_ms)
        
        self.total_gemma_latency_ms += gemma_latency_ms
        self.max_gemma_latency_ms = max(self.max_gemma_latency_ms, gemma_latency_ms)
        
        self.total_ai_latency_ms += total_ai_latency_ms
        self.max_ai_latency_ms = max(self.max_ai_latency_ms, total_ai_latency_ms)
        
        self.total_tokens += token_count

    def record_failure(self):
        self.failed_requests += 1

    def record_dropped_chunk(self):
        self.dropped_chunks += 1

    def get_report(self, current_queue_depth: int) -> Dict[str, Any]:
        total_requests = self.successful_requests + self.failed_requests
        elapsed_sec = time.perf_counter() - self.start_time
        
        avg_queue_wait = (self.total_queue_wait_ms / self.successful_requests) if self.successful_requests > 0 else 0.0
        avg_first_token = (self.total_first_token_latency_ms / self.successful_requests) if self.successful_requests > 0 else 0.0
        avg_gemma = (self.total_gemma_latency_ms / self.successful_requests) if self.successful_requests > 0 else 0.0
        avg_total_ai = (self.total_ai_latency_ms / self.successful_requests) if self.successful_requests > 0 else 0.0
        
        # Calculate tokens per second of generation time
        # Convert total gemma latency back to seconds for rate calculation
        gemma_sec = self.total_gemma_latency_ms / 1000.0
        tokens_per_second = (self.total_tokens / gemma_sec) if gemma_sec > 0 else 0.0
        
        return {
            "elapsed_seconds": elapsed_sec,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "dropped_chunks": self.dropped_chunks,
            "total_requests": total_requests,
            "inference_queue_depth": current_queue_depth,
            
            "avg_queue_wait_ms": avg_queue_wait,
            "max_queue_wait_ms": self.max_queue_wait_ms,
            
            "avg_first_token_latency_ms": avg_first_token,
            "max_first_token_latency_ms": self.max_first_token_latency_ms,
            
            "avg_gemma_latency_ms": avg_gemma,
            "max_gemma_latency_ms": self.max_gemma_latency_ms,
            
            "avg_total_ai_latency_ms": avg_total_ai,
            "max_total_ai_latency_ms": self.max_ai_latency_ms,
            
            "total_tokens": self.total_tokens,
            "tokens_per_second": tokens_per_second
        }

    def log_report(self, current_queue_depth: int):
        report = self.get_report(current_queue_depth)
        
        logger.info(
            f"[AI Telemetry Report] "
            f"Queue Depth: {report['inference_queue_depth']} | "
            f"Success/Fail/Dropped: {report['successful_requests']}/{report['failed_requests']}/{report['dropped_chunks']} | "
            f"Avg Queue Wait: {report['avg_queue_wait_ms']:.2f}ms | "
            f"Avg TTFT: {report['avg_first_token_latency_ms']:.2f}ms | "
            f"Avg Gemma Latency: {report['avg_gemma_latency_ms']:.2f}ms | "
            f"Avg Total AI Latency: {report['avg_total_ai_latency_ms']:.2f}ms | "
            f"Speed: {report['tokens_per_second']:.1f} tokens/sec"
        )
