import asyncio
import logging
import time
from typing import Dict, List, Callable, Union

from ..types.speech import SpeechChunk
from .config import AIConfig, QueueDropPolicy
from .events import AIStartedEvent, AIPartialEvent, AICompletedEvent, AIErrorEvent, TranslationFailedEvent, EventMetrics
from .types import RuntimeRequest, TranslationResult, TranslationMetrics
from .telemetry import AITelemetry

logger = logging.getLogger("onemeta.ai.pipeline_chunk")
AIEventListener = Callable[[Union[AIStartedEvent, AIPartialEvent, AICompletedEvent, AIErrorEvent, TranslationFailedEvent]], None]

class ChunkInferencePipeline:
    """
    Handles chunk-based inference pipeline operations (Stateless Inference Sink -> Inference Queue -> Runtime).
    Decoupled from streaming operations.
    """
    def __init__(self, config: AIConfig, telemetry: AITelemetry, runtime):
        self.config = config
        self.telemetry = telemetry
        self.runtime = runtime
        self.queue = asyncio.Queue(maxsize=config.queue_maxsize)
        
        self._enqueue_times: Dict[str, float] = {}
        self._listeners: List[AIEventListener] = []
        self._worker_task: asyncio.Task | None = None
        self._running = False

    def register_listener(self, listener: AIEventListener) -> None:
        if listener not in self._listeners:
            self._listeners.append(listener)

    def unregister_listener(self, listener: AIEventListener) -> None:
        if listener in self._listeners:
            self._listeners.remove(listener)

    async def _emit(self, event: Union[AIStartedEvent, AIPartialEvent, AICompletedEvent, AIErrorEvent, TranslationFailedEvent]) -> None:
        for listener in self._listeners:
            try:
                if asyncio.iscoroutinefunction(listener):
                    await listener(event)
                else:
                    listener(event)
            except Exception as e:
                logger.error(f"Error executing AI listener callback: {e}", exc_info=True)

    async def start(self) -> None:
        """
        Starts the background worker queue loop.
        """
        if self._running:
            return
        self._running = True
        self._worker_task = asyncio.create_task(self._process_queue())

    async def shutdown(self) -> None:
        """
        Drains the queue and shuts down the background task.
        """
        if not self._running:
            return
        self._running = False
        
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None

        pending_count = self.queue.qsize()
        if pending_count > 0:
            logger.warning(f"ChunkInferencePipeline shutdown: discarding {pending_count} pending chunks in the queue.")
            for _ in range(pending_count):
                try:
                    self.queue.get_nowait()
                    self.queue.task_done()
                    self.telemetry.record_dropped_chunk()
                except (asyncio.QueueEmpty, ValueError):
                    pass
        
        self._enqueue_times.clear()

    def enqueue_chunk(self, chunk: SpeechChunk) -> None:
        """
        Enqueues a SpeechChunk for processing.
        """
        if not self._running:
            logger.warning(f"ChunkInferencePipeline ignoring chunk {chunk.chunk_id}: Pipeline is not running.")
            return

        self._enqueue_times[chunk.chunk_id] = time.perf_counter()

        if self.queue.full():
            policy = self.config.queue_drop_policy
            if policy == QueueDropPolicy.DROP_OLDEST:
                try:
                    evicted = self.queue.get_nowait()
                    self.queue.task_done()
                    self._enqueue_times.pop(evicted.chunk_id, None)
                    self.telemetry.record_dropped_chunk()
                    logger.warning(
                        f"ChunkInferencePipeline queue full. Evicted oldest chunk: {evicted.chunk_id} "
                        f"(DROP_OLDEST). Current depth: {self.queue.qsize()}"
                    )
                except asyncio.QueueEmpty:
                    pass
            elif policy == QueueDropPolicy.DROP_NEWEST:
                self._enqueue_times.pop(chunk.chunk_id, None)
                self.telemetry.record_dropped_chunk()
                logger.warning(
                    f"ChunkInferencePipeline queue full. Discarded newest chunk: {chunk.chunk_id} "
                    f"(DROP_NEWEST). Current depth: {self.queue.qsize()}"
                )
                return

        try:
            self.queue.put_nowait(chunk)
        except asyncio.QueueFull:
            logger.error(f"ChunkInferencePipeline: QueueFull exception triggered for chunk {chunk.chunk_id} after drop logic execution.")

    async def _process_queue(self) -> None:
        """
        Worker consumer loop.
        """
        while self._running:
            try:
                chunk = await self.queue.get()
                try:
                    await self._process_chunk(chunk)
                except Exception as e:
                    logger.error(f"ChunkInferencePipeline error processing chunk {chunk.chunk_id}: {e}", exc_info=True)
                finally:
                    self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"ChunkInferencePipeline queue processing loop encountered unexpected error: {e}", exc_info=True)
                await asyncio.sleep(0.01)

    async def _process_chunk(self, chunk: SpeechChunk) -> None:
        """
        Process a single SpeechChunk using the model runtime.
        """
        chunk_id = chunk.chunk_id
        seq = chunk.sequence_number
        identity = chunk.participant_identity
        room = chunk.room_name
        
        t_dequeue = time.perf_counter()
        t_enqueue = self._enqueue_times.pop(chunk_id, t_dequeue)
        queue_wait_ms = (t_dequeue - t_enqueue) * 1000.0

        await self._emit(AIStartedEvent(
            chunk_id=chunk_id,
            sequence_number=seq,
            timestamp=time.time(),
            metrics=EventMetrics(
                start_timestamp=chunk.start_timestamp,
                end_timestamp=chunk.end_timestamp,
                queue_wait_ms=queue_wait_ms,
                processing_time_ms=chunk.metadata.processing_time_ms
            )
        ))

        t_gemma_start = time.perf_counter()
        first_token_time = None
        cumulative_text = ""

        final_metrics = TranslationMetrics()
        try:
            source_lang = self.config.default_source_lang
            target_lang = self.config.default_target_lang
            
            request = RuntimeRequest(
                audio_bytes=chunk.pcm_data,
                audio_format="pcm16",
                sample_rate=16000,
                source_language=source_lang,
                target_language=target_lang,
                chunk_id=chunk_id,
                sequence_number=seq,
                conversation_id="",
                stream=True,
                metadata={
                    "participant_identity": identity,
                    "room_name": room
                }
            )
            
            async for result in self.runtime.stream_generate(request):
                final_metrics = result.metrics
                
                if result.finished:
                    break
                    
                if first_token_time is None and result.translated_text:
                    first_token_time = time.perf_counter()
                
                cumulative_text += result.translated_text
                
                await self._emit(AIPartialEvent(
                    chunk_id=chunk_id,
                    sequence_number=seq,
                    text_delta=result.translated_text,
                    cumulative_text=cumulative_text,
                    timestamp=time.time(),
                    metrics=EventMetrics()
                ))

            t_gemma_end = time.perf_counter()
            gemma_latency_ms = (t_gemma_end - t_gemma_start) * 1000.0
            first_token_latency_ms = (first_token_time - t_gemma_start) * 1000.0 if first_token_time is not None else gemma_latency_ms
            total_ai_latency_ms = queue_wait_ms + gemma_latency_ms

            await self._emit(AICompletedEvent(
                chunk_id=chunk_id,
                sequence_number=seq,
                full_text=cumulative_text,
                duration_ms=total_ai_latency_ms,
                timestamp=time.time(),
                metrics=EventMetrics(
                    chunk_duration_ms=chunk.duration_ms,
                    ttft_ms=first_token_latency_ms,
                    gemma_latency_ms=gemma_latency_ms,
                    total_ai_latency_ms=total_ai_latency_ms,
                    audio_duration_ms=final_metrics.audio_duration_ms if final_metrics else chunk.duration_ms
                )
            ))

            logger.info(
                f"[Chunk Processed] "
                f"Chunk ID: {chunk_id} | "
                f"Sequence: {seq} | "
                f"Audio Duration: {final_metrics.audio_duration_ms:.1f}ms | "
                f"PCM Size: {len(chunk.pcm_data)} bytes | "
                f"Payload Size: {final_metrics.payload_size_bytes} bytes | "
                f"Send Timestamp: {t_gemma_start:.3f} | "
                f"First Response Timestamp: {first_token_time if first_token_time else t_gemma_end:.3f} | "
                f"Completion Timestamp: {t_gemma_end:.3f} | "
                f"Total Response Time: {total_ai_latency_ms:.1f}ms (TTFT: {first_token_latency_ms:.1f}ms)"
            )

            token_count = len(cumulative_text.split())
            self.telemetry.record_success(
                queue_wait_ms=queue_wait_ms,
                first_token_latency_ms=first_token_latency_ms,
                gemma_latency_ms=gemma_latency_ms,
                total_ai_latency_ms=total_ai_latency_ms,
                token_count=token_count
            )
            self.telemetry.log_report(self.queue.qsize())

        except Exception as e:
            logger.exception(f"ChunkInferencePipeline failed during inference execution for chunk {chunk_id}: {e}")
            now = time.time()
            await self._emit(TranslationFailedEvent(
                chunk_id=chunk_id,
                sequence_number=seq,
                error_message=str(e),
                timestamp=now
            ))
            await self._emit(AIErrorEvent(
                chunk_id=chunk_id,
                sequence_number=seq,
                error_message=str(e),
                timestamp=now
            ))
            
            self.telemetry.record_failure()
            self.telemetry.log_report(self.queue.qsize())
            raise e
