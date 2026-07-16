import asyncio
import logging

logger = logging.getLogger("onemeta.audio_queue")

class AsyncAudioQueue:
    """
    A bounded, non-blocking asynchronous queue for audio processing.
    
    If the queue reaches capacity, it drops the oldest frame (DROP_OLDEST policy)
    to prevent memory leak and backpressure latency buildup. Exposes metrics.
    """
    def __init__(self, maxsize: int = 200):
        self._queue = asyncio.Queue(maxsize=maxsize)
        self.maxsize = maxsize
        self._overflow_count = 0
        self._total_pushed = 0

    @property
    def overflow_count(self) -> int:
        """
        Total number of dropped/evicted frames.
        """
        return self._overflow_count

    @property
    def total_pushed(self) -> int:
        """
        Total number of attempted pushes (whether succeeded or dropped).
        """
        return self._total_pushed

    @property
    def drop_rate(self) -> float:
        """
        Ratio of overflow-dropped frames to total attempted pushes.
        """
        if self._total_pushed == 0:
            return 0.0
        return self._overflow_count / self._total_pushed

    def qsize(self) -> int:
        return self._queue.qsize()

    def put_nowait(self, item) -> bool:
        """
        Pushes an item into the queue.
        If full, drops the oldest item in the queue to make room.
        Returns True if item was added, False if oldest was dropped to make room.
        """
        self._total_pushed += 1
        
        if self._queue.full():
            try:
                # Evict oldest element (DROP_OLDEST policy)
                self._queue.get_nowait()
                self._queue.task_done()
                self._overflow_count += 1
                logger.warning(
                    f"Audio queue full. Evicting oldest frame (DROP_OLDEST). "
                    f"Overflows: {self._overflow_count} | Drop Rate: {self.drop_rate * 100:.2f}%"
                )
            except asyncio.QueueEmpty:
                pass
        
        try:
            self._queue.put_nowait(item)
            return True
        except asyncio.QueueFull:
            return False

    async def get(self):
        return await self._queue.get()

    def task_done(self):
        self._queue.task_done()

    def clear(self):
        """
        Drains all items from the queue and resets metrics.
        """
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except (asyncio.QueueEmpty, ValueError):
                break
        self._overflow_count = 0
        self._total_pushed = 0
