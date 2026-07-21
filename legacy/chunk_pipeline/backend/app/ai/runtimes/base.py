from abc import ABC, abstractmethod
from typing import AsyncIterator
from ..types import RuntimeRequest, TranslationResult

class BaseRuntime(ABC):
    """
    Abstract Base Class representing a local inference engine runtime.
    """
    def __init__(self, config):
        self.config = config

    @abstractmethod
    async def initialize(self) -> None:
        """
        Initializes backend dependencies, verifies model/service, and runs warmup.
        Fails loudly if setup verification fails.
        """
        pass

    @abstractmethod
    async def is_ready(self) -> bool:
        """
        Performs a lightweight health check to verify backend operational readiness.
        """
        pass

    @abstractmethod
    async def stream_generate(self, request: RuntimeRequest) -> AsyncIterator[TranslationResult]:
        """
        Streams generated output tokens from the model wrapped in TranslationResults.
        """
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        """
        Teardown routine for cleaning up sockets, background threads, and model weights.
        """
        pass
