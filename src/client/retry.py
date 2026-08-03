import asyncio
import random
import functools
import structlog
from typing import TypeVar, Callable, Any, Optional, Type

logger = structlog.get_logger()

T = TypeVar('T')

class RetryConfig:
    """Configuration for retry behavior."""
    def __init__(self, max_retries: int = 3, base_delay: float = 0.1,
                 max_delay: float = 5.0, exponential_base: float = 2.0,
                 jitter: bool = True, retryable_exceptions: tuple = (Exception,)):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay with exponential backoff and optional jitter."""
        delay = self.base_delay * (self.exponential_base ** attempt)
        delay = min(delay, self.max_delay)
        if self.jitter:
            delay = delay * (0.5 + random.random())  # Add jitter between 50-150%
        return delay

async def retry_async(func: Callable, config: RetryConfig = None, *args, **kwargs) -> Any:
    """Execute an async function with retry logic."""
    if config is None:
        config = RetryConfig()
        
    attempt = 0
    while True:
        try:
            return await func(*args, **kwargs)
        except config.retryable_exceptions as e:
            attempt += 1
            if attempt > config.max_retries:
                logger.error("max_retries_exceeded", func=func.__name__, attempts=attempt, error=str(e))
                raise
            
            delay = config.calculate_delay(attempt)
            logger.warning("retry_attempt", func=func.__name__, attempt=attempt, 
                           delay=delay, error=str(e))
            await asyncio.sleep(delay)

def retry_decorator(config: RetryConfig = None):
    """Decorator for adding retry logic to async functions."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            return await retry_async(func, config, *args, **kwargs)
        return wrapper
    return decorator
