import asyncio
import logging
import time

from errors import CrawlerError, NetworkError, TransientError

logger = logging.getLogger(__name__)


class RetryStrategy:
    def __init__(
        self, max_retries=3, backoff_factor=2.0, retry_on: list | None = None, base_delay=1.0
    ) -> None:
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor
        self._base_delay = base_delay
        self._errors_by_type = {}
        self._successful_retries = 0
        self._retry_times = []
        self._permanent_failures = []

        if retry_on is None:
            retry_on = [TransientError, NetworkError]
        self._retry_on = tuple(retry_on)

    async def execute_with_retry(self, coro_fn, *args, **kwargs):
        started = time.monotonic()
        for attempt in range(self._max_retries + 1):
            try:
                result = await coro_fn(*args, attempt=attempt, **kwargs)
            except CrawlerError as e:
                err_type = type(e).__name__
                self._errors_by_type[err_type] = self._errors_by_type.get(err_type, 0) + 1
                if not isinstance(e, self._retry_on) or attempt == self._max_retries:
                    self._permanent_failures.append(e.url)
                    logger.error("gave up on %s: %s", e.url, e)
                    raise
                wait = self._base_delay * self._backoff_factor**attempt
                logger.warning(
                    "%s on %s, retry %d/%d in %.1fs",
                    err_type,
                    e.url,
                    attempt + 1,
                    self._max_retries,
                    wait,
                )
                await asyncio.sleep(wait)
                continue
            if attempt > 0:
                self._successful_retries += 1
                self._retry_times.append(time.monotonic() - started)
                logger.info("recovered on attempt %d", attempt + 1)
            return result

    def get_stats(self) -> dict:
        avg = sum(self._retry_times) / len(self._retry_times) if self._retry_times else 0.0
        return {
            "errors_by_type": dict(self._errors_by_type),
            "successful_retries": self._successful_retries,
            "avg_retry_time": round(avg, 2),
            "permanent_failures": list(self._permanent_failures),
        }
