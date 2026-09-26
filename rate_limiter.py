import asyncio
import random
import time


class RateLimiter:
    def __init__(
        self, requests_per_second: float = 1.0, per_domain=True, min_delay=0.0, jitter=0.0
    ) -> None:
        self._interval = max(1 / requests_per_second, min_delay)
        self._requests_per_second = requests_per_second
        self._per_domain = per_domain
        self._next_slot = {}
        self._jitter = jitter
        self._delays = []

    async def acquire(self, domain=None, min_interval: float = 0.0):
        key = domain if self._per_domain else None
        now = time.monotonic()
        slot = max(now, self._next_slot.get(key, now))
        step = max(self._interval, min_interval) + random.uniform(0, self._jitter)
        self._delays.append(step)
        self._next_slot[key] = slot + step
        if slot > now:
            await asyncio.sleep(slot - now)

    @property
    def avg_delay(self) -> float:
        if not self._delays:
            return 0.0
        return sum(self._delays) / len(self._delays)
