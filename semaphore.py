import asyncio
from contextlib import asynccontextmanager
from urllib.parse import urlparse


class SemaphoreManager:
    def __init__(self, max_concurrent=10, per_domain=5) -> None:
        self._global = asyncio.Semaphore(max_concurrent)
        self._domains = {}
        self._active = 0
        self._per_domain = per_domain

    def _for_domain(self, domain):
        return self._domains.setdefault(domain, asyncio.Semaphore(self._per_domain))

    @property
    def active(self):
        return self._active

    @asynccontextmanager
    async def acquire(self, url):
        domain = urlparse(url).hostname
        async with self._global, self._for_domain(domain):
            self._active += 1
            try:
                yield
            finally:
                self._active -= 1
