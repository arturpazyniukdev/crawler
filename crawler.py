import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlparse

from aiohttp import ClientError, ClientResponseError, ClientSession, ClientTimeout, TCPConnector

from crawl_queue import CrawlerQueue
from errors import CrawlerError, NetworkError, ParseError, PermanentError, TransientError
from parser import HTMLParser
from rate_limiter import RateLimiter
from retry import RetryStrategy
from robots import RobotsParser
from semaphore import SemaphoreManager
from storage import DataStorage
from urls import normalize_url

logger = logging.getLogger(__name__)


@dataclass
class FetchResult:
    html: str
    status: int
    content_type: str


class AsyncCrawler:
    def __init__(
        self,
        html_parser: HTMLParser | None = None,
        max_concurrent: int = 10,
        per_domain=5,
        max_depth=1,
        requests_per_second=1.0,
        respect_robots=True,
        rate_per_domain=True,
        user_agent="MyBot/1.0",
        min_delay=0.0,
        jitter=0.0,
        retry_strategy: RetryStrategy | None = None,
        connect_timeout=5.0,
        read_timeout=10.0,
        total_timeout=30.0,
        storage: DataStorage | None = None,
    ):
        self._max_concurrent = max_concurrent
        self._session = None
        self._html_parser = html_parser or HTMLParser()
        self._semaphores = SemaphoreManager(self._max_concurrent, per_domain)
        self._only_domains = None
        self._exclude_patterns = None
        self._include_patterns = None
        self._max_depth = max_depth
        self._rate_limiter = RateLimiter(requests_per_second, rate_per_domain, min_delay, jitter)
        self._respect_robots = respect_robots
        self._user_agent = user_agent
        self._robots = None
        self._blocked = 0
        self._retry_strategy = retry_strategy or RetryStrategy()
        self._timeouts = (connect_timeout, read_timeout, total_timeout)
        self._storage = storage
        self._save_failures = 0

    async def _get_session(self) -> ClientSession:
        if self._session is None:
            connector = TCPConnector(limit=self._max_concurrent, limit_per_host=10)
            headers = {"User-Agent": self._user_agent}
            self._session = ClientSession(
                connector=connector,
                headers=headers,
            )
            self._robots = RobotsParser(self._session)
        return self._session

    async def _fetch_response(self, url: str, attempt: int = 0) -> FetchResult:
        session = await self._get_session()
        connect, read, total = self._timeouts
        scale = 1 + attempt
        timeout = ClientTimeout(
            connect=connect * scale, sock_read=read * scale, total=total * scale
        )

        try:
            logger.info(f"loading started for {url}")
            async with session.get(url, timeout=timeout) as resp:
                resp.raise_for_status()
                html = await resp.text()
                status = resp.status
                content_type = resp.headers.get("Content-Type", "")
                logger.info(f"successfully finished for {url}, status {resp.status}")
        except ClientResponseError as e:
            status = e.status
            if status in (429, 500, 502, 503, 504):
                raise TransientError(url, f"HTTP {status}", status) from e
            raise PermanentError(url, f"HTTP {status}", status) from e
        except TimeoutError as e:
            raise TransientError(url, "timeout") from e
        except ClientError as e:
            raise NetworkError(url, repr(e)) from e

        return FetchResult(html, status, content_type)

    async def fetch_url(self, url: str, attempt: int = 0) -> str:
        return (await self._fetch_response(url, attempt)).html

    async def fetch_urls(self, urls: list[str]) -> dict[str, str | BaseException]:
        coros = [self.fetch_url(url) for url in urls]
        results = await asyncio.gather(*coros, return_exceptions=True)
        return dict(zip(urls, results, strict=True))

    async def fetch_and_parse(self, url: str, attempt=0) -> dict:
        fetched = await self._fetch_response(url, attempt)
        try:
            page = await self._html_parser.parse_html(fetched.html, url)
        except Exception as e:
            raise ParseError(url, repr(e)) from e
        page["status_code"] = fetched.status
        page["content_type"] = fetched.content_type
        page["crawled_at"] = datetime.now(UTC)
        return page

    async def fetch_and_parse_many(self, urls):
        coros = [self.fetch_and_parse(url) for url in urls]
        results = await asyncio.gather(*coros, return_exceptions=True)
        return dict(zip(urls, results, strict=True))

    def _is_domain_allowed(self, url):
        return not (
            self._only_domains is not None and urlparse(url).hostname not in self._only_domains
        )

    def _is_url_allowed(self, url):
        if self._exclude_patterns and any(p in url for p in self._exclude_patterns):
            return False
        if self._include_patterns and not any(p in url for p in self._include_patterns):
            return False
        return True

    async def _report_progress(self):
        start = time.perf_counter()
        prev = 0
        while True:
            await asyncio.sleep(1)
            stats = self._queue.get_stats()
            elapsed = time.perf_counter() - start
            rate = stats["processed"] / elapsed
            logger.info(
                {
                    "processed": stats["processed"],
                    "queued": stats["queued"],
                    "failed": stats["failed"],
                    "blocked": self._blocked,
                    "rate": f"{round(rate, 2)} websites per second",
                    "current_rps": stats["processed"] - prev,
                    "avg_delay": self._rate_limiter.avg_delay,
                }
            )
            prev = stats["processed"]

    async def _fetch_one(self, url, attempt=0):
        delay = self._robots.get_crawl_delay(url, self._user_agent) if self._respect_robots else 0.0
        await self._rate_limiter.acquire(urlparse(url).hostname, delay)
        async with self._semaphores.acquire(url):
            return await self.fetch_and_parse(url, attempt)

    async def _save(self, page: dict) -> None:
        if self._storage is None:
            return
        for attempt in range(3):
            try:
                await self._storage.save(page)
                return
            except Exception:
                logger.warning(
                    "save failed for %s, attempt %d", page["url"], attempt + 1, exc_info=True
                )
                await asyncio.sleep(0.1 * 2**attempt)
        logger.error("giving up saving %s", page["url"])
        self._save_failures += 1

    async def _worker(self, max_pages):
        while True:
            if self._queue.claimed_count() >= max_pages:
                break
            url = await self._queue.get_next()

            if url is None:
                if self._queue.is_drained():
                    break
                await asyncio.sleep(0.05)
                continue

            depth = self._queue.depth_of(url)
            try:
                if self._respect_robots:
                    await self._get_session()
                    await self._robots.fetch_robots(url)
                    if not self._robots.can_fetch(url, self._user_agent):
                        self._blocked += 1
                        self._queue.mark_failed(url, "blocked by robots.txt")
                        logger.warning("blocked by robots.txt: %s", url)
                        continue

                try:
                    res = await self._retry_strategy.execute_with_retry(self._fetch_one, url)
                except CrawlerError as e:
                    self._queue.mark_failed(url, f"{type(e).__name__}: {e}")
                    continue

                links = res.get("links", [])
                if links and depth < self._max_depth:
                    new_links = [
                        i
                        for i in links
                        if not self._queue.is_known(i)
                        and self._is_domain_allowed(i)
                        and self._is_url_allowed(i)
                    ]
                    for n in new_links:
                        self._queue.add_url(n, priority=depth, depth=depth + 1)
                self._queue.mark_processed(url, res)
                await self._save(res)
            finally:
                self._queue.task_done()

    async def crawl(
        self,
        start_urls,
        max_pages=50,
        same_domain_only=False,
        exclude_patterns=None,
        include_patterns=None,
    ):
        self._queue = CrawlerQueue()
        self._blocked = 0
        self._save_failures = 0
        self._exclude_patterns = exclude_patterns
        self._include_patterns = include_patterns
        normalized = [normalize_url(el) for el in start_urls]
        self._only_domains = (
            {urlparse(u).hostname for u in normalized} if same_domain_only else None
        )
        for i in normalized:
            self._queue.add_url(i, priority=-1)

        coros = [self._worker(max_pages) for _ in range(self._max_concurrent)]
        reporter = asyncio.create_task(self._report_progress())
        try:
            await asyncio.gather(*coros)
        finally:
            reporter.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reporter

        return {
            "processed": self._queue._processed_urls,
            "failed": self._queue._failed_urls,
            "visited": list(self._queue._processed_urls.keys() | self._queue._failed_urls.keys()),
            "blocked": self._blocked,
            "avg_delay": round(self._rate_limiter.avg_delay, 2),
            "errors": self._retry_strategy.get_stats(),
            "save_failures": self._save_failures,
        }

    async def close(self):
        if isinstance(self._session, ClientSession):
            await self._session.close()
            self._session = None
        if self._storage is not None:
            await self._storage.close()
