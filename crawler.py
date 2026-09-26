import asyncio
import contextlib
import logging
import time
from urllib.parse import urlparse

from aiohttp import ClientError, ClientResponseError, ClientSession, ClientTimeout, TCPConnector

from crawl_queue import CrawlerQueue
from parser import HTMLParser
from rate_limiter import RateLimiter
from robots import RobotsParser
from semaphore import SemaphoreManager
from urls import normalize_url

logger = logging.getLogger(__name__)


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
        max_retries=3,
        backoff_base=1.0,
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
        self._max_retries = max_retries
        self._backoff_base = backoff_base

    async def _get_session(self) -> ClientSession:
        if self._session is None:
            connector = TCPConnector(limit=self._max_concurrent, limit_per_host=10)
            timeout = ClientTimeout(connect=5, sock_read=10)
            headers = {"User-Agent": self._user_agent}
            self._session = ClientSession(
                timeout=timeout,
                connector=connector,
                headers=headers,
            )
            self._robots = RobotsParser(self._session)
        return self._session

    async def _fetch(self, url: str) -> (None | str) | (str | None):
        session = await self._get_session()

        error = None

        try:
            logger.info(f"loading started for {url}")
            async with session.get(url) as resp:
                resp.raise_for_status()
                html = await resp.text()
                logger.info(f"successfully finished for {url}, status {resp.status}")
        except ClientResponseError:
            error = "ClientResponseError"
        except TimeoutError:
            error = "TimeoutError"
        except ClientError:
            error = "ClientError"
        except Exception:
            error = "Other Error"

        if error:
            logger.warning("failed with %s for %s", error, url)
            return (None, error)

        return (html, None)

    async def fetch_url(self, url: str) -> str | None:
        html, _ = await self._fetch(url)
        return html

    async def fetch_urls(self, urls: list[str]) -> dict[str, str | None]:
        coros = [self.fetch_url(url) for url in urls]
        results = await asyncio.gather(*coros)
        return dict(zip(urls, results, strict=True))

    async def fetch_and_parse(self, url: str) -> dict:
        html, error = await self._fetch(url)
        if html is None:
            return {"error": error, "url": url}
        parsed = await self._html_parser.parse_html(html, url)
        return parsed

    async def fetch_and_parse_many(self, urls):
        coros = [self.fetch_and_parse(url) for url in urls]
        results = await asyncio.gather(*coros)
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

                delay = 0.0
                if self._respect_robots:
                    delay = self._robots.get_crawl_delay(url, self._user_agent)
                await self._rate_limiter.acquire(urlparse(url).hostname, delay)
                async with self._semaphores.acquire(url):
                    res = await self.fetch_and_parse(url)

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
                if "error" in res:
                    attempt = self._queue.attempt_of(url)
                    if attempt < self._max_retries:
                        wait = self._backoff_base * 2**attempt
                        logger.warning("retry %d for %s in %.1fs", attempt + 1, url, wait)
                        await asyncio.sleep(wait)
                        self._queue.requeue(url, depth + 10)
                    else:
                        self._queue.mark_failed(url, res["error"])
                else:
                    self._queue.mark_processed(url, res)
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
        }

    async def close(self):
        if isinstance(self._session, ClientSession):
            await self._session.close()
            self._session = None
