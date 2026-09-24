import asyncio
import contextlib
import json
import logging
import time
from contextlib import asynccontextmanager
from urllib.parse import urldefrag, urljoin, urlparse, urlunparse

from aiohttp import ClientError, ClientResponseError, ClientSession, ClientTimeout, TCPConnector
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def normalize_url(url: str) -> str:
    p = urlparse(url)
    return urlunparse(p._replace(netloc=p.netloc.lower(), path=p.path or "/"))


class HTMLParser:
    def __init__(self) -> None:
        pass

    def _extract_metadata(self, soup: BeautifulSoup):
        title_tag = soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else None

        description_tag = soup.find("meta", attrs={"name": "description"})
        description = description_tag.get("content") if description_tag else None

        keywords_tag = soup.find("meta", attrs={"name": "keywords"})
        keywords = keywords_tag.get("content") if keywords_tag else None

        res = {}

        if title is not None:
            res["title"] = title

        if description is not None:
            res["description"] = description

        if keywords is not None:
            res["keywords"] = keywords

        return res

    def _extract_links(self, soup: BeautifulSoup, url: str):
        anchor_tags = soup.find_all("a")
        links = []
        for a in anchor_tags:
            href = a.get("href")
            if not href:
                continue
            absolute = urljoin(url, href)
            absolute, _ = urldefrag(absolute)
            if urlparse(absolute).scheme not in ("http", "https"):
                continue
            links.append(normalize_url(absolute))

        return list(dict.fromkeys(links))

    def _extract_text(self, soup: BeautifulSoup, selector=None):
        node = soup.select_one(selector) if selector else soup
        if node is None:
            logger.warning("text extraction failed, node is not found")
            return ""
        return node.get_text(separator=" ", strip=True)

    def _extract_images(self, soup: BeautifulSoup):
        img_tags = soup.find_all("img")
        imgs = []
        for t in img_tags:
            imgs.append({"src": t.get("src"), "alt": t.get("alt")})
        return imgs

    def _extract_headers(self, soup: BeautifulSoup):
        h1 = soup.find_all("h1")
        h2 = soup.find_all("h2")
        h3 = soup.find_all("h3")

        res = {}

        if len(h1) > 0:
            res["h1"] = [i.get_text(" ", strip=True) for i in h1]

        if len(h2) > 0:
            res["h2"] = [i.get_text(" ", strip=True) for i in h2]

        if len(h3) > 0:
            res["h3"] = [i.get_text(" ", strip=True) for i in h3]

        return res

    def _extract_lists(self, soup: BeautifulSoup):
        lists = soup.find_all(["ul", "ol"])
        res = []

        for item in lists:
            parents = item.find_parent(["ul", "ol"])
            if parents is not None:
                continue
            list_items = item.find_all("li", recursive=False)
            res.append(
                {"type": item.name, "items": [li.get_text(" ", strip=True) for li in list_items]}
            )

        return res

    def _extract_tables(self, soup: BeautifulSoup):
        tables = []
        table_tags = soup.find_all("table")
        for tag in table_tags:
            parents = tag.find_parent("table")
            if parents is not None:
                continue
            headers = []
            header_row = None
            thead = tag.find("thead")
            if thead is not None:
                header_row = thead.find("tr")
            if header_row is not None:
                for cell in header_row.find_all(["td", "th"], recursive=False):
                    headers.append(cell.get_text(" ", strip=True))
            rows = []
            for tr in tag.find_all("tr"):
                if tr is header_row:
                    continue
                if tag is not tr.find_parent("table"):
                    continue
                row = []
                for cell in tr.find_all(["td", "th"], recursive=False):
                    row.append(cell.get_text(" ", strip=True))
                rows.append(row)
            tables.append({"headers": headers, "rows": rows})
        return tables

    async def parse_html(self, html: str, url: str) -> dict:
        soup = BeautifulSoup(html, "lxml")

        text = ""
        try:
            text = self._extract_text(soup)
        except Exception:
            logger.warning("cannot extract text", exc_info=True)

        links = []

        try:
            links = self._extract_links(soup, url)
        except Exception:
            logger.warning("cannot extract links", exc_info=True)

        images = []

        try:
            images = self._extract_images(soup)
        except Exception:
            logger.warning("cannot extract images", exc_info=True)

        metadata = {}

        try:
            metadata = self._extract_metadata(soup)
        except Exception:
            logger.warning("cannot extract metadata", exc_info=True)

        headers = {}
        try:
            headers = self._extract_headers(soup)
        except Exception:
            logger.warning("cannot extract headers", exc_info=True)

        return {
            "url": url,
            "title": metadata.get("title"),
            "text": text,
            "links": links,
            "images": images,
            "metadata": metadata,
            "headers": headers,
            "statistics": {
                "text_length": len(text),
                "links_count": len(links),
                "images_count": len(images),
            },
        }


class AsyncCrawler:
    def __init__(
        self, html_parser: HTMLParser, max_concurrent: int = 10, per_domain=5, max_depth=1
    ):
        self._max_concurrent = max_concurrent
        self._session = None
        self._html_parser = html_parser
        self._semaphores = SemaphoreManager(self._max_concurrent, per_domain)
        self._only_domains = None
        self._exclude_patterns = None
        self._include_patterns = None
        self._max_depth = max_depth

    async def fetch_url(self, url: str) -> (None | str) | (str | None):
        if self._session is None:
            connector = TCPConnector(limit=self._max_concurrent, limit_per_host=10)
            timeout = ClientTimeout(connect=5, sock_read=10)
            self._session = ClientSession(
                timeout=timeout,
                connector=connector,
            )

        error = None

        try:
            logger.info(f"loading started for {url}")
            async with self._session.get(url) as resp:
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

    async def fetch_urls(self, urls: list[str]) -> dict[str, str | None]:
        coros = [self.fetch_url(url) for url in urls]
        results = await asyncio.gather(*coros)
        return dict(zip(urls, results, strict=True))

    async def fetch_and_parse(self, url: str) -> dict:
        html, error = await self.fetch_url(url)
        if html is None:
            return {"status": "error", "error": error, "url": url}
        parsed = await self._html_parser.parse_html(html, url)
        return {"status": "success", "data": parsed}

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
                    "rate": f"{round(rate, 2)} websites per second",
                }
            )

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
                async with self._semaphores.acquire(url):
                    res = await self.fetch_and_parse(url)
                if (
                    "data" in res
                    and res["data"] is not None
                    and "links" in res["data"]
                    and len(res["data"]["links"]) > 0
                    and depth < self._max_depth
                ):
                    new_links = [
                        i
                        for i in res["data"]["links"]
                        if not self._queue.is_known(i)
                        and self._is_domain_allowed(i)
                        and self._is_url_allowed(i)
                    ]
                    for n in new_links:
                        self._queue.add_url(n, priority=depth, depth=depth + 1)
                if res["status"] == "success":
                    self._queue.mark_processed(url, res)
                elif res["status"] == "error":
                    self._queue.mark_failed(url, res["error"])
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
        }

    async def close(self):
        if isinstance(self._session, ClientSession):
            await self._session.close()
            self._session = None


class CrawlerQueue:
    def __init__(self) -> None:
        self._in_flight = 0
        self._urls = []
        self._enqueued_urls = set()
        self._failed_urls = {}
        self._processed_urls = {}
        self._depths = {}

    def add_url(self, url, priority=0, depth=0):
        if url in self._enqueued_urls:
            return
        self._depths[url] = depth
        self._urls.append((priority, url))
        self._enqueued_urls.add(url)

    async def get_next(self):
        if len(self._urls) == 0:
            return None
        self._in_flight += 1
        self._urls.sort(key=lambda t: t[0])
        return self._urls.pop(0)[1]

    def task_done(self):
        self._in_flight -= 1

    def is_drained(self):
        return len(self._urls) == 0 and self._in_flight == 0

    def mark_processed(self, url, result):
        self._processed_urls[url] = result

    def mark_failed(self, url, error):
        self._failed_urls[url] = error

    def depth_of(self, url):
        return self._depths[url]

    def claimed_count(self):
        return len(self._processed_urls) + len(self._failed_urls) + self._in_flight

    def is_known(self, url):
        return url in self._enqueued_urls

    def get_stats(self):
        return {
            "queued": len(self._urls),
            "in_flight": self._in_flight,
            "processed": len(self._processed_urls),
            "failed": len(self._failed_urls),
        }


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


async def crawler_showcase():
    crawler = AsyncCrawler(max_concurrent=5, html_parser=HTMLParser())
    urls = [
        "https://example.com",
        "https://www.python.org",
        "https://docs.python.org/3/",
        "https://httpbin.org/html",
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/2",
        "https://httpbin.org/status/404",  # ClientResponseError
        "https://httpbin.org/status/500",  # ClientResponseError
        "https://nonexistent-domain-xyz-12345.com",  # ClientConnectorDNSError
        "https://httpbin.org/delay/15",  # TimeoutError (sock_read=10)
    ]
    start = time.perf_counter()
    results = await crawler.fetch_urls(urls)
    await crawler.close()
    print(f"Loaded {len([v for v in results.values() if v is not None])} pages")
    print(time.perf_counter() - start)


async def concurrent():
    crawler = AsyncCrawler(max_concurrent=5, html_parser=HTMLParser())
    start = time.perf_counter()
    await crawler.fetch_urls([f"https://httpbin.org/delay/2?i={i}" for i in range(5)])
    print("concurrent: ", time.perf_counter() - start)
    await crawler.close()


async def sequential():
    crawler = AsyncCrawler(max_concurrent=5, html_parser=HTMLParser())
    urls = [f"https://httpbin.org/delay/2?i={i}" for i in range(5)]
    start = time.perf_counter()
    for url in urls:
        await crawler.fetch_url(url)
    print("sequential: ", time.perf_counter() - start)
    await crawler.close()


async def compare():
    await concurrent()
    await sequential()


async def day2_demo():
    crawler = AsyncCrawler(max_concurrent=5, html_parser=HTMLParser())
    urls = [
        "https://httpbin.org/status/404",
        "https://www.python.org",
        "https://docs.python.org/3/library/stdtypes.html",
        "https://www.iana.org/domains/reserved",
        "https://developer.mozilla.org/en-US/docs/Web/HTML",
        "https://httpbin.org/html",
    ]

    result = await crawler.fetch_and_parse_many(urls)

    await crawler.close()
    return result


async def main():
    crawler = AsyncCrawler(max_concurrent=5, max_depth=2, html_parser=HTMLParser())
    urls = [
        "https://www.python.org",
        "https://books.toscrape.com/",
        "https://docs.python.org/3/library/stdtypes.html",
        "https://www.iana.org/domains/reserved",
        "https://developer.mozilla.org/en-US/docs/Web/HTML",
        "https://httpbin.org/html",
        "https://www.google.com/",
    ]

    try:
        result = await crawler.crawl(urls, max_pages=100, exclude_patterns=["intl"])
    finally:
        await crawler.close()
    return result


if __name__ == "__main__":
    result = asyncio.run(main())
    with open("results.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
