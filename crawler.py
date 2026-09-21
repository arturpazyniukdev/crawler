import asyncio
import json
import logging
import time
from urllib.parse import urldefrag, urljoin, urlparse

from aiohttp import ClientError, ClientResponseError, ClientSession, ClientTimeout, TCPConnector
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


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
            links.append(absolute)

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
        self,
        html_parser: HTMLParser,
        max_concurrent: int = 10,
    ):
        self._max_concurrent = max_concurrent
        self._session = None
        self._html_parser = html_parser

    async def fetch_url(self, url: str) -> str | None:
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
            return None

        return html

    async def fetch_urls(self, urls: list[str]) -> dict[str, str | None]:
        coros = [self.fetch_url(url) for url in urls]
        results = await asyncio.gather(*coros)
        return dict(zip(urls, results, strict=True))

    async def fetch_and_parse(self, url: str) -> dict:
        html = await self.fetch_url(url)
        if html is None:
            return {"status": "error", "data": None, "url": url}
        parsed = await self._html_parser.parse_html(html, url)
        return {"status": "success", "data": parsed}

    async def fetch_and_parse_many(self, urls):
        coros = [self.fetch_and_parse(url) for url in urls]
        results = await asyncio.gather(*coros)
        return dict(zip(urls, results, strict=True))

    async def close(self):
        if isinstance(self._session, ClientSession):
            await self._session.close()
            self._session = None


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


async def main():
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


if __name__ == "__main__":
    result = asyncio.run(main())
    with open("results.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
