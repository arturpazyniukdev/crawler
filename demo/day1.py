import asyncio
import logging
import time

from crawler import AsyncCrawler
from parser import HTMLParser

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


async def main():
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
    print(round(time.perf_counter() - start, 2))


async def concurrent():
    crawler = AsyncCrawler(max_concurrent=5, html_parser=HTMLParser())
    start = time.perf_counter()
    await crawler.fetch_urls([f"https://httpbin.org/delay/2?i={i}" for i in range(5)])
    print("concurrent: ", round(time.perf_counter() - start, 2))
    await crawler.close()


async def sequential():
    crawler = AsyncCrawler(max_concurrent=5, html_parser=HTMLParser())
    urls = [f"https://httpbin.org/delay/2?i={i}" for i in range(5)]
    start = time.perf_counter()
    for url in urls:
        await crawler.fetch_url(url)
    print("sequential: ", round(time.perf_counter() - start, 2))
    await crawler.close()


async def compare():
    await concurrent()
    await sequential()


if __name__ == "__main__":
    asyncio.run(main())
    # asyncio.run(compare())
