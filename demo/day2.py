import asyncio
import json
import logging

from crawler import AsyncCrawler
from parser import HTMLParser

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


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
