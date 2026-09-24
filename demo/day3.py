import asyncio
import json
import logging

from crawler import AsyncCrawler
from parser import HTMLParser

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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