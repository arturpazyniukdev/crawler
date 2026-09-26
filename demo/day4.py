import asyncio
import logging

from crawler import AsyncCrawler

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

ROBOTS_REASON = "blocked by robots.txt"


async def run(name: str, crawler: AsyncCrawler, urls: list[str], **crawl_kwargs) -> None:
    print(f"\n=== {name} ===")
    try:
        result = await crawler.crawl(urls, **crawl_kwargs)
    finally:
        await crawler.close()

    failed = result["failed"]
    blocked = [u for u, reason in failed.items() if reason == ROBOTS_REASON]
    errors = {u: reason for u, reason in failed.items() if reason != ROBOTS_REASON}

    print(f"processed : {len(result['processed'])}")
    print(f"blocked   : {result.get('blocked', len(blocked))}")
    print(f"errors    : {len(errors)}")
    print(f"avg delay : {result.get('avg_delay', 0.0):.2f}s")
    for url in blocked:
        print(f"  BLOCKED {url}")
    for url, reason in errors.items():
        print(f"  FAILED  {reason}: {url}")


async def main() -> None:
    await run(
        "Wikipedia: robots.txt + backoff on 429",
        AsyncCrawler(
            max_concurrent=3,
            requests_per_second=2.0,
            respect_robots=True,
            user_agent="MyBot/1.0",
        ),
        ["https://en.wikipedia.org/wiki/Python_(programming_language)"],
        max_pages=15,
        same_domain_only=True,
    )

    await run(
        "books.toscrape: min_delay + jitter",
        AsyncCrawler(
            max_concurrent=5,
            requests_per_second=2.0,
            respect_robots=True,
            min_delay=0.5,
            jitter=0.3,
            user_agent="MyBot/1.0",
        ),
        ["https://books.toscrape.com/"],
        max_pages=10,
    )


if __name__ == "__main__":
    asyncio.run(main())
