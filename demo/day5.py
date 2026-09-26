import asyncio
import json
import logging
from pathlib import Path

from crawler import AsyncCrawler
from retry import RetryStrategy

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

REPORT_PATH = Path(__file__).with_name("day5_report.json")

URLS = [
    "https://books.toscrape.com/",  # ok, has links
    "https://httpbin.org/status/503",  # transient -> retried, then gives up
    "https://httpbin.org/status/429",  # transient -> retried
    "https://httpbin.org/status/404",  # permanent -> no retry
    "https://httpbin.org/status/403",  # permanent -> no retry
    "https://httpbin.org/delay/1",  # timeout at 3s, recovers when timeout grows
    "https://no-such-host.invalid/",  # network error -> retried
]


async def main() -> None:
    retry_strategy = RetryStrategy(max_retries=2, backoff_factor=2.0, base_delay=0.5)
    crawler = AsyncCrawler(
        max_concurrent=4,
        requests_per_second=4.0,
        respect_robots=False,
        read_timeout=3.0,
        total_timeout=3.0,
        retry_strategy=retry_strategy,
    )

    print("=== crawl with mixed errors ===")
    try:
        result = await crawler.crawl(URLS, max_pages=len(URLS))
    finally:
        await crawler.close()

    errors = result["errors"]
    print(f"processed          : {len(result['processed'])}")
    print(f"failed             : {len(result['failed'])}")
    print(f"successful retries : {errors['successful_retries']}")
    print(f"avg retry time     : {errors['avg_retry_time']}s")
    print("errors by type     :")
    for name, count in sorted(errors["errors_by_type"].items()):
        print(f"  {name:<15} {count}")
    print("permanent failures :")
    for url in errors["permanent_failures"]:
        print(f"  {result['failed'].get(url, '?')}: {url}")

    report = {"errors": errors, "failed": result["failed"]}
    REPORT_PATH.write_text(json.dumps(report, indent=2))
    print(f"\nreport saved to {REPORT_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
