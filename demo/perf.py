import asyncio
import sys
import time
import tracemalloc
import urllib.request

from bs4 import BeautifulSoup

from crawler import AsyncCrawler

START = "https://books.toscrape.com/"


async def run_async(n: int) -> tuple[float, float, list[str]]:
    crawler = AsyncCrawler(
        max_concurrent=20,
        per_domain=20,
        max_depth=3,
        requests_per_second=100,
        respect_robots=False,
    )
    tracemalloc.start()
    t0 = time.perf_counter()
    try:
        result = await crawler.crawl([START], max_pages=n, same_domain_only=True)
    finally:
        await crawler.close()
    elapsed = time.perf_counter() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return elapsed, peak / 1e6, result["visited"]


def run_sync(urls: list[str]) -> tuple[float, float]:
    tracemalloc.start()
    t0 = time.perf_counter()
    for url in urls:
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                html = resp.read()
        except OSError:
            continue
        soup = BeautifulSoup(html, "html.parser")
        _ = soup.title, soup.find_all("a")
    elapsed = time.perf_counter() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return elapsed, peak / 1e6


async def main() -> None:
    sizes = [int(a) for a in sys.argv[1:]] or [50, 100]
    print(f"{'pages':>6} {'async s':>8} {'pages/s':>8} {'MB':>6}", end=" ")
    print(f"{'sync s':>8} {'MB':>6} {'speedup':>8}")
    for i, n in enumerate(sizes):
        a_time, a_mem, urls = await run_async(n)
        if i == 0:
            s_time, s_mem = run_sync(urls)
            sync_cols = f"{s_time:8.1f} {s_mem:6.1f} {s_time / a_time:7.1f}x"
        else:
            sync_cols = f"{'-':>8} {'-':>6} {'-':>8}"
        print(f"{len(urls):>6} {a_time:8.1f} {len(urls) / a_time:8.1f} {a_mem:6.1f} {sync_cols}")


if __name__ == "__main__":
    asyncio.run(main())
