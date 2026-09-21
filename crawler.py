import asyncio
import time

from aiohttp import ClientError, ClientResponseError, ClientSession, ClientTimeout, TCPConnector


class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10):
        self._max_concurrent = max_concurrent
        self._session = None

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
            print(f"loading started for {url}")
            async with self._session.get(url) as resp:
                resp.raise_for_status()
                html = await resp.text()
                print(f"successfully finished for {url}, status {resp.status}")
        except ClientResponseError:
            error = "ClientResponseError"
        except TimeoutError:
            error = "TimeoutError"
        except ClientError:
            error = "ClientError"
        except Exception:
            error = "Other Error"

        if error:
            print(f"failed with {error} for {url}")
            return None

        return html

    async def fetch_urls(self, urls: list[str]) -> dict[str, str | None]:
        coros = [self.fetch_url(url) for url in urls]
        results = await asyncio.gather(*coros)
        return dict(zip(urls, results, strict=True))

    async def close(self):
        if isinstance(self._session, ClientSession):
            await self._session.close()
            self._session = None


async def main():
    crawler = AsyncCrawler(max_concurrent=5)
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
    crawler = AsyncCrawler(max_concurrent=5)
    start = time.perf_counter()
    await crawler.fetch_urls([f"https://httpbin.org/delay/2?i={i}" for i in range(5)])
    print("concurrent: ", time.perf_counter() - start)
    await crawler.close()


async def sequential():
    crawler = AsyncCrawler(max_concurrent=5)
    urls = [f"https://httpbin.org/delay/2?i={i}" for i in range(5)]
    start = time.perf_counter()
    for url in urls:
        await crawler.fetch_url(url)
    print("sequential: ", time.perf_counter() - start)
    await crawler.close()


async def compare():
    await concurrent()
    await sequential()


if __name__ == "__main__":
    asyncio.run(main())
    # asyncio.run(compare())
