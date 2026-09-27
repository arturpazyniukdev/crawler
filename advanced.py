import logging
from pathlib import Path

from aiohttp import ClientSession

from config import CrawlerConfig
from crawler import AsyncCrawler
from logging_setup import setup_logging
from sitemap import SitemapParser
from stats import CrawlerStats
from storage import CSVStorage, DataStorage, JSONStorage, SQLiteStorage

logger = logging.getLogger(__name__)


def build_storage(kind: str, path: str) -> DataStorage | None:
    if kind == "json":
        return JSONStorage(path)
    if kind == "csv":
        return CSVStorage(path)
    if kind == "sqlite":
        return SQLiteStorage(path)
    if kind == "none":
        return None

    raise ValueError(f"unknown storage_type: {kind}")


class AdvancedCrawler:
    def __init__(self, config: CrawlerConfig) -> None:
        self._config = config
        Path(config.storage_path).parent.mkdir(parents=True, exist_ok=True)
        if config.log_file:
            Path(config.log_file).parent.mkdir(parents=True, exist_ok=True)
        setup_logging(config.log_level, config.log_file, console_level="WARNING")
        self._stats = CrawlerStats()
        self._storage = build_storage(config.storage_type, config.storage_path)
        self._crawler = AsyncCrawler(
            max_concurrent=config.max_concurrent,
            per_domain=config.per_domain,
            max_depth=config.max_depth,
            requests_per_second=config.requests_per_second,
            respect_robots=config.respect_robots,
            user_agent=config.user_agent,
            storage=self._storage,
        )
        self._result = None

    @classmethod
    def from_config(cls, path: str) -> "AdvancedCrawler":
        return cls(CrawlerConfig.from_file(path))

    async def _start_urls(self) -> list[str]:
        urls = list(self._config.start_urls)
        if self._config.sitemap_url:
            async with ClientSession() as s:
                urls += await SitemapParser(s).fetch_sitemap(self._config.sitemap_url)
        return urls

    async def crawl(self) -> dict:

        urls = await self._start_urls()
        self._stats.start()
        self._result = await self._crawler.crawl(
            urls,
            max_pages=self._config.max_pages,
            same_domain_only=self._config.same_domain_only,
            include_patterns=self._config.include_patterns or None,
            exclude_patterns=self._config.exclude_patterns or None,
        )
        self._stats.stop()
        for url, page in self._result["processed"].items():
            self._stats.record_success(url, page["status_code"])
        for url, err in self._result["failed"].items():
            self._stats.record_failure(url, err.split(":")[0])
        return self._result

    def get_stats(self) -> dict:
        return self._stats.summary()

    def export_to_json(self, filename):
        self._stats.export_to_json(filename)

    def export_to_html_report(self, filename):
        self._stats.export_to_html_report(filename)

    async def close(self) -> None:
        await self._crawler.close()
