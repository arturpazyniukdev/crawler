# cli.py
import argparse
import asyncio
from pathlib import Path

from advanced import AdvancedCrawler
from config import CrawlerConfig


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Async web crawler")
    p.add_argument("--urls", nargs="+", help="start URLs")
    p.add_argument("--config", help="YAML/JSON config file")
    p.add_argument("--max-pages", type=int)
    p.add_argument("--max-depth", type=int)
    p.add_argument("--output", help="results file (.jsonl/.csv/.db)")
    p.add_argument("--rate-limit", type=float, help="requests per second")
    p.add_argument("--respect-robots", action=argparse.BooleanOptionalAction)
    p.add_argument("--report", help="HTML report path")
    p.add_argument("--log-level")
    return p.parse_args(argv)


def build_config(args: argparse.Namespace) -> CrawlerConfig:
    cfg = CrawlerConfig.from_file(args.config) if args.config else CrawlerConfig()
    if args.urls:
        cfg.start_urls = args.urls
    if args.max_pages is not None:
        cfg.max_pages = args.max_pages
    if args.max_depth is not None:
        cfg.max_depth = args.max_depth
    if args.log_level is not None:
        cfg.log_level = args.log_level
    if args.rate_limit is not None:
        cfg.requests_per_second = args.rate_limit
    if args.respect_robots is not None:
        cfg.respect_robots = args.respect_robots
    if args.output:
        suffix = Path(args.output).suffix.lstrip(".")
        cfg.storage_path = args.output
        cfg.storage_type = {"csv": "csv", "db": "sqlite", "sqlite": "sqlite"}.get(suffix, "json")
    if not cfg.start_urls and not cfg.sitemap_url:
        raise SystemExit("no start URLs: pass --urls or --config")
    return cfg


async def run(args) -> None:
    crawler = AdvancedCrawler(build_config(args))
    try:
        await crawler.crawl()
    finally:
        await crawler.close()
    s = crawler.get_stats()
    print(f"processed {s['total_pages']}, ok {s['successful']}, failed {s['failed']}")
    print(f"{s['pages_per_second']} pages/s in {s['elapsed_seconds']}s")
    if args.report:
        crawler.export_to_html_report(args.report)
        print("report:", args.report)


def main() -> None:
    asyncio.run(run(parse_args()))


if __name__ == "__main__":
    main()
