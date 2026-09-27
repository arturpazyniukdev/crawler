# demo/day7.py
import asyncio

from aiohttp import ClientSession

from advanced import AdvancedCrawler
from sitemap import SitemapParser


async def show_sitemap() -> None:
    async with ClientSession() as s:
        urls = await SitemapParser(s).fetch_sitemap("https://www.sitemaps.org/sitemap.xml")
    print(f"sitemap: {len(urls)} urls, first: {urls[:2]}")


async def main() -> None:
    await show_sitemap()

    crawler = AdvancedCrawler.from_config("config.yaml")
    try:
        await crawler.crawl()
    finally:
        await crawler.close()

    stats = crawler.get_stats()
    print(f"Processed: {stats['total_pages']} pages")
    print(f"Successful: {stats['successful']}")
    print(f"Failed: {stats['failed']}")
    print(f"Speed: {stats['pages_per_second']} pages/s, status codes: {stats['status_codes']}")

    crawler.export_to_json("output/stats.json")
    crawler.export_to_html_report("output/report.html")
    print("report: output/report.html")


if __name__ == "__main__":
    asyncio.run(main())
