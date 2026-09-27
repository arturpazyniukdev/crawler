import asyncio
import csv
import json
import logging
import sqlite3
from pathlib import Path

from crawler import AsyncCrawler
from storage import CSVStorage, DataStorage, JSONStorage, SQLiteStorage

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

OUT = Path(__file__).parent
JSON_PATH = OUT / "results.jsonl"
CSV_PATH = OUT / "results.csv"
DB_PATH = OUT / "crawler.db"
START = ["https://books.toscrape.com/"]
MAX_PAGES = 5


async def crawl_into(storage: DataStorage, name: str) -> None:
    crawler = AsyncCrawler(storage=storage, respect_robots=False, requests_per_second=5)
    try:
        result = await crawler.crawl(START, max_pages=MAX_PAGES)
    finally:
        await crawler.close()
    print(
        f"{name:7} processed {len(result['processed'])}, "
        f"failed {len(result['failed'])}, save_failures {result['save_failures']}"
    )


def read_json(path: Path) -> None:
    with path.open(encoding="utf-8") as f:
        rows = [json.loads(line) for line in f]
    first = rows[0]
    print(f"json    {len(rows)} rows; first: {first['title']!r}, {len(first['links'])} links")
    print(f"        crawled_at={first['crawled_at']}, status={first['status_code']}")


def read_csv(path: Path) -> None:
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []
    first = rows[0]
    links = json.loads(first["links"])
    print(f"csv     {len(rows)} rows, {len(fieldnames)} columns; first: {first['title']!r}")
    print(f"        links cell -> list of {len(links)}, type {type(links).__name__}")


def read_sqlite(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        (count,) = conn.execute("SELECT count(*) FROM pages").fetchone()
        print(f"sqlite  {count} rows")
        for url, title, status in conn.execute(
            "SELECT url, title, status_code FROM pages ORDER BY id LIMIT 3"
        ):
            print(f"        {status} {title!r:40.40} {url}")
        (indexed,) = conn.execute("SELECT count(*) FROM pages WHERE status_code = 200").fetchone()
        print(f"        status_code=200 via index: {indexed}")
    finally:
        conn.close()


async def main() -> None:
    for p in (JSON_PATH, CSV_PATH, DB_PATH):
        p.unlink(missing_ok=True)

    print("=== crawl into each storage ===")
    await crawl_into(JSONStorage(JSON_PATH), "json")
    await crawl_into(CSVStorage(CSV_PATH), "csv")
    await crawl_into(SQLiteStorage(DB_PATH, batch_size=2), "sqlite")

    print("\n=== file sizes ===")
    for p in (JSON_PATH, CSV_PATH, DB_PATH):
        print(f"{p.name:14} {p.stat().st_size:>8} bytes")

    print("\n=== read back ===")
    read_json(JSON_PATH)
    read_csv(CSV_PATH)
    read_sqlite(DB_PATH)


if __name__ == "__main__":
    asyncio.run(main())
