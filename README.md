# Async Web Crawler

Asynchronous web crawler in Python built on `asyncio`, `aiohttp` and BeautifulSoup.
Crawls a site breadth-first with concurrency limits, per-domain rate limiting, `robots.txt`
support, retries, pluggable storage, sitemap discovery, statistics and an HTML report.

## Requirements

Python 3.12+.

```bash
python -m venv .venv
.venv/bin/pip install aiohttp beautifulsoup4 lxml aiofiles aiosqlite pyyaml
```

## Quick start

Command line:

```bash
.venv/bin/python cli.py --urls https://books.toscrape.com/ --max-pages 20 \
    --output output/results.jsonl --report output/report.html
```

From a config file:

```bash
.venv/bin/python cli.py --config config.yaml
```

From Python:

```python
import asyncio
from advanced import AdvancedCrawler


async def main():
    crawler = AdvancedCrawler.from_config("config.yaml")
    try:
        await crawler.crawl()
    finally:
        await crawler.close()
    print(crawler.get_stats())
    crawler.export_to_html_report("output/report.html")


asyncio.run(main())
```

Demos for each development step live in `demo/` and run as modules, for example
`.venv/bin/python -m demo.day7`. A performance comparison against a sequential crawler is
`.venv/bin/python -m demo.perf 100 500 1000`.

## CLI options

| Option | Meaning |
|---|---|
| `--urls URL [URL ...]` | start URLs |
| `--config FILE` | YAML or JSON config file; CLI options override its values |
| `--max-pages N` | stop after N pages |
| `--max-depth N` | follow links up to depth N from the start URLs |
| `--output FILE` | results file; storage type is picked from the suffix: `.csv`, `.db`/`.sqlite`, otherwise JSON lines |
| `--rate-limit R` | requests per second per domain |
| `--respect-robots` / `--no-respect-robots` | obey `robots.txt` (default: obey) |
| `--report FILE` | write an HTML report after the crawl |
| `--log-level LEVEL` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

Either `--urls` or a config with `start_urls` / `sitemap_url` is required.

## Configuration file

YAML or JSON. Every key is optional; unknown keys raise an error. Defaults shown.

```yaml
start_urls: []                 # list of URLs to begin with
sitemap_url: null              # sitemap or sitemap index; its URLs are added to start_urls
max_pages: 50
max_depth: 1
max_concurrent: 10             # total in-flight requests
per_domain: 5                  # in-flight requests per host
requests_per_second: 1.0       # rate limit per host
respect_robots: true
user_agent: MyBot/1.0
same_domain_only: false        # stay on the start URLs' hosts
include_patterns: []           # substrings a URL must contain to be followed
exclude_patterns: []           # substrings that exclude a URL
storage_type: json             # json | csv | sqlite | none
storage_path: results.jsonl
log_file: null                 # enables a rotating log file (1 MB x 3) when set
log_level: INFO
```

`config.yaml` in the repository is a working example. Output directories are created
automatically.

## API

### `AdvancedCrawler` (`advanced.py`)

High-level entry point that wires everything together.

- `AdvancedCrawler(config: CrawlerConfig)` / `AdvancedCrawler.from_config(path)`
- `await crawl() -> dict` — fetches the sitemap if configured, runs the crawl, fills statistics.
  Returns the raw result of `AsyncCrawler.crawl`.
- `get_stats() -> dict` — see `CrawlerStats.summary()`.
- `export_to_json(filename)`, `export_to_html_report(filename)`
- `await close()` — closes the HTTP session and storage.

### `AsyncCrawler` (`crawler.py`)

The core engine. Constructor arguments: `max_concurrent`, `per_domain`, `max_depth`,
`requests_per_second`, `respect_robots`, `rate_per_domain`, `user_agent`, `min_delay`,
`jitter`, `retry_strategy`, `connect_timeout`, `read_timeout`, `total_timeout`, `storage`,
`html_parser`.

- `await fetch_url(url) -> str`, `await fetch_urls(urls) -> dict`
- `await fetch_and_parse(url) -> dict` — page dict with `url`, `title`, `text`, `links`,
  `images`, `headers`, `metadata`, `tables`, `lists`, `statistics`, `status_code`,
  `content_type`, `crawled_at`.
- `await crawl(start_urls, max_pages=50, same_domain_only=False, exclude_patterns=None,
  include_patterns=None) -> dict` with keys `processed`, `failed`, `visited`, `blocked`,
  `avg_delay`, `errors`, `save_failures`. Prints a live progress bar (percent, speed, ETA,
  active tasks, queue size).
- `await close()`

### `CrawlerConfig` (`config.py`)

Dataclass with the fields listed in the configuration section. `CrawlerConfig.from_file(path)`
loads YAML or JSON and rejects unknown keys.

### `CrawlerStats` (`stats.py`)

- `start()`, `stop()`, `record_success(url, status_code)`, `record_failure(url, error)`
- `summary() -> dict` — `total_pages`, `successful`, `failed`, `elapsed_seconds`,
  `pages_per_second`, `status_codes`, `top_domains`, `errors`
- `export_to_json(filename)`, `export_to_html_report(filename)` — the report contains an
  overview table and bar charts for status codes, domains and error types.

### `SitemapParser` (`sitemap.py`)

`SitemapParser(session, max_sitemaps=50)`; `await fetch_sitemap(url) -> list[str]`. Handles
regular sitemaps, sitemap indexes (recursively) and `.gz` files.

### Storage (`storage.py`)

`DataStorage` is the abstract base with `save(page)` and `close()`. Implementations:
`JSONStorage(path)` (JSON lines), `CSVStorage(path)`, `SQLiteStorage(path, batch_size=50)`.

### Supporting modules

- `parser.py` — `HTMLParser.parse_html(html, url)` extracts the page dict above.
- `robots.py` — `RobotsParser`: `fetch_robots`, `can_fetch`, `get_crawl_delay`.
- `rate_limiter.py` — `RateLimiter(requests_per_second, per_domain, min_delay, jitter)`.
- `retry.py` — `RetryStrategy(max_retries=3, backoff_factor=2.0, base_delay=1.0)` with
  exponential backoff for transient errors.
- `semaphore.py` — global and per-domain concurrency limits.
- `crawl_queue.py` — priority queue with visited-set and depth tracking.
- `errors.py` — `CrawlerError` and subclasses `TransientError`, `PermanentError`,
  `NetworkError`, `ParseError`.
- `logging_setup.py` — `setup_logging(level, log_file, console_level)`: console plus
  rotating file handler.
- `urls.py` — URL normalization.

## Development

Lint and format with Ruff (config in `pyproject.toml`):

```bash
.venv/bin/ruff check --fix . && .venv/bin/ruff format .
```
