import html
import json
import time
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse


class CrawlerStats:
    def __init__(self) -> None:
        self._start = None
        self._end = None
        self._successful = 0
        self._failed = 0
        self._status_codes = Counter()
        self._domains = Counter()
        self._errors = Counter()

    def start(self) -> None:
        self._start = time.perf_counter()
        self._end = None

    def stop(self) -> None:
        self._end = time.perf_counter()

    def record_success(self, url: str, status_code: int) -> None:
        parsed = urlparse(url)
        self._successful += 1
        self._status_codes[status_code] += 1
        self._domains[parsed.hostname] += 1

    def record_failure(self, url: str, error: str) -> None:
        parsed = urlparse(url)
        self._failed += 1
        self._domains[parsed.hostname] += 1
        self._errors[error] += 1

    @property
    def elapsed(self) -> float:
        if self._start is None:
            return 0.0
        return (self._end or time.perf_counter()) - self._start

    def summary(self) -> dict:
        total = self._successful + self._failed
        return {
            "total_pages": total,
            "successful": self._successful,
            "failed": self._failed,
            "elapsed_seconds": round(self.elapsed, 2),
            "pages_per_second": 0 if self.elapsed == 0 else round(total / self.elapsed, 2),
            "status_codes": dict(self._status_codes),
            "top_domains": self._domains.most_common(10),
            "errors": dict(self._errors),
        }

    def export_to_json(self, filename: str) -> None:
        text = json.dumps(self.summary(), indent=2, ensure_ascii=False)
        Path(filename).write_text(text, encoding="utf-8")

    @staticmethod
    def _bar_table(title: str, rows: list[tuple[str, int]]) -> str:
        if not rows:
            return ""
        rows = sorted(rows, key=lambda r: -r[1])  # biggest first
        top = rows[0][1]
        lines = []
        for label, n in rows:
            pct = n / top * 100
            lines.append(
                f"<tr><td>{html.escape(str(label))}</td><td>{n}</td>"
                f'<td><div class="bar" style="width:{pct:.0f}%"></div></td></tr>'
            )
        return f"<h2>{title}</h2><table>{''.join(lines)}</table>"

    def export_to_html_report(self, filename: str) -> None:
        s = self.summary()
        overview = "".join(
            f"<tr><td>{k}</td><td>{s[k]}</td></tr>"
            for k in ("total_pages", "successful", "failed", "elapsed_seconds", "pages_per_second")
        )
        doc = (
            "<!doctype html><html><head><meta charset='utf-8'><title>Crawler report</title>"
            "<style>"
            "body{font-family:sans-serif;max-width:800px;margin:2em auto}"
            "table{border-collapse:collapse}td{padding:4px 12px;border-bottom:1px solid #ddd}"
            ".bar{height:12px;background:#4a90e2;min-width:2px}"
            "td:last-child{width:300px}"
            "</style></head><body>"
            "<h1>Crawler report</h1>"
            f"<table>{overview}</table>"
            + self._bar_table("Status codes", list(s["status_codes"].items()))
            + self._bar_table("Top domains", s["top_domains"])
            + self._bar_table("Errors", list(s["errors"].items()))
            + "</body></html>"
        )
        Path(filename).write_text(doc, encoding="utf-8")
