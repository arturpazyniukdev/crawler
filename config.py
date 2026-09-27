# config.py
import json
from dataclasses import dataclass, field, fields
from pathlib import Path

import yaml


@dataclass
class CrawlerConfig:
    start_urls: list[str] = field(default_factory=list)
    sitemap_url: str | None = None
    max_pages: int = 50
    max_depth: int = 1
    max_concurrent: int = 10
    per_domain: int = 5
    requests_per_second: float = 1.0
    respect_robots: bool = True
    user_agent: str = "MyBot/1.0"
    same_domain_only: bool = False
    include_patterns: list[str] = field(default_factory=list)
    exclude_patterns: list[str] = field(default_factory=list)
    storage_type: str = "json"  # json | csv | sqlite | none
    storage_path: str = "results.jsonl"
    log_file: str | None = None
    log_level: str = "INFO"

    @classmethod
    def from_file(cls, path: str) -> "CrawlerConfig":
        p = Path(path)
        text = p.read_text(encoding="utf-8")
        raw = yaml.safe_load(text) if p.suffix in (".yaml", ".yml") else json.loads(text)
        raw = raw or {}
        known = {f.name for f in fields(cls)}
        unknown = set(raw) - known
        if unknown:
            raise ValueError(f"unknown config keys: {unknown}")
        return cls(**raw)
