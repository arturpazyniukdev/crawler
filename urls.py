from urllib.parse import urlparse, urlunparse


def normalize_url(url: str) -> str:
    p = urlparse(url)
    return urlunparse(p._replace(netloc=p.netloc.lower(), path=p.path or "/"))