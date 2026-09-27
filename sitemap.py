import gzip
import logging
import xml.etree.ElementTree as ET

from aiohttp import ClientError, ClientSession

NS = "{http://www.sitemaps.org/schemas/sitemap/0.9}"

logger = logging.getLogger(__name__)


class SitemapParser:
    def __init__(self, session: ClientSession, max_sitemaps: int = 50) -> None:
        self._session = session
        self._max_sitemaps = max_sitemaps
        self._seen = set()

    async def _download(self, url: str) -> bytes | None:
        try:
            async with self._session.get(url) as resp:
                resp.raise_for_status()
                data = await resp.read()
            if url.endswith(".gz"):
                data = gzip.decompress(data)
            return data
        except ClientError as e:
            logger.warning("sitemap download failed for %s: %r", url, e)
            return None

    @staticmethod
    def _parse(xml: bytes) -> tuple[str, list[str]]:
        root = ET.fromstring(xml)
        kind = "index" if root.tag.endswith("sitemapindex") else "urlset"
        locs = [el.text.strip() for el in root.iter(NS + "loc") if el.text]
        return kind, locs

    async def fetch_sitemap(self, url):
        if url in self._seen or len(self._seen) >= self._max_sitemaps:
            return []
        self._seen.add(url)
        data = await self._download(url)
        if data is None:
            return []
        try:
            kind, locs = self._parse(data)
        except ET.ParseError as e:
            logger.warning("bad sitemap xml at %s: %r", url, e)
            return []
        if kind == "index":
            result = []
            for loc in locs:
                result.extend(await self.fetch_sitemap(loc))
            return result
        return locs
