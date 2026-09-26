import logging
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

from aiohttp import ClientError, ClientSession

logger = logging.getLogger(__name__)


class RobotsParser:
    def __init__(self, session: ClientSession) -> None:
        self._session = session
        self._cache: dict[str, RobotFileParser] = {}

    async def fetch_robots(self, base_url: str) -> dict:
        parsed = urlparse(base_url)
        domain = parsed.hostname
        scheme = parsed.scheme
        netloc = parsed.netloc

        if domain in self._cache:
            return {
                "domain": domain,
                "status": None,
                "crawl_delay": self._cache[domain].crawl_delay("*"),
            }

        robots_url = f"{scheme}://{netloc}/robots.txt"

        lines = []
        rp = RobotFileParser()
        status = None
        try:
            async with self._session.get(robots_url) as resp:
                status = resp.status
                if status == 200:
                    text = await resp.text()
                    lines = text.splitlines()
                elif status != 404:
                    logger.warning("robots.txt %s for %s", resp.status, domain)
        except (ClientError, TimeoutError) as e:
            logger.warning("robots.txt fetch failed for %s: %r", domain, e)
        rp.parse(lines)
        self._cache[domain] = rp

        return {"domain": domain, "status": status, "crawl_delay": rp.crawl_delay("*")}

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        parsed = urlparse(url)
        domain = parsed.hostname

        rp = self._cache.get(domain)

        if rp is None:
            return True

        return rp.can_fetch(user_agent, url)

    def get_crawl_delay(self, url: str, user_agent: str = "*") -> float:
        parsed = urlparse(url)
        domain = parsed.hostname

        rp = self._cache.get(domain)

        if rp is None:
            return 0.0

        return rp.crawl_delay(user_agent) or 0.0
