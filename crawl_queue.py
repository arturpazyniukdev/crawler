class CrawlerQueue:
    def __init__(self) -> None:
        self._in_flight = 0
        self._urls = []
        self._enqueued_urls = set()
        self._failed_urls = {}
        self._processed_urls = {}
        self._depths = {}

    def add_url(self, url, priority=0, depth=0):
        if url in self._enqueued_urls:
            return
        self._depths[url] = depth
        self._urls.append((priority, url))
        self._enqueued_urls.add(url)

    async def get_next(self):
        if len(self._urls) == 0:
            return None
        self._in_flight += 1
        self._urls.sort(key=lambda t: t[0])
        return self._urls.pop(0)[1]

    def task_done(self):
        self._in_flight -= 1

    def is_drained(self):
        return len(self._urls) == 0 and self._in_flight == 0

    def mark_processed(self, url, result):
        self._processed_urls[url] = result

    def mark_failed(self, url, error):
        self._failed_urls[url] = error

    def depth_of(self, url):
        return self._depths[url]

    def claimed_count(self):
        return len(self._processed_urls) + len(self._failed_urls) + self._in_flight

    def is_known(self, url):
        return url in self._enqueued_urls

    def get_stats(self):
        return {
            "queued": len(self._urls),
            "in_flight": self._in_flight,
            "processed": len(self._processed_urls),
            "failed": len(self._failed_urls),
        }
