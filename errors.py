class CrawlerError(Exception):
    def __init__(self, url: str, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.url = url
        self.status = status


class TransientError(CrawlerError):
    pass


class PermanentError(CrawlerError):
    pass


class NetworkError(CrawlerError):
    pass


class ParseError(CrawlerError):
    pass
