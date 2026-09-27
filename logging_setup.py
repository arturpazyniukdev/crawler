import logging
from logging.handlers import RotatingFileHandler

FORMAT = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"


def setup_logging(
    level: str = "INFO", log_file: str | None = None, console_level: str | None = None
) -> None:
    root = logging.getLogger()
    root.setLevel(level.upper())
    root.handlers.clear()
    fmt = logging.Formatter(FORMAT)
    console = logging.StreamHandler()
    console.setLevel((console_level or level).upper())
    console.setFormatter(fmt)
    root.addHandler(console)
    if log_file:
        fh = RotatingFileHandler(log_file, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
