import csv
import io
import json
from abc import ABC, abstractmethod
from datetime import datetime

import aiofiles
import aiosqlite


class DataStorage(ABC):
    @abstractmethod
    async def save(self, data: dict) -> None:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass


class JSONStorage(DataStorage):
    def __init__(self, path, indent=None) -> None:
        self._path = path
        self._indent = indent
        self._file = None

    async def _get_file(self):
        if self._file is None:
            self._file = await aiofiles.open(self._path, "a", encoding="utf-8")
        return self._file

    async def save(self, data: dict):
        f = await self._get_file()
        line = json.dumps(data, ensure_ascii=False, indent=self._indent, default=str)
        await f.write(line + "\n")

    async def close(self):
        if self._file:
            await self._file.close()
            self._file = None


class CSVStorage(DataStorage):
    def __init__(self, path, encoding="utf-8") -> None:
        self._path = path
        self._encoding = encoding
        self._file = None
        self._fieldnames = None

    async def _get_file(self):
        if self._file is None:
            self._file = await aiofiles.open(self._path, "a", encoding=self._encoding, newline="")
        return self._file

    def _row(self, data: dict) -> dict:
        return {
            k: json.dumps(v, ensure_ascii=False, default=str) if isinstance(v, (list, dict)) else v
            for k, v in data.items()
        }

    async def save(self, data: dict) -> None:
        f = await self._get_file()
        first = self._fieldnames is None
        if first:
            self._fieldnames = list(data.keys())
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=self._fieldnames)
        if first:
            writer.writeheader()
        writer.writerow(self._row(data))
        await f.write(buf.getvalue())

    async def close(self):
        if self._file:
            await self._file.close()
            self._file = None


class SQLiteStorage(DataStorage):
    COLUMNS = (
        "url",
        "title",
        "text",
        "links",
        "metadata",
        "crawled_at",
        "status_code",
        "content_type",
    )

    def __init__(self, path, batch_size=50) -> None:
        self._path = path
        self._batch_size = batch_size
        self._db = None
        self._buffer = []

    async def init_db(self) -> None:
        self._db = await aiosqlite.connect(self._path)
        await self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY,
                url TEXT UNIQUE,
                title TEXT,
                text TEXT,
                links TEXT,
                metadata TEXT,
                crawled_at TEXT,
                status_code INTEGER,
                content_type TEXT
            )
            """
        )
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_pages_status ON pages(status_code)")
        await self._db.commit()

    @staticmethod
    def _to_cell(value):
        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False)
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    async def save(self, data: dict):
        if self._db is None:
            await self.init_db()
        row = tuple(self._to_cell(data.get(c)) for c in self.COLUMNS)
        self._buffer.append(row)
        if len(self._buffer) >= self._batch_size:
            await self.flush()

    async def flush(self):
        if not self._buffer:
            return
        placeholders = ", ".join("?" * len(self.COLUMNS))
        sql = f"INSERT OR REPLACE INTO pages ({', '.join(self.COLUMNS)}) VALUES ({placeholders})"
        await self._db.executemany(sql, self._buffer)
        await self._db.commit()
        self._buffer.clear()

    async def close(self):
        await self.flush()
        if self._db is not None:
            await self._db.close()
            self._db = None
