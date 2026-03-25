"""Хранение подписок и уже просмотренных тендеров."""
import aiosqlite
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).parent / "tenderbot.db"


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS subscribers (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                subscribed_at TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS seen_tenders (
                tender_id TEXT PRIMARY KEY,
                source_url TEXT,
                seen_at TEXT
            )
            """
        )
        await db.commit()


async def add_subscriber(user_id: int, username: Optional[str] = None):
    import datetime
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT OR REPLACE INTO subscribers (user_id, username, subscribed_at)
            VALUES (?, ?, ?)
            """,
            (user_id, username, datetime.datetime.utcnow().isoformat()),
        )
        await db.commit()


async def remove_subscriber(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM subscribers WHERE user_id = ?", (user_id,))
        await db.commit()


async def get_subscribers():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT user_id FROM subscribers") as cur:
            rows = await cur.fetchall()
    return [r["user_id"] for r in rows]


async def is_subscribed(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM subscribers WHERE user_id = ?", (user_id,)
        ) as cur:
            row = await cur.fetchone()
    return row is not None


async def mark_tender_seen(tender_id: str, source_url: str):
    import datetime
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT OR IGNORE INTO seen_tenders (tender_id, source_url, seen_at)
            VALUES (?, ?, ?)
            """,
            (tender_id, source_url, datetime.datetime.utcnow().isoformat()),
        )
        await db.commit()


async def is_tender_seen(tender_id: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM seen_tenders WHERE tender_id = ?", (tender_id,)
        ) as cur:
            row = await cur.fetchone()
    return row is not None
