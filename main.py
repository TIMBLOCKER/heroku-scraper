import os
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import psycopg2
import yaml


class Config:
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/app")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    TABLE_NAME = os.getenv("TABLE_NAME", "rss_articles")
    FEEDS_FILE = Path(os.getenv("FEEDS_FILE", "feeds.yaml"))


class RSSJob:
    def __init__(self):
        dsn = Config.DATABASE_URL
        if not dsn:
            user = os.getenv("DB_USER", "postgres")
            host = os.getenv("DB_HOST", "localhost")
            port = os.getenv("DB_PORT", "5432")
            name = os.getenv("DB_NAME", "app")
            dsn = f"postgresql://{user}:{Config.DB_PASSWORD}@{host}:{port}/{name}"
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = True

    def ensure_table(self):
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {Config.TABLE_NAME} (
                    id SERIAL PRIMARY KEY,
                    publisher TEXT NOT NULL,
                    canonical_url TEXT NOT NULL UNIQUE,
                    fetched_at TIMESTAMPTZ NOT NULL,
                    published_at TIMESTAMPTZ,
                    embedding_status BOOLEAN NOT NULL DEFAULT FALSE,
                    success BOOLEAN NOT NULL,
                    title TEXT,
                    content TEXT
                );
                """
            )

    def insert_article(self, article):
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {Config.TABLE_NAME} (
                    publisher,
                    canonical_url,
                    fetched_at,
                    published_at,
                    embedding_status,
                    success,
                    title,
                    content
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (canonical_url) DO UPDATE SET
                    publisher = EXCLUDED.publisher,
                    fetched_at = EXCLUDED.fetched_at,
                    published_at = EXCLUDED.published_at,
                    embedding_status = EXCLUDED.embedding_status,
                    success = EXCLUDED.success,
                    title = EXCLUDED.title,
                    content = EXCLUDED.content;
                """,
                (
                    article["publisher"],
                    article["canonical_url"],
                    article["fetched_at"],
                    article["published_at"],
                    article["embedding_status"],
                    article["success"],
                    article["title"],
                    article["content"],
                ),
            )

    def close(self):
        self.conn.close()


def _to_text(value):
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return " ".join(_to_text(v) for v in value if v)
    if isinstance(value, dict):
        return _to_text(value.get("value") or value.get("title") or "")
    return str(value)


def _parse_date(entry, key):
    raw = entry.get(key)
    if not raw:
        return None
    try:
        return datetime(*raw[:6], tzinfo=timezone.utc)
    except Exception:
        return None


def _article_from_entry(publisher, entry):
    link = entry.get("link") or entry.get("id") or ""
    if not link:
        return None

    published_at = _parse_date(entry, "published_parsed") or _parse_date(entry, "updated_parsed")
    if not published_at:
        published_at = datetime.now(timezone.utc)

    content = _to_text(
        entry.get("summary")
        or entry.get("summary_detail")
        or entry.get("content")
        or ""
    )
    title = _to_text(entry.get("title") or "")

    return {
        "publisher": publisher,
        "canonical_url": link,
        "fetched_at": datetime.now(timezone.utc),
        "published_at": published_at,
        "embedding_status": False,
        "success": True,
        "title": title,
        "content": content,
    }


def load_feeds(path):
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return data.get("feeds", [])


def main():
    job = RSSJob()
    try:
        job.ensure_table()

        for item in load_feeds(Config.FEEDS_FILE):
            if not item.get("enabled"):
                continue

            publisher = item.get("publisher", "")
            feed_url = item.get("feed_url")
            if not feed_url:
                continue

            try:
                feed = feedparser.parse(feed_url)
            except Exception:
                continue

            if getattr(feed, "bozo", False):
                continue

            for entry in getattr(feed, "entries", []) or []:
                article = _article_from_entry(publisher, entry)
                if not article:
                    continue
                try:
                    job.insert_article(article)
                except Exception:
                    continue
    finally:
        job.close()


if __name__ == "__main__":
    main()
