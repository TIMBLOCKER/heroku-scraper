import os
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import psycopg2
import yaml


class Config:
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/app")
    TABLE_NAME = os.getenv("TABLE_NAME", "rss_articles")
    FEEDS_FILE = Path(os.getenv("FEEDS_FILE", "feeds.yaml"))


class RSSJob:
    def __init__(self):
        dsn = Config.DATABASE_URL
        if not dsn:
            raise RuntimeError("DATABASE_URL is not set")
        self.conn = psycopg2.connect(dsn, sslmode="require")
        self.conn.autocommit = True

    def ensure_table(self):
        with self.conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
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
                    embedding vector(1536),
                    title TEXT,
                    content TEXT
                );
                """
            )

    def upsert_article(self, article):
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
                    embedding,
                    title,
                    content
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (canonical_url) DO UPDATE SET
                    publisher = EXCLUDED.publisher,
                    fetched_at = EXCLUDED.fetched_at,
                    published_at = EXCLUDED.published_at,
                    embedding_status = EXCLUDED.embedding_status,
                    success = EXCLUDED.success,
                    embedding = EXCLUDED.embedding,
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
                    article.get("embedding"),
                    article["title"],
                    article["content"],
                ),
            )

    def close(self):
        self.conn.close()


def _text(value):
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return " ".join(_text(v) for v in value if v)
    if isinstance(value, dict):
        return _text(value.get("value") or value.get("title") or "")
    return str(value)


def _parse_date(entry, key):
    raw = entry.get(key)
    if not raw:
        return None
    try:
        return datetime(*raw[:6], tzinfo=timezone.utc)
    except Exception:
        return None


def article_from_entry(publisher, entry):
    link = entry.get("link") or entry.get("id") or ""
    if not link:
        return None

    published_at = _parse_date(entry, "published_parsed") or _parse_date(entry, "updated_parsed")
    if not published_at:
        published_at = datetime.now(timezone.utc)

    content_value = entry.get("summary") or entry.get("summary_detail") or entry.get("content") or ""
    title = _text(entry.get("title") or "")
    content = _text(content_value)

    return {
        "publisher": publisher,
        "canonical_url": link,
        "fetched_at": datetime.now(timezone.utc),
        "published_at": published_at,
        "embedding_status": False,
        "success": True,
        "embedding": None,
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
                article = article_from_entry(publisher, entry)
                if not article:
                    continue
                try:
                    job.upsert_article(article)
                except Exception:
                    continue
    finally:
        job.close()


if __name__ == "__main__":
    main()
