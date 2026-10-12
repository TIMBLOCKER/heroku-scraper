import os
import re
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

import feedparser
import psycopg2
import requests
import trafilatura
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
        sslmode = "require" if "heroku" in dsn.lower() else "prefer"
        self.conn = psycopg2.connect(dsn, sslmode=sslmode)
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
                    embedding vector(4096),
                    title TEXT,
                    content TEXT
                );
                """
            )
            cur.execute(
                f"ALTER TABLE {Config.TABLE_NAME} ADD COLUMN IF NOT EXISTS article_language TEXT;"
            )
            cur.execute(
                f"ALTER TABLE {Config.TABLE_NAME} ADD COLUMN IF NOT EXISTS content_scraped TEXT;"
            )
            cur.execute(
                f"""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = %s
                          AND column_name = 'embedding'
                          AND udt_name = '_vector'
                    ) THEN
                        BEGIN
                            EXECUTE format('ALTER TABLE %%I ALTER COLUMN embedding TYPE vector(4096);', %s);
                        EXCEPTION WHEN OTHERS THEN
                            NULL;
                        END;
                    END IF;
                END $$;
                """,
                (Config.TABLE_NAME, Config.TABLE_NAME),
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
                    content,
                    article_language,
                    content_scraped
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (canonical_url) DO UPDATE SET
                    publisher = EXCLUDED.publisher,
                    fetched_at = EXCLUDED.fetched_at,
                    published_at = EXCLUDED.published_at,
                    embedding_status = EXCLUDED.embedding_status,
                    success = EXCLUDED.success,
                    embedding = EXCLUDED.embedding,
                    title = EXCLUDED.title,
                    content = EXCLUDED.content,
                    article_language = EXCLUDED.article_language,
                    content_scraped = EXCLUDED.content_scraped;
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
                    article["article_language"],
                    article.get("content_scraped"),
                ),
            )

    def rescrape_and_update_articles_before(self, cutoff_dt):
        """
        One-off migration helper:
        Re-scrapes and updates all rows with fetched_at < cutoff_dt in one DB transaction.
        """
        old_autocommit = self.conn.autocommit
        self.conn.autocommit = False
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, publisher, canonical_url, published_at
                    FROM {Config.TABLE_NAME}
                    WHERE fetched_at < %s
                    ORDER BY fetched_at ASC;
                    """,
                    (cutoff_dt,),
                )
                rows = cur.fetchall()

            for row_id, publisher, canonical_url, published_at in rows:
                scraped = _extract_canonical_content(canonical_url)
                content_scraped = scraped["content_scraped"]
                success = scraped["success"]
                title = ""
                content = ""
                if content_scraped:
                    title = _strip_html(content_scraped[:160])
                    content = content_scraped
                article_language = _detect_language(f"{title} {content}")

                with self.conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {Config.TABLE_NAME}
                        SET
                            fetched_at = %s,
                            success = %s,
                            title = %s,
                            content = %s,
                            article_language = %s,
                            content_scraped = %s
                        WHERE id = %s;
                        """,
                        (
                            datetime.now(timezone.utc),
                            success,
                            title,
                            content,
                            article_language,
                            content_scraped,
                            row_id,
                        ),
                    )

            self.conn.commit()
            print(f"[INFO] One-off rescrape completed. Updated rows: {len(rows)}")
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.conn.autocommit = old_autocommit

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


class _HTMLTextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, data):
        if data:
            self.parts.append(data)

    def text(self):
        return " ".join(self.parts)


def _strip_html(value):
    if not value:
        return ""
    parser = _HTMLTextExtractor()
    parser.feed(value)
    parser.close()
    cleaned = parser.text()
    return re.sub(r"\s+", " ", cleaned).strip()


def _detect_language(text):
    sample = (text or "").lower()
    if not sample:
        return "en"
    de_markers = (" der ", " die ", " und ", " ist ", " nicht ", " mit ", " auf ")
    en_markers = (" the ", " and ", " is ", " not ", " with ", " for ", " on ")
    de_score = sum(sample.count(marker) for marker in de_markers)
    en_score = sum(sample.count(marker) for marker in en_markers)
    return "de" if de_score >= en_score else "en"


def _is_blocked_or_paywalled(text):
    haystack = (text or "").lower()
    phrases = (
        "captcha",
        "zugriff verweigert",
        "access denied",
        "human verification",
        "verify you are human",
        "subscribe to continue",
        "abo",
        "anmelden, um weiterzulesen",
        "paywall",
        "cookie einstellungen",
    )
    return any(p in haystack for p in phrases)


def _is_plausible_article(result):
    text = (result.get("text") or "").strip()
    title = (result.get("title") or "").strip()
    paragraphs = [p.strip() for p in re.split(r"\n+", text) if p.strip()]
    if not title or len(title) < 8:
        return False
    if len(text) < 800:
        return False
    if len(paragraphs) < 3:
        return False
    if _is_blocked_or_paywalled(text):
        return False
    return True


def _extract_canonical_content(url):
    result = {"success": False, "content_scraped": None}
    if not url:
        return result
    try:
        response = requests.get(url, timeout=20, allow_redirects=True, headers={"User-Agent": "rss-scraper/1.0"})
        content_type = (response.headers.get("Content-Type") or "").lower()
        if response.status_code >= 400 or "text/html" not in content_type:
            return result
        downloaded = trafilatura.fetch_url(response.url)
        if not downloaded:
            return result
        extracted = trafilatura.extract(downloaded, include_comments=False, include_formatting=False)
        metadata = trafilatura.extract_metadata(downloaded)
        text = _strip_html(extracted or "")
        title = _strip_html(metadata.title if metadata else "")
        article_result = {"title": title, "text": text}
        if not _is_plausible_article(article_result):
            return result
        result["success"] = True
        result["content_scraped"] = text
        return result
    except Exception as exc:
        print(f"[WARN] canonical scrape failed for {url}: {exc}")
        return result


def article_from_entry(publisher, entry):
    link = entry.get("link") or entry.get("id") or ""
    if not link:
        return None

    published_at = _parse_date(entry, "published_parsed") or _parse_date(entry, "updated_parsed")
    if not published_at:
        published_at = datetime.now(timezone.utc)

    content_value = entry.get("summary") or entry.get("summary_detail") or entry.get("content") or ""
    title = _text(entry.get("title") or "")
    content = _strip_html(_text(content_value))
    article_language = _detect_language(f"{title} {content}")
    scraped = _extract_canonical_content(link)
    success = scraped["success"]

    return {
        "publisher": publisher,
        "canonical_url": link,
        "fetched_at": datetime.now(timezone.utc),
        "published_at": published_at,
        "embedding_status": False,
        "success": success,
        "embedding": None,
        "title": title,
        "content": content,
        "article_language": article_language,
        "content_scraped": scraped["content_scraped"],
    }


def load_feeds(path):
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return data.get("feeds", [])


def main():
    job = RSSJob()
    try:
        job.ensure_table()
        # ONE-OFF BACKFILL (run once, then comment this block out):
        # This atomically re-scrapes all entries with fetched_at before 2026-10-12 UTC
        # and updates: fetched_at, success, title, content, article_language, content_scraped.
        #
        # After the one-time run is done successfully, comment out or remove the next two lines.
        one_off_cutoff = datetime(2026, 10, 12, tzinfo=timezone.utc)
        job.rescrape_and_update_articles_before(one_off_cutoff)

        for item in load_feeds(Config.FEEDS_FILE):
            if not item.get("enabled"):
                continue

            publisher = item.get("publisher", "")
            feed_url = item.get("feed_url")
            if not feed_url:
                print(f"[WARN] feed without URL for publisher '{publisher}'")
                continue

            try:
                feed = feedparser.parse(feed_url)
            except Exception as exc:
                print(f"[ERROR] failed to parse feed {feed_url}: {exc}")
                continue

            if getattr(feed, "bozo", False):
                print(f"[WARN] malformed feed skipped: {feed_url}")
                continue

            for entry in getattr(feed, "entries", []) or []:
                article = article_from_entry(publisher, entry)
                if not article:
                    print(f"[WARN] invalid entry skipped for feed: {feed_url}")
                    continue
                try:
                    job.upsert_article(article)
                except Exception as exc:
                    print(
                        f"[ERROR] DB upsert failed for URL '{article.get('canonical_url', '<unknown>')}': {exc}"
                    )
    finally:
        job.close()


if __name__ == "__main__":
    main()
