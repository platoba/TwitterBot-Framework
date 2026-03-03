"""
Twitter/X Bookmark Manager

Advanced bookmark organization and intelligence:
- Bookmark CRUD with SQLite persistence
- Folder/collection management (Twitter doesn't support natively)
- Auto-tagging based on content analysis
- Full-text search across bookmarks
- Bookmark analytics (most bookmarked topics, reading queue)
- Export to multiple formats (JSON, CSV, Markdown, HTML)
- Duplicate detection
- Bookmark reminders (resurface old bookmarks)
- Category auto-classification
- Reading time estimation
"""

import csv
import io
import json
import re
import sqlite3
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class BookmarkStatus(str, Enum):
    UNREAD = "unread"
    READ = "read"
    ARCHIVED = "archived"
    STARRED = "starred"
    DELETED = "deleted"


class ExportFormat(str, Enum):
    JSON = "json"
    CSV = "csv"
    MARKDOWN = "markdown"
    HTML = "html"


class ContentCategory(str, Enum):
    TECH = "tech"
    BUSINESS = "business"
    SCIENCE = "science"
    NEWS = "news"
    HUMOR = "humor"
    EDUCATIONAL = "educational"
    PERSONAL = "personal"
    THREAD = "thread"
    MEDIA = "media"
    OTHER = "other"


# Keywords for auto-classification
CATEGORY_KEYWORDS = {
    ContentCategory.TECH: [
        "api", "code", "programming", "developer", "python", "javascript",
        "react", "database", "cloud", "ai", "ml", "algorithm", "software",
        "github", "deploy", "docker", "kubernetes", "devops", "rust", "golang",
        "typescript", "framework", "library", "open source", "startup",
    ],
    ContentCategory.BUSINESS: [
        "revenue", "growth", "marketing", "sales", "startup", "funding",
        "investor", "roi", "conversion", "strategy", "b2b", "saas",
        "entrepreneurship", "vc", "valuation", "acquisition", "ipo",
    ],
    ContentCategory.SCIENCE: [
        "research", "study", "experiment", "data", "peer-reviewed", "journal",
        "hypothesis", "analysis", "published", "findings", "breakthrough",
        "discovery", "clinical", "evidence", "genome", "quantum",
    ],
    ContentCategory.NEWS: [
        "breaking", "report", "update", "announced", "official", "statement",
        "press release", "developing", "confirmed", "exclusive", "source",
    ],
    ContentCategory.HUMOR: [
        "lol", "lmao", "😂", "🤣", "joke", "meme", "funny", "hilarious",
        "comedy", "satirical", "parody",
    ],
    ContentCategory.EDUCATIONAL: [
        "thread", "lesson", "learned", "how to", "guide", "tutorial",
        "explained", "101", "beginner", "tips", "hack", "cheat sheet",
        "masterclass", "deep dive",
    ],
}

# Average reading speed (words per minute)
WORDS_PER_MINUTE = 250


@dataclass
class Bookmark:
    """A bookmarked tweet."""
    bookmark_id: str = ""
    tweet_id: str = ""
    author_username: str = ""
    author_name: str = ""
    text: str = ""
    url: str = ""
    status: BookmarkStatus = BookmarkStatus.UNREAD
    folder_id: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    category: ContentCategory = ContentCategory.OTHER
    notes: str = ""
    reading_time_seconds: int = 0
    is_thread: bool = False
    thread_length: int = 1
    like_count: int = 0
    retweet_count: int = 0
    reply_count: int = 0
    created_at: str = ""          # Tweet creation time
    bookmarked_at: str = ""       # When user bookmarked it
    read_at: Optional[str] = None
    priority: int = 0             # 0=normal, 1=high, 2=urgent

    def __post_init__(self):
        if not self.bookmark_id:
            self.bookmark_id = str(uuid.uuid4())[:12]
        if not self.bookmarked_at:
            self.bookmarked_at = datetime.now(timezone.utc).isoformat()
        if not self.reading_time_seconds:
            words = len(self.text.split())
            self.reading_time_seconds = max(5, int(words / WORDS_PER_MINUTE * 60))


@dataclass
class BookmarkFolder:
    """Folder/collection for organizing bookmarks."""
    folder_id: str = ""
    name: str = ""
    description: str = ""
    color: str = "#3498db"
    icon: str = "📁"
    parent_id: Optional[str] = None
    bookmark_count: int = 0
    created_at: str = ""

    def __post_init__(self):
        if not self.folder_id:
            self.folder_id = str(uuid.uuid4())[:8]
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()


class ContentClassifier:
    """Auto-classify tweet content into categories."""

    @classmethod
    def classify(cls, text: str) -> Tuple[ContentCategory, float]:
        """Classify text into a category with confidence score."""
        text_lower = text.lower()
        scores: Dict[ContentCategory, float] = {}

        for category, keywords in CATEGORY_KEYWORDS.items():
            score = 0.0
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    score += 1.0
                    # Bonus for exact word match
                    if re.search(r'\b' + re.escape(keyword) + r'\b', text_lower):
                        score += 0.5
            scores[category] = score

        if not scores or max(scores.values()) == 0:
            return ContentCategory.OTHER, 0.0

        best = max(scores, key=scores.get)
        max_score = scores[best]
        total = sum(scores.values())
        confidence = round(max_score / max(total, 1), 3)

        return best, confidence

    @classmethod
    def auto_tag(cls, text: str, max_tags: int = 5) -> List[str]:
        """Extract auto-tags from tweet text."""
        tags = []

        # Extract hashtags
        hashtags = re.findall(r'#(\w+)', text)
        tags.extend(hashtags[:3])

        # Extract mentions as tags
        mentions = re.findall(r'@(\w+)', text)
        if mentions:
            tags.append(f"mentions:{len(mentions)}")

        # Content type tags
        if '🧵' in text or 'thread' in text.lower():
            tags.append("thread")
        if re.search(r'https?://\S+', text):
            tags.append("has_link")
        if any(char in text for char in '📸🖼️📷'):
            tags.append("media")

        # Length-based tags
        words = len(text.split())
        if words > 100:
            tags.append("long_read")
        elif words < 20:
            tags.append("quick_read")

        return list(dict.fromkeys(tags))[:max_tags]  # Deduplicate, limit


class BookmarkExporter:
    """Export bookmarks to various formats."""

    @classmethod
    def export(cls, bookmarks: List[Bookmark],
               fmt: ExportFormat = ExportFormat.JSON) -> str:
        """Export bookmarks to specified format."""
        if fmt == ExportFormat.JSON:
            return cls._to_json(bookmarks)
        elif fmt == ExportFormat.CSV:
            return cls._to_csv(bookmarks)
        elif fmt == ExportFormat.MARKDOWN:
            return cls._to_markdown(bookmarks)
        elif fmt == ExportFormat.HTML:
            return cls._to_html(bookmarks)
        return cls._to_json(bookmarks)

    @staticmethod
    def _to_json(bookmarks: List[Bookmark]) -> str:
        data = [asdict(b) for b in bookmarks]
        return json.dumps(data, indent=2, ensure_ascii=False)

    @staticmethod
    def _to_csv(bookmarks: List[Bookmark]) -> str:
        output = io.StringIO()
        fields = ["bookmark_id", "tweet_id", "author_username", "text",
                   "status", "category", "tags", "bookmarked_at", "url"]
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for b in bookmarks:
            row = asdict(b)
            row["tags"] = "; ".join(row.get("tags", []))
            writer.writerow({k: row.get(k, "") for k in fields})
        return output.getvalue()

    @staticmethod
    def _to_markdown(bookmarks: List[Bookmark]) -> str:
        lines = ["# Bookmarks\n"]
        lines.append(f"*Exported {len(bookmarks)} bookmarks "
                      f"on {datetime.now(timezone.utc).strftime('%Y-%m-%d')}*\n")

        # Group by category
        by_cat: Dict[str, List[Bookmark]] = {}
        for b in bookmarks:
            cat = b.category.value
            by_cat.setdefault(cat, []).append(b)

        for cat, items in sorted(by_cat.items()):
            lines.append(f"\n## {cat.title()} ({len(items)})\n")
            for b in items:
                status_icon = {"unread": "📖", "read": "✅", "starred": "⭐",
                               "archived": "📦"}.get(b.status.value, "📌")
                lines.append(f"- {status_icon} **@{b.author_username}**: {b.text[:120]}...")
                if b.tags:
                    lines.append(f"  - Tags: {', '.join(b.tags)}")
                if b.url:
                    lines.append(f"  - [Link]({b.url})")
                lines.append("")

        return '\n'.join(lines)

    @staticmethod
    def _to_html(bookmarks: List[Bookmark]) -> str:
        html = ['<!DOCTYPE html><html><head>',
                '<meta charset="utf-8">',
                '<title>Twitter Bookmarks</title>',
                '<style>body{font-family:system-ui;max-width:800px;margin:0 auto;padding:20px}',
                '.bookmark{border:1px solid #e1e8ed;border-radius:12px;padding:16px;margin:12px 0}',
                '.author{font-weight:bold;color:#1da1f2}',
                '.tags{color:#657786;font-size:0.9em}',
                '.meta{color:#657786;font-size:0.85em;margin-top:8px}',
                '.starred{border-color:#ffd700;background:#fffdf0}',
                '</style></head><body>',
                f'<h1>📚 Bookmarks ({len(bookmarks)})</h1>']

        for b in bookmarks:
            cls = "bookmark starred" if b.status == BookmarkStatus.STARRED else "bookmark"
            html.append(f'<div class="{cls}">')
            html.append(f'<span class="author">@{b.author_username}</span>')
            html.append(f'<p>{b.text}</p>')
            if b.tags:
                html.append(f'<div class="tags">{" ".join("#" + t for t in b.tags)}</div>')
            html.append(f'<div class="meta">{b.category.value} · {b.bookmarked_at[:10]}</div>')
            html.append('</div>')

        html.append('</body></html>')
        return '\n'.join(html)


class BookmarkManager:
    """Main bookmark management engine with SQLite persistence."""

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.classifier = ContentClassifier()
        self.exporter = BookmarkExporter()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self):
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS bookmarks (
                bookmark_id TEXT PRIMARY KEY,
                tweet_id TEXT UNIQUE,
                author_username TEXT,
                author_name TEXT,
                text TEXT NOT NULL,
                url TEXT,
                status TEXT DEFAULT 'unread',
                folder_id TEXT,
                tags TEXT DEFAULT '[]',
                category TEXT DEFAULT 'other',
                notes TEXT DEFAULT '',
                reading_time_seconds INTEGER DEFAULT 0,
                is_thread INTEGER DEFAULT 0,
                thread_length INTEGER DEFAULT 1,
                like_count INTEGER DEFAULT 0,
                retweet_count INTEGER DEFAULT 0,
                reply_count INTEGER DEFAULT 0,
                created_at TEXT,
                bookmarked_at TEXT,
                read_at TEXT,
                priority INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS folders (
                folder_id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                description TEXT DEFAULT '',
                color TEXT DEFAULT '#3498db',
                icon TEXT DEFAULT '📁',
                parent_id TEXT,
                created_at TEXT
            );

            CREATE TABLE IF NOT EXISTS bookmark_reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bookmark_id TEXT NOT NULL,
                remind_at TEXT NOT NULL,
                reminded INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (bookmark_id) REFERENCES bookmarks(bookmark_id)
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS bookmarks_fts USING fts5(
                bookmark_id, text, author_username, tags, notes,
                content=bookmarks, content_rowid=rowid
            );

            CREATE INDEX IF NOT EXISTS idx_bookmarks_status ON bookmarks(status);
            CREATE INDEX IF NOT EXISTS idx_bookmarks_category ON bookmarks(category);
            CREATE INDEX IF NOT EXISTS idx_bookmarks_folder ON bookmarks(folder_id);
            CREATE INDEX IF NOT EXISTS idx_bookmarks_bookmarked ON bookmarks(bookmarked_at);
        """)
        conn.commit()
        conn.close()

    def add_bookmark(self, tweet_id: str, text: str,
                     author_username: str = "",
                     author_name: str = "",
                     url: str = "",
                     folder_id: Optional[str] = None,
                     tags: Optional[List[str]] = None,
                     notes: str = "",
                     priority: int = 0,
                     **kwargs) -> Bookmark:
        """Add a new bookmark."""
        # Auto-classify
        category, confidence = self.classifier.classify(text)

        # Auto-tag if no tags provided
        if not tags:
            tags = self.classifier.auto_tag(text)

        bookmark = Bookmark(
            tweet_id=tweet_id,
            author_username=author_username,
            author_name=author_name,
            text=text,
            url=url or f"https://twitter.com/{author_username}/status/{tweet_id}",
            folder_id=folder_id,
            tags=tags,
            category=category,
            notes=notes,
            priority=priority,
            like_count=kwargs.get("like_count", 0),
            retweet_count=kwargs.get("retweet_count", 0),
            reply_count=kwargs.get("reply_count", 0),
            created_at=kwargs.get("created_at", ""),
            is_thread=kwargs.get("is_thread", False),
            thread_length=kwargs.get("thread_length", 1),
        )

        conn = self._get_conn()

        # Check for duplicate
        existing = conn.execute(
            "SELECT bookmark_id FROM bookmarks WHERE tweet_id = ?",
            (tweet_id,)
        ).fetchone()
        if existing:
            conn.close()
            bookmark.bookmark_id = existing["bookmark_id"]
            return bookmark

        conn.execute("""
            INSERT INTO bookmarks
            (bookmark_id, tweet_id, author_username, author_name, text, url,
             status, folder_id, tags, category, notes, reading_time_seconds,
             is_thread, thread_length, like_count, retweet_count, reply_count,
             created_at, bookmarked_at, priority)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            bookmark.bookmark_id, bookmark.tweet_id,
            bookmark.author_username, bookmark.author_name,
            bookmark.text, bookmark.url,
            bookmark.status.value, bookmark.folder_id,
            json.dumps(bookmark.tags), bookmark.category.value,
            bookmark.notes, bookmark.reading_time_seconds,
            int(bookmark.is_thread), bookmark.thread_length,
            bookmark.like_count, bookmark.retweet_count,
            bookmark.reply_count, bookmark.created_at,
            bookmark.bookmarked_at, bookmark.priority,
        ))

        # Update FTS index
        conn.execute("""
            INSERT INTO bookmarks_fts
            (bookmark_id, text, author_username, tags, notes)
            VALUES (?, ?, ?, ?, ?)
        """, (bookmark.bookmark_id, bookmark.text,
              bookmark.author_username, ' '.join(bookmark.tags),
              bookmark.notes))

        conn.commit()
        conn.close()
        return bookmark

    def get_bookmark(self, bookmark_id: str) -> Optional[Bookmark]:
        """Get a bookmark by ID."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM bookmarks WHERE bookmark_id = ?",
            (bookmark_id,)
        ).fetchone()
        conn.close()

        if not row:
            return None
        return self._row_to_bookmark(row)

    def get_by_tweet_id(self, tweet_id: str) -> Optional[Bookmark]:
        """Get bookmark by tweet ID."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM bookmarks WHERE tweet_id = ?",
            (tweet_id,)
        ).fetchone()
        conn.close()

        if not row:
            return None
        return self._row_to_bookmark(row)

    def update_status(self, bookmark_id: str,
                      status: BookmarkStatus) -> bool:
        """Update bookmark status."""
        conn = self._get_conn()
        update_fields = {"status": status.value}
        if status == BookmarkStatus.READ:
            update_fields["read_at"] = datetime.now(timezone.utc).isoformat()

        set_clause = ", ".join(f"{k} = ?" for k in update_fields)
        result = conn.execute(
            f"UPDATE bookmarks SET {set_clause} WHERE bookmark_id = ?",
            (*update_fields.values(), bookmark_id)
        )
        conn.commit()
        updated = result.rowcount > 0
        conn.close()
        return updated

    def add_tags(self, bookmark_id: str, new_tags: List[str]) -> bool:
        """Add tags to a bookmark."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT tags FROM bookmarks WHERE bookmark_id = ?",
            (bookmark_id,)
        ).fetchone()
        if not row:
            conn.close()
            return False

        existing = json.loads(row["tags"]) if row["tags"] else []
        merged = list(dict.fromkeys(existing + new_tags))

        conn.execute(
            "UPDATE bookmarks SET tags = ? WHERE bookmark_id = ?",
            (json.dumps(merged), bookmark_id)
        )
        conn.commit()
        conn.close()
        return True

    def move_to_folder(self, bookmark_id: str,
                       folder_id: Optional[str]) -> bool:
        """Move bookmark to a folder (None = unfiled)."""
        conn = self._get_conn()
        result = conn.execute(
            "UPDATE bookmarks SET folder_id = ? WHERE bookmark_id = ?",
            (folder_id, bookmark_id)
        )
        conn.commit()
        updated = result.rowcount > 0
        conn.close()
        return updated

    def add_note(self, bookmark_id: str, note: str) -> bool:
        """Add/update note on a bookmark."""
        conn = self._get_conn()
        result = conn.execute(
            "UPDATE bookmarks SET notes = ? WHERE bookmark_id = ?",
            (note, bookmark_id)
        )
        conn.commit()
        updated = result.rowcount > 0
        conn.close()
        return updated

    def delete_bookmark(self, bookmark_id: str) -> bool:
        """Soft-delete a bookmark."""
        return self.update_status(bookmark_id, BookmarkStatus.DELETED)

    def hard_delete(self, bookmark_id: str) -> bool:
        """Permanently delete a bookmark."""
        conn = self._get_conn()
        conn.execute(
            "DELETE FROM bookmark_reminders WHERE bookmark_id = ?",
            (bookmark_id,)
        )
        result = conn.execute(
            "DELETE FROM bookmarks WHERE bookmark_id = ?",
            (bookmark_id,)
        )
        conn.commit()
        deleted = result.rowcount > 0
        conn.close()
        return deleted

    def list_bookmarks(self, status: Optional[BookmarkStatus] = None,
                       category: Optional[ContentCategory] = None,
                       folder_id: Optional[str] = None,
                       tag: Optional[str] = None,
                       limit: int = 50,
                       offset: int = 0,
                       order_by: str = "bookmarked_at DESC") -> List[Bookmark]:
        """List bookmarks with filters."""
        conn = self._get_conn()
        conditions = ["status != 'deleted'"]
        params: list = []

        if status:
            conditions.append("status = ?")
            params.append(status.value)
        if category:
            conditions.append("category = ?")
            params.append(category.value)
        if folder_id:
            conditions.append("folder_id = ?")
            params.append(folder_id)
        if tag:
            conditions.append("tags LIKE ?")
            params.append(f'%"{tag}"%')

        where = " AND ".join(conditions)
        # Whitelist order_by to prevent injection
        allowed_orders = {
            "bookmarked_at DESC", "bookmarked_at ASC",
            "priority DESC", "like_count DESC",
            "reading_time_seconds ASC",
        }
        if order_by not in allowed_orders:
            order_by = "bookmarked_at DESC"

        rows = conn.execute(
            f"SELECT * FROM bookmarks WHERE {where} ORDER BY {order_by} LIMIT ? OFFSET ?",
            (*params, limit, offset)
        ).fetchall()
        conn.close()

        return [self._row_to_bookmark(r) for r in rows]

    def search(self, query: str, limit: int = 20) -> List[Bookmark]:
        """Full-text search across bookmarks."""
        conn = self._get_conn()
        # Use FTS5 for search
        fts_rows = conn.execute("""
            SELECT bookmark_id FROM bookmarks_fts
            WHERE bookmarks_fts MATCH ?
            LIMIT ?
        """, (query, limit)).fetchall()

        if not fts_rows:
            conn.close()
            return []

        ids = [r["bookmark_id"] for r in fts_rows]
        placeholders = ",".join("?" * len(ids))
        rows = conn.execute(
            f"SELECT * FROM bookmarks WHERE bookmark_id IN ({placeholders})",
            ids
        ).fetchall()
        conn.close()

        return [self._row_to_bookmark(r) for r in rows]

    def find_duplicates(self) -> List[Tuple[Bookmark, Bookmark]]:
        """Find potential duplicate bookmarks."""
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT * FROM bookmarks WHERE status != 'deleted'
            ORDER BY bookmarked_at ASC
        """).fetchall()
        conn.close()

        bookmarks = [self._row_to_bookmark(r) for r in rows]
        duplicates = []
        seen_texts: Dict[str, Bookmark] = {}

        for b in bookmarks:
            # Normalize text for comparison
            normalized = re.sub(r'\s+', ' ', b.text.lower().strip())[:100]
            if normalized in seen_texts:
                duplicates.append((seen_texts[normalized], b))
            else:
                seen_texts[normalized] = b

        return duplicates

    # Folder management

    def create_folder(self, name: str, description: str = "",
                      color: str = "#3498db", icon: str = "📁",
                      parent_id: Optional[str] = None) -> BookmarkFolder:
        """Create a new folder."""
        folder = BookmarkFolder(
            name=name, description=description,
            color=color, icon=icon, parent_id=parent_id,
        )

        conn = self._get_conn()
        conn.execute("""
            INSERT INTO folders
            (folder_id, name, description, color, icon, parent_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (folder.folder_id, folder.name, folder.description,
              folder.color, folder.icon, folder.parent_id, folder.created_at))
        conn.commit()
        conn.close()
        return folder

    def list_folders(self) -> List[Dict[str, Any]]:
        """List all folders with bookmark counts."""
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT f.*, COUNT(b.bookmark_id) as bookmark_count
            FROM folders f
            LEFT JOIN bookmarks b ON f.folder_id = b.folder_id AND b.status != 'deleted'
            GROUP BY f.folder_id
            ORDER BY f.name
        """).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def delete_folder(self, folder_id: str,
                      move_bookmarks_to: Optional[str] = None) -> bool:
        """Delete a folder, optionally moving bookmarks."""
        conn = self._get_conn()
        if move_bookmarks_to is not None:
            conn.execute(
                "UPDATE bookmarks SET folder_id = ? WHERE folder_id = ?",
                (move_bookmarks_to, folder_id)
            )
        else:
            conn.execute(
                "UPDATE bookmarks SET folder_id = NULL WHERE folder_id = ?",
                (folder_id,)
            )

        result = conn.execute("DELETE FROM folders WHERE folder_id = ?", (folder_id,))
        conn.commit()
        deleted = result.rowcount > 0
        conn.close()
        return deleted

    # Analytics

    def get_stats(self) -> Dict[str, Any]:
        """Get bookmark statistics."""
        conn = self._get_conn()

        total = conn.execute(
            "SELECT COUNT(*) as c FROM bookmarks WHERE status != 'deleted'"
        ).fetchone()["c"]
        unread = conn.execute(
            "SELECT COUNT(*) as c FROM bookmarks WHERE status = 'unread'"
        ).fetchone()["c"]
        read = conn.execute(
            "SELECT COUNT(*) as c FROM bookmarks WHERE status = 'read'"
        ).fetchone()["c"]
        starred = conn.execute(
            "SELECT COUNT(*) as c FROM bookmarks WHERE status = 'starred'"
        ).fetchone()["c"]
        archived = conn.execute(
            "SELECT COUNT(*) as c FROM bookmarks WHERE status = 'archived'"
        ).fetchone()["c"]

        # Category distribution
        cats = conn.execute("""
            SELECT category, COUNT(*) as c
            FROM bookmarks WHERE status != 'deleted'
            GROUP BY category ORDER BY c DESC
        """).fetchall()

        # Total reading time
        reading_time = conn.execute("""
            SELECT SUM(reading_time_seconds) as total
            FROM bookmarks WHERE status = 'unread'
        """).fetchone()["total"] or 0

        # Top authors
        top_authors = conn.execute("""
            SELECT author_username, COUNT(*) as c
            FROM bookmarks WHERE status != 'deleted' AND author_username != ''
            GROUP BY author_username ORDER BY c DESC LIMIT 10
        """).fetchall()

        # Recent bookmark rate
        week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        this_week = conn.execute(
            "SELECT COUNT(*) as c FROM bookmarks WHERE bookmarked_at > ?",
            (week_ago,)
        ).fetchone()["c"]

        conn.close()

        return {
            "total": total,
            "unread": unread,
            "read": read,
            "starred": starred,
            "archived": archived,
            "read_rate_pct": round(read / max(total, 1) * 100, 1),
            "categories": {r["category"]: r["c"] for r in cats},
            "unread_reading_time_minutes": round(reading_time / 60, 1),
            "top_authors": [{"username": r["author_username"], "count": r["c"]}
                           for r in top_authors],
            "bookmarks_this_week": this_week,
            "avg_per_day": round(this_week / 7, 1),
        }

    def get_reading_queue(self, max_minutes: int = 30) -> List[Bookmark]:
        """Get optimized reading queue that fits in given time."""
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT * FROM bookmarks
            WHERE status = 'unread'
            ORDER BY priority DESC, like_count DESC
        """).fetchall()
        conn.close()

        bookmarks = [self._row_to_bookmark(r) for r in rows]
        queue = []
        total_time = 0
        max_seconds = max_minutes * 60

        for b in bookmarks:
            if total_time + b.reading_time_seconds <= max_seconds:
                queue.append(b)
                total_time += b.reading_time_seconds

        return queue

    # Reminders

    def set_reminder(self, bookmark_id: str, remind_at: str) -> bool:
        """Set a reminder to revisit a bookmark."""
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO bookmark_reminders (bookmark_id, remind_at)
            VALUES (?, ?)
        """, (bookmark_id, remind_at))
        conn.commit()
        conn.close()
        return True

    def get_due_reminders(self) -> List[Dict[str, Any]]:
        """Get reminders that are due."""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        rows = conn.execute("""
            SELECT r.*, b.text, b.author_username, b.url
            FROM bookmark_reminders r
            JOIN bookmarks b ON r.bookmark_id = b.bookmark_id
            WHERE r.remind_at <= ? AND r.reminded = 0
        """, (now,)).fetchall()

        # Mark as reminded
        for r in rows:
            conn.execute(
                "UPDATE bookmark_reminders SET reminded = 1 WHERE id = ?",
                (r["id"],)
            )

        conn.commit()
        conn.close()
        return [dict(r) for r in rows]

    # Export

    def export_bookmarks(self, fmt: ExportFormat = ExportFormat.JSON,
                         status: Optional[BookmarkStatus] = None,
                         category: Optional[ContentCategory] = None) -> str:
        """Export bookmarks to specified format."""
        bookmarks = self.list_bookmarks(status=status, category=category,
                                         limit=10000)
        return self.exporter.export(bookmarks, fmt)

    # Random resurface

    def resurface_random(self, count: int = 3) -> List[Bookmark]:
        """Resurface random old bookmarks for rediscovery."""
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT * FROM bookmarks
            WHERE status IN ('read', 'archived')
            ORDER BY RANDOM()
            LIMIT ?
        """, (count,)).fetchall()
        conn.close()
        return [self._row_to_bookmark(r) for r in rows]

    def _row_to_bookmark(self, row: sqlite3.Row) -> Bookmark:
        """Convert DB row to Bookmark object."""
        return Bookmark(
            bookmark_id=row["bookmark_id"],
            tweet_id=row["tweet_id"],
            author_username=row["author_username"] or "",
            author_name=row["author_name"] or "",
            text=row["text"],
            url=row["url"] or "",
            status=BookmarkStatus(row["status"]),
            folder_id=row["folder_id"],
            tags=json.loads(row["tags"]) if row["tags"] else [],
            category=ContentCategory(row["category"]),
            notes=row["notes"] or "",
            reading_time_seconds=row["reading_time_seconds"],
            is_thread=bool(row["is_thread"]),
            thread_length=row["thread_length"],
            like_count=row["like_count"],
            retweet_count=row["retweet_count"],
            reply_count=row["reply_count"],
            created_at=row["created_at"] or "",
            bookmarked_at=row["bookmarked_at"] or "",
            read_at=row["read_at"],
            priority=row["priority"],
        )
