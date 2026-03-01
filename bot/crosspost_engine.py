"""
Cross-Platform Posting Engine
------------------------------
Adapts Twitter content for cross-posting to LinkedIn, Threads, Mastodon, and Bluesky.
Platform-specific formatting, character limits, media handling, and scheduling.
"""

import re
import json
import sqlite3
import hashlib
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional
from collections import defaultdict


class Platform(Enum):
    TWITTER = "twitter"
    LINKEDIN = "linkedin"
    THREADS = "threads"
    MASTODON = "mastodon"
    BLUESKY = "bluesky"


class ContentType(Enum):
    TEXT = "text"
    THREAD = "thread"
    IMAGE = "image"
    VIDEO = "video"
    POLL = "poll"
    LINK = "link"
    CAROUSEL = "carousel"


class PostStatus(Enum):
    DRAFT = "draft"
    SCHEDULED = "scheduled"
    PUBLISHING = "publishing"
    PUBLISHED = "published"
    FAILED = "failed"
    PARTIAL = "partial"  # Some platforms succeeded


PLATFORM_LIMITS = {
    Platform.TWITTER: {
        "char_limit": 280,
        "thread_max": 25,
        "images_max": 4,
        "video_max": 1,
        "link_shortens": True,
        "link_cost": 23,  # t.co shortening
        "hashtag_limit": 30,
        "supports_polls": True,
        "supports_threads": True,
        "supports_alt_text": True,
    },
    Platform.LINKEDIN: {
        "char_limit": 3000,
        "thread_max": 1,
        "images_max": 20,
        "video_max": 1,
        "link_shortens": False,
        "link_cost": 0,
        "hashtag_limit": 30,
        "supports_polls": True,
        "supports_threads": False,
        "supports_alt_text": True,
    },
    Platform.THREADS: {
        "char_limit": 500,
        "thread_max": 10,
        "images_max": 10,
        "video_max": 1,
        "link_shortens": False,
        "link_cost": 0,
        "hashtag_limit": 30,
        "supports_polls": False,
        "supports_threads": True,
        "supports_alt_text": True,
    },
    Platform.MASTODON: {
        "char_limit": 500,
        "thread_max": 50,
        "images_max": 4,
        "video_max": 1,
        "link_shortens": False,
        "link_cost": 23,  # Mastodon counts links as 23 chars
        "hashtag_limit": 30,
        "supports_polls": True,
        "supports_threads": True,
        "supports_alt_text": True,
    },
    Platform.BLUESKY: {
        "char_limit": 300,
        "thread_max": 50,
        "images_max": 4,
        "video_max": 1,
        "link_shortens": False,
        "link_cost": 0,  # Links shown as cards, don't count
        "hashtag_limit": 10,
        "supports_polls": False,
        "supports_threads": True,
        "supports_alt_text": True,
    },
}


@dataclass
class MediaAttachment:
    """Media file for cross-posting."""
    file_path: str
    media_type: str = "image"  # image, video, gif
    alt_text: str = ""
    width: int = 0
    height: int = 0
    file_size: int = 0


@dataclass
class CrossPost:
    """A post adapted for a specific platform."""
    post_id: str
    source_id: str  # Original post ID
    platform: Platform
    content: str
    content_parts: list[str] = field(default_factory=list)  # For threads
    media: list[MediaAttachment] = field(default_factory=list)
    hashtags: list[str] = field(default_factory=list)
    link: str = ""
    status: PostStatus = PostStatus.DRAFT
    platform_post_id: Optional[str] = None  # ID on the target platform
    published_at: Optional[str] = None
    error: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class SourceContent:
    """Original content to be cross-posted."""
    source_id: str
    content: str
    content_type: ContentType = ContentType.TEXT
    thread_parts: list[str] = field(default_factory=list)
    media: list[MediaAttachment] = field(default_factory=list)
    hashtags: list[str] = field(default_factory=list)
    link: str = ""
    target_platforms: list[Platform] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class ContentAdapter:
    """Adapts content for each platform's format and limits."""

    def __init__(self):
        self._tone_maps = {
            Platform.TWITTER: "casual",
            Platform.LINKEDIN: "professional",
            Platform.THREADS: "conversational",
            Platform.MASTODON: "community",
            Platform.BLUESKY: "casual",
        }

    def adapt(self, source: SourceContent, platform: Platform) -> CrossPost:
        """Adapt source content for a target platform."""
        limits = PLATFORM_LIMITS[platform]
        post_id = hashlib.md5(
            f"{source.source_id}:{platform.value}".encode()
        ).hexdigest()[:12]

        # Adapt content based on platform
        adapted_content = self._adapt_text(source.content, platform, limits)
        adapted_hashtags = self._adapt_hashtags(source.hashtags, platform, limits)
        content_parts = []

        if source.content_type == ContentType.THREAD or source.thread_parts:
            parts = source.thread_parts or [source.content]
            if limits["supports_threads"]:
                content_parts = [
                    self._adapt_text(part, platform, limits)
                    for part in parts[:limits["thread_max"]]
                ]
            else:
                # Merge thread into single post
                merged = self._merge_thread(parts, limits["char_limit"])
                adapted_content = merged
        else:
            # Single post - might need splitting for short-limit platforms
            if len(adapted_content) > limits["char_limit"]:
                if limits["supports_threads"]:
                    content_parts = self._split_to_thread(
                        adapted_content, limits["char_limit"]
                    )
                    adapted_content = content_parts[0] if content_parts else adapted_content
                else:
                    adapted_content = self._truncate_smart(
                        adapted_content, limits["char_limit"]
                    )

        # Add hashtags to content
        if adapted_hashtags:
            hashtag_text = " ".join(f"#{h}" for h in adapted_hashtags)
            if content_parts:
                # Add to last part of thread
                last = content_parts[-1]
                if len(last) + len(hashtag_text) + 2 <= limits["char_limit"]:
                    content_parts[-1] = f"{last}\n\n{hashtag_text}"
            else:
                if len(adapted_content) + len(hashtag_text) + 2 <= limits["char_limit"]:
                    adapted_content = f"{adapted_content}\n\n{hashtag_text}"

        return CrossPost(
            post_id=post_id,
            source_id=source.source_id,
            platform=platform,
            content=adapted_content,
            content_parts=content_parts,
            media=source.media[:limits["images_max"]],
            hashtags=adapted_hashtags,
            link=source.link,
        )

    def _adapt_text(self, text: str, platform: Platform, limits: dict) -> str:
        """Adapt text for platform-specific conventions."""
        adapted = text

        if platform == Platform.LINKEDIN:
            # LinkedIn: more professional, use line breaks for readability
            adapted = self._professionalize(adapted)
            # Add hook + spacing pattern
            lines = adapted.split("\n")
            if len(lines) > 1:
                adapted = lines[0] + "\n\n" + "\n".join(lines[1:])

        elif platform == Platform.MASTODON:
            # Mastodon: hashtags as CamelCase, add CW if needed
            adapted = self._camelcase_hashtags(adapted)

        elif platform == Platform.BLUESKY:
            # Bluesky: shorter, no markdown, handle mentions
            adapted = self._strip_markdown(adapted)
            adapted = self._convert_mentions(adapted, "bluesky")

        elif platform == Platform.THREADS:
            # Threads: casual, emoji-friendly
            adapted = self._casualize(adapted)

        # Enforce character limit
        if len(adapted) > limits["char_limit"]:
            adapted = self._truncate_smart(adapted, limits["char_limit"])

        return adapted

    def _adapt_hashtags(self, hashtags: list[str], platform: Platform,
                        limits: dict) -> list[str]:
        """Adapt hashtags for platform conventions."""
        adapted = hashtags[:limits["hashtag_limit"]]

        if platform == Platform.LINKEDIN:
            # LinkedIn: 3-5 hashtags max for best performance
            adapted = adapted[:5]
        elif platform == Platform.BLUESKY:
            # Bluesky: fewer hashtags
            adapted = adapted[:3]
        elif platform == Platform.MASTODON:
            # Mastodon: CamelCase
            adapted = [self._to_camel_case(h) for h in adapted]

        return adapted

    def _professionalize(self, text: str) -> str:
        """Make text more LinkedIn-appropriate."""
        # Remove casual Twitter elements
        text = re.sub(r'\bimo\b', 'in my opinion', text, flags=re.IGNORECASE)
        text = re.sub(r'\btbh\b', 'to be honest', text, flags=re.IGNORECASE)
        text = re.sub(r'\bngl\b', 'I must say', text, flags=re.IGNORECASE)
        text = re.sub(r'\bw/', 'with', text)
        return text

    def _casualize(self, text: str) -> str:
        """Make text more conversational for Threads."""
        return text

    def _camelcase_hashtags(self, text: str) -> str:
        """Convert #hashtags to #CamelCase in text."""

        def _camel(match):
            tag = match.group(1)
            if tag.isupper() or "_" in tag:
                parts = tag.lower().split("_")
                return "#" + "".join(p.capitalize() for p in parts)
            return match.group(0)

        return re.sub(r'#(\w+)', _camel, text)

    def _to_camel_case(self, text: str) -> str:
        """Convert a string to CamelCase."""
        parts = re.split(r'[_\s-]', text.lower())
        return "".join(p.capitalize() for p in parts)

    def _strip_markdown(self, text: str) -> str:
        """Remove markdown formatting."""
        text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
        text = re.sub(r'\*(.+?)\*', r'\1', text)
        text = re.sub(r'`(.+?)`', r'\1', text)
        text = re.sub(r'\[(.+?)\]\(.+?\)', r'\1', text)
        return text

    def _convert_mentions(self, text: str, platform: str) -> str:
        """Convert @mentions between platforms."""
        # Twitter @handle → platform equivalent
        # For now, keep as-is (cross-platform handles differ)
        return text

    def _truncate_smart(self, text: str, limit: int) -> str:
        """Truncate text at sentence boundary, add ellipsis."""
        if len(text) <= limit:
            return text

        truncated = text[:limit - 3]

        # Try to break at sentence boundary
        for sep in [". ", "! ", "? ", "\n"]:
            idx = truncated.rfind(sep)
            if idx > limit * 0.5:
                return truncated[:idx + 1].rstrip()

        # Break at word boundary
        idx = truncated.rfind(" ")
        if idx > limit * 0.5:
            return truncated[:idx] + "..."

        return truncated + "..."

    def _split_to_thread(self, text: str, char_limit: int) -> list[str]:
        """Split long text into thread-sized parts."""
        sentences = re.split(r'(?<=[.!?])\s+', text)
        parts = []
        current = ""

        # Reserve space for thread numbering (e.g., "1/5 ")
        effective_limit = char_limit - 6

        for sentence in sentences:
            if len(current) + len(sentence) + 1 <= effective_limit:
                current = f"{current} {sentence}".strip() if current else sentence
            else:
                if current:
                    parts.append(current)
                if len(sentence) > effective_limit:
                    # Force-split very long sentences
                    while sentence:
                        parts.append(sentence[:effective_limit])
                        sentence = sentence[effective_limit:]
                else:
                    current = sentence

        if current:
            parts.append(current)

        # Add thread numbering
        total = len(parts)
        if total > 1:
            parts = [f"{i + 1}/{total} {p}" for i, p in enumerate(parts)]

        return parts

    def _merge_thread(self, parts: list[str], char_limit: int) -> str:
        """Merge thread parts into a single post."""
        # Remove thread numbering (1/N, 2/N, etc.)
        cleaned = []
        for part in parts:
            cleaned_part = re.sub(r'^\d+/\d+\s*', '', part).strip()
            if cleaned_part:
                cleaned.append(cleaned_part)

        merged = "\n\n".join(cleaned)
        if len(merged) > char_limit:
            merged = self._truncate_smart(merged, char_limit)
        return merged


class CrossPostStore:
    """SQLite persistence for cross-posting data."""

    def __init__(self, db_path: str = "crosspost.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS source_content (
                    source_id TEXT PRIMARY KEY,
                    content TEXT NOT NULL,
                    content_type TEXT DEFAULT 'text',
                    thread_parts TEXT DEFAULT '[]',
                    hashtags TEXT DEFAULT '[]',
                    link TEXT DEFAULT '',
                    target_platforms TEXT DEFAULT '[]',
                    created_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cross_posts (
                    post_id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    content TEXT NOT NULL,
                    content_parts TEXT DEFAULT '[]',
                    hashtags TEXT DEFAULT '[]',
                    link TEXT DEFAULT '',
                    status TEXT DEFAULT 'draft',
                    platform_post_id TEXT,
                    published_at TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (source_id) REFERENCES source_content(source_id)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS post_analytics (
                    post_id TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    likes INTEGER DEFAULT 0,
                    shares INTEGER DEFAULT 0,
                    comments INTEGER DEFAULT 0,
                    impressions INTEGER DEFAULT 0,
                    clicks INTEGER DEFAULT 0,
                    engagement_rate REAL DEFAULT 0.0,
                    recorded_at TEXT NOT NULL,
                    PRIMARY KEY (post_id, recorded_at)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS platform_config (
                    platform TEXT PRIMARY KEY,
                    enabled INTEGER DEFAULT 1,
                    credentials TEXT DEFAULT '{}',
                    settings TEXT DEFAULT '{}',
                    last_posted TEXT,
                    post_count INTEGER DEFAULT 0
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_cp_source ON cross_posts(source_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_cp_platform ON cross_posts(platform)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_cp_status ON cross_posts(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pa_platform ON post_analytics(platform)")
            conn.commit()

    def save_source(self, source: SourceContent) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO source_content
                (source_id, content, content_type, thread_parts, hashtags, link,
                 target_platforms, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (source.source_id, source.content, source.content_type.value,
                  json.dumps(source.thread_parts), json.dumps(source.hashtags),
                  source.link, json.dumps([p.value for p in source.target_platforms]),
                  source.created_at))
            conn.commit()

    def save_cross_post(self, post: CrossPost) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO cross_posts
                (post_id, source_id, platform, content, content_parts, hashtags,
                 link, status, platform_post_id, published_at, error, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (post.post_id, post.source_id, post.platform.value, post.content,
                  json.dumps(post.content_parts), json.dumps(post.hashtags),
                  post.link, post.status.value, post.platform_post_id,
                  post.published_at, post.error, post.created_at))
            conn.commit()

    def get_cross_posts(self, source_id: str) -> list[CrossPost]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM cross_posts WHERE source_id = ?", (source_id,)
            ).fetchall()
            return [self._row_to_crosspost(r) for r in rows]

    def get_posts_by_status(self, status: PostStatus) -> list[CrossPost]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM cross_posts WHERE status = ?", (status.value,)
            ).fetchall()
            return [self._row_to_crosspost(r) for r in rows]

    def _row_to_crosspost(self, r) -> CrossPost:
        return CrossPost(
            post_id=r["post_id"], source_id=r["source_id"],
            platform=Platform(r["platform"]), content=r["content"],
            content_parts=json.loads(r["content_parts"]),
            hashtags=json.loads(r["hashtags"]),
            link=r["link"], status=PostStatus(r["status"]),
            platform_post_id=r["platform_post_id"],
            published_at=r["published_at"], error=r["error"],
            created_at=r["created_at"],
        )

    def record_analytics(self, post_id: str, platform: str, likes: int = 0,
                         shares: int = 0, comments: int = 0, impressions: int = 0,
                         clicks: int = 0) -> None:
        eng_rate = ((likes + shares + comments) / impressions * 100) if impressions > 0 else 0
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO post_analytics
                (post_id, platform, likes, shares, comments, impressions, clicks,
                 engagement_rate, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (post_id, platform, likes, shares, comments, impressions, clicks,
                  round(eng_rate, 2), datetime.now(timezone.utc).isoformat()))
            conn.commit()

    def get_platform_performance(self, days: int = 30) -> dict[str, dict]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT platform,
                    SUM(likes) as total_likes,
                    SUM(shares) as total_shares,
                    SUM(comments) as total_comments,
                    SUM(impressions) as total_impressions,
                    SUM(clicks) as total_clicks,
                    AVG(engagement_rate) as avg_engagement,
                    COUNT(DISTINCT post_id) as post_count
                FROM post_analytics
                WHERE recorded_at >= ?
                GROUP BY platform
            """, (cutoff,)).fetchall()
            return {
                r["platform"]: {
                    "likes": r["total_likes"],
                    "shares": r["total_shares"],
                    "comments": r["total_comments"],
                    "impressions": r["total_impressions"],
                    "clicks": r["total_clicks"],
                    "avg_engagement": round(r["avg_engagement"], 2),
                    "post_count": r["post_count"],
                }
                for r in rows
            }

    def get_total_stats(self) -> dict:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("""
                SELECT
                    COUNT(DISTINCT source_id) as total_sources,
                    COUNT(*) as total_crossposts,
                    SUM(CASE WHEN status = 'published' THEN 1 ELSE 0 END) as published,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                    COUNT(DISTINCT platform) as platforms_used
                FROM cross_posts
            """).fetchone()
            return dict(row)


class CrossPostEngine:
    """Main orchestrator for cross-platform posting."""

    def __init__(self, db_path: str = "crosspost.db"):
        self.store = CrossPostStore(db_path)
        self.adapter = ContentAdapter()
        self._enabled_platforms: set[Platform] = {
            Platform.TWITTER, Platform.LINKEDIN, Platform.THREADS,
            Platform.MASTODON, Platform.BLUESKY,
        }

    def enable_platform(self, platform: Platform) -> None:
        self._enabled_platforms.add(platform)

    def disable_platform(self, platform: Platform) -> None:
        self._enabled_platforms.discard(platform)

    def get_enabled_platforms(self) -> list[Platform]:
        return sorted(self._enabled_platforms, key=lambda p: p.value)

    def create_crosspost(self, content: str, content_type: ContentType = ContentType.TEXT,
                         thread_parts: Optional[list[str]] = None,
                         hashtags: Optional[list[str]] = None,
                         link: str = "",
                         media: Optional[list[MediaAttachment]] = None,
                         platforms: Optional[list[Platform]] = None) -> dict[Platform, CrossPost]:
        """Create adapted cross-posts for all target platforms."""
        source_id = hashlib.md5(
            f"{content}:{datetime.now(timezone.utc).isoformat()}".encode()
        ).hexdigest()[:12]

        target_platforms = platforms or list(self._enabled_platforms)

        source = SourceContent(
            source_id=source_id,
            content=content,
            content_type=content_type,
            thread_parts=thread_parts or [],
            media=media or [],
            hashtags=hashtags or [],
            link=link,
            target_platforms=target_platforms,
        )
        self.store.save_source(source)

        cross_posts = {}
        for platform in target_platforms:
            if platform in self._enabled_platforms:
                post = self.adapter.adapt(source, platform)
                self.store.save_cross_post(post)
                cross_posts[platform] = post

        return cross_posts

    def preview_crosspost(self, content: str,
                          platform: Platform,
                          hashtags: Optional[list[str]] = None) -> str:
        """Preview how content will look on a specific platform."""
        source = SourceContent(
            source_id="preview",
            content=content,
            hashtags=hashtags or [],
        )
        post = self.adapter.adapt(source, platform)

        limits = PLATFORM_LIMITS[platform]
        char_count = len(post.content)
        char_limit = limits["char_limit"]

        preview = [
            f"📱 Preview: {platform.value.upper()}",
            f"{'─' * 40}",
            post.content,
            f"{'─' * 40}",
            f"Characters: {char_count}/{char_limit}",
        ]

        if post.content_parts:
            preview.append(f"Thread parts: {len(post.content_parts)}")

        if post.hashtags:
            preview.append(f"Hashtags: {' '.join('#' + h for h in post.hashtags)}")

        return "\n".join(preview)

    def get_post_status(self, source_id: str) -> dict:
        """Get cross-posting status for a source post."""
        posts = self.store.get_cross_posts(source_id)
        status_map = {}
        for post in posts:
            emoji = {
                PostStatus.PUBLISHED: "✅",
                PostStatus.FAILED: "❌",
                PostStatus.PUBLISHING: "⏳",
                PostStatus.SCHEDULED: "📅",
                PostStatus.DRAFT: "📝",
                PostStatus.PARTIAL: "⚠️",
            }.get(post.status, "❓")
            status_map[post.platform.value] = {
                "status": post.status.value,
                "emoji": emoji,
                "post_id": post.post_id,
                "platform_post_id": post.platform_post_id,
                "error": post.error,
            }
        return status_map

    def mark_published(self, post_id: str, platform_post_id: str) -> None:
        """Mark a cross-post as successfully published."""
        posts = self.store.get_posts_by_status(PostStatus.DRAFT)
        posts += self.store.get_posts_by_status(PostStatus.SCHEDULED)
        posts += self.store.get_posts_by_status(PostStatus.PUBLISHING)
        for post in posts:
            if post.post_id == post_id:
                post.status = PostStatus.PUBLISHED
                post.platform_post_id = platform_post_id
                post.published_at = datetime.now(timezone.utc).isoformat()
                self.store.save_cross_post(post)
                break

    def mark_failed(self, post_id: str, error: str) -> None:
        """Mark a cross-post as failed."""
        for status in [PostStatus.DRAFT, PostStatus.SCHEDULED, PostStatus.PUBLISHING]:
            for post in self.store.get_posts_by_status(status):
                if post.post_id == post_id:
                    post.status = PostStatus.FAILED
                    post.error = error
                    self.store.save_cross_post(post)
                    return

    def compare_platforms(self, days: int = 30) -> str:
        """Generate platform comparison report."""
        perf = self.store.get_platform_performance(days)
        stats = self.store.get_total_stats()

        lines = [
            f"📊 Cross-Platform Performance ({days} days)",
            f"{'='*50}",
            f"Total Sources: {stats['total_sources']} | Cross-Posts: {stats['total_crossposts']}",
            f"Published: {stats['published']} | Failed: {stats['failed']}",
            f"",
        ]

        if not perf:
            lines.append("No analytics data yet.")
            return "\n".join(lines)

        # Sort by engagement
        sorted_platforms = sorted(perf.items(), key=lambda x: x[1]["avg_engagement"], reverse=True)

        for platform, data in sorted_platforms:
            medal = {"1": "🥇", "2": "🥈", "3": "🥉"}.get(
                str(sorted_platforms.index((platform, data)) + 1), "  "
            )
            lines += [
                f"{medal} {platform.upper()}",
                f"   Posts: {data['post_count']} | Impressions: {data['impressions']:,}",
                f"   Likes: {data['likes']:,} | Shares: {data['shares']:,} | Comments: {data['comments']:,}",
                f"   Clicks: {data['clicks']:,} | Engagement: {data['avg_engagement']}%",
                f"",
            ]

        # Best platform
        if sorted_platforms:
            best = sorted_platforms[0]
            lines.append(f"🏆 Best Platform: {best[0].upper()} ({best[1]['avg_engagement']}% avg engagement)")

        return "\n".join(lines)

    def generate_report(self) -> str:
        """Generate cross-posting status report."""
        stats = self.store.get_total_stats()
        pending = self.store.get_posts_by_status(PostStatus.DRAFT)
        scheduled = self.store.get_posts_by_status(PostStatus.SCHEDULED)
        failed = self.store.get_posts_by_status(PostStatus.FAILED)

        lines = [
            f"🔄 Cross-Post Engine Report",
            f"{'='*45}",
            f"",
            f"📈 Overview:",
            f"  Sources: {stats['total_sources']}",
            f"  Cross-Posts: {stats['total_crossposts']}",
            f"  Published: {stats['published']}",
            f"  Failed: {stats['failed']}",
            f"  Platforms: {stats['platforms_used']}",
            f"",
            f"📝 Pending: {len(pending)} drafts",
            f"📅 Scheduled: {len(scheduled)}",
        ]

        if failed:
            lines += [f"", f"❌ Recent Failures:"]
            for f_post in failed[:5]:
                lines.append(f"  {f_post.platform.value}: {f_post.error or 'Unknown error'}")

        lines += [
            f"",
            f"🎯 Enabled Platforms: {', '.join(p.value for p in self.get_enabled_platforms())}",
        ]

        return "\n".join(lines)
