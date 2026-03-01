"""
Media Manager v1.0
推文媒体管理引擎 - 图片优化 + Alt Text生成 + 媒体调度 + 水印

Features:
- Image validation (format, size, dimensions, aspect ratio)
- Auto-resize for Twitter optimal display
- Alt text generation (template-based)
- Watermark stamping (text overlay)
- Media library with tagging & search
- Upload queue with retry
- GIF detection & handling
- Media usage analytics
"""

import os
import time
import json
import hashlib
import sqlite3
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from pathlib import Path


class MediaType(Enum):
    IMAGE = "image"
    GIF = "gif"
    VIDEO = "video"
    UNKNOWN = "unknown"


class MediaStatus(Enum):
    PENDING = "pending"
    VALIDATED = "validated"
    OPTIMIZED = "optimized"
    UPLOADED = "uploaded"
    FAILED = "failed"
    ARCHIVED = "archived"


# Twitter media constraints
TWITTER_LIMITS = {
    "image": {
        "max_size_bytes": 5 * 1024 * 1024,  # 5MB
        "formats": {"jpg", "jpeg", "png", "webp"},
        "max_width": 4096,
        "max_height": 4096,
        "optimal_width": 1200,
        "optimal_height": 675,  # 16:9
    },
    "gif": {
        "max_size_bytes": 15 * 1024 * 1024,  # 15MB
        "formats": {"gif"},
        "max_frames": 350,
        "max_pixels": 1280 * 1080,
    },
    "video": {
        "max_size_bytes": 512 * 1024 * 1024,  # 512MB
        "formats": {"mp4", "mov"},
        "max_duration_sec": 140,
        "min_duration_sec": 0.5,
        "max_width": 1920,
        "max_height": 1200,
    },
}


@dataclass
class MediaItem:
    """A media item in the library."""
    media_id: str
    file_path: str
    media_type: MediaType = MediaType.IMAGE
    status: MediaStatus = MediaStatus.PENDING
    alt_text: str = ""
    width: int = 0
    height: int = 0
    size_bytes: int = 0
    mime_type: str = ""
    duration_sec: float = 0
    tags: list = field(default_factory=list)
    twitter_media_id: str = ""
    checksum: str = ""
    created_at: float = field(default_factory=time.time)
    uploaded_at: float = 0
    upload_retries: int = 0
    metadata: dict = field(default_factory=dict)

    def is_image(self) -> bool:
        return self.media_type == MediaType.IMAGE

    def is_gif(self) -> bool:
        return self.media_type == MediaType.GIF

    def is_video(self) -> bool:
        return self.media_type == MediaType.VIDEO

    def aspect_ratio(self) -> float:
        if self.height == 0:
            return 0
        return round(self.width / self.height, 2)

    def summary(self) -> dict:
        return {
            "id": self.media_id,
            "type": self.media_type.value,
            "status": self.status.value,
            "size": f"{self.size_bytes / 1024:.1f}KB",
            "dimensions": f"{self.width}x{self.height}" if self.width else "unknown",
            "alt_text": self.alt_text[:50] + "..." if len(self.alt_text) > 50 else self.alt_text,
            "tags": self.tags,
        }


class AltTextGenerator:
    """Template-based alt text generator for accessibility."""

    TEMPLATES = {
        "product": "Photo of {product_name}, {color} color, {material} material, shown from {angle} angle",
        "person": "Photo of a person {action}",
        "chart": "Chart showing {metric} over {period}, trend is {direction}",
        "screenshot": "Screenshot of {app_name} showing {feature}",
        "meme": "Meme image with text: {text}",
        "infographic": "Infographic about {topic} with {count} data points",
        "logo": "Logo of {brand_name}",
        "landscape": "Landscape photo of {location}, featuring {elements}",
        "default": "Image related to {topic}",
    }

    @classmethod
    def generate(cls, template_key: str, **kwargs) -> str:
        template = cls.TEMPLATES.get(template_key, cls.TEMPLATES["default"])
        try:
            return template.format(**{k: v for k, v in kwargs.items() if v})
        except KeyError:
            filled = template
            for k, v in kwargs.items():
                filled = filled.replace(f"{{{k}}}", str(v))
            return filled

    @classmethod
    def from_filename(cls, filename: str) -> str:
        """Generate alt text from filename."""
        name = Path(filename).stem
        name = name.replace("_", " ").replace("-", " ")
        words = name.split()
        if len(words) <= 1:
            return f"Image: {name}"
        return f"Image showing {' '.join(words)}"

    @classmethod
    def list_templates(cls) -> list[str]:
        return list(cls.TEMPLATES.keys())


class MediaValidator:
    """Validate media against Twitter's requirements."""

    @staticmethod
    def detect_type(file_path: str) -> MediaType:
        ext = Path(file_path).suffix.lower().lstrip(".")
        if ext in {"jpg", "jpeg", "png", "webp", "bmp"}:
            return MediaType.IMAGE
        elif ext == "gif":
            return MediaType.GIF
        elif ext in {"mp4", "mov", "avi", "mkv"}:
            return MediaType.VIDEO
        return MediaType.UNKNOWN

    @staticmethod
    def validate(item: MediaItem) -> tuple[bool, list[str]]:
        """Validate a media item. Returns (valid, list_of_issues)."""
        issues = []
        limits = TWITTER_LIMITS.get(item.media_type.value, {})

        if not limits:
            issues.append(f"Unknown media type: {item.media_type.value}")
            return False, issues

        # Size check
        max_size = limits.get("max_size_bytes", 0)
        if max_size and item.size_bytes > max_size:
            issues.append(f"File too large: {item.size_bytes / 1024 / 1024:.1f}MB > {max_size / 1024 / 1024:.0f}MB limit")

        # Format check
        ext = Path(item.file_path).suffix.lower().lstrip(".")
        allowed = limits.get("formats", set())
        if allowed and ext not in allowed:
            issues.append(f"Format '{ext}' not in allowed: {allowed}")

        # Dimension checks
        if item.media_type == MediaType.IMAGE:
            if item.width > limits.get("max_width", 99999):
                issues.append(f"Width {item.width} exceeds max {limits['max_width']}")
            if item.height > limits.get("max_height", 99999):
                issues.append(f"Height {item.height} exceeds max {limits['max_height']}")

        # Video duration
        if item.media_type == MediaType.VIDEO:
            if item.duration_sec > limits.get("max_duration_sec", 999):
                issues.append(f"Duration {item.duration_sec}s exceeds max {limits['max_duration_sec']}s")
            if item.duration_sec < limits.get("min_duration_sec", 0):
                issues.append(f"Duration {item.duration_sec}s below min {limits['min_duration_sec']}s")

        return len(issues) == 0, issues

    @staticmethod
    def suggest_optimization(item: MediaItem) -> list[str]:
        """Suggest optimizations for a media item."""
        suggestions = []
        if item.media_type == MediaType.IMAGE:
            optimal = TWITTER_LIMITS["image"]
            if item.width > optimal["optimal_width"]:
                suggestions.append(f"Resize to {optimal['optimal_width']}px width for optimal display")
            ratio = item.aspect_ratio()
            if ratio and abs(ratio - 1.78) > 0.1:  # 16:9 = 1.78
                suggestions.append("Consider 16:9 aspect ratio (1200x675) for best timeline display")
            if item.size_bytes > 1024 * 1024:
                suggestions.append("Compress to <1MB for faster loading")
        if not item.alt_text:
            suggestions.append("Add alt text for accessibility")
        return suggestions


class MediaLibrary:
    """
    Persistent media library with tagging, search, and analytics.
    """

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.items: dict[str, MediaItem] = {}
        self._conn = sqlite3.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS media_library (
                media_id TEXT PRIMARY KEY,
                file_path TEXT NOT NULL,
                media_type TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                alt_text TEXT DEFAULT '',
                width INTEGER DEFAULT 0,
                height INTEGER DEFAULT 0,
                size_bytes INTEGER DEFAULT 0,
                checksum TEXT DEFAULT '',
                tags TEXT DEFAULT '[]',
                twitter_media_id TEXT DEFAULT '',
                created_at REAL,
                uploaded_at REAL DEFAULT 0
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS media_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                media_id TEXT NOT NULL,
                tweet_id TEXT,
                account_id TEXT,
                used_at REAL DEFAULT (strftime('%s','now'))
            )
        """)
        self._conn.commit()

    def add(self, item: MediaItem) -> bool:
        """Add a media item to the library."""
        if item.media_id in self.items:
            return False
        self.items[item.media_id] = item
        try:
            self._conn.execute(
                "INSERT OR REPLACE INTO media_library (media_id, file_path, media_type, status, alt_text, width, height, size_bytes, checksum, tags, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (item.media_id, item.file_path, item.media_type.value, item.status.value,
                 item.alt_text, item.width, item.height, item.size_bytes, item.checksum,
                 json.dumps(item.tags), item.created_at),
            )
            self._conn.commit()
        except Exception:
            pass
        return True

    def get(self, media_id: str) -> Optional[MediaItem]:
        return self.items.get(media_id)

    def remove(self, media_id: str) -> bool:
        if media_id not in self.items:
            return False
        del self.items[media_id]
        try:
            self._conn.execute("DELETE FROM media_library WHERE media_id=?", (media_id,))
            self._conn.commit()
        except Exception:
            pass
        return True

    def search_by_tag(self, tag: str) -> list[MediaItem]:
        return [i for i in self.items.values() if tag in i.tags]

    def search_by_type(self, media_type: MediaType) -> list[MediaItem]:
        return [i for i in self.items.values() if i.media_type == media_type]

    def search_by_status(self, status: MediaStatus) -> list[MediaItem]:
        return [i for i in self.items.values() if i.status == status]

    def get_unused(self) -> list[MediaItem]:
        """Get media items never used in tweets."""
        used_ids = set()
        try:
            rows = self._conn.execute("SELECT DISTINCT media_id FROM media_usage").fetchall()
            used_ids = {r[0] for r in rows}
        except Exception:
            pass
        return [i for i in self.items.values() if i.media_id not in used_ids]

    def record_usage(self, media_id: str, tweet_id: str = "", account_id: str = ""):
        try:
            self._conn.execute(
                "INSERT INTO media_usage (media_id, tweet_id, account_id) VALUES (?,?,?)",
                (media_id, tweet_id, account_id),
            )
            self._conn.commit()
        except Exception:
            pass

    def get_usage_count(self, media_id: str) -> int:
        try:
            count = self._conn.execute(
                "SELECT COUNT(*) FROM media_usage WHERE media_id=?", (media_id,)
            ).fetchone()[0]
            return count
        except Exception:
            return 0

    def get_stats(self) -> dict:
        """Library statistics."""
        by_type = {}
        by_status = {}
        total_size = 0
        for item in self.items.values():
            by_type[item.media_type.value] = by_type.get(item.media_type.value, 0) + 1
            by_status[item.status.value] = by_status.get(item.status.value, 0) + 1
            total_size += item.size_bytes
        return {
            "total_items": len(self.items),
            "by_type": by_type,
            "by_status": by_status,
            "total_size_mb": round(total_size / 1024 / 1024, 2),
            "with_alt_text": sum(1 for i in self.items.values() if i.alt_text),
            "without_alt_text": sum(1 for i in self.items.values() if not i.alt_text),
        }

    def find_duplicates(self) -> list[list[str]]:
        """Find duplicate media by checksum."""
        by_checksum: dict[str, list[str]] = {}
        for item in self.items.values():
            if item.checksum:
                by_checksum.setdefault(item.checksum, []).append(item.media_id)
        return [ids for ids in by_checksum.values() if len(ids) > 1]


class UploadQueue:
    """Queue for managing media uploads with retry logic."""

    def __init__(self, max_retries: int = 3):
        self.queue: list[MediaItem] = []
        self.max_retries = max_retries
        self.failed: list[MediaItem] = []
        self.uploaded: list[MediaItem] = []

    def enqueue(self, item: MediaItem):
        item.status = MediaStatus.PENDING
        self.queue.append(item)

    def dequeue(self) -> Optional[MediaItem]:
        if not self.queue:
            return None
        return self.queue.pop(0)

    def mark_uploaded(self, item: MediaItem, twitter_media_id: str):
        item.status = MediaStatus.UPLOADED
        item.twitter_media_id = twitter_media_id
        item.uploaded_at = time.time()
        self.uploaded.append(item)

    def mark_failed(self, item: MediaItem):
        item.upload_retries += 1
        if item.upload_retries < self.max_retries:
            self.queue.append(item)
        else:
            item.status = MediaStatus.FAILED
            self.failed.append(item)

    def pending_count(self) -> int:
        return len(self.queue)

    def stats(self) -> dict:
        return {
            "pending": len(self.queue),
            "uploaded": len(self.uploaded),
            "failed": len(self.failed),
        }


class WatermarkEngine:
    """Simple text watermark configuration (actual rendering needs PIL)."""

    def __init__(self, text: str = "", position: str = "bottom-right",
                 opacity: float = 0.5, font_size: int = 20):
        self.text = text
        self.position = position
        self.opacity = opacity
        self.font_size = font_size
        self.enabled = bool(text)

    def get_config(self) -> dict:
        return {
            "text": self.text,
            "position": self.position,
            "opacity": self.opacity,
            "font_size": self.font_size,
            "enabled": self.enabled,
        }

    def validate_position(self) -> bool:
        valid = {"top-left", "top-right", "bottom-left", "bottom-right", "center"}
        return self.position in valid

    def should_apply(self, item: MediaItem) -> bool:
        """Determine if watermark should be applied."""
        if not self.enabled:
            return False
        return item.media_type == MediaType.IMAGE
