"""
Smart Reply Engine v1.0
智能自动回复引擎 — 关键词匹配 + 模板管理 + 冷却控制 + 黑白名单

Features:
- ReplyTemplate: regex/keyword pattern matching with priority
- ConversationTracker: avoid duplicate replies per user
- Cooldown management per template + daily usage limits
- Blacklist/whitelist mechanism for user filtering
- Reply statistics and audit trail
"""

import re
import time
import json
import sqlite3
import hashlib
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple


class MatchType(Enum):
    KEYWORD = "keyword"
    REGEX = "regex"
    EXACT = "exact"
    CONTAINS = "contains"


class SentimentFilter(Enum):
    ANY = "any"
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"


@dataclass
class ReplyTemplate:
    """回复模板"""
    id: str
    name: str
    pattern: str
    response_text: str
    match_type: MatchType = MatchType.KEYWORD
    sentiment_filter: SentimentFilter = SentimentFilter.ANY
    priority: int = 0
    cooldown_seconds: int = 60
    max_uses_per_day: int = 100
    enabled: bool = True
    created_at: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()
        if isinstance(self.match_type, str):
            self.match_type = MatchType(self.match_type)
        if isinstance(self.sentiment_filter, str):
            self.sentiment_filter = SentimentFilter(self.sentiment_filter)


@dataclass
class MatchResult:
    """匹配结果"""
    template: ReplyTemplate
    score: float
    matched_text: str


@dataclass
class ReplyRecord:
    """回复记录"""
    template_id: str
    tweet_id: str
    author_id: str
    reply_text: str
    timestamp: str


class ReplyMatcher:
    """回复模式匹配器"""

    def match(self, text: str, template: ReplyTemplate) -> Optional[MatchResult]:
        """尝试匹配模板"""
        if not template.enabled:
            return None

        text_lower = text.lower()
        pattern_lower = template.pattern.lower()

        if template.match_type == MatchType.EXACT:
            if text_lower == pattern_lower:
                return MatchResult(template=template, score=1.0, matched_text=text)

        elif template.match_type == MatchType.CONTAINS:
            if pattern_lower in text_lower:
                score = len(pattern_lower) / max(len(text_lower), 1)
                return MatchResult(template=template, score=score, matched_text=template.pattern)

        elif template.match_type == MatchType.KEYWORD:
            keywords = [k.strip() for k in template.pattern.split(",")]
            matched = [k for k in keywords if k.lower() in text_lower]
            if matched:
                score = len(matched) / len(keywords)
                return MatchResult(template=template, score=score, matched_text=",".join(matched))

        elif template.match_type == MatchType.REGEX:
            try:
                m = re.search(template.pattern, text, re.IGNORECASE)
                if m:
                    return MatchResult(template=template, score=0.8, matched_text=m.group(0))
            except re.error:
                pass

        return None


class ConversationTracker:
    """对话追踪器 — 避免重复回复"""

    def __init__(self, dedup_window_seconds: int = 3600):
        self._replied: Dict[str, float] = {}  # key: author_id:template_id → timestamp
        self._lock = threading.Lock()
        self.dedup_window = dedup_window_seconds

    def has_replied(self, author_id: str, template_id: str) -> bool:
        """检查是否最近已回复过该用户的该模板"""
        key = f"{author_id}:{template_id}"
        with self._lock:
            ts = self._replied.get(key)
            if ts is None:
                return False
            if time.time() - ts > self.dedup_window:
                del self._replied[key]
                return False
            return True

    def record_reply(self, author_id: str, template_id: str):
        """记录已回复"""
        key = f"{author_id}:{template_id}"
        with self._lock:
            self._replied[key] = time.time()

    def clear(self):
        """清空追踪"""
        with self._lock:
            self._replied.clear()

    def cleanup_expired(self):
        """清理过期记录"""
        now = time.time()
        with self._lock:
            expired = [k for k, v in self._replied.items() if now - v > self.dedup_window]
            for k in expired:
                del self._replied[k]


class SmartReplyEngine:
    """智能自动回复引擎"""

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._templates: Dict[str, ReplyTemplate] = {}
        self._matcher = ReplyMatcher()
        self._tracker = ConversationTracker()
        self._blacklist: set = set()
        self._whitelist: set = set()
        self._whitelist_mode = False  # if True, only whitelist users get replies
        self._init_db()

    def _init_db(self):
        with self._lock:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS reply_templates (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    pattern TEXT NOT NULL,
                    response_text TEXT NOT NULL,
                    match_type TEXT DEFAULT 'keyword',
                    sentiment_filter TEXT DEFAULT 'any',
                    priority INTEGER DEFAULT 0,
                    cooldown_seconds INTEGER DEFAULT 60,
                    max_uses_per_day INTEGER DEFAULT 100,
                    enabled INTEGER DEFAULT 1,
                    created_at TEXT
                );
                CREATE TABLE IF NOT EXISTS reply_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    template_id TEXT,
                    tweet_id TEXT,
                    author_id TEXT,
                    reply_text TEXT,
                    timestamp TEXT
                );
                CREATE TABLE IF NOT EXISTS reply_daily_usage (
                    template_id TEXT,
                    date TEXT,
                    count INTEGER DEFAULT 0,
                    PRIMARY KEY (template_id, date)
                );
            """)
            self._conn.commit()

    def add_template(self, template: ReplyTemplate) -> bool:
        """添加回复模板"""
        with self._lock:
            self._templates[template.id] = template
            self._conn.execute(
                """INSERT OR REPLACE INTO reply_templates
                   (id, name, pattern, response_text, match_type, sentiment_filter,
                    priority, cooldown_seconds, max_uses_per_day, enabled, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (template.id, template.name, template.pattern, template.response_text,
                 template.match_type.value, template.sentiment_filter.value,
                 template.priority, template.cooldown_seconds, template.max_uses_per_day,
                 1 if template.enabled else 0, template.created_at)
            )
            self._conn.commit()
        return True

    def remove_template(self, template_id: str) -> bool:
        """删除回复模板"""
        with self._lock:
            if template_id in self._templates:
                del self._templates[template_id]
            self._conn.execute("DELETE FROM reply_templates WHERE id = ?", (template_id,))
            self._conn.commit()
        return True

    def list_templates(self) -> List[ReplyTemplate]:
        """列出所有模板"""
        return list(self._templates.values())

    def get_template(self, template_id: str) -> Optional[ReplyTemplate]:
        """获取模板"""
        return self._templates.get(template_id)

    def _get_daily_usage(self, template_id: str) -> int:
        """获取模板今日使用次数"""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        row = self._conn.execute(
            "SELECT count FROM reply_daily_usage WHERE template_id = ? AND date = ?",
            (template_id, today)
        ).fetchone()
        return row["count"] if row else 0

    def _increment_daily_usage(self, template_id: str):
        """增加今日使用次数"""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self._conn.execute(
            """INSERT INTO reply_daily_usage (template_id, date, count) VALUES (?, ?, 1)
               ON CONFLICT(template_id, date) DO UPDATE SET count = count + 1""",
            (template_id, today)
        )
        self._conn.commit()

    def _check_cooldown(self, template_id: str) -> bool:
        """检查冷却时间是否已过"""
        row = self._conn.execute(
            "SELECT timestamp FROM reply_history WHERE template_id = ? ORDER BY id DESC LIMIT 1",
            (template_id,)
        ).fetchone()
        if not row:
            return True
        last_ts = datetime.fromisoformat(row["timestamp"])
        template = self._templates.get(template_id)
        if not template:
            return True
        elapsed = (datetime.now(timezone.utc) - last_ts).total_seconds()
        return elapsed >= template.cooldown_seconds

    def add_to_blacklist(self, user_id: str):
        """添加到黑名单"""
        self._blacklist.add(user_id)

    def remove_from_blacklist(self, user_id: str):
        """从黑名单移除"""
        self._blacklist.discard(user_id)

    def add_to_whitelist(self, user_id: str):
        """添加到白名单"""
        self._whitelist.add(user_id)

    def remove_from_whitelist(self, user_id: str):
        """从白名单移除"""
        self._whitelist.discard(user_id)

    def set_whitelist_mode(self, enabled: bool):
        """设置白名单模式"""
        self._whitelist_mode = enabled

    def _is_user_allowed(self, author_id: str) -> bool:
        """检查用户是否被允许"""
        if author_id in self._blacklist:
            return False
        if self._whitelist_mode and author_id not in self._whitelist:
            return False
        return True

    def match_reply(
        self,
        tweet_text: str,
        author_id: str,
        sentiment: Optional[str] = None
    ) -> Optional[MatchResult]:
        """匹配最佳回复模板"""
        if not self._is_user_allowed(author_id):
            return None

        candidates: List[MatchResult] = []

        for template in self._templates.values():
            if not template.enabled:
                continue

            # 情感过滤
            if template.sentiment_filter != SentimentFilter.ANY:
                if sentiment and sentiment != template.sentiment_filter.value:
                    continue

            # 冷却检查
            if not self._check_cooldown(template.id):
                continue

            # 每日上限
            if self._get_daily_usage(template.id) >= template.max_uses_per_day:
                continue

            # 去重检查
            if self._tracker.has_replied(author_id, template.id):
                continue

            result = self._matcher.match(tweet_text, template)
            if result:
                candidates.append(result)

        if not candidates:
            return None

        # 按优先级排序,同优先级按匹配分数
        candidates.sort(key=lambda r: (-r.template.priority, -r.score))
        return candidates[0]

    def execute_reply(self, tweet_id: str, author_id: str, template_id: str, reply_text: str) -> bool:
        """记录回复执行"""
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self._conn.execute(
                "INSERT INTO reply_history (template_id, tweet_id, author_id, reply_text, timestamp) VALUES (?, ?, ?, ?, ?)",
                (template_id, tweet_id, author_id, reply_text, now)
            )
            self._increment_daily_usage(template_id)
            self._conn.commit()
        self._tracker.record_reply(author_id, template_id)
        return True

    def get_reply_stats(self) -> Dict[str, Any]:
        """获取回复统计"""
        total = self._conn.execute("SELECT COUNT(*) as c FROM reply_history").fetchone()["c"]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        today_count = self._conn.execute(
            "SELECT COALESCE(SUM(count), 0) as c FROM reply_daily_usage WHERE date = ?", (today,)
        ).fetchone()["c"]

        by_template = {}
        for row in self._conn.execute(
            "SELECT template_id, COUNT(*) as c FROM reply_history GROUP BY template_id"
        ):
            by_template[row["template_id"]] = row["c"]

        unique_users = self._conn.execute(
            "SELECT COUNT(DISTINCT author_id) as c FROM reply_history"
        ).fetchone()["c"]

        return {
            "total_replies": total,
            "today_replies": today_count,
            "unique_users": unique_users,
            "by_template": by_template,
            "template_count": len(self._templates),
            "blacklist_size": len(self._blacklist),
            "whitelist_size": len(self._whitelist),
        }

    def get_reply_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """获取回复历史"""
        rows = self._conn.execute(
            "SELECT * FROM reply_history ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def close(self):
        """关闭数据库"""
        self._conn.close()
