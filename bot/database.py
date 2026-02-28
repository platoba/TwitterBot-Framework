"""
SQLite持久化层
推文历史 + 分析数据 + 调度队列 + 监控配置
"""

import sqlite3
import json
import threading
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any


class Database:
    """线程安全的SQLite数据库管理器"""

    def __init__(self, db_path: str = "twitterbot.db"):
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path)
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA foreign_keys=ON")
        return self._local.conn

    def _init_db(self):
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS tweet_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tweet_id TEXT UNIQUE,
                author_id TEXT,
                author_username TEXT,
                text TEXT,
                like_count INTEGER DEFAULT 0,
                retweet_count INTEGER DEFAULT 0,
                reply_count INTEGER DEFAULT 0,
                quote_count INTEGER DEFAULT 0,
                impression_count INTEGER DEFAULT 0,
                created_at TEXT,
                collected_at TEXT DEFAULT (datetime('now')),
                source_query TEXT,
                metadata TEXT
            );

            CREATE TABLE IF NOT EXISTS analytics_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                followers_count INTEGER,
                following_count INTEGER,
                tweet_count INTEGER,
                listed_count INTEGER,
                snapshot_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS schedule_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content TEXT NOT NULL,
                scheduled_at TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                tweet_id TEXT,
                error TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                executed_at TEXT,
                metadata TEXT
            );

            CREATE TABLE IF NOT EXISTS monitors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT NOT NULL,
                monitor_type TEXT DEFAULT 'keyword',
                chat_id TEXT,
                is_active INTEGER DEFAULT 1,
                last_checked TEXT,
                last_tweet_id TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                config TEXT
            );

            CREATE TABLE IF NOT EXISTS engagement_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action_type TEXT NOT NULL,
                target_tweet_id TEXT,
                target_username TEXT,
                reply_text TEXT,
                status TEXT DEFAULT 'success',
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS ab_tests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                test_name TEXT NOT NULL,
                variant_a TEXT NOT NULL,
                variant_b TEXT NOT NULL,
                variant_a_tweet_id TEXT,
                variant_b_tweet_id TEXT,
                variant_a_metrics TEXT,
                variant_b_metrics TEXT,
                winner TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT (datetime('now')),
                evaluated_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_tweet_author ON tweet_history(author_username);
            CREATE INDEX IF NOT EXISTS idx_tweet_created ON tweet_history(created_at);
            CREATE INDEX IF NOT EXISTS idx_tweet_query ON tweet_history(source_query);
            CREATE INDEX IF NOT EXISTS idx_schedule_status ON schedule_queue(status, scheduled_at);
            CREATE INDEX IF NOT EXISTS idx_analytics_user ON analytics_snapshots(username, snapshot_at);
            CREATE INDEX IF NOT EXISTS idx_monitor_active ON monitors(is_active);
        """)
        conn.commit()

    # ── Tweet History ──

    def save_tweet(self, tweet: Dict[str, Any], source_query: str = "") -> bool:
        conn = self._get_conn()
        try:
            metrics = tweet.get("public_metrics", {})
            conn.execute("""
                INSERT OR REPLACE INTO tweet_history
                (tweet_id, author_id, author_username, text,
                 like_count, retweet_count, reply_count, quote_count, impression_count,
                 created_at, source_query, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                tweet.get("id"),
                tweet.get("author_id"),
                tweet.get("author_username", ""),
                tweet.get("text", ""),
                metrics.get("like_count", 0),
                metrics.get("retweet_count", 0),
                metrics.get("reply_count", 0),
                metrics.get("quote_count", 0),
                metrics.get("impression_count", 0),
                tweet.get("created_at", ""),
                source_query,
                json.dumps(tweet.get("metadata", {}))
            ))
            conn.commit()
            return True
        except Exception:
            return False

    def save_tweets_batch(self, tweets: List[Dict], source_query: str = "") -> int:
        saved = 0
        for tweet in tweets:
            if self.save_tweet(tweet, source_query):
                saved += 1
        return saved

    def get_tweet_history(self, username: str = "", limit: int = 50) -> List[Dict]:
        conn = self._get_conn()
        if username:
            rows = conn.execute(
                "SELECT * FROM tweet_history WHERE author_username = ? ORDER BY created_at DESC LIMIT ?",
                (username, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM tweet_history ORDER BY created_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def get_top_tweets(self, username: str = "", limit: int = 10, metric: str = "like_count") -> List[Dict]:
        conn = self._get_conn()
        valid_metrics = ["like_count", "retweet_count", "reply_count", "impression_count"]
        if metric not in valid_metrics:
            metric = "like_count"
        if username:
            rows = conn.execute(
                f"SELECT * FROM tweet_history WHERE author_username = ? ORDER BY {metric} DESC LIMIT ?",
                (username, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT * FROM tweet_history ORDER BY {metric} DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Analytics Snapshots ──

    def save_analytics_snapshot(self, username: str, metrics: Dict[str, int]) -> bool:
        conn = self._get_conn()
        try:
            conn.execute("""
                INSERT INTO analytics_snapshots
                (username, followers_count, following_count, tweet_count, listed_count)
                VALUES (?, ?, ?, ?, ?)
            """, (
                username,
                metrics.get("followers_count", 0),
                metrics.get("following_count", 0),
                metrics.get("tweet_count", 0),
                metrics.get("listed_count", 0)
            ))
            conn.commit()
            return True
        except Exception:
            return False

    def get_analytics_history(self, username: str, limit: int = 30) -> List[Dict]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM analytics_snapshots WHERE username = ? ORDER BY snapshot_at DESC LIMIT ?",
            (username, limit)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_follower_growth(self, username: str, days: int = 7) -> Optional[Dict]:
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT followers_count, snapshot_at FROM analytics_snapshots
            WHERE username = ? ORDER BY snapshot_at DESC LIMIT ?
        """, (username, days)).fetchall()
        if len(rows) < 2:
            return None
        latest = rows[0]["followers_count"]
        oldest = rows[-1]["followers_count"]
        return {
            "current": latest,
            "previous": oldest,
            "growth": latest - oldest,
            "growth_rate": round((latest - oldest) / max(oldest, 1) * 100, 2),
            "period_days": days
        }

    # ── Schedule Queue ──

    def add_scheduled_tweet(self, content: str, scheduled_at: str, metadata: Dict = None) -> int:
        conn = self._get_conn()
        cursor = conn.execute("""
            INSERT INTO schedule_queue (content, scheduled_at, metadata)
            VALUES (?, ?, ?)
        """, (content, scheduled_at, json.dumps(metadata or {})))
        conn.commit()
        return cursor.lastrowid

    def get_pending_tweets(self, before: str = None) -> List[Dict]:
        conn = self._get_conn()
        if before is None:
            before = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        rows = conn.execute("""
            SELECT * FROM schedule_queue
            WHERE status = 'pending' AND scheduled_at <= ?
            ORDER BY scheduled_at ASC
        """, (before,)).fetchall()
        return [dict(r) for r in rows]

    def update_schedule_status(self, schedule_id: int, status: str,
                                tweet_id: str = None, error: str = None):
        conn = self._get_conn()
        conn.execute("""
            UPDATE schedule_queue
            SET status = ?, tweet_id = ?, error = ?, executed_at = datetime('now')
            WHERE id = ?
        """, (status, tweet_id, error, schedule_id))
        conn.commit()

    def get_schedule_queue(self, status: str = None, limit: int = 20) -> List[Dict]:
        conn = self._get_conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM schedule_queue WHERE status = ? ORDER BY scheduled_at ASC LIMIT ?",
                (status, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM schedule_queue ORDER BY scheduled_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Monitors ──

    def add_monitor(self, keyword: str, chat_id: str,
                     monitor_type: str = "keyword", config: Dict = None) -> int:
        conn = self._get_conn()
        cursor = conn.execute("""
            INSERT INTO monitors (keyword, monitor_type, chat_id, config)
            VALUES (?, ?, ?, ?)
        """, (keyword, monitor_type, chat_id, json.dumps(config or {})))
        conn.commit()
        return cursor.lastrowid

    def get_active_monitors(self, monitor_type: str = None) -> List[Dict]:
        conn = self._get_conn()
        if monitor_type:
            rows = conn.execute(
                "SELECT * FROM monitors WHERE is_active = 1 AND monitor_type = ?",
                (monitor_type,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM monitors WHERE is_active = 1"
            ).fetchall()
        return [dict(r) for r in rows]

    def update_monitor(self, monitor_id: int, last_tweet_id: str = None):
        conn = self._get_conn()
        conn.execute("""
            UPDATE monitors SET last_checked = datetime('now'), last_tweet_id = ?
            WHERE id = ?
        """, (last_tweet_id, monitor_id))
        conn.commit()

    def deactivate_monitor(self, keyword: str) -> bool:
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE monitors SET is_active = 0 WHERE keyword = ? AND is_active = 1",
            (keyword,)
        )
        conn.commit()
        return cursor.rowcount > 0

    # ── Engagement Log ──

    def log_engagement(self, action_type: str, target_tweet_id: str = "",
                        target_username: str = "", reply_text: str = "",
                        status: str = "success"):
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO engagement_log
            (action_type, target_tweet_id, target_username, reply_text, status)
            VALUES (?, ?, ?, ?, ?)
        """, (action_type, target_tweet_id, target_username, reply_text, status))
        conn.commit()

    def get_engagement_stats(self, days: int = 7) -> Dict[str, int]:
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT action_type, COUNT(*) as cnt
            FROM engagement_log
            WHERE created_at >= datetime('now', ?)
            GROUP BY action_type
        """, (f"-{days} days",)).fetchall()
        return {r["action_type"]: r["cnt"] for r in rows}

    # ── A/B Tests ──

    def create_ab_test(self, test_name: str, variant_a: str, variant_b: str) -> int:
        conn = self._get_conn()
        cursor = conn.execute("""
            INSERT INTO ab_tests (test_name, variant_a, variant_b)
            VALUES (?, ?, ?)
        """, (test_name, variant_a, variant_b))
        conn.commit()
        return cursor.lastrowid

    def update_ab_test(self, test_id: int, **kwargs):
        conn = self._get_conn()
        allowed = ["variant_a_tweet_id", "variant_b_tweet_id",
                    "variant_a_metrics", "variant_b_metrics",
                    "winner", "status", "evaluated_at"]
        sets = []
        vals = []
        for k, v in kwargs.items():
            if k in allowed:
                sets.append(f"{k} = ?")
                vals.append(v)
        if sets:
            vals.append(test_id)
            conn.execute(f"UPDATE ab_tests SET {', '.join(sets)} WHERE id = ?", vals)
            conn.commit()

    def get_ab_tests(self, status: str = None, limit: int = 20) -> List[Dict]:
        conn = self._get_conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM ab_tests WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                (status, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM ab_tests ORDER BY created_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def close(self):
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
