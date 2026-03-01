"""
Follower Analytics - 粉丝分析与追踪

Features:
- Follower/following growth tracking
- Unfollower detection
- Follower quality scoring
- Audience demographics from bios
- Follow-back ratio
"""

import time
import sqlite3
import re
import logging
from typing import Optional, List, Dict, Tuple, Set
from dataclasses import dataclass, asdict
from collections import Counter
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class FollowerRecord:
    """Tracked follower"""
    user_id: str
    username: str
    display_name: str = ""
    followers_count: int = 0
    following_count: int = 0
    tweet_count: int = 0
    bio: str = ""
    location: str = ""
    is_following_back: bool = False
    quality_score: float = 0.0
    first_seen: float = 0.0
    last_seen: float = 0.0

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class GrowthReport:
    """Growth analysis report"""
    period: str
    followers_start: int
    followers_end: int
    net_change: int
    new_followers: int
    unfollowers: int
    follow_back_rate: float
    avg_quality: float

    def to_dict(self) -> Dict:
        return asdict(self)

    def format_report(self) -> str:
        trend = "📈" if self.net_change > 0 else ("📉" if self.net_change < 0 else "➡️")
        return (
            f"{trend} Growth Report ({self.period})\n"
            f"Followers: {self.followers_start} → {self.followers_end} ({self.net_change:+d})\n"
            f"New: +{self.new_followers} | Lost: -{self.unfollowers}\n"
            f"Follow-back rate: {self.follow_back_rate:.1%}\n"
            f"Avg quality: {self.avg_quality:.1f}/10"
        )


class FollowerAnalytics:
    """
    Follower analysis and growth tracking.

    Usage:
        analytics = FollowerAnalytics(db)
        analytics.record_snapshot(followers_list)
        report = analytics.get_growth_report("7d")
        unfollowers = analytics.detect_unfollowers()
    """

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS follower_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        username TEXT NOT NULL,
        display_name TEXT DEFAULT '',
        followers_count INTEGER DEFAULT 0,
        following_count INTEGER DEFAULT 0,
        tweet_count INTEGER DEFAULT 0,
        bio TEXT DEFAULT '',
        location TEXT DEFAULT '',
        is_following_back INTEGER DEFAULT 0,
        quality_score REAL DEFAULT 0.0,
        snapshot_at REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS follower_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        username TEXT NOT NULL,
        event_type TEXT NOT NULL,
        recorded_at REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS follower_counts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        total_followers INTEGER NOT NULL,
        total_following INTEGER NOT NULL,
        recorded_at REAL NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_fs_user ON follower_snapshots(user_id);
    CREATE INDEX IF NOT EXISTS idx_fs_time ON follower_snapshots(snapshot_at);
    CREATE INDEX IF NOT EXISTS idx_fe_type ON follower_events(event_type);
    """

    # Bot detection heuristics
    BOT_PATTERNS = [
        r"follow.?back",
        r"dm.?for.?(promo|shout)",
        r"buy.?followers",
        r"\b(crypto|nft|web3).*(earn|profit|invest)",
    ]

    def __init__(self, db):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self):
        conn = self.db._get_conn()
        conn.executescript(self.SCHEMA)
        conn.commit()

    def calculate_quality(self, follower: Dict) -> float:
        """
        Score follower quality 0-10.

        Factors:
        - Engagement ratio (followers vs following)
        - Account activity (tweet count)
        - Bio quality (non-empty, not spammy)
        - Follow-back status
        """
        score = 5.0  # baseline

        # Engagement ratio
        followers = follower.get("followers_count", 0)
        following = follower.get("following_count", 1)
        ratio = followers / max(following, 1)
        if ratio > 2:
            score += 1.5
        elif ratio > 0.5:
            score += 0.5
        elif ratio < 0.1:
            score -= 1.5

        # Activity
        tweets = follower.get("tweet_count", 0)
        if tweets > 100:
            score += 1.0
        elif tweets > 10:
            score += 0.5
        elif tweets == 0:
            score -= 2.0

        # Bio quality
        bio = follower.get("bio", "")
        if bio and len(bio) > 20:
            score += 0.5
        if not bio:
            score -= 0.5

        # Bot detection
        if self._is_likely_bot(follower):
            score -= 3.0

        # Follow-back
        if follower.get("is_following_back"):
            score += 0.5

        return max(0.0, min(10.0, round(score, 1)))

    def _is_likely_bot(self, follower: Dict) -> bool:
        """Heuristic bot detection"""
        bio = follower.get("bio", "").lower()
        for pattern in self.BOT_PATTERNS:
            if re.search(pattern, bio, re.IGNORECASE):
                return True

        # Extreme follow ratio
        following = follower.get("following_count", 0)
        followers = follower.get("followers_count", 0)
        if following > 5000 and followers < 100:
            return True

        # No tweets but following many
        tweets = follower.get("tweet_count", 0)
        if tweets == 0 and following > 500:
            return True

        return False

    def record_snapshot(self, followers: List[Dict]) -> Dict:
        """
        Record a snapshot of current followers.

        Args:
            followers: List of follower dicts with user_id, username, etc.

        Returns:
            Stats dict with new, lost, total
        """
        now = time.time()
        conn = self.db._get_conn()

        # Get previous snapshot user IDs
        prev_ids = set()
        prev_rows = conn.execute(
            """SELECT DISTINCT user_id FROM follower_snapshots
               WHERE snapshot_at = (SELECT MAX(snapshot_at) FROM follower_snapshots)"""
        ).fetchall()
        prev_ids = {r["user_id"] for r in prev_rows}

        current_ids = set()
        for f in followers:
            quality = self.calculate_quality(f)
            conn.execute(
                """INSERT INTO follower_snapshots
                   (user_id, username, display_name, followers_count, following_count,
                    tweet_count, bio, location, is_following_back, quality_score, snapshot_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    f["user_id"], f.get("username", ""), f.get("display_name", ""),
                    f.get("followers_count", 0), f.get("following_count", 0),
                    f.get("tweet_count", 0), f.get("bio", ""),
                    f.get("location", ""), int(f.get("is_following_back", False)),
                    quality, now,
                ),
            )
            current_ids.add(f["user_id"])

        # Detect new followers
        new_ids = current_ids - prev_ids
        for uid in new_ids:
            username = next((f.get("username", "") for f in followers if f["user_id"] == uid), "")
            conn.execute(
                "INSERT INTO follower_events (user_id, username, event_type, recorded_at) VALUES (?, ?, 'follow', ?)",
                (uid, username, now),
            )

        # Detect unfollowers
        lost_ids = prev_ids - current_ids
        for uid in lost_ids:
            conn.execute(
                "INSERT INTO follower_events (user_id, username, event_type, recorded_at) VALUES (?, ?, 'unfollow', ?)",
                (uid, "", now),
            )

        # Record totals
        conn.execute(
            "INSERT INTO follower_counts (total_followers, total_following, recorded_at) VALUES (?, 0, ?)",
            (len(current_ids), now),
        )

        conn.commit()
        return {
            "total": len(current_ids),
            "new": len(new_ids),
            "lost": len(lost_ids),
            "snapshot_at": now,
        }

    def detect_unfollowers(self, limit: int = 50) -> List[Dict]:
        """Get recent unfollower events"""
        conn = self.db._get_conn()
        rows = conn.execute(
            """SELECT * FROM follower_events
               WHERE event_type = 'unfollow'
               ORDER BY recorded_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_growth_report(self, period: str = "7d") -> GrowthReport:
        """Generate growth report for a period"""
        conn = self.db._get_conn()

        # Parse period
        days = 7
        if period.endswith("d"):
            days = int(period[:-1])
        elif period.endswith("w"):
            days = int(period[:-1]) * 7

        since = time.time() - (days * 86400)

        # Get follower counts
        counts = conn.execute(
            "SELECT * FROM follower_counts ORDER BY recorded_at DESC LIMIT 1"
        ).fetchone()
        end_count = counts["total_followers"] if counts else 0

        old_counts = conn.execute(
            "SELECT * FROM follower_counts WHERE recorded_at <= ? ORDER BY recorded_at DESC LIMIT 1",
            (since,),
        ).fetchone()
        start_count = old_counts["total_followers"] if old_counts else end_count

        # Events in period
        new_count = conn.execute(
            "SELECT COUNT(*) FROM follower_events WHERE event_type = 'follow' AND recorded_at >= ?",
            (since,),
        ).fetchone()[0]

        lost_count = conn.execute(
            "SELECT COUNT(*) FROM follower_events WHERE event_type = 'unfollow' AND recorded_at >= ?",
            (since,),
        ).fetchone()[0]

        # Follow-back rate from latest snapshot
        latest = conn.execute(
            "SELECT MAX(snapshot_at) as ts FROM follower_snapshots"
        ).fetchone()
        fb_rate = 0.0
        avg_quality = 0.0

        if latest and latest["ts"]:
            ts = latest["ts"]
            total = conn.execute(
                "SELECT COUNT(*) FROM follower_snapshots WHERE snapshot_at = ?", (ts,)
            ).fetchone()[0]
            fb = conn.execute(
                "SELECT COUNT(*) FROM follower_snapshots WHERE snapshot_at = ? AND is_following_back = 1",
                (ts,),
            ).fetchone()[0]
            fb_rate = fb / total if total > 0 else 0
            avg_q = conn.execute(
                "SELECT AVG(quality_score) FROM follower_snapshots WHERE snapshot_at = ?", (ts,)
            ).fetchone()[0]
            avg_quality = avg_q or 0

        return GrowthReport(
            period=period,
            followers_start=start_count,
            followers_end=end_count,
            net_change=end_count - start_count,
            new_followers=new_count,
            unfollowers=lost_count,
            follow_back_rate=round(fb_rate, 4),
            avg_quality=round(avg_quality, 1),
        )

    def get_demographics(self) -> Dict:
        """Analyze audience demographics from bios and locations"""
        conn = self.db._get_conn()

        latest = conn.execute(
            "SELECT MAX(snapshot_at) as ts FROM follower_snapshots"
        ).fetchone()
        if not latest or not latest["ts"]:
            return {"keywords": [], "locations": [], "total": 0}

        rows = conn.execute(
            "SELECT bio, location FROM follower_snapshots WHERE snapshot_at = ?",
            (latest["ts"],),
        ).fetchall()

        # Keyword extraction from bios
        word_counts: Counter = Counter()
        location_counts: Counter = Counter()

        for r in rows:
            bio = r["bio"] or ""
            words = re.findall(r"\b\w{4,}\b", bio.lower())
            word_counts.update(words)

            loc = (r["location"] or "").strip()
            if loc:
                location_counts[loc] += 1

        # Filter common stop words
        stop_words = {"that", "this", "with", "from", "have", "been", "will", "your",
                      "about", "more", "just", "like", "also", "into", "than", "some"}
        keywords = [(w, c) for w, c in word_counts.most_common(30) if w not in stop_words]

        return {
            "keywords": keywords[:20],
            "locations": location_counts.most_common(15),
            "total": len(rows),
        }
