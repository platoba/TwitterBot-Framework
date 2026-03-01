"""
Twitter Lists Manager - 列表管理与自动策展

Features:
- Create/delete/manage Twitter lists
- Auto-curate based on criteria (niche, engagement, followers)
- List member analysis (overlap, growth)
- Import/export list members
"""

import time
import sqlite3
import logging
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class ListConfig:
    """List auto-curation config"""
    name: str
    description: str = ""
    is_private: bool = False
    min_followers: int = 0
    max_followers: int = 0
    min_engagement_rate: float = 0.0
    keywords: List[str] = field(default_factory=list)
    exclude_keywords: List[str] = field(default_factory=list)
    max_members: int = 500


@dataclass
class ListMember:
    """Twitter list member"""
    user_id: str
    username: str
    display_name: str = ""
    followers_count: int = 0
    following_count: int = 0
    engagement_rate: float = 0.0
    bio: str = ""
    added_at: float = 0.0

    def to_dict(self) -> Dict:
        return asdict(self)

    @property
    def follow_ratio(self) -> float:
        if self.following_count == 0:
            return 0.0
        return round(self.followers_count / self.following_count, 2)


@dataclass
class ListStats:
    """List analytics"""
    list_name: str
    member_count: int
    avg_followers: float
    avg_engagement: float
    top_members: List[Dict]
    growth_7d: int = 0

    def to_dict(self) -> Dict:
        return asdict(self)


class ListsManager:
    """
    Twitter Lists management and auto-curation.

    Usage:
        manager = ListsManager(db)
        manager.create_list("AI Researchers", keywords=["AI", "ML", "NLP"])
        manager.add_member("ai_list_1", member)
        stats = manager.get_list_stats("ai_list_1")
    """

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS twitter_lists (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT DEFAULT '',
        is_private INTEGER DEFAULT 0,
        config_json TEXT DEFAULT '{}',
        member_count INTEGER DEFAULT 0,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS list_members (
        list_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        username TEXT NOT NULL,
        display_name TEXT DEFAULT '',
        followers_count INTEGER DEFAULT 0,
        following_count INTEGER DEFAULT 0,
        engagement_rate REAL DEFAULT 0.0,
        bio TEXT DEFAULT '',
        added_at REAL NOT NULL,
        PRIMARY KEY (list_id, user_id)
    );

    CREATE TABLE IF NOT EXISTS list_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        list_id TEXT NOT NULL,
        member_count INTEGER NOT NULL,
        snapshot_at REAL NOT NULL
    );
    """

    def __init__(self, db):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self):
        import json
        conn = self.db._get_conn()
        conn.executescript(self.SCHEMA)
        conn.commit()

    def create_list(
        self,
        name: str,
        description: str = "",
        is_private: bool = False,
        config: Optional[ListConfig] = None,
    ) -> str:
        """Create a new managed list"""
        import json
        list_id = f"list_{int(time.time())}_{name[:10].replace(' ', '_').lower()}"
        now = time.time()
        config_json = json.dumps(asdict(config)) if config else "{}"

        conn = self.db._get_conn()
        conn.execute(
            """INSERT INTO twitter_lists (id, name, description, is_private, config_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (list_id, name, description, int(is_private), config_json, now, now),
        )
        conn.commit()
        return list_id

    def delete_list(self, list_id: str) -> bool:
        """Delete a list and all its members"""
        conn = self.db._get_conn()
        conn.execute("DELETE FROM list_members WHERE list_id = ?", (list_id,))
        conn.execute("DELETE FROM list_snapshots WHERE list_id = ?", (list_id,))
        cursor = conn.execute("DELETE FROM twitter_lists WHERE id = ?", (list_id,))
        conn.commit()
        return cursor.rowcount > 0

    def get_lists(self) -> List[Dict]:
        """Get all managed lists"""
        conn = self.db._get_conn()
        rows = conn.execute(
            "SELECT * FROM twitter_lists ORDER BY created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def add_member(self, list_id: str, member: ListMember) -> bool:
        """Add a member to a list"""
        conn = self.db._get_conn()
        try:
            conn.execute(
                """INSERT OR REPLACE INTO list_members
                   (list_id, user_id, username, display_name, followers_count,
                    following_count, engagement_rate, bio, added_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (list_id, member.user_id, member.username, member.display_name,
                 member.followers_count, member.following_count,
                 member.engagement_rate, member.bio, member.added_at or time.time()),
            )
            conn.execute(
                "UPDATE twitter_lists SET member_count = (SELECT COUNT(*) FROM list_members WHERE list_id = ?), updated_at = ? WHERE id = ?",
                (list_id, time.time(), list_id),
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to add member: {e}")
            return False

    def remove_member(self, list_id: str, user_id: str) -> bool:
        """Remove a member from a list"""
        conn = self.db._get_conn()
        cursor = conn.execute(
            "DELETE FROM list_members WHERE list_id = ? AND user_id = ?",
            (list_id, user_id),
        )
        if cursor.rowcount > 0:
            conn.execute(
                "UPDATE twitter_lists SET member_count = (SELECT COUNT(*) FROM list_members WHERE list_id = ?), updated_at = ? WHERE id = ?",
                (list_id, time.time(), list_id),
            )
        conn.commit()
        return cursor.rowcount > 0

    def get_members(self, list_id: str, sort_by: str = "followers_count") -> List[Dict]:
        """Get all members of a list"""
        valid_sorts = {"followers_count", "engagement_rate", "added_at", "username"}
        if sort_by not in valid_sorts:
            sort_by = "followers_count"

        conn = self.db._get_conn()
        rows = conn.execute(
            f"SELECT * FROM list_members WHERE list_id = ? ORDER BY {sort_by} DESC",
            (list_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_list_stats(self, list_id: str) -> Optional[ListStats]:
        """Get statistics for a list"""
        conn = self.db._get_conn()

        list_row = conn.execute(
            "SELECT * FROM twitter_lists WHERE id = ?", (list_id,)
        ).fetchone()
        if not list_row:
            return None

        members = conn.execute(
            "SELECT * FROM list_members WHERE list_id = ? ORDER BY followers_count DESC",
            (list_id,),
        ).fetchall()

        if not members:
            return ListStats(
                list_name=list_row["name"],
                member_count=0,
                avg_followers=0,
                avg_engagement=0,
                top_members=[],
            )

        avg_followers = sum(m["followers_count"] for m in members) / len(members)
        avg_engagement = sum(m["engagement_rate"] for m in members) / len(members)
        top = [dict(m) for m in members[:5]]

        return ListStats(
            list_name=list_row["name"],
            member_count=len(members),
            avg_followers=round(avg_followers, 1),
            avg_engagement=round(avg_engagement, 4),
            top_members=top,
        )

    def find_overlap(self, list_id_a: str, list_id_b: str) -> List[str]:
        """Find members that exist in both lists"""
        conn = self.db._get_conn()
        rows = conn.execute(
            """SELECT a.user_id FROM list_members a
               INNER JOIN list_members b ON a.user_id = b.user_id
               WHERE a.list_id = ? AND b.list_id = ?""",
            (list_id_a, list_id_b),
        ).fetchall()
        return [r["user_id"] for r in rows]

    def export_members(self, list_id: str) -> List[Dict]:
        """Export list members for backup/transfer"""
        return self.get_members(list_id)

    def import_members(self, list_id: str, members_data: List[Dict]) -> int:
        """Import members from exported data"""
        imported = 0
        for m in members_data:
            member = ListMember(
                user_id=m["user_id"],
                username=m["username"],
                display_name=m.get("display_name", ""),
                followers_count=m.get("followers_count", 0),
                following_count=m.get("following_count", 0),
                engagement_rate=m.get("engagement_rate", 0),
                bio=m.get("bio", ""),
                added_at=m.get("added_at", time.time()),
            )
            if self.add_member(list_id, member):
                imported += 1
        return imported

    def snapshot(self, list_id: str):
        """Take a snapshot of current member count for growth tracking"""
        conn = self.db._get_conn()
        count = conn.execute(
            "SELECT COUNT(*) FROM list_members WHERE list_id = ?", (list_id,)
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO list_snapshots (list_id, member_count, snapshot_at) VALUES (?, ?, ?)",
            (list_id, count, time.time()),
        )
        conn.commit()
