"""
Twitter Spaces Manager v1.0
Spaces管理: 创建/排期/主持人/嘉宾/录制/分析

Features:
- Space scheduling with reminders
- Speaker/listener management (invite, promote, demote)
- Live space metrics tracking (listeners, speakers, duration)
- Recording and highlight generation
- Post-space analytics (peak listeners, engagement, growth)
- Recurring space series support
- SQLite persistence for all space data
"""

import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional, List, Dict, Any


class SpaceStatus(Enum):
    DRAFT = "draft"
    SCHEDULED = "scheduled"
    LIVE = "live"
    ENDED = "ended"
    CANCELLED = "cancelled"


class ParticipantRole(Enum):
    HOST = "host"
    CO_HOST = "co_host"
    SPEAKER = "speaker"
    LISTENER = "listener"


class Space:
    """Twitter Space数据模型"""

    def __init__(
        self,
        title: str,
        space_id: str = None,
        status: str = "draft",
        scheduled_at: str = None,
        started_at: str = None,
        ended_at: str = None,
        topic: str = "",
        description: str = "",
        is_recorded: bool = False,
        is_recurring: bool = False,
        recurrence_rule: str = "",  # weekly|biweekly|monthly
        series_id: str = None,
        max_speakers: int = 11,
        language: str = "en",
        created_at: str = None,
    ):
        self.space_id = space_id or str(uuid.uuid4())[:12]
        self.title = title
        self.status = status
        self.scheduled_at = scheduled_at
        self.started_at = started_at
        self.ended_at = ended_at
        self.topic = topic
        self.description = description
        self.is_recorded = is_recorded
        self.is_recurring = is_recurring
        self.recurrence_rule = recurrence_rule
        self.series_id = series_id
        self.max_speakers = max_speakers
        self.language = language
        self.created_at = created_at or datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        return {
            "space_id": self.space_id,
            "title": self.title,
            "status": self.status,
            "scheduled_at": self.scheduled_at,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "topic": self.topic,
            "description": self.description,
            "is_recorded": self.is_recorded,
            "is_recurring": self.is_recurring,
            "recurrence_rule": self.recurrence_rule,
            "series_id": self.series_id,
            "max_speakers": self.max_speakers,
            "language": self.language,
            "created_at": self.created_at,
        }

    @property
    def duration_minutes(self) -> float:
        """计算Space时长(分钟)"""
        if not self.started_at:
            return 0.0
        start = datetime.fromisoformat(self.started_at)
        if self.ended_at:
            end = datetime.fromisoformat(self.ended_at)
        else:
            end = datetime.now(timezone.utc)
        return round((end - start).total_seconds() / 60, 1)


class SpacesManager:
    """Spaces管理引擎"""

    TOPICS = [
        "tech", "crypto", "ai", "marketing", "business",
        "startup", "ecommerce", "creator", "gaming", "health",
        "education", "politics", "music", "sports", "other",
    ]

    def __init__(self, db_path: str = "twitterbot.db"):
        self.db_path = db_path
        self._init_tables()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_tables(self):
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS spaces (
                space_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                status TEXT DEFAULT 'draft',
                scheduled_at TEXT,
                started_at TEXT,
                ended_at TEXT,
                topic TEXT DEFAULT '',
                description TEXT DEFAULT '',
                is_recorded INTEGER DEFAULT 0,
                is_recurring INTEGER DEFAULT 0,
                recurrence_rule TEXT DEFAULT '',
                series_id TEXT,
                max_speakers INTEGER DEFAULT 11,
                language TEXT DEFAULT 'en',
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS space_participants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                space_id TEXT NOT NULL,
                username TEXT NOT NULL,
                role TEXT DEFAULT 'listener',
                invited_at TEXT,
                joined_at TEXT,
                left_at TEXT,
                speaking_time_seconds INTEGER DEFAULT 0,
                FOREIGN KEY (space_id) REFERENCES spaces(space_id),
                UNIQUE(space_id, username)
            );
            CREATE TABLE IF NOT EXISTS space_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                space_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                listener_count INTEGER DEFAULT 0,
                speaker_count INTEGER DEFAULT 0,
                total_participants INTEGER DEFAULT 0,
                FOREIGN KEY (space_id) REFERENCES spaces(space_id)
            );
            CREATE TABLE IF NOT EXISTS space_highlights (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                space_id TEXT NOT NULL,
                timestamp_seconds INTEGER DEFAULT 0,
                title TEXT DEFAULT '',
                description TEXT DEFAULT '',
                speaker TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (space_id) REFERENCES spaces(space_id)
            );
        """)
        conn.commit()
        conn.close()

    # ── Space CRUD ───────────────────────────────────────

    def create_space(self, title: str, **kwargs) -> Space:
        """创建Space"""
        space = Space(title=title, **kwargs)
        conn = self._get_conn()
        conn.execute(
            """INSERT INTO spaces (space_id, title, status, scheduled_at, started_at,
               ended_at, topic, description, is_recorded, is_recurring, recurrence_rule,
               series_id, max_speakers, language, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (space.space_id, space.title, space.status, space.scheduled_at,
             space.started_at, space.ended_at, space.topic, space.description,
             1 if space.is_recorded else 0, 1 if space.is_recurring else 0,
             space.recurrence_rule, space.series_id, space.max_speakers,
             space.language, space.created_at),
        )
        conn.commit()
        conn.close()
        return space

    def get_space(self, space_id: str) -> Optional[dict]:
        """获取Space详情"""
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM spaces WHERE space_id=?", (space_id,)).fetchone()
        if not row:
            conn.close()
            return None
        space = dict(row)
        participants = conn.execute(
            "SELECT * FROM space_participants WHERE space_id=?", (space_id,)
        ).fetchall()
        space["participants"] = [dict(p) for p in participants]
        conn.close()
        return space

    def list_spaces(self, status: str = None, limit: int = 50) -> List[dict]:
        """列出Spaces"""
        conn = self._get_conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM spaces WHERE status=? ORDER BY created_at DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM spaces ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def schedule_space(self, space_id: str, scheduled_at: str) -> bool:
        """排期Space"""
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE spaces SET status='scheduled', scheduled_at=? "
            "WHERE space_id=? AND status='draft'",
            (scheduled_at, space_id),
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return updated

    def start_space(self, space_id: str) -> bool:
        """开始Space"""
        now = datetime.now(timezone.utc).isoformat()
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE spaces SET status='live', started_at=? "
            "WHERE space_id=? AND status IN ('draft', 'scheduled')",
            (now, space_id),
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return updated

    def end_space(self, space_id: str) -> bool:
        """结束Space"""
        now = datetime.now(timezone.utc).isoformat()
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE spaces SET status='ended', ended_at=? "
            "WHERE space_id=? AND status='live'",
            (now, space_id),
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return updated

    def cancel_space(self, space_id: str) -> bool:
        """取消Space"""
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE spaces SET status='cancelled' "
            "WHERE space_id=? AND status IN ('draft', 'scheduled')",
            (space_id,),
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return updated

    # ── Participants ─────────────────────────────────────

    def invite_participant(self, space_id: str, username: str,
                           role: str = "speaker") -> bool:
        """邀请参与者"""
        now = datetime.now(timezone.utc).isoformat()
        conn = self._get_conn()
        try:
            conn.execute(
                """INSERT INTO space_participants (space_id, username, role, invited_at)
                   VALUES (?, ?, ?, ?)""",
                (space_id, username, role, now),
            )
            conn.commit()
            conn.close()
            return True
        except sqlite3.IntegrityError:
            conn.close()
            return False

    def join_participant(self, space_id: str, username: str,
                         role: str = "listener") -> bool:
        """参与者加入"""
        now = datetime.now(timezone.utc).isoformat()
        conn = self._get_conn()
        # Check if already exists
        row = conn.execute(
            "SELECT id FROM space_participants WHERE space_id=? AND username=?",
            (space_id, username),
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE space_participants SET joined_at=?, role=? "
                "WHERE space_id=? AND username=?",
                (now, role, space_id, username),
            )
        else:
            conn.execute(
                """INSERT INTO space_participants (space_id, username, role, joined_at)
                   VALUES (?, ?, ?, ?)""",
                (space_id, username, role, now),
            )
        conn.commit()
        conn.close()
        return True

    def leave_participant(self, space_id: str, username: str) -> bool:
        """参与者离开"""
        now = datetime.now(timezone.utc).isoformat()
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE space_participants SET left_at=? WHERE space_id=? AND username=? AND left_at IS NULL",
            (now, space_id, username),
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return updated

    def promote_to_speaker(self, space_id: str, username: str) -> bool:
        """提升为发言人"""
        conn = self._get_conn()
        # Check max speakers
        space = conn.execute("SELECT max_speakers FROM spaces WHERE space_id=?", (space_id,)).fetchone()
        if space:
            current_speakers = conn.execute(
                "SELECT COUNT(*) as c FROM space_participants "
                "WHERE space_id=? AND role IN ('speaker', 'co_host', 'host') AND left_at IS NULL",
                (space_id,),
            ).fetchone()["c"]
            if current_speakers >= space["max_speakers"]:
                conn.close()
                return False

        cursor = conn.execute(
            "UPDATE space_participants SET role='speaker' "
            "WHERE space_id=? AND username=? AND role='listener'",
            (space_id, username),
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return updated

    def demote_to_listener(self, space_id: str, username: str) -> bool:
        """降级为听众"""
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE space_participants SET role='listener' "
            "WHERE space_id=? AND username=? AND role='speaker'",
            (space_id, username),
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return updated

    def get_participants(self, space_id: str, role: str = None) -> List[dict]:
        """获取参与者列表"""
        conn = self._get_conn()
        if role:
            rows = conn.execute(
                "SELECT * FROM space_participants WHERE space_id=? AND role=?",
                (space_id, role),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM space_participants WHERE space_id=?",
                (space_id,),
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def update_speaking_time(self, space_id: str, username: str, seconds: int) -> bool:
        """更新发言时长"""
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE space_participants SET speaking_time_seconds=? "
            "WHERE space_id=? AND username=?",
            (seconds, space_id, username),
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return updated

    # ── Live Metrics ─────────────────────────────────────

    def record_metric(self, space_id: str, listener_count: int,
                      speaker_count: int = 0) -> int:
        """记录实时指标"""
        now = datetime.now(timezone.utc).isoformat()
        total = listener_count + speaker_count
        conn = self._get_conn()
        cursor = conn.execute(
            """INSERT INTO space_metrics (space_id, timestamp, listener_count,
               speaker_count, total_participants)
               VALUES (?, ?, ?, ?, ?)""",
            (space_id, now, listener_count, speaker_count, total),
        )
        conn.commit()
        row_id = cursor.lastrowid
        conn.close()
        return row_id

    def get_metrics(self, space_id: str) -> List[dict]:
        """获取Space指标"""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM space_metrics WHERE space_id=? ORDER BY timestamp",
            (space_id,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Highlights ───────────────────────────────────────

    def add_highlight(self, space_id: str, timestamp_seconds: int,
                      title: str, description: str = "", speaker: str = "") -> int:
        """添加亮点标记"""
        conn = self._get_conn()
        cursor = conn.execute(
            """INSERT INTO space_highlights (space_id, timestamp_seconds, title, description, speaker)
               VALUES (?, ?, ?, ?, ?)""",
            (space_id, timestamp_seconds, title, description, speaker),
        )
        conn.commit()
        row_id = cursor.lastrowid
        conn.close()
        return row_id

    def get_highlights(self, space_id: str) -> List[dict]:
        """获取亮点"""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM space_highlights WHERE space_id=? ORDER BY timestamp_seconds",
            (space_id,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Analytics ────────────────────────────────────────

    def get_space_analytics(self, space_id: str) -> dict:
        """Space分析报告"""
        space = self.get_space(space_id)
        if not space:
            return {"error": "Space not found"}

        metrics = self.get_metrics(space_id)
        participants = space.get("participants", [])
        highlights = self.get_highlights(space_id)

        # Peak listeners
        peak_listeners = max((m["listener_count"] for m in metrics), default=0)
        peak_total = max((m["total_participants"] for m in metrics), default=0)
        avg_listeners = (
            round(sum(m["listener_count"] for m in metrics) / len(metrics), 1)
            if metrics else 0
        )

        # Duration
        duration_min = 0.0
        if space.get("started_at"):
            s = Space(title="", started_at=space["started_at"], ended_at=space.get("ended_at"))
            duration_min = s.duration_minutes

        # Speaker stats
        speakers = [p for p in participants if p["role"] in ("speaker", "co_host", "host")]
        total_speaking_time = sum(p.get("speaking_time_seconds", 0) for p in speakers)

        return {
            "space_id": space_id,
            "title": space["title"],
            "status": space["status"],
            "duration_minutes": duration_min,
            "peak_listeners": peak_listeners,
            "peak_total_participants": peak_total,
            "avg_listeners": avg_listeners,
            "total_participants": len(participants),
            "total_speakers": len(speakers),
            "total_speaking_time_seconds": total_speaking_time,
            "metric_snapshots": len(metrics),
            "highlights_count": len(highlights),
        }

    # ── Recurring Series ─────────────────────────────────

    def create_series_next(self, space_id: str) -> Optional[Space]:
        """为周期性Space创建下一期"""
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM spaces WHERE space_id=?", (space_id,)).fetchone()
        conn.close()

        if not row or not row["is_recurring"] or not row["recurrence_rule"]:
            return None

        # Calculate next date
        rule = row["recurrence_rule"]
        base_date = row.get("scheduled_at") or row.get("created_at")
        if not base_date:
            return None

        try:
            base = datetime.fromisoformat(base_date)
        except (ValueError, TypeError):
            return None

        if rule == "weekly":
            next_date = base + timedelta(weeks=1)
        elif rule == "biweekly":
            next_date = base + timedelta(weeks=2)
        elif rule == "monthly":
            next_date = base + timedelta(days=30)
        else:
            return None

        return self.create_space(
            title=row["title"],
            topic=row["topic"],
            description=row["description"],
            is_recorded=bool(row["is_recorded"]),
            is_recurring=True,
            recurrence_rule=rule,
            series_id=row["series_id"] or space_id,
            max_speakers=row["max_speakers"],
            language=row["language"],
            scheduled_at=next_date.isoformat(),
            status="scheduled",
        )

    def get_upcoming(self, hours: int = 24) -> List[dict]:
        """获取即将开始的Spaces"""
        now = datetime.now(timezone.utc).isoformat()
        future = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()
        conn = self._get_conn()
        rows = conn.execute(
            """SELECT * FROM spaces
               WHERE status='scheduled' AND scheduled_at >= ? AND scheduled_at <= ?
               ORDER BY scheduled_at""",
            (now, future),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def export_analytics_csv(self, space_id: str) -> str:
        """导出分析CSV"""
        metrics = self.get_metrics(space_id)
        lines = ["timestamp,listener_count,speaker_count,total_participants"]
        for m in metrics:
            lines.append(
                f"{m['timestamp']},{m['listener_count']},"
                f"{m['speaker_count']},{m['total_participants']}"
            )
        return "\n".join(lines)
