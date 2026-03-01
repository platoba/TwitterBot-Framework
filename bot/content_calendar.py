"""
Content Calendar for Twitter/X
内容日历: 规划/分类/最佳时间/节日预设/审核流程/iCal导出
"""

import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any


class CalendarEntry:
    """日历条目"""

    def __init__(
        self,
        title: str,
        content: str = "",
        entry_id: str = None,
        category: str = "original",  # original|retweet|reply|quote|thread
        status: str = "draft",  # draft|review|approved|scheduled|published|rejected
        scheduled_at: str = None,
        published_at: str = None,
        hashtags: list = None,
        notes: str = "",
        reviewer: str = "",
        tweet_id: str = None,
        created_at: str = None,
    ):
        self.entry_id = entry_id or str(uuid.uuid4())
        self.title = title
        self.content = content
        self.category = category
        self.status = status
        self.scheduled_at = scheduled_at
        self.published_at = published_at
        self.hashtags = hashtags or []
        self.notes = notes
        self.reviewer = reviewer
        self.tweet_id = tweet_id
        self.created_at = created_at or datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        return {
            "entry_id": self.entry_id,
            "title": self.title,
            "content": self.content,
            "category": self.category,
            "status": self.status,
            "scheduled_at": self.scheduled_at,
            "published_at": self.published_at,
            "hashtags": self.hashtags,
            "notes": self.notes,
            "reviewer": self.reviewer,
            "tweet_id": self.tweet_id,
            "created_at": self.created_at,
        }


# 节日/事件预设
PRESET_EVENTS = {
    "01-01": "New Year's Day 🎉",
    "02-14": "Valentine's Day ❤️",
    "03-08": "International Women's Day 💪",
    "03-17": "St. Patrick's Day ☘️",
    "04-01": "April Fools' Day 🃏",
    "04-22": "Earth Day 🌍",
    "05-01": "May Day / Labor Day 👷",
    "05-04": "Star Wars Day ⭐",
    "06-05": "World Environment Day 🌱",
    "07-04": "Independence Day (US) 🇺🇸",
    "09-01": "Labor Day (US) 🏖️",
    "10-31": "Halloween 🎃",
    "11-11": "Singles' Day / Veterans Day 🛍️",
    "11-25": "Black Friday 🏷️",
    "11-28": "Cyber Monday 💻",
    "12-25": "Christmas 🎄",
    "12-31": "New Year's Eve 🥂",
}

# 最佳发布时间 (UTC, by day of week)
OPTIMAL_TIMES = {
    0: ["13:00", "17:00", "20:00"],  # Monday
    1: ["09:00", "13:00", "18:00"],  # Tuesday
    2: ["09:00", "12:00", "17:00"],  # Wednesday
    3: ["09:00", "13:00", "17:00"],  # Thursday
    4: ["09:00", "11:00", "16:00"],  # Friday
    5: ["10:00", "14:00"],           # Saturday
    6: ["10:00", "15:00"],           # Sunday
}


class ContentCalendar:
    """内容日历管理器"""

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
            CREATE TABLE IF NOT EXISTS content_calendar (
                entry_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                content TEXT DEFAULT '',
                category TEXT DEFAULT 'original',
                status TEXT DEFAULT 'draft',
                scheduled_at TEXT,
                published_at TEXT,
                hashtags TEXT DEFAULT '[]',
                notes TEXT DEFAULT '',
                reviewer TEXT DEFAULT '',
                tweet_id TEXT,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS calendar_presets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                month_day TEXT NOT NULL UNIQUE,
                event_name TEXT NOT NULL,
                template TEXT DEFAULT '',
                active INTEGER DEFAULT 1
            );
        """)
        # Seed presets
        for md, name in PRESET_EVENTS.items():
            conn.execute(
                "INSERT OR IGNORE INTO calendar_presets (month_day, event_name) VALUES (?, ?)",
                (md, name),
            )
        conn.commit()
        conn.close()

    # ── CRUD ─────────────────────────────────────────────

    def add_entry(self, title: str, content: str = "", category: str = "original",
                  scheduled_at: str = None, hashtags: list = None, notes: str = "") -> CalendarEntry:
        """添加日历条目"""
        entry = CalendarEntry(
            title=title, content=content, category=category,
            scheduled_at=scheduled_at, hashtags=hashtags, notes=notes,
        )
        conn = self._get_conn()
        conn.execute(
            "INSERT INTO content_calendar "
            "(entry_id, title, content, category, status, scheduled_at, hashtags, notes, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (entry.entry_id, entry.title, entry.content, entry.category,
             entry.status, entry.scheduled_at, json.dumps(entry.hashtags),
             entry.notes, entry.created_at),
        )
        conn.commit()
        conn.close()
        return entry

    def update_entry(self, entry_id: str, **kwargs) -> bool:
        """更新日历条目"""
        allowed = {"title", "content", "category", "scheduled_at", "hashtags", "notes", "reviewer"}
        updates = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
        if not updates:
            return False

        if "hashtags" in updates and isinstance(updates["hashtags"], list):
            updates["hashtags"] = json.dumps(updates["hashtags"])

        set_clause = ", ".join(f"{k}=?" for k in updates)
        values = list(updates.values()) + [entry_id]

        conn = self._get_conn()
        cursor = conn.execute(
            f"UPDATE content_calendar SET {set_clause} WHERE entry_id=?", values,
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return updated

    def get_entry(self, entry_id: str) -> Optional[dict]:
        """获取条目详情"""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM content_calendar WHERE entry_id=?", (entry_id,),
        ).fetchone()
        conn.close()
        if not row:
            return None
        d = dict(row)
        if isinstance(d.get("hashtags"), str):
            try:
                d["hashtags"] = json.loads(d["hashtags"])
            except (json.JSONDecodeError, TypeError):
                d["hashtags"] = []
        return d

    def delete_entry(self, entry_id: str) -> bool:
        """删除条目"""
        conn = self._get_conn()
        cursor = conn.execute(
            "DELETE FROM content_calendar WHERE entry_id=?", (entry_id,),
        )
        conn.commit()
        deleted = cursor.rowcount > 0
        conn.close()
        return deleted

    # ── Status Transitions ───────────────────────────────

    VALID_TRANSITIONS = {
        "draft": ["review", "approved", "rejected"],
        "review": ["approved", "rejected", "draft"],
        "approved": ["scheduled", "draft"],
        "scheduled": ["published", "draft"],
        "rejected": ["draft"],
    }

    def transition_status(self, entry_id: str, new_status: str, reviewer: str = "") -> bool:
        """审核流程状态转换"""
        entry = self.get_entry(entry_id)
        if not entry:
            return False

        current = entry["status"]
        allowed = self.VALID_TRANSITIONS.get(current, [])
        if new_status not in allowed:
            return False

        conn = self._get_conn()
        if new_status == "published":
            conn.execute(
                "UPDATE content_calendar SET status=?, published_at=?, reviewer=? WHERE entry_id=?",
                (new_status, datetime.now(timezone.utc).isoformat(), reviewer, entry_id),
            )
        else:
            conn.execute(
                "UPDATE content_calendar SET status=?, reviewer=? WHERE entry_id=?",
                (new_status, reviewer, entry_id),
            )
        conn.commit()
        conn.close()
        return True

    # ── Views ────────────────────────────────────────────

    def view_day(self, date: str = None) -> List[dict]:
        """查看某天的内容"""
        date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM content_calendar WHERE scheduled_at LIKE ? ORDER BY scheduled_at",
            (f"{date}%",),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def view_week(self, start_date: str = None) -> List[dict]:
        """查看一周的内容"""
        if start_date:
            start = datetime.strptime(start_date, "%Y-%m-%d")
        else:
            start = datetime.now(timezone.utc)
            start = start - timedelta(days=start.weekday())  # Monday

        end = start + timedelta(days=7)
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")

        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM content_calendar WHERE scheduled_at >= ? AND scheduled_at < ? ORDER BY scheduled_at",
            (start_str, end_str),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def view_month(self, year: int = None, month: int = None) -> List[dict]:
        """查看一月的内容"""
        now = datetime.now(timezone.utc)
        year = year or now.year
        month = month or now.month
        prefix = f"{year}-{month:02d}"

        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM content_calendar WHERE scheduled_at LIKE ? ORDER BY scheduled_at",
            (f"{prefix}%",),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_by_status(self, status: str, limit: int = 50) -> List[dict]:
        """按状态获取条目"""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM content_calendar WHERE status=? ORDER BY scheduled_at LIMIT ?",
            (status, limit),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Best Times ───────────────────────────────────────

    def suggest_times(self, date: str = None, count: int = 3) -> List[str]:
        """推荐最佳发布时间"""
        if date:
            dt = datetime.strptime(date, "%Y-%m-%d")
        else:
            dt = datetime.now(timezone.utc) + timedelta(days=1)

        day_of_week = dt.weekday()
        times = OPTIMAL_TIMES.get(day_of_week, ["12:00"])
        date_str = dt.strftime("%Y-%m-%d")

        suggestions = []
        for t in times[:count]:
            suggestions.append(f"{date_str}T{t}:00Z")
        return suggestions

    # ── Presets ───────────────────────────────────────────

    def get_upcoming_events(self, days: int = 30) -> List[dict]:
        """获取即将到来的节日/事件"""
        now = datetime.now(timezone.utc)
        events = []

        for i in range(days):
            check_date = now + timedelta(days=i)
            md = check_date.strftime("%m-%d")
            if md in PRESET_EVENTS:
                events.append({
                    "date": check_date.strftime("%Y-%m-%d"),
                    "month_day": md,
                    "event": PRESET_EVENTS[md],
                    "days_away": i,
                })
        return events

    def add_preset(self, month_day: str, event_name: str, template: str = "") -> bool:
        """添加自定义事件预设"""
        conn = self._get_conn()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO calendar_presets (month_day, event_name, template) VALUES (?, ?, ?)",
                (month_day, event_name, template),
            )
            conn.commit()
            conn.close()
            return True
        except Exception:
            conn.close()
            return False

    # ── iCal Export ───────────────────────────────────────

    def export_ical(self, days: int = 30) -> str:
        """导出iCal格式"""
        now = datetime.now(timezone.utc)
        end = now + timedelta(days=days)

        lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//TwitterBot//ContentCalendar//EN",
            "CALSCALE:GREGORIAN",
        ]

        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM content_calendar WHERE scheduled_at IS NOT NULL "
            "AND scheduled_at >= ? AND scheduled_at <= ? ORDER BY scheduled_at",
            (now.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")),
        ).fetchall()
        conn.close()

        for row in rows:
            entry = dict(row)
            scheduled = entry.get("scheduled_at", "")
            if not scheduled:
                continue

            # Parse scheduled_at
            try:
                dt = datetime.fromisoformat(scheduled.replace("Z", "+00:00"))
                dtstart = dt.strftime("%Y%m%dT%H%M%SZ")
                dtend = (dt + timedelta(minutes=15)).strftime("%Y%m%dT%H%M%SZ")
            except (ValueError, AttributeError):
                continue

            lines.extend([
                "BEGIN:VEVENT",
                f"UID:{entry['entry_id']}@twitterbot",
                f"DTSTART:{dtstart}",
                f"DTEND:{dtend}",
                f"SUMMARY:{entry['title']}",
                f"DESCRIPTION:{entry.get('content', '')[:200]}",
                f"STATUS:{'CONFIRMED' if entry['status'] in ('approved', 'scheduled') else 'TENTATIVE'}",
                "END:VEVENT",
            ])

        lines.append("END:VCALENDAR")
        return "\r\n".join(lines)

    # ── Stats ────────────────────────────────────────────

    def stats(self) -> dict:
        """日历统计"""
        conn = self._get_conn()
        total = conn.execute("SELECT COUNT(*) FROM content_calendar").fetchone()[0]
        by_status = {}
        for row in conn.execute(
            "SELECT status, COUNT(*) as cnt FROM content_calendar GROUP BY status"
        ).fetchall():
            by_status[row["status"]] = row["cnt"]

        by_category = {}
        for row in conn.execute(
            "SELECT category, COUNT(*) as cnt FROM content_calendar GROUP BY category"
        ).fetchall():
            by_category[row["category"]] = row["cnt"]

        conn.close()
        return {
            "total": total,
            "by_status": by_status,
            "by_category": by_category,
        }
