"""
Twitter Poll Engine v1.0
投票管理: 创建/发布/追踪/分析/策略优化

Features:
- Poll creation with 2-4 options
- Duration management (5min to 7 days)
- Real-time vote tracking and distribution analysis
- Poll performance analytics (votes/min, engagement correlation)
- Poll strategy suggestions (timing, topic, format)
- A/B testing integration for poll effectiveness
- Historical pattern analysis
- SQLite persistence
"""

import json
import sqlite3
import uuid
import math
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any


class PollOption:
    """投票选项"""

    def __init__(self, label: str, votes: int = 0, option_id: str = None):
        self.option_id = option_id or str(uuid.uuid4())[:8]
        self.label = label
        self.votes = votes

    def to_dict(self) -> dict:
        return {
            "option_id": self.option_id,
            "label": self.label,
            "votes": self.votes,
        }


class Poll:
    """Twitter投票数据模型"""

    def __init__(
        self,
        question: str,
        options: List[Dict[str, Any]] = None,
        poll_id: str = None,
        tweet_id: str = None,
        status: str = "draft",
        duration_minutes: int = 1440,  # 24 hours default
        category: str = "general",
        started_at: str = None,
        ended_at: str = None,
        created_at: str = None,
    ):
        self.poll_id = poll_id or str(uuid.uuid4())[:12]
        self.question = question
        self.options = options or []
        self.tweet_id = tweet_id
        self.status = status
        self.duration_minutes = max(5, min(10080, duration_minutes))  # 5min - 7 days
        self.category = category
        self.started_at = started_at
        self.ended_at = ended_at
        self.created_at = created_at or datetime.now(timezone.utc).isoformat()

    @property
    def total_votes(self) -> int:
        return sum(o.get("votes", 0) for o in self.options)

    @property
    def vote_distribution(self) -> List[Dict[str, Any]]:
        """投票分布百分比"""
        total = self.total_votes
        result = []
        for opt in self.options:
            pct = (opt.get("votes", 0) / total * 100) if total > 0 else 0.0
            result.append({
                "label": opt.get("label", ""),
                "votes": opt.get("votes", 0),
                "percentage": round(pct, 2),
            })
        return result

    @property
    def winner(self) -> Optional[dict]:
        """获胜选项"""
        if not self.options:
            return None
        return max(self.options, key=lambda o: o.get("votes", 0))

    def to_dict(self) -> dict:
        return {
            "poll_id": self.poll_id,
            "question": self.question,
            "options": self.options,
            "tweet_id": self.tweet_id,
            "status": self.status,
            "duration_minutes": self.duration_minutes,
            "category": self.category,
            "total_votes": self.total_votes,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "created_at": self.created_at,
        }


class PollEngine:
    """投票引擎: 创建/追踪/分析/策略"""

    CATEGORIES = [
        "general", "product", "tech", "opinion", "fun",
        "market_research", "feedback", "prediction", "trivia",
    ]

    OPTIMAL_DURATIONS = {
        "quick_engagement": 60,      # 1 hour
        "standard": 1440,            # 24 hours
        "extended": 4320,            # 3 days
        "max_reach": 10080,          # 7 days
    }

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
            CREATE TABLE IF NOT EXISTS polls (
                poll_id TEXT PRIMARY KEY,
                question TEXT NOT NULL,
                options TEXT DEFAULT '[]',
                tweet_id TEXT,
                status TEXT DEFAULT 'draft',
                duration_minutes INTEGER DEFAULT 1440,
                category TEXT DEFAULT 'general',
                started_at TEXT,
                ended_at TEXT,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS poll_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                poll_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                votes_snapshot TEXT DEFAULT '{}',
                total_votes INTEGER DEFAULT 0,
                FOREIGN KEY (poll_id) REFERENCES polls(poll_id)
            );
            CREATE TABLE IF NOT EXISTS poll_strategies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL,
                best_duration_minutes INTEGER DEFAULT 1440,
                best_option_count INTEGER DEFAULT 4,
                avg_votes REAL DEFAULT 0,
                avg_engagement_rate REAL DEFAULT 0,
                sample_count INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.commit()
        conn.close()

    # ── Poll CRUD ────────────────────────────────────────

    def create_poll(self, question: str, options: List[str],
                    duration_minutes: int = 1440, category: str = "general",
                    tweet_id: str = None) -> Poll:
        """创建投票 (2-4个选项)"""
        if len(options) < 2:
            raise ValueError("Poll must have at least 2 options")
        if len(options) > 4:
            raise ValueError("Poll can have at most 4 options")

        poll_options = [
            PollOption(label=label).to_dict()
            for label in options
        ]
        poll = Poll(
            question=question,
            options=poll_options,
            duration_minutes=duration_minutes,
            category=category,
            tweet_id=tweet_id,
        )

        conn = self._get_conn()
        conn.execute(
            """INSERT INTO polls (poll_id, question, options, tweet_id, status,
               duration_minutes, category, started_at, ended_at, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (poll.poll_id, poll.question, json.dumps(poll.options),
             poll.tweet_id, poll.status, poll.duration_minutes,
             poll.category, poll.started_at, poll.ended_at, poll.created_at),
        )
        conn.commit()
        conn.close()
        return poll

    def get_poll(self, poll_id: str) -> Optional[Poll]:
        """获取投票"""
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM polls WHERE poll_id=?", (poll_id,)).fetchone()
        conn.close()
        if not row:
            return None
        return Poll(
            question=row["question"],
            options=json.loads(row["options"]),
            poll_id=row["poll_id"],
            tweet_id=row["tweet_id"],
            status=row["status"],
            duration_minutes=row["duration_minutes"],
            category=row["category"],
            started_at=row["started_at"],
            ended_at=row["ended_at"],
            created_at=row["created_at"],
        )

    def list_polls(self, status: str = None, category: str = None,
                   limit: int = 50) -> List[dict]:
        """列出投票"""
        conn = self._get_conn()
        query = "SELECT * FROM polls WHERE 1=1"
        params = []
        if status:
            query += " AND status=?"
            params.append(status)
        if category:
            query += " AND category=?"
            params.append(category)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
        conn.close()
        results = []
        for r in rows:
            d = dict(r)
            d["options"] = json.loads(d["options"])
            results.append(d)
        return results

    def start_poll(self, poll_id: str) -> bool:
        """启动投票"""
        now = datetime.now(timezone.utc).isoformat()
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE polls SET status='active', started_at=? "
            "WHERE poll_id=? AND status='draft'",
            (now, poll_id),
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return updated

    def end_poll(self, poll_id: str) -> bool:
        """结束投票"""
        now = datetime.now(timezone.utc).isoformat()
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE polls SET status='ended', ended_at=? "
            "WHERE poll_id=? AND status='active'",
            (now, poll_id),
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return updated

    def delete_poll(self, poll_id: str) -> bool:
        """删除投票"""
        conn = self._get_conn()
        cursor = conn.execute("DELETE FROM polls WHERE poll_id=?", (poll_id,))
        conn.execute("DELETE FROM poll_snapshots WHERE poll_id=?", (poll_id,))
        conn.commit()
        deleted = cursor.rowcount > 0
        conn.close()
        return deleted

    # ── Vote Tracking ────────────────────────────────────

    def update_votes(self, poll_id: str, votes: Dict[str, int]) -> bool:
        """更新投票数据 {option_id: vote_count}"""
        conn = self._get_conn()
        row = conn.execute("SELECT options FROM polls WHERE poll_id=?", (poll_id,)).fetchone()
        if not row:
            conn.close()
            return False

        options = json.loads(row["options"])
        for opt in options:
            if opt["option_id"] in votes:
                opt["votes"] = votes[opt["option_id"]]

        conn.execute(
            "UPDATE polls SET options=? WHERE poll_id=?",
            (json.dumps(options), poll_id),
        )
        conn.commit()
        conn.close()
        return True

    def record_snapshot(self, poll_id: str) -> int:
        """记录投票快照"""
        poll = self.get_poll(poll_id)
        if not poll:
            return -1

        now = datetime.now(timezone.utc).isoformat()
        votes_map = {o["option_id"]: o["votes"] for o in poll.options}
        conn = self._get_conn()
        cursor = conn.execute(
            """INSERT INTO poll_snapshots (poll_id, timestamp, votes_snapshot, total_votes)
               VALUES (?, ?, ?, ?)""",
            (poll_id, now, json.dumps(votes_map), poll.total_votes),
        )
        conn.commit()
        row_id = cursor.lastrowid
        conn.close()
        return row_id

    def get_snapshots(self, poll_id: str) -> List[dict]:
        """获取投票快照"""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM poll_snapshots WHERE poll_id=? ORDER BY timestamp",
            (poll_id,),
        ).fetchall()
        conn.close()
        results = []
        for r in rows:
            d = dict(r)
            d["votes_snapshot"] = json.loads(d["votes_snapshot"])
            results.append(d)
        return results

    # ── Analytics ────────────────────────────────────────

    def analyze_poll(self, poll_id: str) -> dict:
        """分析投票结果"""
        poll = self.get_poll(poll_id)
        if not poll:
            return {"error": "Poll not found"}

        distribution = poll.vote_distribution
        winner = poll.winner
        total = poll.total_votes

        # 计算分散度 (Shannon entropy)
        entropy = 0.0
        if total > 0:
            for opt in distribution:
                p = opt["percentage"] / 100
                if p > 0:
                    entropy -= p * math.log2(p)
        max_entropy = math.log2(len(distribution)) if distribution else 1
        dispersion = round(entropy / max_entropy, 4) if max_entropy > 0 else 0

        # Votes per minute
        vpm = 0.0
        if poll.started_at:
            start = datetime.fromisoformat(poll.started_at)
            end = datetime.fromisoformat(poll.ended_at) if poll.ended_at else datetime.now(timezone.utc)
            minutes = (end - start).total_seconds() / 60
            vpm = round(total / minutes, 2) if minutes > 0 else 0

        # 竞争度 (top 2 options差距)
        sorted_opts = sorted(distribution, key=lambda x: x["votes"], reverse=True)
        competitiveness = 0.0
        if len(sorted_opts) >= 2 and total > 0:
            gap = abs(sorted_opts[0]["percentage"] - sorted_opts[1]["percentage"])
            competitiveness = round(100 - gap, 2)

        return {
            "poll_id": poll_id,
            "question": poll.question,
            "status": poll.status,
            "total_votes": total,
            "distribution": distribution,
            "winner": winner,
            "entropy": round(entropy, 4),
            "dispersion": dispersion,  # 0=unanimous, 1=perfectly even
            "votes_per_minute": vpm,
            "competitiveness": competitiveness,  # 0=landslide, 100=tie
            "option_count": len(poll.options),
        }

    def get_category_stats(self) -> Dict[str, dict]:
        """按类别统计投票表现"""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM polls WHERE status='ended'"
        ).fetchall()
        conn.close()

        categories: Dict[str, dict] = {}
        for r in rows:
            cat = r["category"]
            options = json.loads(r["options"])
            total = sum(o.get("votes", 0) for o in options)

            if cat not in categories:
                categories[cat] = {
                    "count": 0,
                    "total_votes": 0,
                    "avg_votes": 0,
                    "best_poll": None,
                    "best_votes": 0,
                }
            categories[cat]["count"] += 1
            categories[cat]["total_votes"] += total
            if total > categories[cat]["best_votes"]:
                categories[cat]["best_votes"] = total
                categories[cat]["best_poll"] = r["question"]

        for cat in categories:
            c = categories[cat]
            c["avg_votes"] = round(c["total_votes"] / c["count"], 1) if c["count"] > 0 else 0

        return categories

    # ── Strategy ─────────────────────────────────────────

    def suggest_duration(self, category: str = "general",
                         goal: str = "standard") -> int:
        """建议投票时长"""
        if goal in self.OPTIMAL_DURATIONS:
            return self.OPTIMAL_DURATIONS[goal]

        # Check historical data for this category
        conn = self._get_conn()
        row = conn.execute(
            "SELECT AVG(duration_minutes) as avg_dur FROM polls "
            "WHERE category=? AND status='ended'",
            (category,),
        ).fetchone()
        conn.close()

        if row and row["avg_dur"]:
            return round(row["avg_dur"])
        return 1440

    def suggest_option_count(self, question_type: str = "opinion") -> int:
        """建议选项数量"""
        suggestions = {
            "yes_no": 2,
            "preference": 3,
            "opinion": 4,
            "trivia": 4,
            "prediction": 2,
            "this_or_that": 2,
        }
        return suggestions.get(question_type, 4)

    def generate_follow_up_ideas(self, poll_id: str) -> List[str]:
        """根据投票结果生成后续内容思路"""
        poll = self.get_poll(poll_id)
        if not poll:
            return []

        ideas = []
        analysis = self.analyze_poll(poll_id)
        total = analysis.get("total_votes", 0)
        winner = analysis.get("winner")

        if total > 0 and winner:
            winner_label = winner.get("label", "")
            ideas.append(f"Thread: Deep dive into why '{winner_label}' won the poll")
            ideas.append(f"Follow-up poll: Narrow down within '{winner_label}' options")

        if analysis.get("competitiveness", 0) > 70:
            ideas.append("Debate thread: The community is split! Here's both sides")
            ideas.append("Poll series: Break down the close race into sub-topics")

        if analysis.get("dispersion", 0) > 0.9:
            ideas.append("Analysis thread: The community has diverse opinions on this")

        if total < 50:
            ideas.append("Repost with engagement hook to boost participation")

        if not ideas:
            ideas.append("Share results with commentary")
            ideas.append("Create a follow-up poll with related topic")

        return ideas

    def export_results_text(self, poll_id: str) -> str:
        """导出投票结果为文本"""
        poll = self.get_poll(poll_id)
        if not poll:
            return "Poll not found"

        lines = [f"📊 {poll.question}", ""]
        distribution = poll.vote_distribution
        total = poll.total_votes

        for opt in distribution:
            bar_len = int(opt["percentage"] / 5) if opt["percentage"] > 0 else 0
            bar = "█" * bar_len + "░" * (20 - bar_len)
            lines.append(f"  {opt['label']}: {bar} {opt['percentage']}% ({opt['votes']} votes)")

        lines.append(f"\nTotal votes: {total}")
        if poll.status == "ended":
            winner = poll.winner
            if winner:
                lines.append(f"Winner: {winner.get('label', 'N/A')}")

        return "\n".join(lines)
