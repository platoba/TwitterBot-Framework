"""
Growth Engine for Twitter/X
关注者增长策略 + 留存分析 + 增长实验 + 增长报告
"""

import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any


class GrowthExperiment:
    """增长实验记录"""

    def __init__(
        self,
        name: str,
        strategy: str,
        experiment_id: str = None,
        status: str = "active",
        metrics_before: dict = None,
        metrics_after: dict = None,
        notes: str = "",
        started_at: str = None,
        ended_at: str = None,
    ):
        self.experiment_id = experiment_id or str(uuid.uuid4())
        self.name = name
        self.strategy = strategy
        self.status = status
        self.metrics_before = metrics_before or {}
        self.metrics_after = metrics_after or {}
        self.notes = notes
        self.started_at = started_at or datetime.now(timezone.utc).isoformat()
        self.ended_at = ended_at

    def to_dict(self) -> dict:
        return {
            "experiment_id": self.experiment_id,
            "name": self.name,
            "strategy": self.strategy,
            "status": self.status,
            "metrics_before": self.metrics_before,
            "metrics_after": self.metrics_after,
            "notes": self.notes,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
        }


class GrowthEngine:
    """增长引擎: 策略管理 + 留存分析 + 实验追踪 + 增长报告"""

    STRATEGIES = [
        "keyword_targeting",     # 基于关键词发现目标用户
        "kol_engagement",        # 与行业KOL互动
        "hashtag_riding",        # 热门标签蹭流量
        "thread_building",       # 长线程内容建设
        "reply_engagement",      # 主动回复互动
        "content_calendar",      # 定期内容日历
        "cross_promotion",       # 跨平台推广
        "collaboration",         # 合作互推
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
            CREATE TABLE IF NOT EXISTS growth_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                followers INTEGER DEFAULT 0,
                following INTEGER DEFAULT 0,
                tweets INTEGER DEFAULT 0,
                impressions INTEGER DEFAULT 0,
                profile_visits INTEGER DEFAULT 0,
                mentions INTEGER DEFAULT 0,
                new_followers INTEGER DEFAULT 0,
                lost_followers INTEGER DEFAULT 0,
                snapshot_date TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS growth_experiments (
                experiment_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                strategy TEXT NOT NULL,
                status TEXT DEFAULT 'active',
                metrics_before TEXT DEFAULT '{}',
                metrics_after TEXT DEFAULT '{}',
                notes TEXT DEFAULT '',
                started_at TEXT,
                ended_at TEXT
            );
            CREATE TABLE IF NOT EXISTS growth_targets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                category TEXT DEFAULT 'prospect',
                source_strategy TEXT DEFAULT '',
                keywords TEXT DEFAULT '[]',
                engaged INTEGER DEFAULT 0,
                followed INTEGER DEFAULT 0,
                followed_back INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.commit()
        conn.close()

    # ── Snapshots ────────────────────────────────────────

    def record_snapshot(
        self,
        followers: int,
        following: int = 0,
        tweets: int = 0,
        impressions: int = 0,
        profile_visits: int = 0,
        mentions: int = 0,
        new_followers: int = 0,
        lost_followers: int = 0,
        snapshot_date: str = None,
    ) -> int:
        """记录增长快照"""
        date = snapshot_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        conn = self._get_conn()
        cursor = conn.execute(
            "INSERT INTO growth_snapshots "
            "(followers, following, tweets, impressions, profile_visits, mentions, "
            "new_followers, lost_followers, snapshot_date) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (followers, following, tweets, impressions, profile_visits,
             mentions, new_followers, lost_followers, date),
        )
        conn.commit()
        row_id = cursor.lastrowid
        conn.close()
        return row_id

    def get_snapshots(self, days: int = 30) -> List[dict]:
        """获取最近N天的增长快照"""
        conn = self._get_conn()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = conn.execute(
            "SELECT * FROM growth_snapshots WHERE snapshot_date >= ? ORDER BY snapshot_date",
            (cutoff,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_growth_rate(self, days: int = 7) -> dict:
        """计算增长率"""
        snapshots = self.get_snapshots(days)
        if len(snapshots) < 2:
            return {
                "period_days": days,
                "start_followers": 0,
                "end_followers": 0,
                "net_growth": 0,
                "growth_rate": 0.0,
                "avg_daily_growth": 0.0,
            }

        start = snapshots[0]
        end = snapshots[-1]
        net = end["followers"] - start["followers"]
        rate = (net / start["followers"] * 100) if start["followers"] > 0 else 0.0
        avg_daily = net / len(snapshots) if snapshots else 0.0

        return {
            "period_days": days,
            "start_followers": start["followers"],
            "end_followers": end["followers"],
            "net_growth": net,
            "growth_rate": round(rate, 2),
            "avg_daily_growth": round(avg_daily, 1),
        }

    # ── Retention ────────────────────────────────────────

    def retention_analysis(self, days: int = 30) -> dict:
        """留存分析: 新增vs流失"""
        snapshots = self.get_snapshots(days)
        if not snapshots:
            return {"total_new": 0, "total_lost": 0, "net": 0, "retention_rate": 0.0}

        total_new = sum(s.get("new_followers", 0) for s in snapshots)
        total_lost = sum(s.get("lost_followers", 0) for s in snapshots)
        net = total_new - total_lost
        retention = ((total_new - total_lost) / total_new * 100) if total_new > 0 else 0.0

        return {
            "period_days": days,
            "total_new": total_new,
            "total_lost": total_lost,
            "net": net,
            "retention_rate": round(retention, 2),
            "churn_rate": round(100 - retention, 2) if total_new > 0 else 0.0,
        }

    # ── Experiments ──────────────────────────────────────

    def create_experiment(self, name: str, strategy: str, metrics_before: dict = None, notes: str = "") -> GrowthExperiment:
        """创建增长实验"""
        exp = GrowthExperiment(
            name=name, strategy=strategy,
            metrics_before=metrics_before or {}, notes=notes,
        )
        conn = self._get_conn()
        conn.execute(
            "INSERT INTO growth_experiments "
            "(experiment_id, name, strategy, status, metrics_before, notes, started_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (exp.experiment_id, exp.name, exp.strategy, exp.status,
             json.dumps(exp.metrics_before), exp.notes, exp.started_at),
        )
        conn.commit()
        conn.close()
        return exp

    def complete_experiment(self, experiment_id: str, metrics_after: dict = None, notes: str = "") -> bool:
        """完成实验"""
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE growth_experiments SET status='completed', metrics_after=?, "
            "notes=CASE WHEN ? != '' THEN ? ELSE notes END, ended_at=? "
            "WHERE experiment_id=? AND status='active'",
            (json.dumps(metrics_after or {}), notes, notes,
             datetime.now(timezone.utc).isoformat(), experiment_id),
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return updated

    def list_experiments(self, status: str = None, limit: int = 50) -> List[dict]:
        """列出实验"""
        conn = self._get_conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM growth_experiments WHERE status=? ORDER BY started_at DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM growth_experiments ORDER BY started_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        conn.close()
        results = []
        for r in rows:
            d = dict(r)
            for field in ("metrics_before", "metrics_after"):
                if isinstance(d.get(field), str):
                    try:
                        d[field] = json.loads(d[field])
                    except (json.JSONDecodeError, TypeError):
                        d[field] = {}
            results.append(d)
        return results

    def get_experiment(self, experiment_id: str) -> Optional[dict]:
        """获取实验详情"""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM growth_experiments WHERE experiment_id=?",
            (experiment_id,),
        ).fetchone()
        conn.close()
        if not row:
            return None
        d = dict(row)
        for field in ("metrics_before", "metrics_after"):
            if isinstance(d.get(field), str):
                try:
                    d[field] = json.loads(d[field])
                except (json.JSONDecodeError, TypeError):
                    d[field] = {}
        return d

    # ── Target Discovery ─────────────────────────────────

    def add_target(self, username: str, category: str = "prospect",
                   source_strategy: str = "", keywords: list = None) -> int:
        """添加增长目标用户"""
        conn = self._get_conn()
        cursor = conn.execute(
            "INSERT OR IGNORE INTO growth_targets "
            "(username, category, source_strategy, keywords) VALUES (?, ?, ?, ?)",
            (username, category, source_strategy, json.dumps(keywords or [])),
        )
        conn.commit()
        row_id = cursor.lastrowid
        conn.close()
        return row_id

    def get_targets(self, category: str = None, limit: int = 100) -> List[dict]:
        """获取目标用户列表"""
        conn = self._get_conn()
        if category:
            rows = conn.execute(
                "SELECT * FROM growth_targets WHERE category=? LIMIT ?",
                (category, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM growth_targets LIMIT ?", (limit,),
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def mark_engaged(self, username: str) -> bool:
        """标记已互动"""
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE growth_targets SET engaged=1 WHERE username=?",
            (username,),
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return updated

    # ── Growth Report ────────────────────────────────────

    def generate_report(self, days: int = 30) -> dict:
        """生成增长报告"""
        growth = self.get_growth_rate(days)
        retention = self.retention_analysis(days)
        experiments = self.list_experiments(limit=10)
        snapshots = self.get_snapshots(days)

        # Best day
        best_day = None
        if snapshots:
            best = max(snapshots, key=lambda s: s.get("new_followers", 0))
            best_day = {
                "date": best["snapshot_date"],
                "new_followers": best.get("new_followers", 0),
            }

        return {
            "period_days": days,
            "growth": growth,
            "retention": retention,
            "best_day": best_day,
            "total_snapshots": len(snapshots),
            "recent_experiments": len(experiments),
            "strategies_tested": list(set(e.get("strategy", "") for e in experiments)),
        }

    def export_csv(self, days: int = 30) -> str:
        """导出CSV"""
        snapshots = self.get_snapshots(days)
        lines = ["date,followers,following,tweets,impressions,new_followers,lost_followers"]
        for s in snapshots:
            lines.append(
                f"{s['snapshot_date']},{s['followers']},{s['following']},"
                f"{s['tweets']},{s['impressions']},{s.get('new_followers',0)},"
                f"{s.get('lost_followers',0)}"
            )
        return "\n".join(lines)
