"""
Trend Tracker v1.0
趋势追踪引擎 — 实时热点检测 + 相关度评估 + 参与时机建议

Features:
- TrendCollector: 多源趋势采集 (hashtag频次, 话题爆发, 关键词突增)
- BurstDetector: 爆发检测算法 (移动窗口 + Z-score异常检测)
- RelevanceEngine: 与自身Niche的相关度评估
- OpportunityScorer: 参与时机评分 (早期=高分, 衰退=低分)
- TrendAlert: 趋势预警 + 优先级分级
- TrendHistory: 历史趋势归档 + 复现检测
"""

import json
import logging
import math
import sqlite3
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Dict, List, Optional, Tuple, Deque

logger = logging.getLogger(__name__)


# ── 数据模型 ──


class TrendPhase(Enum):
    """趋势阶段"""
    EMERGING = "emerging"       # 萌芽期 (最佳参与时机)
    RISING = "rising"           # 上升期
    PEAKING = "peaking"         # 高峰期
    DECLINING = "declining"     # 衰退期
    DEAD = "dead"               # 已过时


class TrendPriority(Enum):
    """趋势优先级"""
    CRITICAL = "critical"       # 必须立即参与
    HIGH = "high"               # 强烈建议参与
    MEDIUM = "medium"           # 可以参与
    LOW = "low"                 # 可忽略
    IRRELEVANT = "irrelevant"   # 不相关


@dataclass
class TrendItem:
    """趋势条目"""
    keyword: str
    volume: int = 0                 # 当前提及量
    volume_change: float = 0.0      # 提及量变化率
    phase: str = "emerging"
    priority: str = "medium"
    relevance_score: float = 0.0    # 与niche相关度 (0-1)
    opportunity_score: float = 0.0  # 参与时机评分 (0-1)
    first_seen: str = ""
    last_seen: str = ""
    peak_volume: int = 0
    sample_tweets: List[str] = field(default_factory=list)
    related_hashtags: List[str] = field(default_factory=list)
    category: str = ""

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict) -> "TrendItem":
        known = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in d.items() if k in known}
        return cls(**filtered)


@dataclass
class TrendAlert:
    """趋势预警"""
    trend: TrendItem
    alert_type: str             # "new_trend", "phase_change", "volume_spike", "opportunity"
    message: str = ""
    created_at: str = ""
    acknowledged: bool = False

    def to_dict(self) -> Dict:
        d = asdict(self)
        d["trend"] = self.trend.to_dict()
        return d


# ── 爆发检测器 ──


class BurstDetector:
    """基于Z-score的爆发检测"""

    def __init__(self, window_size: int = 24, z_threshold: float = 2.0):
        self.window_size = window_size
        self.z_threshold = z_threshold
        self._history: Dict[str, Deque[int]] = defaultdict(lambda: deque(maxlen=window_size))

    def add_observation(self, keyword: str, count: int):
        """添加观测值"""
        self._history[keyword].append(count)

    def is_burst(self, keyword: str, current_count: int) -> Tuple[bool, float]:
        """检测是否爆发"""
        history = self._history.get(keyword, deque())
        if len(history) < 3:
            # 数据不足，用简单阈值
            return current_count > 50, 0.0

        values = list(history)
        mean = sum(values) / len(values)
        if mean == 0:
            return current_count > 10, 0.0

        variance = sum((v - mean) ** 2 for v in values) / len(values)
        std = math.sqrt(variance) if variance > 0 else 1.0

        z_score = (current_count - mean) / std
        return z_score > self.z_threshold, round(z_score, 2)

    def get_trend_phase(self, keyword: str) -> TrendPhase:
        """判断趋势阶段"""
        history = list(self._history.get(keyword, deque()))
        if len(history) < 3:
            return TrendPhase.EMERGING

        recent = history[-3:]
        older = history[:-3] if len(history) > 3 else history[:1]

        recent_avg = sum(recent) / len(recent)
        older_avg = sum(older) / len(older) if older else 0

        if older_avg == 0:
            return TrendPhase.EMERGING if recent_avg < 100 else TrendPhase.RISING

        growth = (recent_avg - older_avg) / older_avg

        # 最近3个值的趋势
        if len(recent) >= 3:
            if recent[-1] < recent[-2] < recent[-3]:
                return TrendPhase.DECLINING
            if recent[-1] > recent[-2] and recent[-2] > recent[-3]:
                if growth > 0.5:
                    return TrendPhase.RISING
                return TrendPhase.PEAKING

        if growth > 1.0:
            return TrendPhase.EMERGING
        elif growth > 0.3:
            return TrendPhase.RISING
        elif growth > -0.1:
            return TrendPhase.PEAKING
        elif growth > -0.5:
            return TrendPhase.DECLINING
        return TrendPhase.DEAD

    def volume_change_rate(self, keyword: str) -> float:
        """计算量变化率"""
        history = list(self._history.get(keyword, deque()))
        if len(history) < 2:
            return 0.0
        prev = history[-2] if history[-2] > 0 else 1
        return (history[-1] - prev) / prev


# ── 相关度引擎 ──


class RelevanceEngine:
    """趋势与Niche的相关度评估"""

    def __init__(self, niche_keywords: List[str] = None, niche_hashtags: List[str] = None):
        self.niche_keywords = [k.lower() for k in (niche_keywords or [])]
        self.niche_hashtags = [h.lower().lstrip("#") for h in (niche_hashtags or [])]

    def score(self, trend: TrendItem) -> float:
        """评估趋势相关度"""
        if not self.niche_keywords and not self.niche_hashtags:
            return 0.5  # 无niche配置时给中性分

        score = 0.0
        keyword_lower = trend.keyword.lower()

        # 直接匹配
        if keyword_lower in self.niche_keywords:
            score += 0.5
        if keyword_lower.lstrip("#") in self.niche_hashtags:
            score += 0.5

        # 部分匹配
        for nk in self.niche_keywords:
            if nk in keyword_lower or keyword_lower in nk:
                score += 0.2
                break

        # 相关hashtag匹配
        for rh in trend.related_hashtags:
            rh_lower = rh.lower().lstrip("#")
            if rh_lower in self.niche_hashtags or rh_lower in self.niche_keywords:
                score += 0.1

        # 样本推文内容匹配
        if trend.sample_tweets:
            match_count = 0
            for tweet in trend.sample_tweets[:5]:
                tweet_lower = tweet.lower()
                if any(kw in tweet_lower for kw in self.niche_keywords):
                    match_count += 1
            if trend.sample_tweets:
                score += 0.2 * (match_count / min(5, len(trend.sample_tweets)))

        return min(1.0, score)


# ── 时机评分 ──


class OpportunityScorer:
    """参与时机评分器"""

    PHASE_SCORES = {
        TrendPhase.EMERGING: 1.0,
        TrendPhase.RISING: 0.8,
        TrendPhase.PEAKING: 0.5,
        TrendPhase.DECLINING: 0.2,
        TrendPhase.DEAD: 0.0,
    }

    def score(self, trend: TrendItem, phase: TrendPhase, relevance: float) -> float:
        """综合时机评分"""
        phase_score = self.PHASE_SCORES.get(phase, 0.5)

        # 相关度加权
        weighted = phase_score * 0.4 + relevance * 0.4

        # 量级加成 (大趋势更值得参与)
        volume_bonus = min(0.2, math.log1p(trend.volume) / 20)
        weighted += volume_bonus

        return round(min(1.0, weighted), 3)

    def get_priority(self, opportunity_score: float, relevance: float) -> TrendPriority:
        """确定优先级"""
        combined = opportunity_score * 0.6 + relevance * 0.4

        if combined >= 0.8:
            return TrendPriority.CRITICAL
        elif combined >= 0.6:
            return TrendPriority.HIGH
        elif combined >= 0.4:
            return TrendPriority.MEDIUM
        elif combined >= 0.2:
            return TrendPriority.LOW
        return TrendPriority.IRRELEVANT


# ── 趋势历史 ──


class TrendHistory:
    """趋势历史归档"""

    def __init__(self, db_path: str = "trend_history.db"):
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path)
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
        return self._local.conn

    def _init_db(self):
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS trends (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT NOT NULL,
                volume INTEGER DEFAULT 0,
                volume_change REAL DEFAULT 0,
                phase TEXT DEFAULT 'emerging',
                relevance_score REAL DEFAULT 0,
                opportunity_score REAL DEFAULT 0,
                priority TEXT DEFAULT 'medium',
                related_hashtags TEXT DEFAULT '[]',
                category TEXT DEFAULT '',
                recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS trend_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                message TEXT DEFAULT '',
                acknowledged INTEGER DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_trends_kw ON trends(keyword);
            CREATE INDEX IF NOT EXISTS idx_trends_time ON trends(recorded_at);
            CREATE INDEX IF NOT EXISTS idx_trends_phase ON trends(phase);
            CREATE INDEX IF NOT EXISTS idx_alerts_ack ON trend_alerts(acknowledged);
        """)
        conn.commit()

    def record(self, trend: TrendItem):
        """记录趋势快照"""
        conn = self._get_conn()
        conn.execute(
            """INSERT INTO trends(keyword, volume, volume_change, phase, relevance_score,
               opportunity_score, priority, related_hashtags, category)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (
                trend.keyword, trend.volume, trend.volume_change, trend.phase,
                trend.relevance_score, trend.opportunity_score, trend.priority,
                json.dumps(trend.related_hashtags), trend.category,
            ),
        )
        conn.commit()

    def add_alert(self, alert: TrendAlert) -> int:
        """添加预警"""
        conn = self._get_conn()
        cursor = conn.execute(
            "INSERT INTO trend_alerts(keyword, alert_type, message) VALUES(?,?,?)",
            (alert.trend.keyword, alert.alert_type, alert.message),
        )
        conn.commit()
        return cursor.lastrowid

    def acknowledge_alert(self, alert_id: int):
        """确认预警"""
        conn = self._get_conn()
        conn.execute("UPDATE trend_alerts SET acknowledged=1 WHERE id=?", (alert_id,))
        conn.commit()

    def get_unacknowledged(self, limit: int = 20) -> List[Dict]:
        """获取未确认预警"""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM trend_alerts WHERE acknowledged=0 ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_trend_history(self, keyword: str, days: int = 7) -> List[Dict]:
        """获取关键词历史"""
        conn = self._get_conn()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = conn.execute(
            "SELECT * FROM trends WHERE keyword=? AND recorded_at>=? ORDER BY recorded_at",
            (keyword, cutoff),
        ).fetchall()
        return [dict(r) for r in rows]

    def find_recurring(self, min_occurrences: int = 3, days: int = 90) -> List[Dict]:
        """发现周期性趋势"""
        conn = self._get_conn()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = conn.execute(
            """SELECT keyword, COUNT(DISTINCT date(recorded_at)) as occurrence_days,
                      MAX(volume) as max_volume, AVG(relevance_score) as avg_relevance
               FROM trends WHERE recorded_at>=?
               GROUP BY keyword HAVING occurrence_days>=?
               ORDER BY occurrence_days DESC""",
            (cutoff, min_occurrences),
        ).fetchall()
        return [dict(r) for r in rows]

    def hot_keywords(self, hours: int = 24, limit: int = 20) -> List[Dict]:
        """最近热词"""
        conn = self._get_conn()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        rows = conn.execute(
            """SELECT keyword, MAX(volume) as max_volume, MAX(opportunity_score) as best_opportunity,
                      COUNT(*) as snapshots
               FROM trends WHERE recorded_at>=?
               GROUP BY keyword ORDER BY max_volume DESC LIMIT ?""",
            (cutoff, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def stats(self) -> Dict:
        """统计"""
        conn = self._get_conn()
        total_trends = conn.execute("SELECT COUNT(DISTINCT keyword) FROM trends").fetchone()[0]
        total_snapshots = conn.execute("SELECT COUNT(*) FROM trends").fetchone()[0]
        total_alerts = conn.execute("SELECT COUNT(*) FROM trend_alerts").fetchone()[0]
        unack_alerts = conn.execute("SELECT COUNT(*) FROM trend_alerts WHERE acknowledged=0").fetchone()[0]
        return {
            "unique_trends": total_trends,
            "total_snapshots": total_snapshots,
            "total_alerts": total_alerts,
            "unacknowledged_alerts": unack_alerts,
        }


# ── 组合接口 ──


class TrendTracker:
    """趋势追踪引擎 — 统一入口"""

    def __init__(
        self,
        niche_keywords: List[str] = None,
        niche_hashtags: List[str] = None,
        db_dir: str = ".",
        burst_window: int = 24,
        burst_threshold: float = 2.0,
    ):
        self.burst_detector = BurstDetector(burst_window, burst_threshold)
        self.relevance = RelevanceEngine(niche_keywords, niche_hashtags)
        self.opportunity = OpportunityScorer()
        self.history = TrendHistory(f"{db_dir}/trend_history.db")
        self._active_trends: Dict[str, TrendItem] = {}

    def process_trending(self, trending_data: List[Dict]) -> List[TrendItem]:
        """处理趋势数据"""
        results = []
        now = datetime.now(timezone.utc).isoformat()

        for data in trending_data:
            keyword = data.get("keyword", data.get("name", ""))
            volume = data.get("volume", data.get("tweet_volume", 0)) or 0

            if not keyword:
                continue

            # 爆发检测
            self.burst_detector.add_observation(keyword, volume)
            is_burst, z_score = self.burst_detector.is_burst(keyword, volume)

            # 阶段判断
            phase = self.burst_detector.get_trend_phase(keyword)

            # 构建趋势条目
            trend = TrendItem(
                keyword=keyword,
                volume=volume,
                volume_change=self.burst_detector.volume_change_rate(keyword),
                phase=phase.value,
                first_seen=self._active_trends.get(keyword, TrendItem(keyword=keyword)).first_seen or now,
                last_seen=now,
                peak_volume=max(volume, self._active_trends.get(keyword, TrendItem(keyword=keyword)).peak_volume),
                sample_tweets=data.get("sample_tweets", [])[:5],
                related_hashtags=data.get("related_hashtags", data.get("related", []))[:10],
                category=data.get("category", ""),
            )

            # 相关度评分
            trend.relevance_score = self.relevance.score(trend)

            # 时机评分
            trend.opportunity_score = self.opportunity.score(trend, phase, trend.relevance_score)

            # 优先级
            priority = self.opportunity.get_priority(trend.opportunity_score, trend.relevance_score)
            trend.priority = priority.value

            # 生成预警
            old_trend = self._active_trends.get(keyword)
            alerts = self._check_alerts(trend, old_trend, is_burst, z_score)
            for alert in alerts:
                self.history.add_alert(alert)

            # 记录历史
            self.history.record(trend)

            # 更新活跃趋势
            self._active_trends[keyword] = trend
            results.append(trend)

        # 按时机评分排序
        results.sort(key=lambda t: t.opportunity_score, reverse=True)
        return results

    def _check_alerts(
        self, trend: TrendItem, old: Optional[TrendItem], is_burst: bool, z_score: float
    ) -> List[TrendAlert]:
        """检查是否需要预警"""
        alerts = []
        now = datetime.now(timezone.utc).isoformat()

        # 新趋势
        if old is None and trend.relevance_score > 0.3:
            alerts.append(TrendAlert(
                trend=trend,
                alert_type="new_trend",
                message=f"🆕 New relevant trend: {trend.keyword} (relevance: {trend.relevance_score:.2f})",
                created_at=now,
            ))

        # 爆发
        if is_burst and z_score > 3.0:
            alerts.append(TrendAlert(
                trend=trend,
                alert_type="volume_spike",
                message=f"📈 Volume spike: {trend.keyword} z-score={z_score} volume={trend.volume}",
                created_at=now,
            ))

        # 阶段变化
        if old and old.phase != trend.phase:
            alerts.append(TrendAlert(
                trend=trend,
                alert_type="phase_change",
                message=f"🔄 Phase change: {trend.keyword} {old.phase} → {trend.phase}",
                created_at=now,
            ))

        # 高时机窗口
        if trend.opportunity_score > 0.8 and trend.relevance_score > 0.5:
            alerts.append(TrendAlert(
                trend=trend,
                alert_type="opportunity",
                message=f"🎯 High opportunity: {trend.keyword} score={trend.opportunity_score:.2f}",
                created_at=now,
            ))

        return alerts

    def get_active_trends(
        self,
        min_relevance: float = 0.0,
        phase: Optional[str] = None,
        priority: Optional[str] = None,
    ) -> List[TrendItem]:
        """获取活跃趋势"""
        trends = list(self._active_trends.values())

        if min_relevance > 0:
            trends = [t for t in trends if t.relevance_score >= min_relevance]
        if phase:
            trends = [t for t in trends if t.phase == phase]
        if priority:
            trends = [t for t in trends if t.priority == priority]

        trends.sort(key=lambda t: t.opportunity_score, reverse=True)
        return trends

    def get_actionable(self, top_n: int = 5) -> List[TrendItem]:
        """获取最值得参与的趋势"""
        trends = self.get_active_trends(min_relevance=0.3)
        # 过滤掉已过时的
        actionable = [t for t in trends if t.phase not in ("dead", "declining")]
        return actionable[:top_n]

    def suggest_content(self, trend: TrendItem) -> List[Dict]:
        """建议参与内容"""
        suggestions = []

        # 基于趋势阶段
        if trend.phase == "emerging":
            suggestions.append({
                "type": "first_mover",
                "prompt": f"Write a thought-leadership tweet about {trend.keyword} before it goes mainstream",
                "timing": "post immediately",
            })
        elif trend.phase == "rising":
            suggestions.append({
                "type": "hot_take",
                "prompt": f"Share your unique perspective on the trending topic: {trend.keyword}",
                "timing": "post within 2 hours",
            })
        elif trend.phase == "peaking":
            suggestions.append({
                "type": "thread",
                "prompt": f"Write a comprehensive thread about {trend.keyword} with data and insights",
                "timing": "post within 4 hours",
            })

        # 通用建议
        if trend.related_hashtags:
            top_tags = trend.related_hashtags[:5]
            suggestions.append({
                "type": "hashtag_ride",
                "prompt": f"Create content using these trending hashtags: {', '.join(top_tags)}",
                "timing": "flexible",
            })

        return suggestions

    def export_report(self, trends: List[TrendItem], format: str = "text") -> str:
        """导出趋势报告"""
        if format == "json":
            return json.dumps([t.to_dict() for t in trends], indent=2, default=str)
        elif format == "text":
            lines = [f"📊 Trend Report ({len(trends)} trends)", "=" * 50]
            for i, t in enumerate(trends, 1):
                phase_emoji = {
                    "emerging": "🌱", "rising": "📈", "peaking": "🔥",
                    "declining": "📉", "dead": "💀",
                }.get(t.phase, "❓")
                prio_emoji = {
                    "critical": "🚨", "high": "🔴", "medium": "🟡",
                    "low": "🟢", "irrelevant": "⚪",
                }.get(t.priority, "⚪")
                lines.append(
                    f"#{i} {phase_emoji} {prio_emoji} {t.keyword} "
                    f"[Vol:{t.volume} Δ{t.volume_change:+.1%}]"
                )
                lines.append(
                    f"   Relevance:{t.relevance_score:.2f} "
                    f"Opportunity:{t.opportunity_score:.2f} "
                    f"Phase:{t.phase}"
                )
                if t.related_hashtags:
                    lines.append(f"   Tags: {', '.join(t.related_hashtags[:5])}")
                lines.append("")
            return "\n".join(lines)
        return ""

    def stats(self) -> Dict:
        """统计"""
        active = len(self._active_trends)
        by_phase = defaultdict(int)
        by_priority = defaultdict(int)
        for t in self._active_trends.values():
            by_phase[t.phase] += 1
            by_priority[t.priority] += 1

        db_stats = self.history.stats()
        return {
            "active_trends": active,
            "by_phase": dict(by_phase),
            "by_priority": dict(by_priority),
            **db_stats,
        }
