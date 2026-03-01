"""
Thread Monetization Engine
--------------------------
Detect viral potential → insert optimized CTAs → track conversions → revenue attribution.
Turns high-engagement Twitter threads into monetization machines.
"""

import json
import sqlite3
import hashlib
import re
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional
from pathlib import Path


class CTAType(Enum):
    LINK = "link"
    PRODUCT = "product"
    NEWSLETTER = "newsletter"
    LEAD_MAGNET = "lead_magnet"
    AFFILIATE = "affiliate"
    COURSE = "course"
    SERVICE = "service"
    DONATION = "donation"


class PlacementPosition(Enum):
    THREAD_END = "thread_end"
    MID_THREAD = "mid_thread"
    REPLY_FIRST = "reply_first"
    PINNED = "pinned"
    QUOTE_RETWEET = "quote_retweet"


class ViralStage(Enum):
    DORMANT = "dormant"
    WARMING = "warming"
    TRENDING = "trending"
    VIRAL = "viral"
    PEAK = "peak"
    DECLINING = "declining"


@dataclass
class CTAConfig:
    """CTA configuration for monetization."""
    cta_id: str
    cta_type: CTAType
    text: str
    url: str
    emoji: str = "👇"
    placement: PlacementPosition = PlacementPosition.THREAD_END
    min_engagement_rate: float = 2.0  # % threshold to trigger
    min_impressions: int = 1000
    active: bool = True
    ab_variant: str = "A"
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def render(self) -> str:
        """Render the CTA as tweet text."""
        return f"\n\n{self.emoji} {self.text}\n{self.url}"

    def to_dict(self) -> dict:
        d = asdict(self)
        d["cta_type"] = self.cta_type.value
        d["placement"] = self.placement.value
        return d


@dataclass
class ConversionEvent:
    """Tracks a single conversion from CTA click."""
    event_id: str
    thread_id: str
    cta_id: str
    click_count: int = 0
    conversion_count: int = 0
    revenue: float = 0.0
    currency: str = "USD"
    source: str = "twitter"
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class ThreadMetrics:
    """Engagement metrics for a thread."""
    thread_id: str
    tweet_count: int = 0
    total_likes: int = 0
    total_retweets: int = 0
    total_replies: int = 0
    total_impressions: int = 0
    total_bookmarks: int = 0
    engagement_rate: float = 0.0
    viral_stage: ViralStage = ViralStage.DORMANT
    velocity: float = 0.0  # engagements per hour
    peak_hour: Optional[int] = None
    monetized: bool = False
    cta_inserted_at: Optional[str] = None
    last_checked: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class ViralDetector:
    """Detects viral potential of threads based on engagement velocity."""

    THRESHOLDS = {
        ViralStage.DORMANT: {"velocity": 0, "eng_rate": 0},
        ViralStage.WARMING: {"velocity": 5, "eng_rate": 1.5},
        ViralStage.TRENDING: {"velocity": 20, "eng_rate": 3.0},
        ViralStage.VIRAL: {"velocity": 50, "eng_rate": 5.0},
        ViralStage.PEAK: {"velocity": 100, "eng_rate": 8.0},
        ViralStage.DECLINING: {"velocity": -10, "eng_rate": 0},
    }

    def __init__(self):
        self._history: dict[str, list[dict]] = {}

    def record_snapshot(self, thread_id: str, metrics: dict) -> None:
        """Record engagement snapshot for velocity tracking."""
        if thread_id not in self._history:
            self._history[thread_id] = []
        self._history[thread_id].append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "likes": metrics.get("likes", 0),
            "retweets": metrics.get("retweets", 0),
            "replies": metrics.get("replies", 0),
            "impressions": metrics.get("impressions", 0),
        })
        # Keep last 48 snapshots (48 hours of hourly checks)
        if len(self._history[thread_id]) > 48:
            self._history[thread_id] = self._history[thread_id][-48:]

    def calculate_velocity(self, thread_id: str) -> float:
        """Calculate engagement velocity (engagements/hour)."""
        history = self._history.get(thread_id, [])
        if len(history) < 2:
            return 0.0

        latest = history[-1]
        oldest = history[0]

        try:
            t_latest = datetime.fromisoformat(latest["timestamp"])
            t_oldest = datetime.fromisoformat(oldest["timestamp"])
        except (ValueError, KeyError):
            return 0.0

        hours = max((t_latest - t_oldest).total_seconds() / 3600, 0.01)

        eng_latest = latest["likes"] + latest["retweets"] + latest["replies"]
        eng_oldest = oldest["likes"] + oldest["retweets"] + oldest["replies"]

        return (eng_latest - eng_oldest) / hours

    def classify_stage(self, thread_id: str, metrics: dict) -> ViralStage:
        """Classify the viral stage of a thread."""
        velocity = self.calculate_velocity(thread_id)
        eng_rate = metrics.get("engagement_rate", 0)

        # Check declining first (negative velocity after peak)
        if velocity < self.THRESHOLDS[ViralStage.DECLINING]["velocity"]:
            history = self._history.get(thread_id, [])
            if len(history) > 3:
                return ViralStage.DECLINING

        # Classify by thresholds (highest first)
        for stage in [ViralStage.PEAK, ViralStage.VIRAL, ViralStage.TRENDING, ViralStage.WARMING]:
            thresh = self.THRESHOLDS[stage]
            if velocity >= thresh["velocity"] and eng_rate >= thresh["eng_rate"]:
                return stage

        return ViralStage.DORMANT

    def get_viral_score(self, thread_id: str, metrics: dict) -> float:
        """Calculate viral score 0-100."""
        velocity = self.calculate_velocity(thread_id)
        eng_rate = metrics.get("engagement_rate", 0)
        impressions = metrics.get("impressions", 0)

        # Weighted scoring
        velocity_score = min(velocity / 100, 1.0) * 40
        eng_score = min(eng_rate / 10, 1.0) * 30
        reach_score = min(impressions / 100000, 1.0) * 30

        return round(velocity_score + eng_score + reach_score, 1)


class CTAOptimizer:
    """Optimizes CTA placement and text based on performance data."""

    def __init__(self):
        self._performance: dict[str, list[dict]] = {}

    def record_performance(self, cta_id: str, impressions: int, clicks: int,
                           conversions: int, revenue: float = 0.0) -> None:
        """Record CTA performance data point."""
        if cta_id not in self._performance:
            self._performance[cta_id] = []
        self._performance[cta_id].append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "impressions": impressions,
            "clicks": clicks,
            "conversions": conversions,
            "revenue": revenue,
            "ctr": (clicks / impressions * 100) if impressions > 0 else 0,
            "conv_rate": (conversions / clicks * 100) if clicks > 0 else 0,
        })

    def get_best_placement(self) -> PlacementPosition:
        """Determine the best placement based on aggregate CTR."""
        placement_stats: dict[str, dict] = {}

        for cta_id, records in self._performance.items():
            for record in records:
                # Group by placement from cta_id prefix
                placement = cta_id.split("_")[0] if "_" in cta_id else "thread_end"
                if placement not in placement_stats:
                    placement_stats[placement] = {"clicks": 0, "impressions": 0}
                placement_stats[placement]["clicks"] += record["clicks"]
                placement_stats[placement]["impressions"] += record["impressions"]

        best = PlacementPosition.THREAD_END
        best_ctr = 0.0
        for placement_key, stats in placement_stats.items():
            ctr = (stats["clicks"] / stats["impressions"] * 100) if stats["impressions"] > 0 else 0
            if ctr > best_ctr:
                best_ctr = ctr
                try:
                    best = PlacementPosition(placement_key)
                except ValueError:
                    pass
        return best

    def get_ab_winner(self, cta_id_a: str, cta_id_b: str) -> Optional[str]:
        """Determine A/B test winner based on conversion rate."""
        perf_a = self._performance.get(cta_id_a, [])
        perf_b = self._performance.get(cta_id_b, [])

        if not perf_a or not perf_b:
            return None

        total_a = sum(r["conversions"] for r in perf_a)
        clicks_a = sum(r["clicks"] for r in perf_a)
        total_b = sum(r["conversions"] for r in perf_b)
        clicks_b = sum(r["clicks"] for r in perf_b)

        rate_a = (total_a / clicks_a * 100) if clicks_a > 0 else 0
        rate_b = (total_b / clicks_b * 100) if clicks_b > 0 else 0

        # Need 30+ clicks minimum for statistical significance
        if clicks_a < 30 or clicks_b < 30:
            return None

        if abs(rate_a - rate_b) < 0.5:
            return None  # Too close to call

        return cta_id_a if rate_a > rate_b else cta_id_b

    def suggest_cta_text(self, thread_topic: str, cta_type: CTAType) -> list[str]:
        """Suggest CTA texts based on thread topic and type."""
        templates = {
            CTAType.NEWSLETTER: [
                f"📬 想要更多 {thread_topic} 的深度内容？订阅我的Newsletter",
                f"🔔 每周一篇 {thread_topic} 精选，不容错过",
                f"💡 这只是冰山一角。{thread_topic} 完整分析在Newsletter里",
            ],
            CTAType.PRODUCT: [
                f"🛒 用这个工具让 {thread_topic} 效率翻倍",
                f"⚡ 上面的方法论已经产品化了，点击了解",
                f"🎯 一键解决 {thread_topic} 的所有痛点",
            ],
            CTAType.AFFILIATE: [
                f"📦 我用的 {thread_topic} 工具（含优惠链接）",
                f"💰 省时又省钱的 {thread_topic} 推荐",
                f"🔗 本帖提到的所有工具链接",
            ],
            CTAType.LEAD_MAGNET: [
                f"📥 免费下载：{thread_topic} 完整Checklist",
                f"🎁 {thread_topic} 模板免费领（回复 'YES' 发你）",
                f"📋 {thread_topic} 速查手册，保存备用",
            ],
            CTAType.COURSE: [
                f"🎓 系统学习 {thread_topic}？新课程已上线",
                f"📚 从零到精通 {thread_topic}，视频课限时优惠",
                f"🏆 {thread_topic} 实战训练营，名额有限",
            ],
            CTAType.SERVICE: [
                f"🤝 需要 {thread_topic} 专业服务？DM咨询",
                f"💼 {thread_topic} 代运营/咨询，已帮100+客户",
                f"📞 一对一 {thread_topic} 诊断，预约中",
            ],
            CTAType.DONATION: [
                f"☕ 觉得有帮助？请我喝杯咖啡",
                f"🙏 创作不易，感谢支持",
                f"❤️ 如果这个线程帮到你了，考虑打赏支持",
            ],
            CTAType.LINK: [
                f"🔗 完整版在这里，更多 {thread_topic} 详情",
                f"📖 想了解更多？阅读原文",
                f"👉 详细教程和代码都在这里",
            ],
        }
        return templates.get(cta_type, [f"👇 了解更多关于 {thread_topic}"])


class MonetizationStore:
    """SQLite persistence for monetization data."""

    def __init__(self, db_path: str = "monetization.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cta_configs (
                    cta_id TEXT PRIMARY KEY,
                    cta_type TEXT NOT NULL,
                    text TEXT NOT NULL,
                    url TEXT NOT NULL,
                    emoji TEXT DEFAULT '👇',
                    placement TEXT DEFAULT 'thread_end',
                    min_engagement_rate REAL DEFAULT 2.0,
                    min_impressions INTEGER DEFAULT 1000,
                    active INTEGER DEFAULT 1,
                    ab_variant TEXT DEFAULT 'A',
                    created_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS thread_metrics (
                    thread_id TEXT PRIMARY KEY,
                    tweet_count INTEGER DEFAULT 0,
                    total_likes INTEGER DEFAULT 0,
                    total_retweets INTEGER DEFAULT 0,
                    total_replies INTEGER DEFAULT 0,
                    total_impressions INTEGER DEFAULT 0,
                    total_bookmarks INTEGER DEFAULT 0,
                    engagement_rate REAL DEFAULT 0.0,
                    viral_stage TEXT DEFAULT 'dormant',
                    velocity REAL DEFAULT 0.0,
                    peak_hour INTEGER,
                    monetized INTEGER DEFAULT 0,
                    cta_inserted_at TEXT,
                    last_checked TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS conversions (
                    event_id TEXT PRIMARY KEY,
                    thread_id TEXT NOT NULL,
                    cta_id TEXT NOT NULL,
                    click_count INTEGER DEFAULT 0,
                    conversion_count INTEGER DEFAULT 0,
                    revenue REAL DEFAULT 0.0,
                    currency TEXT DEFAULT 'USD',
                    source TEXT DEFAULT 'twitter',
                    timestamp TEXT NOT NULL,
                    FOREIGN KEY (thread_id) REFERENCES thread_metrics(thread_id),
                    FOREIGN KEY (cta_id) REFERENCES cta_configs(cta_id)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS revenue_daily (
                    date TEXT NOT NULL,
                    cta_id TEXT NOT NULL,
                    thread_id TEXT NOT NULL,
                    clicks INTEGER DEFAULT 0,
                    conversions INTEGER DEFAULT 0,
                    revenue REAL DEFAULT 0.0,
                    PRIMARY KEY (date, cta_id, thread_id)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_conv_thread ON conversions(thread_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_conv_cta ON conversions(cta_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_revenue_date ON revenue_daily(date)")
            conn.commit()

    def save_cta(self, cta: CTAConfig) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO cta_configs
                (cta_id, cta_type, text, url, emoji, placement, min_engagement_rate,
                 min_impressions, active, ab_variant, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (cta.cta_id, cta.cta_type.value, cta.text, cta.url, cta.emoji,
                  cta.placement.value, cta.min_engagement_rate, cta.min_impressions,
                  1 if cta.active else 0, cta.ab_variant, cta.created_at))
            conn.commit()

    def get_cta(self, cta_id: str) -> Optional[CTAConfig]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM cta_configs WHERE cta_id = ?", (cta_id,)).fetchone()
            if not row:
                return None
            return CTAConfig(
                cta_id=row["cta_id"],
                cta_type=CTAType(row["cta_type"]),
                text=row["text"],
                url=row["url"],
                emoji=row["emoji"],
                placement=PlacementPosition(row["placement"]),
                min_engagement_rate=row["min_engagement_rate"],
                min_impressions=row["min_impressions"],
                active=bool(row["active"]),
                ab_variant=row["ab_variant"],
                created_at=row["created_at"],
            )

    def list_active_ctas(self) -> list[CTAConfig]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM cta_configs WHERE active = 1").fetchall()
            return [CTAConfig(
                cta_id=r["cta_id"], cta_type=CTAType(r["cta_type"]),
                text=r["text"], url=r["url"], emoji=r["emoji"],
                placement=PlacementPosition(r["placement"]),
                min_engagement_rate=r["min_engagement_rate"],
                min_impressions=r["min_impressions"],
                active=True, ab_variant=r["ab_variant"], created_at=r["created_at"],
            ) for r in rows]

    def save_thread_metrics(self, m: ThreadMetrics) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO thread_metrics
                (thread_id, tweet_count, total_likes, total_retweets, total_replies,
                 total_impressions, total_bookmarks, engagement_rate, viral_stage,
                 velocity, peak_hour, monetized, cta_inserted_at, last_checked)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (m.thread_id, m.tweet_count, m.total_likes, m.total_retweets,
                  m.total_replies, m.total_impressions, m.total_bookmarks,
                  m.engagement_rate, m.viral_stage.value, m.velocity,
                  m.peak_hour, 1 if m.monetized else 0, m.cta_inserted_at,
                  m.last_checked))
            conn.commit()

    def get_thread_metrics(self, thread_id: str) -> Optional[ThreadMetrics]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM thread_metrics WHERE thread_id = ?",
                               (thread_id,)).fetchone()
            if not row:
                return None
            return ThreadMetrics(
                thread_id=row["thread_id"], tweet_count=row["tweet_count"],
                total_likes=row["total_likes"], total_retweets=row["total_retweets"],
                total_replies=row["total_replies"], total_impressions=row["total_impressions"],
                total_bookmarks=row["total_bookmarks"], engagement_rate=row["engagement_rate"],
                viral_stage=ViralStage(row["viral_stage"]), velocity=row["velocity"],
                peak_hour=row["peak_hour"], monetized=bool(row["monetized"]),
                cta_inserted_at=row["cta_inserted_at"], last_checked=row["last_checked"],
            )

    def get_viral_threads(self, min_stage: ViralStage = ViralStage.TRENDING) -> list[ThreadMetrics]:
        """Get threads at or above a viral stage."""
        stages = [ViralStage.TRENDING, ViralStage.VIRAL, ViralStage.PEAK]
        idx = stages.index(min_stage) if min_stage in stages else 0
        target_stages = [s.value for s in stages[idx:]]

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            placeholders = ",".join("?" * len(target_stages))
            rows = conn.execute(
                f"SELECT * FROM thread_metrics WHERE viral_stage IN ({placeholders}) ORDER BY velocity DESC",
                target_stages
            ).fetchall()
            return [ThreadMetrics(
                thread_id=r["thread_id"], tweet_count=r["tweet_count"],
                total_likes=r["total_likes"], total_retweets=r["total_retweets"],
                total_replies=r["total_replies"], total_impressions=r["total_impressions"],
                total_bookmarks=r["total_bookmarks"], engagement_rate=r["engagement_rate"],
                viral_stage=ViralStage(r["viral_stage"]), velocity=r["velocity"],
                peak_hour=r["peak_hour"], monetized=bool(r["monetized"]),
                cta_inserted_at=r["cta_inserted_at"], last_checked=r["last_checked"],
            ) for r in rows]

    def record_conversion(self, event: ConversionEvent) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO conversions
                (event_id, thread_id, cta_id, click_count, conversion_count,
                 revenue, currency, source, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (event.event_id, event.thread_id, event.cta_id,
                  event.click_count, event.conversion_count,
                  event.revenue, event.currency, event.source, event.timestamp))

            # Update daily revenue
            date = event.timestamp[:10]
            conn.execute("""
                INSERT INTO revenue_daily (date, cta_id, thread_id, clicks, conversions, revenue)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, cta_id, thread_id) DO UPDATE SET
                    clicks = clicks + excluded.clicks,
                    conversions = conversions + excluded.conversions,
                    revenue = revenue + excluded.revenue
            """, (date, event.cta_id, event.thread_id,
                  event.click_count, event.conversion_count, event.revenue))
            conn.commit()

    def get_revenue_summary(self, days: int = 30) -> dict:
        """Get revenue summary for last N days."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("""
                SELECT
                    COALESCE(SUM(clicks), 0) as total_clicks,
                    COALESCE(SUM(conversions), 0) as total_conversions,
                    COALESCE(SUM(revenue), 0) as total_revenue,
                    COUNT(DISTINCT thread_id) as monetized_threads,
                    COUNT(DISTINCT cta_id) as active_ctas
                FROM revenue_daily
                WHERE date >= ?
            """, (cutoff,)).fetchone()

            daily = conn.execute("""
                SELECT date, SUM(clicks) as clicks, SUM(conversions) as conversions,
                       SUM(revenue) as revenue
                FROM revenue_daily
                WHERE date >= ?
                GROUP BY date
                ORDER BY date DESC
            """, (cutoff,)).fetchall()

            return {
                "period_days": days,
                "total_clicks": row["total_clicks"],
                "total_conversions": row["total_conversions"],
                "total_revenue": round(row["total_revenue"], 2),
                "monetized_threads": row["monetized_threads"],
                "active_ctas": row["active_ctas"],
                "avg_daily_revenue": round(row["total_revenue"] / max(days, 1), 2),
                "conversion_rate": round(
                    (row["total_conversions"] / row["total_clicks"] * 100)
                    if row["total_clicks"] > 0 else 0, 2
                ),
                "daily_breakdown": [dict(r) for r in daily],
            }

    def get_top_threads_by_revenue(self, limit: int = 10) -> list[dict]:
        """Get top revenue-generating threads."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT thread_id, SUM(clicks) as total_clicks,
                       SUM(conversions) as total_conversions,
                       SUM(revenue) as total_revenue
                FROM revenue_daily
                GROUP BY thread_id
                ORDER BY total_revenue DESC
                LIMIT ?
            """, (limit,)).fetchall()
            return [dict(r) for r in rows]


class ThreadMonetizer:
    """Main orchestrator: detect viral threads → optimize CTAs → track revenue."""

    def __init__(self, db_path: str = "monetization.db"):
        self.store = MonetizationStore(db_path)
        self.detector = ViralDetector()
        self.optimizer = CTAOptimizer()

    def create_cta(self, cta_type: CTAType, text: str, url: str,
                   placement: PlacementPosition = PlacementPosition.THREAD_END,
                   emoji: str = "👇", min_engagement_rate: float = 2.0,
                   min_impressions: int = 1000) -> CTAConfig:
        """Create and save a new CTA configuration."""
        cta_id = hashlib.md5(f"{text}:{url}:{cta_type.value}".encode()).hexdigest()[:12]
        cta = CTAConfig(
            cta_id=cta_id,
            cta_type=cta_type,
            text=text,
            url=url,
            emoji=emoji,
            placement=placement,
            min_engagement_rate=min_engagement_rate,
            min_impressions=min_impressions,
        )
        self.store.save_cta(cta)
        return cta

    def update_thread_metrics(self, thread_id: str, metrics: dict) -> ThreadMetrics:
        """Update thread engagement metrics and classify viral stage."""
        self.detector.record_snapshot(thread_id, metrics)
        velocity = self.detector.calculate_velocity(thread_id)
        stage = self.detector.classify_stage(thread_id, metrics)

        total_eng = (metrics.get("likes", 0) + metrics.get("retweets", 0) +
                     metrics.get("replies", 0))
        impressions = metrics.get("impressions", 1)
        eng_rate = (total_eng / impressions * 100) if impressions > 0 else 0

        existing = self.store.get_thread_metrics(thread_id)

        tm = ThreadMetrics(
            thread_id=thread_id,
            tweet_count=metrics.get("tweet_count", existing.tweet_count if existing else 0),
            total_likes=metrics.get("likes", 0),
            total_retweets=metrics.get("retweets", 0),
            total_replies=metrics.get("replies", 0),
            total_impressions=metrics.get("impressions", 0),
            total_bookmarks=metrics.get("bookmarks", 0),
            engagement_rate=round(eng_rate, 2),
            viral_stage=stage,
            velocity=round(velocity, 2),
            monetized=existing.monetized if existing else False,
            cta_inserted_at=existing.cta_inserted_at if existing else None,
        )
        self.store.save_thread_metrics(tm)
        return tm

    def should_monetize(self, thread_id: str) -> tuple[bool, str]:
        """Determine if a thread is ready for monetization."""
        tm = self.store.get_thread_metrics(thread_id)
        if not tm:
            return False, "No metrics found for thread"

        if tm.monetized:
            return False, "Thread already monetized"

        ctas = self.store.list_active_ctas()
        if not ctas:
            return False, "No active CTAs configured"

        # Check against CTA thresholds
        for cta in ctas:
            if (tm.engagement_rate >= cta.min_engagement_rate and
                    tm.total_impressions >= cta.min_impressions):
                return True, f"Thread qualifies: {tm.engagement_rate:.1f}% eng rate, {tm.total_impressions} impressions"

        return False, (f"Below threshold: {tm.engagement_rate:.1f}% eng rate, "
                       f"{tm.total_impressions} impressions")

    def get_best_cta_for_thread(self, thread_id: str) -> Optional[CTAConfig]:
        """Select the best CTA for a given thread based on its metrics."""
        tm = self.store.get_thread_metrics(thread_id)
        if not tm:
            return None

        ctas = self.store.list_active_ctas()
        if not ctas:
            return None

        # Filter qualifying CTAs
        qualifying = [
            c for c in ctas
            if tm.engagement_rate >= c.min_engagement_rate and
               tm.total_impressions >= c.min_impressions
        ]
        if not qualifying:
            return ctas[0] if ctas else None

        # Prefer higher-value CTA types
        type_priority = {
            CTAType.PRODUCT: 8, CTAType.COURSE: 7, CTAType.SERVICE: 7,
            CTAType.AFFILIATE: 6, CTAType.LEAD_MAGNET: 5,
            CTAType.NEWSLETTER: 4, CTAType.LINK: 3, CTAType.DONATION: 2,
        }
        qualifying.sort(
            key=lambda c: type_priority.get(c.cta_type, 0),
            reverse=True
        )
        return qualifying[0]

    def mark_monetized(self, thread_id: str) -> None:
        """Mark a thread as monetized (CTA inserted)."""
        tm = self.store.get_thread_metrics(thread_id)
        if tm:
            tm.monetized = True
            tm.cta_inserted_at = datetime.now(timezone.utc).isoformat()
            self.store.save_thread_metrics(tm)

    def record_conversion(self, thread_id: str, cta_id: str,
                          clicks: int = 0, conversions: int = 0,
                          revenue: float = 0.0) -> ConversionEvent:
        """Record a conversion event."""
        event_id = hashlib.md5(
            f"{thread_id}:{cta_id}:{datetime.now(timezone.utc).isoformat()}".encode()
        ).hexdigest()[:16]

        event = ConversionEvent(
            event_id=event_id,
            thread_id=thread_id,
            cta_id=cta_id,
            click_count=clicks,
            conversion_count=conversions,
            revenue=revenue,
        )
        self.store.record_conversion(event)
        self.optimizer.record_performance(cta_id, clicks * 10, clicks, conversions, revenue)
        return event

    def get_viral_score(self, thread_id: str) -> float:
        """Get viral score for a thread."""
        tm = self.store.get_thread_metrics(thread_id)
        if not tm:
            return 0.0
        metrics = {
            "engagement_rate": tm.engagement_rate,
            "impressions": tm.total_impressions,
        }
        return self.detector.get_viral_score(thread_id, metrics)

    def generate_report(self, days: int = 30) -> str:
        """Generate monetization report."""
        summary = self.store.get_revenue_summary(days)
        top_threads = self.store.get_top_threads_by_revenue(5)
        viral = self.store.get_viral_threads()

        lines = [
            f"💰 Thread Monetization Report ({days} days)",
            f"{'='*45}",
            f"",
            f"📊 Revenue Summary:",
            f"  Total Revenue: ${summary['total_revenue']:,.2f}",
            f"  Daily Average: ${summary['avg_daily_revenue']:,.2f}",
            f"  Total Clicks: {summary['total_clicks']:,}",
            f"  Conversions: {summary['total_conversions']:,}",
            f"  Conv. Rate: {summary['conversion_rate']}%",
            f"  Monetized Threads: {summary['monetized_threads']}",
            f"  Active CTAs: {summary['active_ctas']}",
        ]

        if top_threads:
            lines += [
                f"",
                f"🏆 Top Revenue Threads:",
            ]
            for i, t in enumerate(top_threads, 1):
                lines.append(
                    f"  {i}. {t['thread_id'][:12]}... "
                    f"${t['total_revenue']:,.2f} "
                    f"({t['total_conversions']} conv, "
                    f"{t['total_clicks']} clicks)"
                )

        if viral:
            lines += [
                f"",
                f"🔥 Viral Threads ({len(viral)}):",
            ]
            for v in viral[:5]:
                emoji = {"trending": "📈", "viral": "🚀", "peak": "⚡"}.get(
                    v.viral_stage.value, "📊"
                )
                lines.append(
                    f"  {emoji} {v.thread_id[:12]}... "
                    f"[{v.viral_stage.value.upper()}] "
                    f"{v.engagement_rate}% eng, "
                    f"{v.total_impressions:,} imp, "
                    f"{'✅ CTA' if v.monetized else '❌ No CTA'}"
                )

        return "\n".join(lines)

    def generate_json_report(self, days: int = 30) -> dict:
        """Generate machine-readable report."""
        return {
            "revenue": self.store.get_revenue_summary(days),
            "top_threads": self.store.get_top_threads_by_revenue(10),
            "viral_threads": [
                {
                    "thread_id": v.thread_id,
                    "viral_stage": v.viral_stage.value,
                    "engagement_rate": v.engagement_rate,
                    "impressions": v.total_impressions,
                    "velocity": v.velocity,
                    "monetized": v.monetized,
                }
                for v in self.store.get_viral_threads()
            ],
            "active_ctas": [c.to_dict() for c in self.store.list_active_ctas()],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
