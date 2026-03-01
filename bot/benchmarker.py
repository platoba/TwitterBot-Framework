"""
Performance Benchmarker v1.0
账号表现基准 + 时期对比 + 目标追踪 + 健康评分
"""

import json
import logging
import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from bot.database import Database

logger = logging.getLogger(__name__)


@dataclass
class PeriodStats:
    """时期统计数据"""
    period_name: str
    start_date: str
    end_date: str
    tweet_count: int = 0
    total_impressions: int = 0
    total_engagements: int = 0
    total_likes: int = 0
    total_retweets: int = 0
    total_replies: int = 0
    follower_delta: int = 0
    avg_engagement_rate: float = 0.0
    best_tweet_id: Optional[str] = None
    best_tweet_content: Optional[str] = None
    posting_frequency: float = 0.0  # tweets per day

    @property
    def avg_impressions_per_tweet(self) -> float:
        if self.tweet_count == 0:
            return 0
        return round(self.total_impressions / self.tweet_count, 1)

    @property
    def avg_engagements_per_tweet(self) -> float:
        if self.tweet_count == 0:
            return 0
        return round(self.total_engagements / self.tweet_count, 1)

    @property
    def like_to_retweet_ratio(self) -> float:
        if self.total_retweets == 0:
            return 0
        return round(self.total_likes / self.total_retweets, 2)

    def to_dict(self) -> Dict:
        return {
            "period_name": self.period_name,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "tweet_count": self.tweet_count,
            "total_impressions": self.total_impressions,
            "total_engagements": self.total_engagements,
            "total_likes": self.total_likes,
            "total_retweets": self.total_retweets,
            "total_replies": self.total_replies,
            "follower_delta": self.follower_delta,
            "avg_engagement_rate": self.avg_engagement_rate,
            "avg_impressions_per_tweet": self.avg_impressions_per_tweet,
            "avg_engagements_per_tweet": self.avg_engagements_per_tweet,
            "like_to_retweet_ratio": self.like_to_retweet_ratio,
            "posting_frequency": self.posting_frequency,
            "best_tweet_id": self.best_tweet_id,
        }


@dataclass
class PeriodComparison:
    """两个时期的对比"""
    current: PeriodStats
    previous: PeriodStats

    @property
    def impressions_change_pct(self) -> float:
        if self.previous.total_impressions == 0:
            return 0
        return round(
            (self.current.total_impressions - self.previous.total_impressions)
            / self.previous.total_impressions * 100, 1
        )

    @property
    def engagement_change_pct(self) -> float:
        if self.previous.total_engagements == 0:
            return 0
        return round(
            (self.current.total_engagements - self.previous.total_engagements)
            / self.previous.total_engagements * 100, 1
        )

    @property
    def engagement_rate_change(self) -> float:
        return round(
            self.current.avg_engagement_rate - self.previous.avg_engagement_rate, 2
        )

    @property
    def frequency_change(self) -> float:
        return round(self.current.posting_frequency - self.previous.posting_frequency, 2)

    @property
    def overall_trend(self) -> str:
        """综合趋势判断"""
        score = 0
        if self.impressions_change_pct > 10:
            score += 1
        elif self.impressions_change_pct < -10:
            score -= 1
        if self.engagement_change_pct > 10:
            score += 1
        elif self.engagement_change_pct < -10:
            score -= 1
        if self.engagement_rate_change > 0.5:
            score += 1
        elif self.engagement_rate_change < -0.5:
            score -= 1

        if score >= 2:
            return "strong_growth"
        elif score == 1:
            return "growth"
        elif score == 0:
            return "stable"
        elif score == -1:
            return "decline"
        return "strong_decline"

    def to_dict(self) -> Dict:
        return {
            "current": self.current.to_dict(),
            "previous": self.previous.to_dict(),
            "changes": {
                "impressions_pct": self.impressions_change_pct,
                "engagement_pct": self.engagement_change_pct,
                "engagement_rate": self.engagement_rate_change,
                "frequency": self.frequency_change,
                "overall_trend": self.overall_trend,
            },
        }


@dataclass
class HealthScore:
    """账号健康评分"""
    overall: float = 0.0           # 0-100
    consistency: float = 0.0       # 发帖一致性
    engagement_quality: float = 0.0 # 互动质量
    growth_momentum: float = 0.0   # 增长动力
    content_diversity: float = 0.0 # 内容多样性
    audience_retention: float = 0.0 # 受众留存
    recommendations: List[str] = field(default_factory=list)

    @property
    def grade(self) -> str:
        if self.overall >= 90:
            return "S"
        elif self.overall >= 75:
            return "A"
        elif self.overall >= 60:
            return "B"
        elif self.overall >= 40:
            return "C"
        return "D"

    def to_dict(self) -> Dict:
        return {
            "overall": self.overall,
            "grade": self.grade,
            "consistency": self.consistency,
            "engagement_quality": self.engagement_quality,
            "growth_momentum": self.growth_momentum,
            "content_diversity": self.content_diversity,
            "audience_retention": self.audience_retention,
            "recommendations": self.recommendations,
        }


@dataclass
class GrowthTarget:
    """增长目标"""
    metric: str  # impressions, engagements, followers, tweets
    target_value: int
    current_value: int = 0
    deadline: Optional[str] = None
    created_at: str = ""

    @property
    def progress_pct(self) -> float:
        if self.target_value <= 0:
            return 0
        return min(round(self.current_value / self.target_value * 100, 1), 100)

    @property
    def is_achieved(self) -> bool:
        return self.current_value >= self.target_value

    @property
    def remaining(self) -> int:
        return max(self.target_value - self.current_value, 0)

    @property
    def days_remaining(self) -> Optional[int]:
        if not self.deadline:
            return None
        try:
            dl = datetime.fromisoformat(self.deadline)
            now = datetime.now(timezone.utc)
            return max((dl - now).days, 0)
        except ValueError:
            return None

    @property
    def daily_pace_needed(self) -> Optional[float]:
        days = self.days_remaining
        if days is None or days <= 0:
            return None
        return round(self.remaining / days, 1)

    def to_dict(self) -> Dict:
        return {
            "metric": self.metric,
            "target_value": self.target_value,
            "current_value": self.current_value,
            "progress_pct": self.progress_pct,
            "is_achieved": self.is_achieved,
            "remaining": self.remaining,
            "deadline": self.deadline,
            "days_remaining": self.days_remaining,
            "daily_pace_needed": self.daily_pace_needed,
        }


class Benchmarker:
    """表现基准分析器"""

    def __init__(self, db: Database):
        self.db = db
        self._ensure_table()

    def _ensure_table(self):
        conn = self.db._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS performance_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tweet_id TEXT,
                content TEXT,
                impressions INTEGER DEFAULT 0,
                likes INTEGER DEFAULT 0,
                retweets INTEGER DEFAULT 0,
                replies INTEGER DEFAULT 0,
                engagement_rate REAL DEFAULT 0,
                posted_at TEXT DEFAULT (datetime('now')),
                tags TEXT DEFAULT '[]'
            );
            CREATE INDEX IF NOT EXISTS idx_perf_date ON performance_log(posted_at);

            CREATE TABLE IF NOT EXISTS follower_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                follower_count INTEGER DEFAULT 0,
                following_count INTEGER DEFAULT 0,
                recorded_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS growth_targets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric TEXT NOT NULL,
                target_value INTEGER NOT NULL,
                current_value INTEGER DEFAULT 0,
                deadline TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                achieved_at TEXT
            );
        """)
        conn.commit()

    # ── 数据记录 ──

    def log_tweet(self, tweet_id: str = None, content: str = "",
                  impressions: int = 0, likes: int = 0,
                  retweets: int = 0, replies: int = 0,
                  posted_at: str = None, tags: List[str] = None):
        """记录推文表现"""
        eng = likes + retweets + replies
        eng_rate = eng / max(impressions, 1) * 100
        conn = self.db._get_conn()
        conn.execute("""
            INSERT INTO performance_log
            (tweet_id, content, impressions, likes, retweets, replies,
             engagement_rate, posted_at, tags)
            VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?, datetime('now')), ?)
        """, (tweet_id, content, impressions, likes, retweets, replies,
              round(eng_rate, 2), posted_at, json.dumps(tags or [])))
        conn.commit()

    def log_followers(self, follower_count: int, following_count: int = 0):
        """记录粉丝快照"""
        conn = self.db._get_conn()
        conn.execute("""
            INSERT INTO follower_snapshots (follower_count, following_count)
            VALUES (?, ?)
        """, (follower_count, following_count))
        conn.commit()

    # ── 时期分析 ──

    def get_period_stats(self, start_date: str, end_date: str,
                         period_name: str = "") -> PeriodStats:
        """获取指定时期的统计"""
        conn = self.db._get_conn()
        rows = conn.execute("""
            SELECT * FROM performance_log
            WHERE posted_at >= ? AND posted_at <= ?
            ORDER BY posted_at
        """, (start_date, end_date)).fetchall()

        if not rows:
            return PeriodStats(
                period_name=period_name or f"{start_date} ~ {end_date}",
                start_date=start_date,
                end_date=end_date,
            )

        total_imp = sum(r["impressions"] for r in rows)
        total_likes = sum(r["likes"] for r in rows)
        total_rt = sum(r["retweets"] for r in rows)
        total_replies = sum(r["replies"] for r in rows)
        total_eng = total_likes + total_rt + total_replies
        avg_er = sum(r["engagement_rate"] for r in rows) / len(rows)

        # 发帖频率
        try:
            dt_start = datetime.fromisoformat(start_date)
            dt_end = datetime.fromisoformat(end_date)
            days = max((dt_end - dt_start).days, 1)
            freq = len(rows) / days
        except ValueError:
            freq = 0

        # 最佳推文
        best = max(rows, key=lambda r: r["likes"] + r["retweets"] + r["replies"])

        # 粉丝变化
        follower_delta = 0
        fsnapshots = conn.execute("""
            SELECT follower_count FROM follower_snapshots
            WHERE recorded_at >= ? AND recorded_at <= ?
            ORDER BY recorded_at
        """, (start_date, end_date)).fetchall()
        if len(fsnapshots) >= 2:
            follower_delta = fsnapshots[-1]["follower_count"] - fsnapshots[0]["follower_count"]

        return PeriodStats(
            period_name=period_name or f"{start_date} ~ {end_date}",
            start_date=start_date,
            end_date=end_date,
            tweet_count=len(rows),
            total_impressions=total_imp,
            total_engagements=total_eng,
            total_likes=total_likes,
            total_retweets=total_rt,
            total_replies=total_replies,
            follower_delta=follower_delta,
            avg_engagement_rate=round(avg_er, 2),
            best_tweet_id=best["tweet_id"],
            best_tweet_content=best["content"],
            posting_frequency=round(freq, 2),
        )

    def compare_periods(self, current_start: str, current_end: str,
                        previous_start: str, previous_end: str) -> PeriodComparison:
        """对比两个时期"""
        current = self.get_period_stats(current_start, current_end, "Current")
        previous = self.get_period_stats(previous_start, previous_end, "Previous")
        return PeriodComparison(current=current, previous=previous)

    def week_over_week(self) -> PeriodComparison:
        """本周 vs 上周"""
        now = datetime.now(timezone.utc)
        this_week_start = (now - timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        last_week_start = this_week_start - timedelta(days=7)

        return self.compare_periods(
            this_week_start.isoformat(), now.isoformat(),
            last_week_start.isoformat(), this_week_start.isoformat(),
        )

    # ── 健康评分 ──

    def health_check(self, days: int = 30) -> HealthScore:
        """计算账号健康评分"""
        now = datetime.now(timezone.utc)
        start = (now - timedelta(days=days)).isoformat()
        end = now.isoformat()

        stats = self.get_period_stats(start, end, f"Last {days} days")

        # 1. 一致性 (0-100): 基于发帖频率
        if stats.posting_frequency >= 1:
            consistency = min(stats.posting_frequency / 2 * 100, 100)
        else:
            consistency = stats.posting_frequency * 50

        # 2. 互动质量 (0-100)
        if stats.avg_engagement_rate >= 5:
            eng_quality = 100
        elif stats.avg_engagement_rate >= 2:
            eng_quality = 60 + (stats.avg_engagement_rate - 2) / 3 * 40
        else:
            eng_quality = stats.avg_engagement_rate / 2 * 60

        # 3. 增长动力 (0-100)
        if stats.follower_delta > 0:
            growth = min(math.log1p(stats.follower_delta) / math.log1p(100) * 100, 100)
        elif stats.follower_delta == 0:
            growth = 30
        else:
            growth = max(30 + stats.follower_delta, 0)

        # 4. 内容多样性 (0-100): 基于推文间的差异
        diversity = self._calc_diversity(start, end)

        # 5. 受众留存 (0-100): 基于赞转比
        if stats.like_to_retweet_ratio > 0:
            if stats.like_to_retweet_ratio >= 3:
                retention = 80
            elif stats.like_to_retweet_ratio >= 1.5:
                retention = 60 + (stats.like_to_retweet_ratio - 1.5) / 1.5 * 20
            else:
                retention = stats.like_to_retweet_ratio / 1.5 * 60
        else:
            retention = 30

        overall = (
            consistency * 0.2
            + eng_quality * 0.3
            + growth * 0.2
            + diversity * 0.15
            + retention * 0.15
        )

        recommendations = []
        if consistency < 50:
            recommendations.append("提高发帖频率，保持每天至少1条推文")
        if eng_quality < 50:
            recommendations.append("优化内容质量，多用提问和CTA提升互动率")
        if growth < 40:
            recommendations.append("加强粉丝增长策略，参与热门话题讨论")
        if diversity < 40:
            recommendations.append("丰富内容类型，加入thread、投票、图片等")
        if retention < 40:
            recommendations.append("关注受众留存，回复评论并建立社区感")

        return HealthScore(
            overall=round(overall, 1),
            consistency=round(consistency, 1),
            engagement_quality=round(eng_quality, 1),
            growth_momentum=round(growth, 1),
            content_diversity=round(diversity, 1),
            audience_retention=round(retention, 1),
            recommendations=recommendations,
        )

    def _calc_diversity(self, start: str, end: str) -> float:
        """计算内容多样性"""
        conn = self.db._get_conn()
        rows = conn.execute("""
            SELECT content, tags FROM performance_log
            WHERE posted_at >= ? AND posted_at <= ?
        """, (start, end)).fetchall()

        if len(rows) < 3:
            return 50  # 数据不足给中间分

        # 基于标签多样性
        all_tags = set()
        tagged_count = 0
        for r in rows:
            try:
                tags = json.loads(r["tags"])
                if tags:
                    all_tags.update(tags)
                    tagged_count += 1
            except (json.JSONDecodeError, TypeError):
                pass

        # 基于内容长度方差
        lengths = [len(r["content"] or "") for r in rows]
        if lengths:
            avg_len = sum(lengths) / len(lengths)
            variance = sum((l - avg_len) ** 2 for l in lengths) / len(lengths)
            std_dev = math.sqrt(variance)
            length_diversity = min(std_dev / 50 * 100, 100)
        else:
            length_diversity = 0

        tag_diversity = min(len(all_tags) / max(len(rows), 1) * 200, 100)
        return round((tag_diversity + length_diversity) / 2, 1)

    # ── 目标管理 ──

    def set_target(self, metric: str, target_value: int,
                   deadline: str = None) -> GrowthTarget:
        """设置增长目标"""
        conn = self.db._get_conn()
        conn.execute("""
            INSERT INTO growth_targets (metric, target_value, deadline)
            VALUES (?, ?, ?)
        """, (metric, target_value, deadline))
        conn.commit()

        return GrowthTarget(
            metric=metric,
            target_value=target_value,
            deadline=deadline,
            created_at=datetime.now(timezone.utc).isoformat(),
        )

    def get_targets(self, include_achieved: bool = False) -> List[GrowthTarget]:
        """获取所有目标"""
        conn = self.db._get_conn()
        query = "SELECT * FROM growth_targets"
        if not include_achieved:
            query += " WHERE achieved_at IS NULL"
        query += " ORDER BY created_at DESC"

        rows = conn.execute(query).fetchall()
        return [
            GrowthTarget(
                metric=r["metric"],
                target_value=r["target_value"],
                current_value=r["current_value"],
                deadline=r["deadline"],
                created_at=r["created_at"],
            )
            for r in rows
        ]

    def update_target(self, metric: str, current_value: int):
        """更新目标进度"""
        conn = self.db._get_conn()
        conn.execute("""
            UPDATE growth_targets
            SET current_value = ?
            WHERE metric = ? AND achieved_at IS NULL
        """, (current_value, metric))

        # 检查是否达成
        conn.execute("""
            UPDATE growth_targets
            SET achieved_at = datetime('now')
            WHERE metric = ? AND current_value >= target_value AND achieved_at IS NULL
        """, (metric,))
        conn.commit()

    # ── 报告 ──

    def format_health_report(self, days: int = 30) -> str:
        """格式化健康报告"""
        health = self.health_check(days)
        grade_emoji = {"S": "🏆", "A": "⭐", "B": "👍", "C": "😐", "D": "⚠️"}

        lines = [
            f"{grade_emoji.get(health.grade, '❓')} *Account Health: {health.overall}/100* [{health.grade}]",
            f"_(Last {days} days)_\n",
            "📊 *Breakdown*",
            f"  Consistency: {health.consistency}/100 {'✅' if health.consistency >= 60 else '⚠️'}",
            f"  Engagement Quality: {health.engagement_quality}/100 {'✅' if health.engagement_quality >= 60 else '⚠️'}",
            f"  Growth Momentum: {health.growth_momentum}/100 {'✅' if health.growth_momentum >= 60 else '⚠️'}",
            f"  Content Diversity: {health.content_diversity}/100 {'✅' if health.content_diversity >= 60 else '⚠️'}",
            f"  Audience Retention: {health.audience_retention}/100 {'✅' if health.audience_retention >= 60 else '⚠️'}",
        ]

        if health.recommendations:
            lines.append("\n💡 *Recommendations*")
            for i, r in enumerate(health.recommendations, 1):
                lines.append(f"  {i}. {r}")

        return "\n".join(lines)

    def format_comparison(self, comparison: PeriodComparison) -> str:
        """格式化时期对比报告"""
        trend_emoji = {
            "strong_growth": "🚀", "growth": "📈",
            "stable": "➡️", "decline": "📉", "strong_decline": "⚠️",
        }

        def _arrow(val: float) -> str:
            if val > 0:
                return f"↑{val:+.1f}%"
            elif val < 0:
                return f"↓{val:.1f}%"
            return "→ 0%"

        trend = comparison.overall_trend
        lines = [
            f"{trend_emoji.get(trend, '❓')} *Period Comparison* — {trend.replace('_', ' ').title()}\n",
            f"📅 Current: {comparison.current.period_name}",
            f"📅 Previous: {comparison.previous.period_name}\n",
            "📊 *Changes*",
            f"  Impressions: {comparison.current.total_impressions:,} ({_arrow(comparison.impressions_change_pct)})",
            f"  Engagements: {comparison.current.total_engagements:,} ({_arrow(comparison.engagement_change_pct)})",
            f"  Eng Rate: {comparison.current.avg_engagement_rate}% ({comparison.engagement_rate_change:+.2f}pp)",
            f"  Frequency: {comparison.current.posting_frequency:.1f}/day ({comparison.frequency_change:+.2f})",
            f"  Tweets: {comparison.current.tweet_count} vs {comparison.previous.tweet_count}",
        ]

        return "\n".join(lines)

    def format_targets(self) -> str:
        """格式化目标追踪报告"""
        targets = self.get_targets()
        if not targets:
            return "🎯 暂无增长目标"

        lines = ["🎯 *Growth Targets*\n"]
        for t in targets:
            bar = "█" * int(t.progress_pct / 5) + "░" * (20 - int(t.progress_pct / 5))
            lines.append(f"  {t.metric}: {bar} {t.progress_pct}%")
            lines.append(f"    {t.current_value:,} / {t.target_value:,}")
            if t.daily_pace_needed is not None:
                lines.append(f"    Need {t.daily_pace_needed}/day ({t.days_remaining} days left)")
            lines.append("")

        return "\n".join(lines)
