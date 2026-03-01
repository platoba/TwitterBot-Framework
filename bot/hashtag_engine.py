"""
Hashtag Research Engine v1.0
标签研究 + 热度追踪 + 最优标签推荐 + 竞争度分析
轻量级方案：基于历史推文数据分析，无外部API依赖
"""

import json
import logging
import re
import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Set

from bot.database import Database

logger = logging.getLogger(__name__)


@dataclass
class HashtagStats:
    """标签统计数据"""
    tag: str
    usage_count: int = 0
    total_impressions: int = 0
    total_engagements: int = 0
    avg_engagement_rate: float = 0.0
    peak_hour: Optional[int] = None
    first_seen: Optional[str] = None
    last_seen: Optional[str] = None
    co_tags: List[str] = field(default_factory=list)
    trend: str = "stable"  # rising, falling, stable, new

    @property
    def avg_impressions(self) -> float:
        if self.usage_count == 0:
            return 0
        return round(self.total_impressions / self.usage_count, 1)

    @property
    def avg_engagements(self) -> float:
        if self.usage_count == 0:
            return 0
        return round(self.total_engagements / self.usage_count, 1)

    @property
    def score(self) -> float:
        """综合评分 = 互动率×40 + log(使用量)×30 + 趋势×30"""
        trend_bonus = {"rising": 1.5, "new": 1.2, "stable": 1.0, "falling": 0.6}
        base = (
            self.avg_engagement_rate * 40
            + math.log1p(self.usage_count) * 30
            + trend_bonus.get(self.trend, 1.0) * 30
        )
        return round(base, 2)

    def to_dict(self) -> Dict:
        return {
            "tag": self.tag,
            "usage_count": self.usage_count,
            "total_impressions": self.total_impressions,
            "total_engagements": self.total_engagements,
            "avg_engagement_rate": self.avg_engagement_rate,
            "avg_impressions": self.avg_impressions,
            "avg_engagements": self.avg_engagements,
            "peak_hour": self.peak_hour,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "co_tags": self.co_tags,
            "trend": self.trend,
            "score": self.score,
        }


@dataclass
class HashtagSet:
    """标签组合推荐"""
    tags: List[str]
    predicted_reach: float = 0
    diversity_score: float = 0
    competition_level: str = "medium"  # low, medium, high

    @property
    def tag_count(self) -> int:
        return len(self.tags)

    def to_dict(self) -> Dict:
        return {
            "tags": self.tags,
            "predicted_reach": self.predicted_reach,
            "diversity_score": self.diversity_score,
            "competition_level": self.competition_level,
            "tag_count": self.tag_count,
        }


class HashtagEngine:
    """标签研究引擎"""

    # 常见标签分类
    CATEGORIES = {
        "tech": ["tech", "ai", "ml", "python", "javascript", "coding", "dev",
                 "software", "startup", "saas", "web3", "crypto", "blockchain"],
        "marketing": ["marketing", "seo", "growth", "branding", "content",
                      "socialmedia", "digital", "ads", "copywriting", "leads"],
        "ecommerce": ["ecommerce", "shopify", "amazon", "dropshipping", "fba",
                      "retail", "dtc", "b2b", "crossborder", "wholesale"],
        "business": ["business", "entrepreneur", "startup", "funding", "vc",
                     "revenue", "profit", "strategy", "leadership", "ceo"],
        "lifestyle": ["life", "motivation", "success", "mindset", "productivity",
                      "health", "fitness", "travel", "food", "fashion"],
    }

    def __init__(self, db: Database):
        self.db = db
        self._ensure_table()

    def _ensure_table(self):
        conn = self.db._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS hashtag_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tag TEXT NOT NULL,
                tweet_id TEXT,
                impressions INTEGER DEFAULT 0,
                engagements INTEGER DEFAULT 0,
                engagement_rate REAL DEFAULT 0,
                hour_of_day INTEGER DEFAULT 0,
                co_tags TEXT DEFAULT '[]',
                recorded_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_hashtag_tag ON hashtag_usage(tag);
            CREATE INDEX IF NOT EXISTS idx_hashtag_date ON hashtag_usage(recorded_at);

            CREATE TABLE IF NOT EXISTS hashtag_blacklist (
                tag TEXT PRIMARY KEY,
                reason TEXT DEFAULT '',
                added_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.commit()

    # ── 数据记录 ──

    def record_usage(self, tweet_content: str, tweet_id: str = None,
                     impressions: int = 0, engagements: int = 0,
                     posted_at: str = None):
        """记录推文中标签的使用数据"""
        tags = self.extract_hashtags(tweet_content)
        if not tags:
            return

        hour = 0
        if posted_at:
            try:
                dt = datetime.fromisoformat(posted_at)
                hour = dt.hour
            except ValueError:
                pass

        eng_rate = engagements / max(impressions, 1) * 100

        conn = self.db._get_conn()
        for tag in tags:
            co = [t for t in tags if t != tag]
            conn.execute("""
                INSERT INTO hashtag_usage (tag, tweet_id, impressions, engagements,
                    engagement_rate, hour_of_day, co_tags, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?, datetime('now')))
            """, (tag, tweet_id, impressions, engagements,
                  round(eng_rate, 2), hour, json.dumps(co), posted_at))
        conn.commit()

    @staticmethod
    def extract_hashtags(text: str) -> List[str]:
        """从文本中提取标签"""
        return [m.lower() for m in re.findall(r'#(\w+)', text)]

    # ── 分析 ──

    def get_stats(self, tag: str) -> Optional[HashtagStats]:
        """获取单个标签的统计数据"""
        conn = self.db._get_conn()
        rows = conn.execute(
            "SELECT * FROM hashtag_usage WHERE tag = ? ORDER BY recorded_at",
            (tag.lower().lstrip("#"),)
        ).fetchall()

        if not rows:
            return None

        usage = len(rows)
        total_imp = sum(r["impressions"] for r in rows)
        total_eng = sum(r["engagements"] for r in rows)
        avg_er = sum(r["engagement_rate"] for r in rows) / usage if usage else 0

        # 高峰时段
        hour_counts = Counter(r["hour_of_day"] for r in rows)
        peak_hour = hour_counts.most_common(1)[0][0] if hour_counts else None

        # 共现标签
        all_co = []
        for r in rows:
            try:
                all_co.extend(json.loads(r["co_tags"]))
            except (json.JSONDecodeError, TypeError):
                pass
        top_co = [t for t, _ in Counter(all_co).most_common(5)]

        # 趋势检测
        trend = self._detect_trend(rows)

        return HashtagStats(
            tag=tag.lower().lstrip("#"),
            usage_count=usage,
            total_impressions=total_imp,
            total_engagements=total_eng,
            avg_engagement_rate=round(avg_er, 2),
            peak_hour=peak_hour,
            first_seen=rows[0]["recorded_at"] if rows else None,
            last_seen=rows[-1]["recorded_at"] if rows else None,
            co_tags=top_co,
            trend=trend,
        )

    def _detect_trend(self, rows: list) -> str:
        """基于最近数据检测趋势"""
        if len(rows) < 3:
            return "new" if len(rows) <= 1 else "stable"

        # 对比前半和后半的互动率
        mid = len(rows) // 2
        first_half = [r["engagement_rate"] for r in rows[:mid]]
        second_half = [r["engagement_rate"] for r in rows[mid:]]

        avg_first = sum(first_half) / len(first_half) if first_half else 0
        avg_second = sum(second_half) / len(second_half) if second_half else 0

        if avg_second > avg_first * 1.2:
            return "rising"
        elif avg_second < avg_first * 0.8:
            return "falling"
        return "stable"

    def top_hashtags(self, limit: int = 20, category: str = None,
                     min_usage: int = 2) -> List[HashtagStats]:
        """获取表现最好的标签"""
        conn = self.db._get_conn()
        rows = conn.execute("""
            SELECT tag, COUNT(*) as cnt,
                   SUM(impressions) as total_imp,
                   SUM(engagements) as total_eng,
                   AVG(engagement_rate) as avg_er
            FROM hashtag_usage
            GROUP BY tag
            HAVING cnt >= ?
            ORDER BY avg_er DESC
            LIMIT ?
        """, (min_usage, limit * 3)).fetchall()

        results = []
        category_tags = set(self.CATEGORIES.get(category, [])) if category else None

        for r in rows:
            tag = r["tag"]
            if category_tags and tag not in category_tags:
                continue

            stats = HashtagStats(
                tag=tag,
                usage_count=r["cnt"],
                total_impressions=r["total_imp"] or 0,
                total_engagements=r["total_eng"] or 0,
                avg_engagement_rate=round(r["avg_er"] or 0, 2),
            )
            results.append(stats)

        results.sort(key=lambda s: s.score, reverse=True)
        return results[:limit]

    def suggest_hashtags(self, content: str, max_tags: int = 5,
                         existing_tags: List[str] = None) -> HashtagSet:
        """基于内容智能推荐标签组合"""
        existing = set(t.lower().lstrip("#") for t in (existing_tags or []))
        words = set(re.findall(r'\b\w{4,}\b', content.lower()))

        # 1. 从内容匹配类别
        matched_categories = []
        for cat, tags in self.CATEGORIES.items():
            overlap = words.intersection(set(tags))
            if overlap:
                matched_categories.append((cat, len(overlap)))
        matched_categories.sort(key=lambda x: x[1], reverse=True)

        # 2. 获取高绩效标签
        candidates: List[HashtagStats] = []
        for cat, _ in matched_categories[:2]:
            candidates.extend(self.top_hashtags(limit=10, category=cat, min_usage=1))

        # 3. 添加共现标签
        for tag in existing:
            stats = self.get_stats(tag)
            if stats and stats.co_tags:
                for co in stats.co_tags[:3]:
                    co_stats = self.get_stats(co)
                    if co_stats:
                        candidates.append(co_stats)

        # 4. 去重+排除已有+去黑名单
        seen: Set[str] = set(existing)
        blacklist = self._get_blacklist()
        filtered = []
        for s in candidates:
            if s.tag not in seen and s.tag not in blacklist:
                seen.add(s.tag)
                filtered.append(s)

        # 5. 多样性选择：不全选同一类别
        filtered.sort(key=lambda s: s.score, reverse=True)
        selected = filtered[:max_tags]

        # 预测到达量
        predicted_reach = sum(s.avg_impressions for s in selected) if selected else 0

        # 竞争度判断
        avg_usage = sum(s.usage_count for s in selected) / max(len(selected), 1)
        if avg_usage > 50:
            competition = "high"
        elif avg_usage > 10:
            competition = "medium"
        else:
            competition = "low"

        return HashtagSet(
            tags=[f"#{s.tag}" for s in selected],
            predicted_reach=round(predicted_reach, 1),
            diversity_score=round(len(set(s.trend for s in selected)) / max(len(selected), 1), 2),
            competition_level=competition,
        )

    # ── 黑名单 ──

    def blacklist_tag(self, tag: str, reason: str = ""):
        """将标签加入黑名单"""
        conn = self.db._get_conn()
        conn.execute(
            "INSERT OR REPLACE INTO hashtag_blacklist (tag, reason) VALUES (?, ?)",
            (tag.lower().lstrip("#"), reason)
        )
        conn.commit()

    def remove_blacklist(self, tag: str):
        conn = self.db._get_conn()
        conn.execute("DELETE FROM hashtag_blacklist WHERE tag = ?",
                     (tag.lower().lstrip("#"),))
        conn.commit()

    def _get_blacklist(self) -> Set[str]:
        conn = self.db._get_conn()
        rows = conn.execute("SELECT tag FROM hashtag_blacklist").fetchall()
        return {r["tag"] for r in rows}

    # ── 时段分析 ──

    def best_posting_hours(self, tag: str = None, limit: int = 5) -> List[Dict]:
        """分析最佳发帖时段"""
        conn = self.db._get_conn()
        if tag:
            rows = conn.execute("""
                SELECT hour_of_day, AVG(engagement_rate) as avg_er,
                       COUNT(*) as cnt, SUM(impressions) as total_imp
                FROM hashtag_usage WHERE tag = ?
                GROUP BY hour_of_day
                ORDER BY avg_er DESC LIMIT ?
            """, (tag.lower().lstrip("#"), limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT hour_of_day, AVG(engagement_rate) as avg_er,
                       COUNT(*) as cnt, SUM(impressions) as total_imp
                FROM hashtag_usage
                GROUP BY hour_of_day
                ORDER BY avg_er DESC LIMIT ?
            """, (limit,)).fetchall()

        return [
            {
                "hour": r["hour_of_day"],
                "avg_engagement_rate": round(r["avg_er"] or 0, 2),
                "sample_count": r["cnt"],
                "total_impressions": r["total_imp"] or 0,
            }
            for r in rows
        ]

    # ── 报告 ──

    def format_report(self, limit: int = 10) -> str:
        """格式化标签分析报告"""
        top = self.top_hashtags(limit=limit, min_usage=1)
        if not top:
            return "📊 暂无标签数据"

        trend_emoji = {"rising": "📈", "falling": "📉", "stable": "➡️", "new": "🆕"}

        lines = [f"🏷️ *Top Hashtags* ({len(top)})\n"]
        for i, s in enumerate(top, 1):
            emoji = trend_emoji.get(s.trend, "❓")
            lines.append(
                f"{i}. `#{s.tag}` {emoji} "
                f"Score: {s.score} | Used: {s.usage_count}x | "
                f"ER: {s.avg_engagement_rate}%"
            )
            if s.co_tags:
                lines.append(f"   Co-tags: {', '.join('#' + t for t in s.co_tags[:3])}")

        hours = self.best_posting_hours(limit=3)
        if hours:
            lines.append("\n⏰ *Best Posting Hours*")
            for h in hours:
                lines.append(f"  {h['hour']:02d}:00 — ER: {h['avg_engagement_rate']}% ({h['sample_count']} samples)")

        return "\n".join(lines)
