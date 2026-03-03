"""
Influencer Finder v1.0
KOL发现引擎 — 精准挖掘垂类影响者 + 互动质量评分 + 合作价值评估

Features:
- NicheScorer: 基于关键词/话题相关度评分
- EngagementQuality: 区分真互动 vs 水军互动
- GrowthTracker: 粉丝增长轨迹分析
- InfluencerRanker: 多维加权综合排名
- CooperationEstimator: 合作ROI预估
- WatchList: 持久化关注列表 + 变动追踪
"""

import json
import logging
import math
import sqlite3
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Dict, List, Optional, Set

logger = logging.getLogger(__name__)


# ── 数据模型 ──


class InfluencerTier(Enum):
    """影响者等级"""
    NANO = "nano"           # 1K-10K
    MICRO = "micro"         # 10K-50K
    MID = "mid"             # 50K-500K
    MACRO = "macro"         # 500K-1M
    MEGA = "mega"           # 1M+

    @classmethod
    def from_followers(cls, count: int) -> "InfluencerTier":
        if count < 10_000:
            return cls.NANO
        elif count < 50_000:
            return cls.MICRO
        elif count < 500_000:
            return cls.MID
        elif count < 1_000_000:
            return cls.MACRO
        return cls.MEGA


@dataclass
class InfluencerProfile:
    """影响者画像"""
    user_id: str
    username: str
    display_name: str = ""
    bio: str = ""
    followers: int = 0
    following: int = 0
    tweet_count: int = 0
    verified: bool = False
    created_at: Optional[str] = None

    # 计算指标
    tier: str = ""
    niche_score: float = 0.0
    engagement_score: float = 0.0
    quality_score: float = 0.0
    growth_score: float = 0.0
    overall_score: float = 0.0
    cooperation_value: float = 0.0

    # 元数据
    discovered_at: str = ""
    last_updated: str = ""
    tags: List[str] = field(default_factory=list)
    notes: str = ""

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict) -> "InfluencerProfile":
        known = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in d.items() if k in known}
        return cls(**filtered)


@dataclass
class EngagementSample:
    """互动样本"""
    tweet_id: str
    likes: int = 0
    retweets: int = 0
    replies: int = 0
    quotes: int = 0
    impressions: int = 0
    created_at: str = ""

    @property
    def total_engagement(self) -> int:
        return self.likes + self.retweets + self.replies + self.quotes

    @property
    def engagement_rate(self) -> float:
        if self.impressions > 0:
            return self.total_engagement / self.impressions
        return 0.0


@dataclass
class NicheConfig:
    """垂类配置"""
    name: str
    keywords: List[str] = field(default_factory=list)
    hashtags: List[str] = field(default_factory=list)
    seed_accounts: List[str] = field(default_factory=list)
    min_followers: int = 1000
    max_followers: int = 10_000_000
    min_engagement_rate: float = 0.01
    language: str = "en"

    def to_dict(self) -> Dict:
        return asdict(self)


# ── Niche相关度评分 ──


class NicheScorer:
    """垂类相关度评分器"""

    def __init__(self, config: NicheConfig):
        self.config = config
        self._keywords_lower = [k.lower() for k in config.keywords]
        self._hashtags_lower = [h.lower().lstrip("#") for h in config.hashtags]

    def score_bio(self, bio: str) -> float:
        """Bio相关度评分 (0-1)"""
        if not bio:
            return 0.0
        bio_lower = bio.lower()
        hits = sum(1 for kw in self._keywords_lower if kw in bio_lower)
        tag_hits = sum(1 for tag in self._hashtags_lower if tag in bio_lower)
        total = hits + tag_hits
        max_possible = len(self._keywords_lower) + len(self._hashtags_lower)
        if max_possible == 0:
            return 0.0
        # 对数衰减，避免堆砌关键词得高分
        return min(1.0, math.log1p(total) / math.log1p(max_possible))

    def score_tweets(self, tweets: List[Dict]) -> float:
        """推文内容相关度评分 (0-1)"""
        if not tweets:
            return 0.0
        relevant = 0
        for tweet in tweets:
            text = tweet.get("text", "").lower()
            if any(kw in text for kw in self._keywords_lower):
                relevant += 1
            elif any(tag in text for tag in self._hashtags_lower):
                relevant += 1
        return min(1.0, relevant / len(tweets))

    def score(self, profile: InfluencerProfile, tweets: List[Dict] = None) -> float:
        """综合Niche评分"""
        bio_score = self.score_bio(profile.bio)
        tweet_score = self.score_tweets(tweets or [])
        # Bio权重0.4, 推文内容0.6
        return bio_score * 0.4 + tweet_score * 0.6


# ── 互动质量评估 ──


class EngagementQualityAnalyzer:
    """互动质量分析器 — 区分真互动和水军"""

    def __init__(self):
        self.suspicious_thresholds = {
            "like_reply_ratio_max": 100,  # 点赞/回复 > 100 可疑
            "retweet_reply_ratio_max": 50,  # 转推/回复 > 50 可疑
            "zero_reply_pct_max": 0.9,    # 90%以上推文0回复可疑
            "engagement_cv_min": 0.3,      # 互动量变异系数过低(太均匀)可疑
        }

    def analyze(self, samples: List[EngagementSample]) -> Dict:
        """分析互动质量"""
        if not samples:
            return {"quality_score": 0.0, "flags": ["no_samples"], "details": {}}

        flags = []
        details = {}

        # 1. 点赞/回复比
        total_likes = sum(s.likes for s in samples)
        total_replies = sum(s.replies for s in samples)
        if total_replies > 0:
            lr_ratio = total_likes / total_replies
            details["like_reply_ratio"] = round(lr_ratio, 1)
            if lr_ratio > self.suspicious_thresholds["like_reply_ratio_max"]:
                flags.append("suspicious_like_ratio")
        elif total_likes > 0:
            flags.append("zero_replies_with_likes")
            details["like_reply_ratio"] = float("inf")

        # 2. 转推/回复比
        total_rt = sum(s.retweets for s in samples)
        if total_replies > 0:
            rr_ratio = total_rt / total_replies
            details["retweet_reply_ratio"] = round(rr_ratio, 1)
            if rr_ratio > self.suspicious_thresholds["retweet_reply_ratio_max"]:
                flags.append("suspicious_retweet_ratio")

        # 3. 零回复推文占比
        zero_reply = sum(1 for s in samples if s.replies == 0)
        zero_pct = zero_reply / len(samples)
        details["zero_reply_pct"] = round(zero_pct, 2)
        if zero_pct > self.suspicious_thresholds["zero_reply_pct_max"]:
            flags.append("mostly_zero_replies")

        # 4. 互动量变异系数(CV)
        engagements = [s.total_engagement for s in samples]
        if engagements:
            mean = sum(engagements) / len(engagements)
            if mean > 0:
                variance = sum((e - mean) ** 2 for e in engagements) / len(engagements)
                cv = math.sqrt(variance) / mean
                details["engagement_cv"] = round(cv, 3)
                if cv < self.suspicious_thresholds["engagement_cv_min"]:
                    flags.append("too_uniform_engagement")

        # 5. 互动率分布
        rates = [s.engagement_rate for s in samples if s.impressions > 0]
        if rates:
            avg_rate = sum(rates) / len(rates)
            details["avg_engagement_rate"] = round(avg_rate, 4)
        else:
            avg_rate = 0.0
            if engagements:
                # 估算: 假设印象 = 粉丝 * 0.1
                avg_rate = mean / 10000 if mean > 0 else 0.0
                details["estimated_engagement_rate"] = round(avg_rate, 4)

        # 质量评分
        penalty = len(flags) * 0.15
        base_score = 1.0
        # 有回复是好信号
        reply_bonus = min(0.2, (total_replies / max(1, len(samples))) * 0.01)
        quality_score = max(0.0, min(1.0, base_score - penalty + reply_bonus))

        return {
            "quality_score": round(quality_score, 3),
            "flags": flags,
            "details": details,
        }


# ── 增长轨迹分析 ──


class GrowthTracker:
    """粉丝增长轨迹分析"""

    def __init__(self, db_path: str = "influencer_growth.db"):
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
            CREATE TABLE IF NOT EXISTS growth_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                followers INTEGER NOT NULL,
                following INTEGER NOT NULL,
                tweet_count INTEGER NOT NULL DEFAULT 0,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(user_id, recorded_at)
            );
            CREATE INDEX IF NOT EXISTS idx_growth_user ON growth_snapshots(user_id);
            CREATE INDEX IF NOT EXISTS idx_growth_time ON growth_snapshots(recorded_at);
        """)
        conn.commit()

    def record(self, user_id: str, followers: int, following: int, tweet_count: int = 0):
        """记录粉丝数快照"""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO growth_snapshots(user_id, followers, following, tweet_count, recorded_at) VALUES(?,?,?,?,?)",
                (user_id, followers, following, tweet_count, now),
            )
            conn.commit()
        except sqlite3.Error as e:
            logger.error("Growth record failed for %s: %s", user_id, e)

    def get_history(self, user_id: str, days: int = 30) -> List[Dict]:
        """获取粉丝历史"""
        conn = self._get_conn()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = conn.execute(
            "SELECT followers, following, tweet_count, recorded_at FROM growth_snapshots WHERE user_id=? AND recorded_at>=? ORDER BY recorded_at",
            (user_id, cutoff),
        ).fetchall()
        return [dict(r) for r in rows]

    def calculate_growth(self, user_id: str, days: int = 30) -> Dict:
        """计算增长指标"""
        history = self.get_history(user_id, days)
        if len(history) < 2:
            return {
                "growth_rate": 0.0,
                "daily_avg": 0.0,
                "trend": "insufficient_data",
                "data_points": len(history),
            }

        first = history[0]["followers"]
        last = history[-1]["followers"]
        diff = last - first

        # 计算日均增长
        try:
            t0 = datetime.fromisoformat(history[0]["recorded_at"])
            t1 = datetime.fromisoformat(history[-1]["recorded_at"])
            span_days = max(1, (t1 - t0).days)
        except (ValueError, TypeError):
            span_days = max(1, days)

        daily_avg = diff / span_days
        growth_rate = diff / max(1, first)

        # 趋势判定
        if len(history) >= 5:
            mid = len(history) // 2
            first_half_avg = sum(h["followers"] for h in history[:mid]) / mid
            second_half_avg = sum(h["followers"] for h in history[mid:]) / (len(history) - mid)
            if second_half_avg > first_half_avg * 1.05:
                trend = "accelerating"
            elif second_half_avg < first_half_avg * 0.95:
                trend = "decelerating"
            else:
                trend = "steady"
        else:
            trend = "growing" if diff > 0 else ("declining" if diff < 0 else "flat")

        return {
            "growth_rate": round(growth_rate, 4),
            "daily_avg": round(daily_avg, 1),
            "absolute_change": diff,
            "trend": trend,
            "data_points": len(history),
            "period_days": span_days,
        }

    def growth_score(self, user_id: str, days: int = 30) -> float:
        """增长评分 (0-1)"""
        metrics = self.calculate_growth(user_id, days)
        if metrics["trend"] == "insufficient_data":
            return 0.5  # 中性

        rate = metrics["growth_rate"]
        # sigmoid映射: 30天增长10%=0.73, 50%=0.95
        score = 1 / (1 + math.exp(-10 * rate))
        # 趋势加成
        trend_bonus = {
            "accelerating": 0.1,
            "growing": 0.05,
            "steady": 0,
            "decelerating": -0.05,
            "declining": -0.1,
            "flat": 0,
        }
        score = max(0.0, min(1.0, score + trend_bonus.get(metrics["trend"], 0)))
        return round(score, 3)


# ── 合作价值评估 ──


class CooperationEstimator:
    """合作价值评估器"""

    # CPE基准 (每互动成本, USD)
    CPE_BENCHMARKS = {
        InfluencerTier.NANO: (0.05, 0.15),
        InfluencerTier.MICRO: (0.10, 0.30),
        InfluencerTier.MID: (0.20, 0.60),
        InfluencerTier.MACRO: (0.50, 1.50),
        InfluencerTier.MEGA: (1.00, 3.00),
    }

    # 预估报价基准 (USD/推文)
    RATE_BENCHMARKS = {
        InfluencerTier.NANO: (10, 100),
        InfluencerTier.MICRO: (100, 500),
        InfluencerTier.MID: (500, 5000),
        InfluencerTier.MACRO: (5000, 20000),
        InfluencerTier.MEGA: (20000, 100000),
    }

    def estimate(
        self,
        profile: InfluencerProfile,
        avg_engagement: float = 0.0,
        niche_score: float = 0.0,
    ) -> Dict:
        """评估合作价值"""
        tier = InfluencerTier.from_followers(profile.followers)
        cpe_range = self.CPE_BENCHMARKS.get(tier, (0.1, 0.5))
        rate_range = self.RATE_BENCHMARKS.get(tier, (50, 500))

        # 预估报价
        est_rate = (rate_range[0] + rate_range[1]) / 2

        # 预估CPE
        est_cpe = (cpe_range[0] + cpe_range[1]) / 2

        # 预估互动数
        if avg_engagement > 0:
            est_engagements = avg_engagement
        else:
            # 用粉丝数 * 默认互动率估算
            default_rates = {
                InfluencerTier.NANO: 0.05,
                InfluencerTier.MICRO: 0.03,
                InfluencerTier.MID: 0.02,
                InfluencerTier.MACRO: 0.015,
                InfluencerTier.MEGA: 0.01,
            }
            est_engagements = profile.followers * default_rates.get(tier, 0.02)

        # 合作效率 = 互动数 / 报价
        efficiency = est_engagements / max(1, est_rate)

        # Niche匹配加成
        value_multiplier = 1.0 + (niche_score * 0.5)

        # 综合价值评分 (0-100)
        value_score = min(100, efficiency * value_multiplier * 50)

        return {
            "tier": tier.value,
            "estimated_rate_usd": round(est_rate, 0),
            "rate_range": rate_range,
            "estimated_cpe": round(est_cpe, 3),
            "estimated_engagements": round(est_engagements, 0),
            "efficiency": round(efficiency, 3),
            "niche_multiplier": round(value_multiplier, 2),
            "cooperation_value_score": round(value_score, 1),
        }


# ── 综合排名器 ──


class InfluencerRanker:
    """多维加权综合排名"""

    DEFAULT_WEIGHTS = {
        "niche": 0.30,
        "engagement_quality": 0.25,
        "growth": 0.20,
        "cooperation_value": 0.15,
        "authenticity": 0.10,
    }

    def __init__(self, weights: Optional[Dict[str, float]] = None):
        self.weights = weights or self.DEFAULT_WEIGHTS.copy()
        # 归一化
        total = sum(self.weights.values())
        if total > 0:
            self.weights = {k: v / total for k, v in self.weights.items()}

    def rank(self, profiles: List[InfluencerProfile]) -> List[InfluencerProfile]:
        """根据overall_score排序"""
        return sorted(profiles, key=lambda p: p.overall_score, reverse=True)

    def calculate_overall(
        self,
        niche_score: float,
        quality_score: float,
        growth_score: float,
        cooperation_score: float,
        authenticity_score: float = 0.5,
    ) -> float:
        """计算综合评分"""
        scores = {
            "niche": niche_score,
            "engagement_quality": quality_score,
            "growth": growth_score,
            "cooperation_value": cooperation_score / 100,  # 归一化到0-1
            "authenticity": authenticity_score,
        }
        total = sum(self.weights.get(k, 0) * v for k, v in scores.items())
        return round(min(1.0, max(0.0, total)), 3)


# ── 关注列表持久化 ──


class WatchList:
    """影响者关注列表"""

    def __init__(self, db_path: str = "influencer_watchlist.db"):
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
            CREATE TABLE IF NOT EXISTS watchlist (
                user_id TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                display_name TEXT DEFAULT '',
                tier TEXT DEFAULT '',
                niche_score REAL DEFAULT 0,
                quality_score REAL DEFAULT 0,
                growth_score REAL DEFAULT 0,
                overall_score REAL DEFAULT 0,
                cooperation_value REAL DEFAULT 0,
                tags TEXT DEFAULT '[]',
                notes TEXT DEFAULT '',
                added_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                status TEXT DEFAULT 'watching'
            );
            CREATE TABLE IF NOT EXISTS watchlist_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY(user_id) REFERENCES watchlist(user_id)
            );
            CREATE INDEX IF NOT EXISTS idx_wl_status ON watchlist(status);
            CREATE INDEX IF NOT EXISTS idx_wl_score ON watchlist(overall_score DESC);
            CREATE INDEX IF NOT EXISTS idx_wle_user ON watchlist_events(user_id);
        """)
        conn.commit()

    def add(self, profile: InfluencerProfile, status: str = "watching") -> bool:
        """添加到关注列表"""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        try:
            conn.execute(
                """INSERT OR REPLACE INTO watchlist
                   (user_id, username, display_name, tier, niche_score, quality_score,
                    growth_score, overall_score, cooperation_value, tags, notes, added_at, updated_at, status)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    profile.user_id, profile.username, profile.display_name,
                    profile.tier, profile.niche_score, profile.quality_score,
                    profile.growth_score, profile.overall_score, profile.cooperation_value,
                    json.dumps(profile.tags), profile.notes, now, now, status,
                ),
            )
            conn.execute(
                "INSERT INTO watchlist_events(user_id, event_type, new_value) VALUES(?,?,?)",
                (profile.user_id, "added", status),
            )
            conn.commit()
            return True
        except sqlite3.Error as e:
            logger.error("WatchList add failed: %s", e)
            return False

    def remove(self, user_id: str) -> bool:
        """从列表移除"""
        conn = self._get_conn()
        try:
            conn.execute("DELETE FROM watchlist WHERE user_id=?", (user_id,))
            conn.execute(
                "INSERT INTO watchlist_events(user_id, event_type) VALUES(?,?)",
                (user_id, "removed"),
            )
            conn.commit()
            return True
        except sqlite3.Error as e:
            logger.error("WatchList remove failed: %s", e)
            return False

    def get(self, user_id: str) -> Optional[Dict]:
        """获取单个"""
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM watchlist WHERE user_id=?", (user_id,)).fetchone()
        return dict(row) if row else None

    def list_all(
        self,
        status: Optional[str] = None,
        min_score: float = 0.0,
        tier: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict]:
        """列出关注列表"""
        conn = self._get_conn()
        query = "SELECT * FROM watchlist WHERE overall_score >= ?"
        params: List = [min_score]
        if status:
            query += " AND status=?"
            params.append(status)
        if tier:
            query += " AND tier=?"
            params.append(tier)
        query += " ORDER BY overall_score DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def update_scores(
        self,
        user_id: str,
        niche_score: Optional[float] = None,
        quality_score: Optional[float] = None,
        growth_score: Optional[float] = None,
        overall_score: Optional[float] = None,
    ) -> bool:
        """更新评分"""
        conn = self._get_conn()
        sets = []
        params = []
        if niche_score is not None:
            sets.append("niche_score=?")
            params.append(niche_score)
        if quality_score is not None:
            sets.append("quality_score=?")
            params.append(quality_score)
        if growth_score is not None:
            sets.append("growth_score=?")
            params.append(growth_score)
        if overall_score is not None:
            sets.append("overall_score=?")
            params.append(overall_score)
        if not sets:
            return False
        sets.append("updated_at=?")
        params.append(datetime.now(timezone.utc).isoformat())
        params.append(user_id)
        try:
            conn.execute(f"UPDATE watchlist SET {', '.join(sets)} WHERE user_id=?", params)
            conn.commit()
            return True
        except sqlite3.Error as e:
            logger.error("WatchList update failed: %s", e)
            return False

    def get_events(self, user_id: str, limit: int = 50) -> List[Dict]:
        """获取变动事件"""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM watchlist_events WHERE user_id=? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def stats(self) -> Dict:
        """统计概览"""
        conn = self._get_conn()
        total = conn.execute("SELECT COUNT(*) FROM watchlist").fetchone()[0]
        by_tier = {}
        for row in conn.execute("SELECT tier, COUNT(*) as cnt FROM watchlist GROUP BY tier").fetchall():
            by_tier[row["tier"]] = row["cnt"]
        by_status = {}
        for row in conn.execute("SELECT status, COUNT(*) as cnt FROM watchlist GROUP BY status").fetchall():
            by_status[row["status"]] = row["cnt"]
        avg_score_row = conn.execute("SELECT AVG(overall_score) FROM watchlist").fetchone()
        avg_score = avg_score_row[0] if avg_score_row[0] else 0.0
        return {
            "total": total,
            "by_tier": by_tier,
            "by_status": by_status,
            "avg_overall_score": round(avg_score, 3),
        }


# ── 组合接口 ──


class InfluencerFinder:
    """影响者发现引擎 — 统一入口"""

    def __init__(
        self,
        niche: Optional[NicheConfig] = None,
        db_dir: str = ".",
        weights: Optional[Dict[str, float]] = None,
    ):
        self.niche = niche or NicheConfig(name="general")
        self.scorer = NicheScorer(self.niche)
        self.quality_analyzer = EngagementQualityAnalyzer()
        self.growth_tracker = GrowthTracker(f"{db_dir}/influencer_growth.db")
        self.cooperation = CooperationEstimator()
        self.ranker = InfluencerRanker(weights)
        self.watchlist = WatchList(f"{db_dir}/influencer_watchlist.db")

    def evaluate(
        self,
        profile: InfluencerProfile,
        tweets: Optional[List[Dict]] = None,
        samples: Optional[List[EngagementSample]] = None,
    ) -> InfluencerProfile:
        """完整评估一个影响者"""
        # 1. Tier
        profile.tier = InfluencerTier.from_followers(profile.followers).value

        # 2. Niche评分
        profile.niche_score = self.scorer.score(profile, tweets or [])

        # 3. 互动质量
        quality = self.quality_analyzer.analyze(samples or [])
        profile.quality_score = quality["quality_score"]

        # 4. 增长评分
        self.growth_tracker.record(
            profile.user_id, profile.followers, profile.following, profile.tweet_count
        )
        profile.growth_score = self.growth_tracker.growth_score(profile.user_id)

        # 5. 合作价值
        avg_eng = 0.0
        if samples:
            avg_eng = sum(s.total_engagement for s in samples) / len(samples)
        coop = self.cooperation.estimate(profile, avg_eng, profile.niche_score)
        profile.cooperation_value = coop["cooperation_value_score"]

        # 6. 综合评分
        profile.overall_score = self.ranker.calculate_overall(
            profile.niche_score,
            profile.quality_score,
            profile.growth_score,
            profile.cooperation_value,
        )

        # 时间戳
        now = datetime.now(timezone.utc).isoformat()
        if not profile.discovered_at:
            profile.discovered_at = now
        profile.last_updated = now

        return profile

    def batch_evaluate(
        self,
        profiles: List[InfluencerProfile],
        tweets_map: Optional[Dict[str, List[Dict]]] = None,
        samples_map: Optional[Dict[str, List[EngagementSample]]] = None,
    ) -> List[InfluencerProfile]:
        """批量评估 + 排名"""
        tweets_map = tweets_map or {}
        samples_map = samples_map or {}
        results = []
        for p in profiles:
            tweets = tweets_map.get(p.user_id, [])
            samples = samples_map.get(p.user_id, [])
            results.append(self.evaluate(p, tweets, samples))
        return self.ranker.rank(results)

    def discover_from_seed(self, seed_usernames: List[str]) -> List[str]:
        """从种子账号发现更多候选(返回用户名列表供API抓取)"""
        # 策略: 种子账号的互动者 / 关注者交集
        candidates: Set[str] = set()
        for u in seed_usernames:
            candidates.add(u)
        # 真实实现需要API调用，这里返回种子列表
        return list(candidates)

    def export_report(self, profiles: List[InfluencerProfile], format: str = "json") -> str:
        """导出评估报告"""
        if format == "json":
            return json.dumps([p.to_dict() for p in profiles], indent=2, default=str)
        elif format == "csv":
            lines = ["username,tier,niche,quality,growth,cooperation,overall"]
            for p in profiles:
                lines.append(
                    f"{p.username},{p.tier},{p.niche_score:.3f},{p.quality_score:.3f},"
                    f"{p.growth_score:.3f},{p.cooperation_value:.1f},{p.overall_score:.3f}"
                )
            return "\n".join(lines)
        elif format == "text":
            lines = [f"🔍 Influencer Report ({len(profiles)} profiles)", "=" * 50]
            for i, p in enumerate(profiles, 1):
                lines.append(
                    f"#{i} @{p.username} [{p.tier}] "
                    f"Score:{p.overall_score:.3f} | "
                    f"Niche:{p.niche_score:.2f} Quality:{p.quality_score:.2f} "
                    f"Growth:{p.growth_score:.2f}"
                )
            return "\n".join(lines)
        return ""
