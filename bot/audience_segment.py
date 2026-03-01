"""
Audience Segmentation Engine v1.0
受众细分引擎 — 行为聚类 + 兴趣检测 + Engagement分级 + 时区分布 + 画像摘要

Features:
- AudienceProfile: comprehensive user profile with engagement tiers
- SegmentRule: flexible rule-based segmentation
- Interest detection from interaction content
- Timezone distribution analysis
- Persona summary generation
- Content recommendations per segment
"""

import json
import re
import sqlite3
import threading
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any, Set, Tuple


class EngagementTier(Enum):
    COLD = "cold"           # 0 interactions
    WARM = "warm"           # 1-5 interactions
    HOT = "hot"             # 6-20 interactions
    SUPERFAN = "superfan"   # 20+ interactions


class InteractionType(Enum):
    LIKE = "like"
    RETWEET = "retweet"
    REPLY = "reply"
    QUOTE = "quote"
    MENTION = "mention"
    DM = "dm"
    FOLLOW = "follow"
    BOOKMARK = "bookmark"


class RuleOperator(Enum):
    EQUALS = "equals"
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"


@dataclass
class AudienceProfile:
    """受众画像"""
    user_id: str
    username: str = ""
    engagement_tier: EngagementTier = EngagementTier.COLD
    interests: List[str] = field(default_factory=list)
    timezone_offset: Optional[int] = None
    avg_response_time_mins: float = 0.0
    last_active: str = ""
    interaction_count: int = 0
    first_seen: str = ""
    bio: str = ""
    followers: int = 0
    following: int = 0


@dataclass
class SegmentRule:
    """细分规则"""
    field: str
    operator: RuleOperator
    value: Any

    def evaluate(self, profile: AudienceProfile) -> bool:
        """评估规则"""
        actual = getattr(profile, self.field, None)

        if actual is None:
            return False

        if self.operator == RuleOperator.EQUALS:
            if isinstance(actual, Enum):
                return actual.value == self.value
            return actual == self.value

        elif self.operator == RuleOperator.GREATER_THAN:
            return actual > self.value

        elif self.operator == RuleOperator.LESS_THAN:
            return actual < self.value

        elif self.operator == RuleOperator.IN:
            if isinstance(actual, list):
                return any(v in actual for v in self.value)
            if isinstance(actual, Enum):
                return actual.value in self.value
            return actual in self.value

        elif self.operator == RuleOperator.NOT_IN:
            if isinstance(actual, list):
                return not any(v in actual for v in self.value)
            if isinstance(actual, Enum):
                return actual.value not in self.value
            return actual not in self.value

        elif self.operator == RuleOperator.CONTAINS:
            if isinstance(actual, (list, str)):
                return self.value in actual
            return False

        return False


@dataclass
class Segment:
    """受众细分"""
    name: str
    description: str = ""
    rules: List[SegmentRule] = field(default_factory=list)
    created_at: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()


# 兴趣关键词映射
INTEREST_KEYWORDS: Dict[str, List[str]] = {
    "tech": ["code", "python", "javascript", "api", "dev", "software", "ai", "ml", "data",
             "programming", "github", "web", "app", "docker", "cloud", "linux"],
    "crypto": ["bitcoin", "btc", "eth", "crypto", "defi", "nft", "blockchain", "web3",
               "token", "wallet", "mining"],
    "marketing": ["seo", "marketing", "growth", "brand", "ads", "content", "social media",
                  "email", "conversion", "funnel", "leads"],
    "ecommerce": ["shopify", "amazon", "ecommerce", "dropship", "store", "product", "sell",
                  "buyer", "supplier", "inventory"],
    "finance": ["invest", "stock", "market", "trading", "finance", "money", "portfolio",
                "dividend", "returns"],
    "design": ["design", "ui", "ux", "figma", "photoshop", "creative", "illustration",
               "typography", "color", "branding"],
    "gaming": ["game", "gaming", "esports", "stream", "twitch", "console", "pc gaming",
               "rpg", "fps", "moba"],
    "health": ["health", "fitness", "workout", "gym", "diet", "nutrition", "wellness",
               "mental health", "yoga", "meditation"],
}


class AudienceSegmenter:
    """受众细分引擎"""

    TIER_THRESHOLDS = {
        EngagementTier.COLD: 0,
        EngagementTier.WARM: 1,
        EngagementTier.HOT: 6,
        EngagementTier.SUPERFAN: 21,
    }

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._profiles: Dict[str, AudienceProfile] = {}
        self._segments: Dict[str, Segment] = {}
        self._init_db()

    def _init_db(self):
        with self._lock:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS interactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    interaction_type TEXT NOT NULL,
                    content TEXT DEFAULT '',
                    metadata TEXT DEFAULT '{}',
                    timestamp TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_interactions_user ON interactions(user_id);
                CREATE INDEX IF NOT EXISTS idx_interactions_ts ON interactions(timestamp);

                CREATE TABLE IF NOT EXISTS audience_profiles (
                    user_id TEXT PRIMARY KEY,
                    username TEXT DEFAULT '',
                    engagement_tier TEXT DEFAULT 'cold',
                    interests TEXT DEFAULT '[]',
                    timezone_offset INTEGER,
                    bio TEXT DEFAULT '',
                    followers INTEGER DEFAULT 0,
                    following INTEGER DEFAULT 0,
                    first_seen TEXT,
                    last_active TEXT,
                    interaction_count INTEGER DEFAULT 0
                );
            """)
            self._conn.commit()

    def add_interaction(
        self,
        user_id: str,
        interaction_type: InteractionType,
        content: str = "",
        metadata: Optional[Dict] = None
    ):
        """记录互动"""
        now = datetime.now(timezone.utc).isoformat()
        meta_json = json.dumps(metadata or {})

        with self._lock:
            self._conn.execute(
                "INSERT INTO interactions (user_id, interaction_type, content, metadata, timestamp) VALUES (?, ?, ?, ?, ?)",
                (user_id, interaction_type.value, content, meta_json, now)
            )
            self._conn.commit()

        # 更新profile
        self._update_profile(user_id, content, now)

    def _update_profile(self, user_id: str, content: str, timestamp: str):
        """更新用户画像"""
        if user_id not in self._profiles:
            self._profiles[user_id] = AudienceProfile(
                user_id=user_id,
                first_seen=timestamp,
            )

        profile = self._profiles[user_id]
        profile.interaction_count += 1
        profile.last_active = timestamp
        profile.engagement_tier = self.classify_engagement_tier(user_id)

        # 检测兴趣
        if content:
            new_interests = self._detect_interests_from_text(content)
            for interest in new_interests:
                if interest not in profile.interests:
                    profile.interests.append(interest)

    def classify_engagement_tier(self, user_id: str) -> EngagementTier:
        """分类engagement等级"""
        count = self._conn.execute(
            "SELECT COUNT(*) as c FROM interactions WHERE user_id = ?", (user_id,)
        ).fetchone()["c"]

        if count >= self.TIER_THRESHOLDS[EngagementTier.SUPERFAN]:
            return EngagementTier.SUPERFAN
        elif count >= self.TIER_THRESHOLDS[EngagementTier.HOT]:
            return EngagementTier.HOT
        elif count >= self.TIER_THRESHOLDS[EngagementTier.WARM]:
            return EngagementTier.WARM
        return EngagementTier.COLD

    def _detect_interests_from_text(self, text: str) -> List[str]:
        """从文本检测兴趣标签"""
        text_lower = text.lower()
        detected = []
        for interest, keywords in INTEREST_KEYWORDS.items():
            score = sum(1 for kw in keywords if kw in text_lower)
            if score >= 2:
                detected.append(interest)
        return detected

    def detect_interests(self, user_id: str) -> List[str]:
        """从用户所有互动内容推断兴趣"""
        rows = self._conn.execute(
            "SELECT content FROM interactions WHERE user_id = ? AND content != ''",
            (user_id,)
        ).fetchall()
        all_text = " ".join(r["content"] for r in rows)
        return self._detect_interests_from_text(all_text)

    def get_timezone_distribution(self) -> Dict[str, int]:
        """粉丝时区分布"""
        dist: Dict[str, int] = Counter()
        for profile in self._profiles.values():
            if profile.timezone_offset is not None:
                key = f"UTC{profile.timezone_offset:+d}"
                dist[key] += 1
            else:
                dist["unknown"] += 1
        return dict(dist)

    def get_activity_distribution(self) -> Dict[int, int]:
        """活跃时间分布(按小时)"""
        rows = self._conn.execute("SELECT timestamp FROM interactions").fetchall()
        hourly: Dict[int, int] = Counter()
        for r in rows:
            try:
                ts = datetime.fromisoformat(r["timestamp"])
                hourly[ts.hour] += 1
            except (ValueError, TypeError):
                pass
        return dict(sorted(hourly.items()))

    def create_segment(self, name: str, rules: List[SegmentRule], description: str = "") -> Segment:
        """创建自定义细分"""
        segment = Segment(name=name, description=description, rules=rules)
        self._segments[name] = segment
        return segment

    def get_segment_members(self, segment_name: str) -> List[AudienceProfile]:
        """获取细分成员"""
        segment = self._segments.get(segment_name)
        if not segment:
            return []

        members = []
        for profile in self._profiles.values():
            if all(rule.evaluate(profile) for rule in segment.rules):
                members.append(profile)
        return members

    def list_segments(self) -> List[Dict[str, Any]]:
        """列出所有细分"""
        results = []
        for name, seg in self._segments.items():
            members = self.get_segment_members(name)
            results.append({
                "name": name,
                "description": seg.description,
                "rules_count": len(seg.rules),
                "member_count": len(members),
                "created_at": seg.created_at,
            })
        return results

    def get_persona_summary(self) -> Dict[str, Any]:
        """受众画像摘要"""
        profiles = list(self._profiles.values())
        if not profiles:
            return {"total_audience": 0}

        tier_dist = Counter(p.engagement_tier.value for p in profiles)
        all_interests = Counter()
        for p in profiles:
            all_interests.update(p.interests)

        active_threshold = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        active_count = sum(1 for p in profiles if p.last_active >= active_threshold)

        return {
            "total_audience": len(profiles),
            "active_7d": active_count,
            "tier_distribution": dict(tier_dist),
            "top_interests": dict(all_interests.most_common(10)),
            "avg_interactions": round(
                sum(p.interaction_count for p in profiles) / len(profiles), 1
            ),
            "timezone_distribution": self.get_timezone_distribution(),
        }

    def recommend_content_for_segment(self, segment_name: str) -> Dict[str, Any]:
        """针对细分群体的内容建议"""
        members = self.get_segment_members(segment_name)
        if not members:
            return {"segment": segment_name, "recommendations": []}

        all_interests = Counter()
        for m in members:
            all_interests.update(m.interests)

        tier_dist = Counter(m.engagement_tier.value for m in members)

        recommendations = []
        top_interests = [i for i, _ in all_interests.most_common(3)]

        if top_interests:
            recommendations.append(f"Focus on {', '.join(top_interests)} topics")

        if tier_dist.get("superfan", 0) > 0:
            recommendations.append("Create exclusive/behind-scenes content for superfans")

        if tier_dist.get("cold", 0) > len(members) * 0.5:
            recommendations.append("Increase engagement CTAs to warm up cold audience")

        activity = self.get_activity_distribution()
        if activity:
            peak_hour = max(activity, key=activity.get) if activity else None
            if peak_hour is not None:
                recommendations.append(f"Post around {peak_hour}:00 UTC for max reach")

        return {
            "segment": segment_name,
            "member_count": len(members),
            "top_interests": top_interests,
            "recommendations": recommendations,
        }

    def get_profile(self, user_id: str) -> Optional[AudienceProfile]:
        """获取用户画像"""
        return self._profiles.get(user_id)

    def set_profile_info(self, user_id: str, **kwargs):
        """设置用户信息"""
        if user_id not in self._profiles:
            self._profiles[user_id] = AudienceProfile(user_id=user_id)
        profile = self._profiles[user_id]
        for k, v in kwargs.items():
            if hasattr(profile, k):
                setattr(profile, k, v)

    def get_total_profiles(self) -> int:
        return len(self._profiles)

    def get_total_interactions(self) -> int:
        return self._conn.execute("SELECT COUNT(*) as c FROM interactions").fetchone()["c"]

    def close(self):
        self._conn.close()
