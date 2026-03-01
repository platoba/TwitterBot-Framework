"""
Audience Analyzer - 受众分析引擎 v3.0
粉丝画像 + 活跃时段 + 兴趣图谱 + 分群 + 增长追踪
"""

import logging
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from bot.twitter_api import TwitterAPI
from bot.database import Database

logger = logging.getLogger(__name__)


@dataclass
class AudienceSegment:
    """受众分群"""
    name: str
    description: str
    user_ids: List[str] = field(default_factory=list)
    criteria: Dict = field(default_factory=dict)

    @property
    def size(self) -> int:
        return len(self.user_ids)

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "description": self.description,
            "size": self.size,
            "criteria": self.criteria,
        }


@dataclass
class AudienceProfile:
    """受众画像"""
    total_analyzed: int = 0
    verified_pct: float = 0.0
    avg_followers: float = 0.0
    avg_following: float = 0.0
    avg_tweets: float = 0.0
    median_followers: int = 0
    top_locations: List[Tuple[str, int]] = field(default_factory=list)
    top_languages: List[Tuple[str, int]] = field(default_factory=list)
    top_interests: List[Tuple[str, int]] = field(default_factory=list)
    active_hours: Dict[int, int] = field(default_factory=dict)
    follower_tiers: Dict[str, int] = field(default_factory=dict)
    account_age_distribution: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "total_analyzed": self.total_analyzed,
            "verified_pct": self.verified_pct,
            "avg_followers": round(self.avg_followers, 1),
            "avg_following": round(self.avg_following, 1),
            "avg_tweets": round(self.avg_tweets, 1),
            "median_followers": self.median_followers,
            "top_locations": self.top_locations[:10],
            "top_languages": self.top_languages[:10],
            "top_interests": self.top_interests[:20],
            "active_hours": self.active_hours,
            "follower_tiers": self.follower_tiers,
            "account_age_distribution": self.account_age_distribution,
        }


class AudienceAnalyzer:
    """受众分析引擎"""

    FOLLOWER_TIERS = {
        "nano": (0, 1000),
        "micro": (1000, 10000),
        "mid": (10000, 100000),
        "macro": (100000, 1000000),
        "mega": (1000000, float("inf")),
    }

    INTEREST_KEYWORDS = {
        "tech": ["developer", "coding", "programming", "software", "tech", "AI", "ML", "data", "web3", "crypto"],
        "business": ["entrepreneur", "startup", "founder", "CEO", "business", "marketing", "growth"],
        "creative": ["designer", "artist", "photographer", "writer", "creator", "content"],
        "finance": ["trading", "investing", "finance", "stocks", "crypto", "DeFi", "FinTech"],
        "ecommerce": ["ecommerce", "shopify", "amazon", "dropshipping", "seller", "FBA"],
        "marketing": ["marketing", "SEO", "ads", "social media", "growth hacking", "PPC"],
        "education": ["teacher", "professor", "student", "learning", "education", "university"],
        "health": ["fitness", "health", "wellness", "nutrition", "yoga", "mental health"],
    }

    def __init__(self, api: TwitterAPI, db: Database):
        self.api = api
        self.db = db
        self._cache: Dict[str, List[Dict]] = {}

    def fetch_followers(self, username: str, max_count: int = 200) -> List[Dict]:
        """获取粉丝列表(带缓存)"""
        cache_key = f"followers:{username}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        user_id = self.api.resolve_username(username)
        if not user_id:
            return []

        followers = []
        pagination_token = None

        while len(followers) < max_count:
            batch_size = min(100, max_count - len(followers))
            data = self.api.get_followers(user_id, max_results=batch_size,
                                           pagination_token=pagination_token)
            if not data or "data" not in data:
                break

            followers.extend(data["data"])
            pagination_token = data.get("meta", {}).get("next_token")
            if not pagination_token:
                break

        self._cache[cache_key] = followers
        return followers

    def fetch_following(self, username: str, max_count: int = 200) -> List[Dict]:
        """获取关注列表"""
        cache_key = f"following:{username}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        user_id = self.api.resolve_username(username)
        if not user_id:
            return []

        following = []
        pagination_token = None

        while len(following) < max_count:
            batch_size = min(100, max_count - len(following))
            data = self.api.get_following(user_id, max_results=batch_size,
                                           pagination_token=pagination_token)
            if not data or "data" not in data:
                break

            following.extend(data["data"])
            pagination_token = data.get("meta", {}).get("next_token")
            if not pagination_token:
                break

        self._cache[cache_key] = following
        return following

    def build_profile(self, users: List[Dict]) -> AudienceProfile:
        """从用户列表构建受众画像"""
        if not users:
            return AudienceProfile()

        profile = AudienceProfile(total_analyzed=len(users))

        # 基础统计
        followers_counts = []
        following_counts = []
        tweet_counts = []
        verified_count = 0
        locations = Counter()
        languages = Counter()
        interests = Counter()
        account_ages = Counter()
        now = datetime.now(timezone.utc)

        for user in users:
            metrics = user.get("public_metrics", {})
            fc = metrics.get("followers_count", 0)
            followers_counts.append(fc)
            following_counts.append(metrics.get("following_count", 0))
            tweet_counts.append(metrics.get("tweet_count", 0))

            if user.get("verified"):
                verified_count += 1

            loc = user.get("location", "")
            if loc:
                locations[loc.strip()] += 1

            # 从bio提取兴趣
            bio = user.get("description", "").lower()
            for interest, keywords in self.INTEREST_KEYWORDS.items():
                for kw in keywords:
                    if kw.lower() in bio:
                        interests[interest] += 1
                        break

            # 账号年龄
            created = user.get("created_at", "")
            if created:
                try:
                    created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                    age_days = (now - created_dt).days
                    if age_days < 90:
                        account_ages["< 3 months"] += 1
                    elif age_days < 365:
                        account_ages["3-12 months"] += 1
                    elif age_days < 365 * 3:
                        account_ages["1-3 years"] += 1
                    elif age_days < 365 * 5:
                        account_ages["3-5 years"] += 1
                    else:
                        account_ages["5+ years"] += 1
                except (ValueError, TypeError):
                    pass

        # 汇总
        profile.verified_pct = round(verified_count / len(users) * 100, 1) if users else 0

        if followers_counts:
            profile.avg_followers = sum(followers_counts) / len(followers_counts)
            profile.avg_following = sum(following_counts) / len(following_counts)
            profile.avg_tweets = sum(tweet_counts) / len(tweet_counts)
            sorted_fc = sorted(followers_counts)
            profile.median_followers = sorted_fc[len(sorted_fc) // 2]

        profile.top_locations = locations.most_common(10)
        profile.top_interests = interests.most_common(20)
        profile.account_age_distribution = dict(account_ages)

        # 粉丝层级分布
        tier_counts = Counter()
        for fc in followers_counts:
            for tier_name, (low, high) in self.FOLLOWER_TIERS.items():
                if low <= fc < high:
                    tier_counts[tier_name] += 1
                    break
        profile.follower_tiers = dict(tier_counts)

        return profile

    def analyze_activity_patterns(self, username: str,
                                   tweet_limit: int = 100) -> Dict:
        """分析发推活跃时段"""
        user_id = self.api.resolve_username(username)
        if not user_id:
            return {}

        data = self.api.get_user_tweets(user_id, max_results=min(tweet_limit, 100))
        if not data or "data" not in data:
            return {}

        hour_counts = Counter()
        day_counts = Counter()
        hourly_engagement = defaultdict(list)

        for tweet in data["data"]:
            created = tweet.get("created_at", "")
            if not created:
                continue
            try:
                dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                hour_counts[dt.hour] += 1
                day_counts[dt.strftime("%A")] += 1

                eng = tweet.get("public_metrics", {})
                total_eng = eng.get("like_count", 0) + eng.get("retweet_count", 0)
                hourly_engagement[dt.hour].append(total_eng)
            except (ValueError, TypeError):
                continue

        # 最佳发帖时间
        best_hours = []
        for hour, engs in hourly_engagement.items():
            avg_eng = sum(engs) / len(engs) if engs else 0
            best_hours.append((hour, avg_eng, len(engs)))
        best_hours.sort(key=lambda x: x[1], reverse=True)

        return {
            "hour_distribution": dict(hour_counts.most_common()),
            "day_distribution": dict(day_counts.most_common()),
            "best_posting_hours": [
                {"hour": h, "avg_engagement": round(e, 1), "sample_size": s}
                for h, e, s in best_hours[:5]
            ],
            "total_analyzed": sum(hour_counts.values()),
        }

    def segment_audience(self, users: List[Dict]) -> List[AudienceSegment]:
        """自动分群"""
        segments = []

        # 1. 按粉丝层级
        for tier_name, (low, high) in self.FOLLOWER_TIERS.items():
            tier_users = [
                u.get("id", "")
                for u in users
                if low <= u.get("public_metrics", {}).get("followers_count", 0) < high
            ]
            if tier_users:
                segments.append(AudienceSegment(
                    name=f"tier_{tier_name}",
                    description=f"Followers {low:,}-{high:,.0f}" if high != float("inf") else f"Followers {low:,}+",
                    user_ids=tier_users,
                    criteria={"followers_min": low, "followers_max": high if high != float("inf") else None},
                ))

        # 2. 按兴趣
        for interest, keywords in self.INTEREST_KEYWORDS.items():
            interest_users = []
            for u in users:
                bio = u.get("description", "").lower()
                if any(kw.lower() in bio for kw in keywords):
                    interest_users.append(u.get("id", ""))
            if interest_users:
                segments.append(AudienceSegment(
                    name=f"interest_{interest}",
                    description=f"Interested in {interest}",
                    user_ids=interest_users,
                    criteria={"interest": interest},
                ))

        # 3. 高活跃度 (高推文数)
        active_users = [
            u.get("id", "")
            for u in users
            if u.get("public_metrics", {}).get("tweet_count", 0) > 5000
        ]
        if active_users:
            segments.append(AudienceSegment(
                name="high_activity",
                description="5000+ tweets, highly active users",
                user_ids=active_users,
                criteria={"min_tweets": 5000},
            ))

        # 4. 验证账号
        verified_users = [u.get("id", "") for u in users if u.get("verified")]
        if verified_users:
            segments.append(AudienceSegment(
                name="verified",
                description="Verified accounts",
                user_ids=verified_users,
                criteria={"verified": True},
            ))

        return segments

    def find_influencers(self, users: List[Dict],
                          min_followers: int = 10000,
                          top_n: int = 20) -> List[Dict]:
        """从受众中找影响力大号"""
        influencers = []
        for user in users:
            metrics = user.get("public_metrics", {})
            fc = metrics.get("followers_count", 0)
            if fc >= min_followers:
                # 影响力得分 = 粉丝数 * 互动比
                tweets = max(metrics.get("tweet_count", 1), 1)
                listed = metrics.get("listed_count", 0)
                score = fc * 0.5 + listed * 100 + tweets * 0.01

                influencers.append({
                    "id": user.get("id"),
                    "username": user.get("username", ""),
                    "name": user.get("name", ""),
                    "followers": fc,
                    "following": metrics.get("following_count", 0),
                    "tweets": metrics.get("tweet_count", 0),
                    "listed": listed,
                    "verified": user.get("verified", False),
                    "description": user.get("description", "")[:100],
                    "influence_score": round(score, 1),
                })

        influencers.sort(key=lambda x: x["influence_score"], reverse=True)
        return influencers[:top_n]

    def overlap_analysis(self, username_a: str, username_b: str,
                          max_count: int = 200) -> Dict:
        """分析两个账号的粉丝重叠"""
        followers_a = self.fetch_followers(username_a, max_count)
        followers_b = self.fetch_followers(username_b, max_count)

        ids_a = {u.get("id") for u in followers_a}
        ids_b = {u.get("id") for u in followers_b}

        overlap = ids_a & ids_b
        unique_a = ids_a - ids_b
        unique_b = ids_b - ids_a

        total_union = len(ids_a | ids_b) or 1

        return {
            "username_a": username_a,
            "username_b": username_b,
            "followers_a": len(ids_a),
            "followers_b": len(ids_b),
            "overlap_count": len(overlap),
            "overlap_pct": round(len(overlap) / total_union * 100, 1),
            "unique_a": len(unique_a),
            "unique_b": len(unique_b),
            "jaccard_index": round(len(overlap) / total_union, 4),
        }

    def growth_forecast(self, username: str, days_ahead: int = 30) -> Optional[Dict]:
        """基于历史数据预测粉丝增长"""
        history = self.db.get_analytics_history(username, limit=30)
        if len(history) < 3:
            return None

        # 简单线性回归
        counts = [h["followers_count"] for h in reversed(history)]
        n = len(counts)
        x_mean = (n - 1) / 2
        y_mean = sum(counts) / n

        numerator = sum((i - x_mean) * (y - y_mean) for i, y in enumerate(counts))
        denominator = sum((i - x_mean) ** 2 for i in range(n))

        if denominator == 0:
            return None

        slope = numerator / denominator  # followers per data point
        intercept = y_mean - slope * x_mean

        current = counts[-1]
        forecast = current + slope * days_ahead

        return {
            "current_followers": current,
            "daily_growth": round(slope, 1),
            "forecast_days": days_ahead,
            "forecast_followers": max(0, round(forecast)),
            "growth_rate_pct": round(slope / max(current, 1) * 100, 3),
            "data_points": n,
            "confidence": "high" if n >= 14 else "medium" if n >= 7 else "low",
        }

    def format_profile(self, profile: AudienceProfile) -> str:
        """格式化受众画像"""
        lines = [
            f"👥 *受众画像* (分析 {profile.total_analyzed} 用户)\n",
            f"📊 平均粉丝: {profile.avg_followers:,.0f} | 中位: {profile.median_followers:,}",
            f"📝 平均推文: {profile.avg_tweets:,.0f}",
            f"✅ 认证比例: {profile.verified_pct}%\n",
        ]

        if profile.follower_tiers:
            lines.append("📈 *粉丝层级分布*")
            for tier, count in sorted(profile.follower_tiers.items()):
                pct = round(count / max(profile.total_analyzed, 1) * 100, 1)
                bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
                lines.append(f"  {tier}: {bar} {pct}% ({count})")
            lines.append("")

        if profile.top_interests:
            lines.append("🎯 *兴趣分布*")
            for interest, count in profile.top_interests[:8]:
                pct = round(count / max(profile.total_analyzed, 1) * 100, 1)
                lines.append(f"  {interest}: {pct}% ({count})")
            lines.append("")

        if profile.top_locations:
            lines.append("🌍 *Top位置*")
            for loc, count in profile.top_locations[:5]:
                lines.append(f"  {loc}: {count}")

        return "\n".join(lines)

    def clear_cache(self):
        """清除缓存"""
        self._cache.clear()
