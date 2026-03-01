"""
Competitor Analysis Engine v1.0
竞品账号追踪 + 对比分析 + 内容策略洞察 + 增长基准
"""

import json
import logging
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from bot.twitter_api import TwitterAPI
from bot.database import Database

logger = logging.getLogger(__name__)


@dataclass
class CompetitorProfile:
    """竞品画像"""
    username: str
    user_id: str = ""
    name: str = ""
    description: str = ""
    followers: int = 0
    following: int = 0
    tweet_count: int = 0
    listed: int = 0
    verified: bool = False
    created_at: str = ""
    avg_likes: float = 0
    avg_retweets: float = 0
    avg_replies: float = 0
    engagement_rate: float = 0
    posting_frequency: float = 0  # tweets per day
    top_hashtags: List[Tuple[str, int]] = field(default_factory=list)
    content_types: Dict[str, int] = field(default_factory=dict)
    active_hours: List[int] = field(default_factory=list)
    analyzed_at: str = ""

    def __post_init__(self):
        if not self.analyzed_at:
            self.analyzed_at = datetime.now(timezone.utc).isoformat()

    @property
    def followers_following_ratio(self) -> float:
        if self.following == 0:
            return 0
        return round(self.followers / self.following, 2)

    def to_dict(self) -> Dict:
        return {
            "username": self.username,
            "user_id": self.user_id,
            "name": self.name,
            "description": self.description[:200],
            "followers": self.followers,
            "following": self.following,
            "tweet_count": self.tweet_count,
            "listed": self.listed,
            "verified": self.verified,
            "ff_ratio": self.followers_following_ratio,
            "avg_likes": round(self.avg_likes, 1),
            "avg_retweets": round(self.avg_retweets, 1),
            "avg_replies": round(self.avg_replies, 1),
            "engagement_rate": round(self.engagement_rate, 3),
            "posting_frequency": round(self.posting_frequency, 2),
            "top_hashtags": self.top_hashtags[:10],
            "content_types": self.content_types,
            "active_hours": self.active_hours[:5],
            "analyzed_at": self.analyzed_at,
        }


@dataclass
class CompetitorComparison:
    """竞品对比结果"""
    my_username: str
    competitors: List[CompetitorProfile]
    my_profile: Optional[CompetitorProfile] = None
    benchmarks: Dict = field(default_factory=dict)
    insights: List[str] = field(default_factory=list)
    generated_at: str = ""

    def __post_init__(self):
        if not self.generated_at:
            self.generated_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> Dict:
        return {
            "my_username": self.my_username,
            "my_profile": self.my_profile.to_dict() if self.my_profile else None,
            "competitors": [c.to_dict() for c in self.competitors],
            "benchmarks": self.benchmarks,
            "insights": self.insights,
            "generated_at": self.generated_at,
        }


class CompetitorAnalyzer:
    """竞品分析引擎"""

    def __init__(self, api: TwitterAPI, db: Database):
        self.api = api
        self.db = db
        self._competitor_cache: Dict[str, CompetitorProfile] = {}

    def _ensure_table(self):
        """确保competitor表存在"""
        conn = self.db._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS competitors (
                username TEXT PRIMARY KEY,
                user_id TEXT,
                profile_data TEXT,
                tracked_since TEXT DEFAULT (datetime('now')),
                last_analyzed TEXT DEFAULT (datetime('now')),
                is_active INTEGER DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS competitor_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                followers INTEGER,
                following INTEGER,
                tweets INTEGER,
                engagement_rate REAL,
                avg_likes REAL,
                snapshot_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (username) REFERENCES competitors(username)
            );
            CREATE INDEX IF NOT EXISTS idx_comp_snap_user
                ON competitor_snapshots(username, snapshot_at);
        """)
        conn.commit()

    def add_competitor(self, username: str) -> Optional[CompetitorProfile]:
        """添加竞品账号"""
        self._ensure_table()
        profile = self.analyze_account(username)
        if not profile:
            return None

        conn = self.db._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO competitors (username, user_id, profile_data, last_analyzed)
            VALUES (?, ?, ?, datetime('now'))
        """, (username, profile.user_id, json.dumps(profile.to_dict())))
        conn.commit()

        self._save_snapshot(profile)
        self._competitor_cache[username] = profile
        return profile

    def remove_competitor(self, username: str) -> bool:
        """移除竞品"""
        self._ensure_table()
        conn = self.db._get_conn()
        cursor = conn.execute(
            "UPDATE competitors SET is_active = 0 WHERE username = ?",
            (username,)
        )
        conn.commit()
        self._competitor_cache.pop(username, None)
        return cursor.rowcount > 0

    def list_competitors(self) -> List[Dict]:
        """列出所有追踪中的竞品"""
        self._ensure_table()
        conn = self.db._get_conn()
        rows = conn.execute(
            "SELECT username, user_id, profile_data, tracked_since, last_analyzed "
            "FROM competitors WHERE is_active = 1"
        ).fetchall()
        results = []
        for r in rows:
            try:
                data = json.loads(r["profile_data"])
                data["tracked_since"] = r["tracked_since"]
                data["last_analyzed"] = r["last_analyzed"]
                results.append(data)
            except (json.JSONDecodeError, TypeError):
                results.append({"username": r["username"]})
        return results

    def analyze_account(self, username: str,
                         tweet_limit: int = 50) -> Optional[CompetitorProfile]:
        """分析单个账号"""
        user_data = self.api.get_user(username)
        if not user_data or "data" not in user_data:
            logger.warning(f"Cannot fetch user: {username}")
            return None

        user = user_data["data"]
        metrics = user.get("public_metrics", {})

        profile = CompetitorProfile(
            username=username,
            user_id=user.get("id", ""),
            name=user.get("name", ""),
            description=user.get("description", ""),
            followers=metrics.get("followers_count", 0),
            following=metrics.get("following_count", 0),
            tweet_count=metrics.get("tweet_count", 0),
            listed=metrics.get("listed_count", 0),
            verified=user.get("verified", False),
            created_at=user.get("created_at", ""),
        )

        # 分析最近推文
        tweets_data = self.api.get_user_tweets(
            profile.user_id, max_results=min(tweet_limit, 100)
        )
        if tweets_data and "data" in tweets_data:
            tweets = tweets_data["data"]
            self._analyze_tweets(profile, tweets)

        self._competitor_cache[username] = profile
        return profile

    def _analyze_tweets(self, profile: CompetitorProfile,
                         tweets: List[Dict]):
        """从推文中提取分析指标"""
        if not tweets:
            return

        likes = []
        retweets = []
        replies = []
        hashtags = Counter()
        content_types = Counter()
        hour_counts = Counter()
        dates = []

        for tweet in tweets:
            m = tweet.get("public_metrics", {})
            likes.append(m.get("like_count", 0))
            retweets.append(m.get("retweet_count", 0))
            replies.append(m.get("reply_count", 0))

            text = tweet.get("text", "")

            # 提取hashtag
            import re
            tags = re.findall(r'#(\w+)', text)
            hashtags.update(tags)

            # 内容类型分类
            if text.startswith("RT @"):
                content_types["retweet"] += 1
            elif text.startswith("@"):
                content_types["reply"] += 1
            elif "http" in text:
                content_types["link_share"] += 1
            elif "🧵" in text or "thread" in text.lower():
                content_types["thread"] += 1
            elif "?" in text:
                content_types["question"] += 1
            else:
                content_types["original"] += 1

            # 发帖时间
            created = tweet.get("created_at", "")
            if created:
                try:
                    dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                    hour_counts[dt.hour] += 1
                    dates.append(dt)
                except (ValueError, TypeError):
                    pass

        profile.avg_likes = statistics.mean(likes) if likes else 0
        profile.avg_retweets = statistics.mean(retweets) if retweets else 0
        profile.avg_replies = statistics.mean(replies) if replies else 0

        # 互动率
        total_engagement = sum(likes) + sum(retweets) + sum(replies)
        total_impressions = profile.followers * len(tweets) if profile.followers else 1
        profile.engagement_rate = total_engagement / total_impressions * 100

        profile.top_hashtags = hashtags.most_common(15)
        profile.content_types = dict(content_types)

        # 最活跃时段
        if hour_counts:
            profile.active_hours = [h for h, _ in hour_counts.most_common(5)]

        # 发帖频率(推文/天)
        if len(dates) >= 2:
            dates.sort()
            span_days = max((dates[-1] - dates[0]).days, 1)
            profile.posting_frequency = len(dates) / span_days

    def _save_snapshot(self, profile: CompetitorProfile):
        """保存竞品快照"""
        conn = self.db._get_conn()
        conn.execute("""
            INSERT INTO competitor_snapshots
            (username, followers, following, tweets, engagement_rate, avg_likes)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            profile.username, profile.followers, profile.following,
            profile.tweet_count, profile.engagement_rate, profile.avg_likes
        ))
        conn.commit()

    def get_growth_history(self, username: str,
                            limit: int = 30) -> List[Dict]:
        """获取竞品增长历史"""
        self._ensure_table()
        conn = self.db._get_conn()
        rows = conn.execute("""
            SELECT * FROM competitor_snapshots
            WHERE username = ? ORDER BY snapshot_at DESC LIMIT ?
        """, (username, limit)).fetchall()
        return [dict(r) for r in rows]

    def compare(self, my_username: str,
                competitor_usernames: List[str] = None) -> CompetitorComparison:
        """全面对比分析"""
        if competitor_usernames is None:
            tracked = self.list_competitors()
            competitor_usernames = [c["username"] for c in tracked]

        my_profile = self.analyze_account(my_username)
        competitors = []
        for username in competitor_usernames:
            cached = self._competitor_cache.get(username)
            if cached:
                competitors.append(cached)
            else:
                profile = self.analyze_account(username)
                if profile:
                    competitors.append(profile)

        comparison = CompetitorComparison(
            my_username=my_username,
            my_profile=my_profile,
            competitors=competitors,
        )

        if competitors:
            comparison.benchmarks = self._compute_benchmarks(
                my_profile, competitors
            )
            comparison.insights = self._generate_insights(
                my_profile, competitors, comparison.benchmarks
            )

        return comparison

    def _compute_benchmarks(self, my_profile: Optional[CompetitorProfile],
                             competitors: List[CompetitorProfile]) -> Dict:
        """计算行业基准"""
        if not competitors:
            return {}

        all_profiles = competitors[:]
        if my_profile:
            all_profiles.append(my_profile)

        followers_list = [p.followers for p in competitors]
        engagement_list = [p.engagement_rate for p in competitors]
        frequency_list = [p.posting_frequency for p in competitors if p.posting_frequency > 0]
        likes_list = [p.avg_likes for p in competitors]

        benchmarks = {
            "followers": {
                "min": min(followers_list) if followers_list else 0,
                "max": max(followers_list) if followers_list else 0,
                "avg": round(statistics.mean(followers_list), 0) if followers_list else 0,
                "median": statistics.median(followers_list) if followers_list else 0,
            },
            "engagement_rate": {
                "min": round(min(engagement_list), 3) if engagement_list else 0,
                "max": round(max(engagement_list), 3) if engagement_list else 0,
                "avg": round(statistics.mean(engagement_list), 3) if engagement_list else 0,
                "median": round(statistics.median(engagement_list), 3) if engagement_list else 0,
            },
            "posting_frequency": {
                "avg": round(statistics.mean(frequency_list), 2) if frequency_list else 0,
                "median": round(statistics.median(frequency_list), 2) if frequency_list else 0,
            },
            "avg_likes": {
                "avg": round(statistics.mean(likes_list), 1) if likes_list else 0,
                "median": round(statistics.median(likes_list), 1) if likes_list else 0,
            },
        }

        if my_profile:
            benchmarks["my_rank"] = {
                "followers": self._rank(my_profile.followers, followers_list),
                "engagement": self._rank(my_profile.engagement_rate, engagement_list),
                "avg_likes": self._rank(my_profile.avg_likes, likes_list),
            }

        return benchmarks

    def _rank(self, value: float, values: List[float]) -> Dict:
        """计算排名"""
        all_values = sorted(values + [value], reverse=True)
        rank = all_values.index(value) + 1
        total = len(all_values)
        return {
            "rank": rank,
            "total": total,
            "percentile": round((1 - (rank - 1) / max(total - 1, 1)) * 100, 1),
        }

    def _generate_insights(self, my_profile: Optional[CompetitorProfile],
                            competitors: List[CompetitorProfile],
                            benchmarks: Dict) -> List[str]:
        """生成竞品洞察"""
        insights = []

        if not my_profile or not competitors:
            return insights

        avg_followers = benchmarks.get("followers", {}).get("avg", 0)
        avg_engagement = benchmarks.get("engagement_rate", {}).get("avg", 0)
        avg_frequency = benchmarks.get("posting_frequency", {}).get("avg", 0)

        # 粉丝对比
        if my_profile.followers < avg_followers * 0.5:
            insights.append(
                f"📉 粉丝数({my_profile.followers:,})低于竞品均值({avg_followers:,.0f})，"
                "建议加大内容投入和互动策略"
            )
        elif my_profile.followers > avg_followers * 1.5:
            insights.append(
                f"📈 粉丝数({my_profile.followers:,})领先竞品，保持优势"
            )

        # 互动率
        if my_profile.engagement_rate < avg_engagement * 0.7:
            insights.append(
                f"⚠️ 互动率({my_profile.engagement_rate:.3f}%)低于行业基准({avg_engagement:.3f}%)，"
                "建议优化内容质量，增加提问和互动"
            )
        elif my_profile.engagement_rate > avg_engagement * 1.3:
            insights.append(
                f"🔥 互动率({my_profile.engagement_rate:.3f}%)高于竞品，内容策略有效"
            )

        # 发帖频率
        if avg_frequency > 0 and my_profile.posting_frequency < avg_frequency * 0.5:
            insights.append(
                f"⏰ 发帖频率({my_profile.posting_frequency:.1f}条/天)低于竞品({avg_frequency:.1f}条/天)，"
                "建议增加发帖频次"
            )

        # 标签策略
        all_tags = Counter()
        for c in competitors:
            for tag, count in c.top_hashtags:
                all_tags[tag] += count

        my_tags = set(t for t, _ in my_profile.top_hashtags)
        popular_missing = [
            tag for tag, _ in all_tags.most_common(10)
            if tag not in my_tags
        ]
        if popular_missing:
            insights.append(
                f"🏷️ 竞品常用但你未使用的标签: #{' #'.join(popular_missing[:5])}"
            )

        # 内容类型差异
        competitor_types = Counter()
        for c in competitors:
            for ctype, count in c.content_types.items():
                competitor_types[ctype] += count

        top_types = [t for t, _ in competitor_types.most_common(3)]
        my_types = set(my_profile.content_types.keys())
        missing_types = [t for t in top_types if t not in my_types]
        if missing_types:
            insights.append(
                f"📝 竞品常用内容类型你还未尝试: {', '.join(missing_types)}"
            )

        return insights

    def content_gap_analysis(self, my_username: str,
                              competitor_usernames: List[str]) -> Dict:
        """内容差距分析"""
        my_profile = self._competitor_cache.get(my_username)
        if not my_profile:
            my_profile = self.analyze_account(my_username)

        competitor_tags = Counter()
        competitor_types = Counter()
        competitor_hours = Counter()

        for username in competitor_usernames:
            profile = self._competitor_cache.get(username)
            if not profile:
                profile = self.analyze_account(username)
            if profile:
                for tag, count in profile.top_hashtags:
                    competitor_tags[tag] += count
                for ctype, count in profile.content_types.items():
                    competitor_types[ctype] += count
                for hour in profile.active_hours:
                    competitor_hours[hour] += 1

        my_tags = {t for t, _ in (my_profile.top_hashtags if my_profile else [])}
        my_types = set((my_profile.content_types if my_profile else {}).keys())

        return {
            "missing_hashtags": [
                {"tag": tag, "competitor_usage": count}
                for tag, count in competitor_tags.most_common(20)
                if tag not in my_tags
            ][:10],
            "missing_content_types": [
                t for t in competitor_types
                if t not in my_types
            ],
            "recommended_hours": [
                h for h, _ in competitor_hours.most_common(5)
            ],
            "competitor_count": len(competitor_usernames),
        }

    def format_comparison(self, comparison: CompetitorComparison) -> str:
        """格式化对比报告"""
        lines = [
            f"📊 *竞品分析报告*",
            f"🕐 {comparison.generated_at[:16]}\n",
        ]

        if comparison.my_profile:
            mp = comparison.my_profile
            lines.append(f"👤 *我的账号* @{mp.username}")
            lines.append(f"  粉丝: {mp.followers:,} | 互动率: {mp.engagement_rate:.3f}%")
            lines.append(f"  日均发帖: {mp.posting_frequency:.1f} | 均赞: {mp.avg_likes:.0f}\n")

        lines.append(f"🏢 *竞品 ({len(comparison.competitors)})*")
        for c in sorted(comparison.competitors,
                        key=lambda x: x.followers, reverse=True):
            lines.append(
                f"  @{c.username}: {c.followers:,}粉 | "
                f"互动{c.engagement_rate:.3f}% | "
                f"均赞{c.avg_likes:.0f}"
            )

        if comparison.benchmarks:
            bm = comparison.benchmarks
            lines.append(f"\n📏 *行业基准*")
            lines.append(f"  粉丝均值: {bm['followers']['avg']:,.0f}")
            lines.append(f"  互动率均值: {bm['engagement_rate']['avg']:.3f}%")
            lines.append(f"  发帖频率均值: {bm['posting_frequency']['avg']:.1f}条/天")

            if "my_rank" in bm:
                rank = bm["my_rank"]
                lines.append(f"\n🏆 *我的排名*")
                lines.append(f"  粉丝: #{rank['followers']['rank']}/{rank['followers']['total']}")
                lines.append(f"  互动: #{rank['engagement']['rank']}/{rank['engagement']['total']}")

        if comparison.insights:
            lines.append(f"\n💡 *洞察*")
            for insight in comparison.insights:
                lines.append(f"  {insight}")

        return "\n".join(lines)
