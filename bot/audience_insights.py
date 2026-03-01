"""
Audience Insights Engine v1.0
受众智能分析 — 粉丝时区分布 + 互动热力图 + 兴趣聚类 + 相似受众发现 + 人口统计

Features:
- FollowerProfile: rich follower data model with demographics
- TimezoneAnalyzer: follower timezone distribution + optimal reach windows
- EngagementHeatmap: hour×day engagement heatmap + peak detection
- InterestCluster: bio/tweet topic clustering + interest graph
- LookalikeDiscovery: find similar audiences across accounts
- AudienceReport: comprehensive audience intelligence report
"""

import json
import math
import re
import sqlite3
import threading
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple, Set


class AudienceSegment(Enum):
    """受众分层"""
    SUPER_FAN = "super_fan"          # Top 1% engagement
    ACTIVE = "active"                 # Regular engagers
    CASUAL = "casual"                 # Occasional interaction
    LURKER = "lurker"                 # Follows but rarely engages
    INACTIVE = "inactive"             # No recent activity
    BOT_SUSPECT = "bot_suspect"       # Suspicious patterns


class InterestCategory(Enum):
    """兴趣分类"""
    TECH = "tech"
    BUSINESS = "business"
    MARKETING = "marketing"
    CRYPTO = "crypto"
    AI_ML = "ai_ml"
    DESIGN = "design"
    GAMING = "gaming"
    FINANCE = "finance"
    ECOMMERCE = "ecommerce"
    LIFESTYLE = "lifestyle"
    NEWS = "news"
    EDUCATION = "education"
    SPORTS = "sports"
    ENTERTAINMENT = "entertainment"
    OTHER = "other"


# Interest keyword mapping
INTEREST_KEYWORDS: Dict[InterestCategory, List[str]] = {
    InterestCategory.TECH: [
        "developer", "engineer", "coding", "programming", "software",
        "tech", "devops", "fullstack", "backend", "frontend", "api",
        "open source", "linux", "cloud", "saas", "startup",
    ],
    InterestCategory.BUSINESS: [
        "entrepreneur", "ceo", "founder", "business", "startup",
        "growth", "strategy", "management", "leadership", "consulting",
    ],
    InterestCategory.MARKETING: [
        "marketing", "seo", "content", "social media", "growth hacking",
        "copywriting", "brand", "advertising", "digital marketing", "influencer",
    ],
    InterestCategory.CRYPTO: [
        "crypto", "bitcoin", "ethereum", "blockchain", "web3",
        "defi", "nft", "token", "dao", "solana",
    ],
    InterestCategory.AI_ML: [
        "ai", "machine learning", "deep learning", "llm", "gpt",
        "neural", "data science", "nlp", "computer vision", "ml",
    ],
    InterestCategory.DESIGN: [
        "design", "ui", "ux", "figma", "creative",
        "graphic", "illustration", "typography", "product design",
    ],
    InterestCategory.GAMING: [
        "gaming", "gamer", "esports", "game dev", "streamer",
        "twitch", "playstation", "xbox", "nintendo",
    ],
    InterestCategory.FINANCE: [
        "finance", "investing", "trading", "stocks", "fintech",
        "wealth", "portfolio", "economics", "banking",
    ],
    InterestCategory.ECOMMERCE: [
        "ecommerce", "shopify", "amazon", "dropshipping", "seller",
        "retail", "store", "marketplace", "product",
    ],
    InterestCategory.LIFESTYLE: [
        "travel", "food", "fitness", "health", "wellness",
        "photography", "fashion", "beauty", "lifestyle",
    ],
    InterestCategory.NEWS: [
        "journalist", "reporter", "news", "media", "press",
        "editor", "breaking", "politics",
    ],
    InterestCategory.EDUCATION: [
        "teacher", "professor", "education", "learning", "student",
        "university", "research", "academic", "phd",
    ],
    InterestCategory.SPORTS: [
        "sports", "football", "basketball", "soccer", "athlete",
        "coach", "fitness", "running", "mma",
    ],
    InterestCategory.ENTERTAINMENT: [
        "music", "film", "movie", "actor", "singer",
        "comedy", "podcast", "youtube", "creator",
    ],
}


@dataclass
class FollowerProfile:
    """粉丝画像"""
    user_id: str
    username: str
    display_name: str = ""
    bio: str = ""
    location: str = ""
    followers_count: int = 0
    following_count: int = 0
    tweet_count: int = 0
    created_at: Optional[str] = None
    verified: bool = False
    profile_image_url: str = ""
    # Computed fields
    engagement_score: float = 0.0
    segment: AudienceSegment = AudienceSegment.CASUAL
    interests: List[InterestCategory] = field(default_factory=list)
    timezone_offset: Optional[int] = None  # UTC offset in hours
    language: str = "en"
    bot_probability: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["segment"] = self.segment.value
        d["interests"] = [i.value for i in self.interests]
        return d


@dataclass
class HeatmapCell:
    """热力图单元格"""
    day: int  # 0=Monday, 6=Sunday
    hour: int  # 0-23
    engagement_count: int = 0
    impression_count: int = 0
    engagement_rate: float = 0.0
    tweet_count: int = 0

    @property
    def intensity(self) -> float:
        """归一化强度 0-1"""
        if self.tweet_count == 0:
            return 0.0
        return min(1.0, self.engagement_rate * 10)


@dataclass
class InterestClusterResult:
    """兴趣聚类结果"""
    category: InterestCategory
    follower_count: int
    percentage: float
    top_keywords: List[str]
    sample_users: List[str]
    avg_engagement: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "category": self.category.value,
            "follower_count": self.follower_count,
            "percentage": round(self.percentage, 2),
            "top_keywords": self.top_keywords,
            "sample_users": self.sample_users[:5],
            "avg_engagement": round(self.avg_engagement, 4),
        }


@dataclass
class LookalikeResult:
    """相似受众结果"""
    source_account: str
    target_account: str
    overlap_count: int
    overlap_percentage: float
    shared_interests: List[InterestCategory]
    similarity_score: float  # 0-1
    recommended_action: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_account": self.source_account,
            "target_account": self.target_account,
            "overlap_count": self.overlap_count,
            "overlap_percentage": round(self.overlap_percentage, 2),
            "shared_interests": [i.value for i in self.shared_interests],
            "similarity_score": round(self.similarity_score, 4),
            "recommended_action": self.recommended_action,
        }


class AudienceInsightsDB:
    """受众洞察数据库"""

    def __init__(self, db_path: str = "audience_insights.db"):
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
            CREATE TABLE IF NOT EXISTS follower_profiles (
                user_id TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                display_name TEXT DEFAULT '',
                bio TEXT DEFAULT '',
                location TEXT DEFAULT '',
                followers_count INTEGER DEFAULT 0,
                following_count INTEGER DEFAULT 0,
                tweet_count INTEGER DEFAULT 0,
                created_at TEXT,
                verified INTEGER DEFAULT 0,
                engagement_score REAL DEFAULT 0.0,
                segment TEXT DEFAULT 'casual',
                interests TEXT DEFAULT '[]',
                timezone_offset INTEGER,
                language TEXT DEFAULT 'en',
                bot_probability REAL DEFAULT 0.0,
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS engagement_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                tweet_id TEXT,
                occurred_at TEXT DEFAULT (datetime('now')),
                metadata TEXT DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS audience_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account TEXT NOT NULL,
                total_followers INTEGER,
                segment_distribution TEXT DEFAULT '{}',
                interest_distribution TEXT DEFAULT '{}',
                timezone_distribution TEXT DEFAULT '{}',
                snapshot_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_follower_segment
                ON follower_profiles(segment);
            CREATE INDEX IF NOT EXISTS idx_engagement_user
                ON engagement_events(user_id);
            CREATE INDEX IF NOT EXISTS idx_engagement_time
                ON engagement_events(occurred_at);
        """)
        conn.commit()

    def save_profile(self, profile: FollowerProfile):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO follower_profiles
            (user_id, username, display_name, bio, location,
             followers_count, following_count, tweet_count,
             created_at, verified, engagement_score, segment,
             interests, timezone_offset, language, bot_probability, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            profile.user_id, profile.username, profile.display_name,
            profile.bio, profile.location, profile.followers_count,
            profile.following_count, profile.tweet_count,
            profile.created_at, int(profile.verified),
            profile.engagement_score, profile.segment.value,
            json.dumps([i.value for i in profile.interests]),
            profile.timezone_offset, profile.language, profile.bot_probability,
        ))
        conn.commit()

    def save_profiles_batch(self, profiles: List[FollowerProfile]):
        conn = self._get_conn()
        for p in profiles:
            conn.execute("""
                INSERT OR REPLACE INTO follower_profiles
                (user_id, username, display_name, bio, location,
                 followers_count, following_count, tweet_count,
                 created_at, verified, engagement_score, segment,
                 interests, timezone_offset, language, bot_probability, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """, (
                p.user_id, p.username, p.display_name,
                p.bio, p.location, p.followers_count,
                p.following_count, p.tweet_count,
                p.created_at, int(p.verified),
                p.engagement_score, p.segment.value,
                json.dumps([i.value for i in p.interests]),
                p.timezone_offset, p.language, p.bot_probability,
            ))
        conn.commit()

    def get_profile(self, user_id: str) -> Optional[FollowerProfile]:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM follower_profiles WHERE user_id = ?", (user_id,)
        ).fetchone()
        if not row:
            return None
        return self._row_to_profile(row)

    def get_profiles_by_segment(self, segment: AudienceSegment) -> List[FollowerProfile]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM follower_profiles WHERE segment = ?", (segment.value,)
        ).fetchall()
        return [self._row_to_profile(r) for r in rows]

    def get_all_profiles(self) -> List[FollowerProfile]:
        conn = self._get_conn()
        rows = conn.execute("SELECT * FROM follower_profiles").fetchall()
        return [self._row_to_profile(r) for r in rows]

    def count_profiles(self) -> int:
        conn = self._get_conn()
        return conn.execute("SELECT COUNT(*) FROM follower_profiles").fetchone()[0]

    def record_engagement(self, user_id: str, event_type: str,
                          tweet_id: str = "", metadata: Optional[Dict] = None):
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO engagement_events (user_id, event_type, tweet_id, metadata)
            VALUES (?, ?, ?, ?)
        """, (user_id, event_type, tweet_id, json.dumps(metadata or {})))
        conn.commit()

    def get_engagement_count(self, user_id: str, days: int = 30) -> int:
        conn = self._get_conn()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        row = conn.execute("""
            SELECT COUNT(*) FROM engagement_events
            WHERE user_id = ? AND occurred_at >= ?
        """, (user_id, cutoff)).fetchone()
        return row[0]

    def save_snapshot(self, account: str, total: int,
                      segments: Dict, interests: Dict, timezones: Dict):
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO audience_snapshots
            (account, total_followers, segment_distribution,
             interest_distribution, timezone_distribution)
            VALUES (?, ?, ?, ?, ?)
        """, (account, total, json.dumps(segments),
              json.dumps(interests), json.dumps(timezones)))
        conn.commit()

    def _row_to_profile(self, row) -> FollowerProfile:
        interests_raw = json.loads(row["interests"]) if row["interests"] else []
        interests = []
        for i in interests_raw:
            try:
                interests.append(InterestCategory(i))
            except ValueError:
                pass
        return FollowerProfile(
            user_id=row["user_id"],
            username=row["username"],
            display_name=row["display_name"] or "",
            bio=row["bio"] or "",
            location=row["location"] or "",
            followers_count=row["followers_count"],
            following_count=row["following_count"],
            tweet_count=row["tweet_count"],
            created_at=row["created_at"],
            verified=bool(row["verified"]),
            engagement_score=row["engagement_score"],
            segment=AudienceSegment(row["segment"]),
            interests=interests,
            timezone_offset=row["timezone_offset"],
            language=row["language"] or "en",
            bot_probability=row["bot_probability"],
        )

    def close(self):
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None


class TimezoneAnalyzer:
    """粉丝时区分布分析"""

    # Common location → UTC offset mapping
    LOCATION_TZ_MAP = {
        # Americas
        "new york": -5, "nyc": -5, "boston": -5, "miami": -5,
        "chicago": -6, "dallas": -6, "houston": -6, "austin": -6,
        "denver": -7, "phoenix": -7, "salt lake": -7,
        "los angeles": -8, "san francisco": -8, "seattle": -8, "la": -8, "sf": -8,
        "toronto": -5, "vancouver": -8, "montreal": -5,
        "brazil": -3, "são paulo": -3, "rio": -3,
        "mexico": -6, "bogota": -5, "buenos aires": -3, "lima": -5,
        # Europe
        "london": 0, "uk": 0, "dublin": 0,
        "paris": 1, "berlin": 1, "amsterdam": 1, "madrid": 1, "rome": 1,
        "brussels": 1, "zurich": 1, "vienna": 1, "stockholm": 1,
        "athens": 2, "helsinki": 2, "bucharest": 2, "istanbul": 3,
        "moscow": 3, "kyiv": 2,
        # Asia
        "dubai": 4, "mumbai": 5, "india": 5, "delhi": 5, "bangalore": 5,
        "bangkok": 7, "jakarta": 7, "singapore": 8, "kuala lumpur": 8,
        "beijing": 8, "shanghai": 8, "china": 8, "hong kong": 8,
        "taipei": 8, "tokyo": 9, "japan": 9, "seoul": 9, "korea": 9,
        # Oceania
        "sydney": 11, "melbourne": 11, "australia": 11, "auckland": 13,
        # Africa
        "lagos": 1, "nairobi": 3, "cairo": 2, "johannesburg": 2,
    }

    @classmethod
    def infer_timezone(cls, location: str) -> Optional[int]:
        """从位置推断UTC偏移"""
        if not location:
            return None
        loc_lower = location.lower().strip()
        for key, offset in cls.LOCATION_TZ_MAP.items():
            if key in loc_lower:
                return offset
        return None

    @classmethod
    def analyze_distribution(cls, profiles: List[FollowerProfile]) -> Dict[int, int]:
        """分析时区分布"""
        dist: Dict[int, int] = defaultdict(int)
        for p in profiles:
            tz = p.timezone_offset
            if tz is None:
                tz = cls.infer_timezone(p.location)
            if tz is not None:
                dist[tz] += 1
        return dict(sorted(dist.items()))

    @classmethod
    def optimal_posting_windows(cls, profiles: List[FollowerProfile],
                                 top_n: int = 3) -> List[Dict[str, Any]]:
        """计算最佳发帖时间窗口（覆盖最多受众）"""
        dist = cls.analyze_distribution(profiles)
        if not dist:
            return []

        total = sum(dist.values())
        windows = []

        # Evaluate each UTC hour for active users (9AM-9PM local)
        for utc_hour in range(24):
            reachable = 0
            for tz_offset, count in dist.items():
                local_hour = (utc_hour + tz_offset) % 24
                # Active hours: 9AM-9PM
                if 9 <= local_hour <= 21:
                    # Peak weight for 10AM-2PM and 7PM-9PM
                    if 10 <= local_hour <= 14 or 19 <= local_hour <= 21:
                        reachable += count * 1.5
                    else:
                        reachable += count
            windows.append({
                "utc_hour": utc_hour,
                "reachable_score": round(reachable, 1),
                "reach_percentage": round(reachable / total * 100, 1) if total > 0 else 0,
            })

        windows.sort(key=lambda w: w["reachable_score"], reverse=True)
        return windows[:top_n]

    @classmethod
    def coverage_report(cls, profiles: List[FollowerProfile]) -> Dict[str, Any]:
        """时区覆盖报告"""
        dist = cls.analyze_distribution(profiles)
        total = sum(dist.values())
        unknown = len(profiles) - total

        regions = {
            "americas": sum(v for k, v in dist.items() if -10 <= k <= -3),
            "europe_africa": sum(v for k, v in dist.items() if -1 <= k <= 3),
            "asia_pacific": sum(v for k, v in dist.items() if 4 <= k <= 13),
        }

        return {
            "total_known": total,
            "total_unknown": unknown,
            "distribution": dist,
            "regions": regions,
            "dominant_region": max(regions, key=regions.get) if regions else "unknown",
            "optimal_windows": cls.optimal_posting_windows(profiles),
        }


class EngagementHeatmap:
    """互动热力图"""

    def __init__(self):
        self._cells: Dict[Tuple[int, int], HeatmapCell] = {}
        # Initialize 7×24 grid
        for day in range(7):
            for hour in range(24):
                self._cells[(day, hour)] = HeatmapCell(day=day, hour=hour)

    def record(self, day: int, hour: int,
               engagements: int = 1, impressions: int = 0):
        """记录互动事件"""
        key = (day % 7, hour % 24)
        cell = self._cells[key]
        cell.engagement_count += engagements
        cell.impression_count += impressions
        cell.tweet_count += 1
        if cell.impression_count > 0:
            cell.engagement_rate = cell.engagement_count / cell.impression_count
        elif cell.tweet_count > 0:
            cell.engagement_rate = cell.engagement_count / cell.tweet_count

    def record_from_datetime(self, dt: datetime,
                              engagements: int = 1, impressions: int = 0):
        """从datetime记录"""
        self.record(dt.weekday(), dt.hour, engagements, impressions)

    def get_cell(self, day: int, hour: int) -> HeatmapCell:
        return self._cells[(day % 7, hour % 24)]

    def get_peak_times(self, top_n: int = 5) -> List[HeatmapCell]:
        """获取峰值时段"""
        cells = sorted(
            self._cells.values(),
            key=lambda c: c.engagement_count,
            reverse=True,
        )
        return [c for c in cells[:top_n] if c.engagement_count > 0]

    def get_dead_zones(self) -> List[HeatmapCell]:
        """获取低活跃时段"""
        avg = self._average_engagement()
        if avg == 0:
            return []
        return [c for c in self._cells.values()
                if c.engagement_count < avg * 0.2 and c.tweet_count > 0]

    def get_day_summary(self) -> Dict[str, float]:
        """每日互动汇总"""
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday",
                     "Friday", "Saturday", "Sunday"]
        summary = {}
        for day in range(7):
            total = sum(
                self._cells[(day, h)].engagement_count for h in range(24)
            )
            summary[day_names[day]] = total
        return summary

    def get_hour_summary(self) -> Dict[int, float]:
        """每小时互动汇总"""
        summary = {}
        for hour in range(24):
            total = sum(
                self._cells[(d, hour)].engagement_count for d in range(7)
            )
            summary[hour] = total
        return summary

    def to_matrix(self) -> List[List[int]]:
        """转换为7×24矩阵"""
        matrix = []
        for day in range(7):
            row = [self._cells[(day, h)].engagement_count for h in range(24)]
            matrix.append(row)
        return matrix

    def _average_engagement(self) -> float:
        active = [c.engagement_count for c in self._cells.values()
                  if c.engagement_count > 0]
        return sum(active) / len(active) if active else 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "matrix": self.to_matrix(),
            "peak_times": [
                {"day": c.day, "hour": c.hour, "count": c.engagement_count}
                for c in self.get_peak_times()
            ],
            "day_summary": self.get_day_summary(),
            "hour_summary": self.get_hour_summary(),
        }


class InterestAnalyzer:
    """兴趣分析器"""

    @classmethod
    def classify_interests(cls, bio: str, tweets: Optional[List[str]] = None
                           ) -> List[InterestCategory]:
        """从bio和推文中识别兴趣"""
        text = bio.lower()
        if tweets:
            text += " " + " ".join(t.lower() for t in tweets[:10])

        matched: Dict[InterestCategory, int] = defaultdict(int)
        for category, keywords in INTEREST_KEYWORDS.items():
            for kw in keywords:
                if kw in text:
                    matched[category] += 1

        # Return categories with at least 1 keyword match, sorted by count
        result = sorted(matched.items(), key=lambda x: x[1], reverse=True)
        return [cat for cat, count in result if count >= 1][:5]

    @classmethod
    def cluster_audience(cls, profiles: List[FollowerProfile]
                         ) -> List[InterestClusterResult]:
        """对受众进行兴趣聚类"""
        clusters: Dict[InterestCategory, List[FollowerProfile]] = defaultdict(list)

        for p in profiles:
            interests = p.interests or cls.classify_interests(p.bio)
            if not interests:
                clusters[InterestCategory.OTHER].append(p)
            else:
                for interest in interests:
                    clusters[interest].append(p)

        total = len(profiles) if profiles else 1
        results = []

        for category, members in sorted(clusters.items(),
                                          key=lambda x: len(x[1]),
                                          reverse=True):
            # Find top keywords for this cluster
            all_bios = " ".join(m.bio.lower() for m in members)
            keywords_for_cat = INTEREST_KEYWORDS.get(category, [])
            top_kw = sorted(
                keywords_for_cat,
                key=lambda kw: all_bios.count(kw),
                reverse=True,
            )[:5]

            avg_eng = (
                sum(m.engagement_score for m in members) / len(members)
                if members else 0
            )

            results.append(InterestClusterResult(
                category=category,
                follower_count=len(members),
                percentage=len(members) / total * 100,
                top_keywords=top_kw,
                sample_users=[m.username for m in members[:5]],
                avg_engagement=avg_eng,
            ))

        return results

    @classmethod
    def interest_overlap(cls, profiles_a: List[FollowerProfile],
                         profiles_b: List[FollowerProfile]
                         ) -> Dict[str, Any]:
        """两组受众的兴趣重叠分析"""
        clusters_a = cls.cluster_audience(profiles_a)
        clusters_b = cls.cluster_audience(profiles_b)

        cats_a = {c.category for c in clusters_a if c.percentage > 5}
        cats_b = {c.category for c in clusters_b if c.percentage > 5}

        shared = cats_a & cats_b
        unique_a = cats_a - cats_b
        unique_b = cats_b - cats_a

        return {
            "shared_interests": [c.value for c in shared],
            "unique_to_a": [c.value for c in unique_a],
            "unique_to_b": [c.value for c in unique_b],
            "overlap_ratio": len(shared) / len(cats_a | cats_b) if (cats_a | cats_b) else 0,
        }


class BotDetector:
    """Bot检测器"""

    @classmethod
    def calculate_bot_probability(cls, profile: FollowerProfile) -> float:
        """计算Bot概率 (0-1)"""
        score = 0.0
        factors = 0

        # Factor 1: Following/Followers ratio
        if profile.followers_count > 0:
            ratio = profile.following_count / profile.followers_count
            if ratio > 10:
                score += 0.3
            elif ratio > 5:
                score += 0.15
            factors += 1

        # Factor 2: Empty or generic bio
        if not profile.bio or len(profile.bio) < 10:
            score += 0.15
            factors += 1
        else:
            factors += 1

        # Factor 3: Default profile image (no custom avatar)
        if not profile.profile_image_url or "default" in profile.profile_image_url:
            score += 0.2
            factors += 1
        else:
            factors += 1

        # Factor 4: Account age vs tweet count
        if profile.created_at:
            try:
                created = datetime.fromisoformat(profile.created_at.replace("Z", "+00:00"))
                age_days = (datetime.now(timezone.utc) - created).days
                if age_days > 0:
                    tweets_per_day = profile.tweet_count / age_days
                    if tweets_per_day > 50:
                        score += 0.25
                    elif tweets_per_day < 0.01 and age_days > 365:
                        score += 0.15
            except (ValueError, TypeError):
                pass
            factors += 1

        # Factor 5: Username pattern (random-looking)
        if re.match(r'^[a-zA-Z]+\d{5,}$', profile.username):
            score += 0.2
            factors += 1
        else:
            factors += 1

        # Factor 6: Very high following count with low engagement
        if profile.following_count > 5000 and profile.engagement_score < 0.01:
            score += 0.15
            factors += 1
        else:
            factors += 1

        return min(1.0, score)

    @classmethod
    def detect_bots(cls, profiles: List[FollowerProfile],
                    threshold: float = 0.5) -> List[FollowerProfile]:
        """批量检测Bot"""
        bots = []
        for p in profiles:
            prob = cls.calculate_bot_probability(p)
            p.bot_probability = prob
            if prob >= threshold:
                p.segment = AudienceSegment.BOT_SUSPECT
                bots.append(p)
        return bots


class SegmentClassifier:
    """受众分层分类器"""

    @classmethod
    def classify(cls, profile: FollowerProfile,
                 engagement_count_30d: int = 0,
                 avg_engagement: float = 0.0) -> AudienceSegment:
        """根据互动频率分层"""
        # Bot check first
        if profile.bot_probability >= 0.5:
            return AudienceSegment.BOT_SUSPECT

        # Engagement-based classification
        if engagement_count_30d >= 20 or profile.engagement_score >= avg_engagement * 3:
            return AudienceSegment.SUPER_FAN
        elif engagement_count_30d >= 5 or profile.engagement_score >= avg_engagement * 1.5:
            return AudienceSegment.ACTIVE
        elif engagement_count_30d >= 1:
            return AudienceSegment.CASUAL
        elif profile.tweet_count > 0:
            return AudienceSegment.LURKER
        else:
            return AudienceSegment.INACTIVE

    @classmethod
    def classify_batch(cls, profiles: List[FollowerProfile],
                       engagement_counts: Optional[Dict[str, int]] = None
                       ) -> Dict[AudienceSegment, List[FollowerProfile]]:
        """批量分层"""
        if not profiles:
            return {}

        avg_eng = sum(p.engagement_score for p in profiles) / len(profiles)
        counts = engagement_counts or {}
        result: Dict[AudienceSegment, List[FollowerProfile]] = defaultdict(list)

        for p in profiles:
            eng_count = counts.get(p.user_id, 0)
            segment = cls.classify(p, eng_count, avg_eng)
            p.segment = segment
            result[segment].append(p)

        return dict(result)


class LookalikeEngine:
    """相似受众发现"""

    @classmethod
    def calculate_similarity(cls,
                              profiles_a: List[FollowerProfile],
                              profiles_b: List[FollowerProfile],
                              account_a: str = "account_a",
                              account_b: str = "account_b"
                              ) -> LookalikeResult:
        """计算两个账号受众相似度"""
        ids_a = {p.user_id for p in profiles_a}
        ids_b = {p.user_id for p in profiles_b}
        overlap = ids_a & ids_b

        total = len(ids_a | ids_b) if (ids_a | ids_b) else 1
        overlap_pct = len(overlap) / len(ids_a) * 100 if ids_a else 0

        # Interest overlap
        interest_data = InterestAnalyzer.interest_overlap(profiles_a, profiles_b)
        shared = [InterestCategory(i) for i in interest_data["shared_interests"]]

        # Jaccard similarity
        jaccard = len(overlap) / total

        # Determine action
        if jaccard > 0.3:
            action = "High overlap — collaborate or differentiate content"
        elif jaccard > 0.1:
            action = "Moderate overlap — cross-promote for growth"
        elif len(shared) >= 2:
            action = "Low user overlap but shared interests — good acquisition target"
        else:
            action = "Low similarity — different audience segments"

        return LookalikeResult(
            source_account=account_a,
            target_account=account_b,
            overlap_count=len(overlap),
            overlap_percentage=overlap_pct,
            shared_interests=shared,
            similarity_score=jaccard,
            recommended_action=action,
        )

    @classmethod
    def find_lookalikes(cls,
                        source_profiles: List[FollowerProfile],
                        candidate_groups: Dict[str, List[FollowerProfile]],
                        source_name: str = "source",
                        min_similarity: float = 0.05
                        ) -> List[LookalikeResult]:
        """从候选账号中找相似受众"""
        results = []
        for name, profiles in candidate_groups.items():
            result = cls.calculate_similarity(
                source_profiles, profiles, source_name, name
            )
            if result.similarity_score >= min_similarity:
                results.append(result)

        results.sort(key=lambda r: r.similarity_score, reverse=True)
        return results


class AudienceReportGenerator:
    """受众洞察报告生成器"""

    @classmethod
    def generate(cls, profiles: List[FollowerProfile],
                 account_name: str = "account",
                 heatmap: Optional[EngagementHeatmap] = None
                 ) -> Dict[str, Any]:
        """生成完整受众洞察报告"""
        if not profiles:
            return {"error": "No profiles to analyze", "account": account_name}

        # Segment distribution
        segments = SegmentClassifier.classify_batch(profiles)
        segment_dist = {
            seg.value: len(members)
            for seg, members in segments.items()
        }

        # Interest clusters
        clusters = InterestAnalyzer.cluster_audience(profiles)

        # Timezone coverage
        tz_report = TimezoneAnalyzer.coverage_report(profiles)

        # Bot detection
        bots = BotDetector.detect_bots(profiles)

        # Key metrics
        avg_followers = sum(p.followers_count for p in profiles) / len(profiles)
        avg_following = sum(p.following_count for p in profiles) / len(profiles)
        verified_count = sum(1 for p in profiles if p.verified)

        report = {
            "account": account_name,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_followers_analyzed": len(profiles),
            "summary": {
                "avg_follower_count": round(avg_followers),
                "avg_following_count": round(avg_following),
                "verified_followers": verified_count,
                "verified_percentage": round(verified_count / len(profiles) * 100, 2),
                "bot_suspects": len(bots),
                "bot_percentage": round(len(bots) / len(profiles) * 100, 2),
            },
            "segments": segment_dist,
            "top_interests": [c.to_dict() for c in clusters[:10]],
            "timezone": tz_report,
        }

        if heatmap:
            report["engagement_heatmap"] = heatmap.to_dict()

        return report

    @classmethod
    def generate_text(cls, report: Dict[str, Any]) -> str:
        """生成文本格式报告"""
        lines = []
        lines.append(f"📊 Audience Insights Report — @{report.get('account', 'unknown')}")
        lines.append(f"Generated: {report.get('generated_at', 'N/A')}")
        lines.append(f"Followers Analyzed: {report.get('total_followers_analyzed', 0)}")
        lines.append("")

        summary = report.get("summary", {})
        lines.append("📈 Summary:")
        lines.append(f"  Avg Followers: {summary.get('avg_follower_count', 0):,}")
        lines.append(f"  Avg Following: {summary.get('avg_following_count', 0):,}")
        lines.append(f"  Verified: {summary.get('verified_followers', 0)} ({summary.get('verified_percentage', 0)}%)")
        lines.append(f"  Bot Suspects: {summary.get('bot_suspects', 0)} ({summary.get('bot_percentage', 0)}%)")
        lines.append("")

        segments = report.get("segments", {})
        if segments:
            lines.append("👥 Segments:")
            for seg, count in sorted(segments.items(), key=lambda x: x[1], reverse=True):
                lines.append(f"  {seg}: {count}")
            lines.append("")

        interests = report.get("top_interests", [])
        if interests:
            lines.append("🎯 Top Interests:")
            for i in interests[:5]:
                lines.append(f"  {i['category']}: {i['follower_count']} ({i['percentage']}%)")
            lines.append("")

        tz = report.get("timezone", {})
        regions = tz.get("regions", {})
        if regions:
            lines.append("🌍 Geographic Distribution:")
            for region, count in sorted(regions.items(), key=lambda x: x[1], reverse=True):
                lines.append(f"  {region}: {count}")

        windows = tz.get("optimal_windows", [])
        if windows:
            lines.append("")
            lines.append("⏰ Best Posting Times (UTC):")
            for w in windows:
                lines.append(f"  {w['utc_hour']:02d}:00 — reach score {w['reachable_score']}")

        return "\n".join(lines)

    @classmethod
    def export_json(cls, report: Dict[str, Any], filepath: str):
        """导出JSON报告"""
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

    @classmethod
    def export_csv(cls, profiles: List[FollowerProfile], filepath: str):
        """导出粉丝CSV"""
        import csv
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "user_id", "username", "display_name", "bio", "location",
                "followers", "following", "tweets", "verified",
                "segment", "interests", "engagement_score", "bot_probability",
            ])
            for p in profiles:
                writer.writerow([
                    p.user_id, p.username, p.display_name, p.bio, p.location,
                    p.followers_count, p.following_count, p.tweet_count,
                    p.verified, p.segment.value,
                    ",".join(i.value for i in p.interests),
                    round(p.engagement_score, 4), round(p.bot_probability, 4),
                ])
