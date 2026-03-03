"""
受众Lookalike发现引擎
分析相似账号粉丝, 找到高匹配度潜在受众
支持: 粉丝重叠分析 + 兴趣评分 + 相似度排名 + 自动外展推荐
"""

import json
import sqlite3
import threading
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Dict, List, Optional, Set, Any


class SimilarityMetric(Enum):
    """相似度计算方法"""
    JACCARD = "jaccard"
    COSINE = "cosine"
    OVERLAP = "overlap"
    DICE = "dice"


class AudienceSegment(Enum):
    """受众分段"""
    HIGH_VALUE = "high_value"        # 高互动+高相关
    WARM_LEAD = "warm_lead"          # 中等互动+高相关
    COLD_LEAD = "cold_lead"          # 低互动但相关
    COMPETITOR_FAN = "competitor_fan" # 竞品粉丝
    INDUSTRY_PEER = "industry_peer"  # 行业同行


@dataclass
class UserProfile:
    """用户画像"""
    user_id: str
    username: str
    display_name: str = ""
    bio: str = ""
    followers_count: int = 0
    following_count: int = 0
    tweet_count: int = 0
    verified: bool = False
    interests: List[str] = field(default_factory=list)
    engagement_rate: float = 0.0
    last_active: Optional[str] = None
    source_account: str = ""  # 从哪个种子账号发现的

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict) -> "UserProfile":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class LookalikeResult:
    """Lookalike分析结果"""
    user: UserProfile
    similarity_score: float      # 0-1
    overlap_sources: List[str]   # 哪些种子账号有重叠
    overlap_count: int           # 重叠种子数量
    segment: AudienceSegment
    recommended_action: str      # follow/engage/dm/skip
    confidence: float            # 0-1 置信度

    def to_dict(self) -> Dict:
        d = asdict(self)
        d["segment"] = self.segment.value
        return d


@dataclass
class OverlapAnalysis:
    """粉丝重叠分析"""
    account_a: str
    account_b: str
    followers_a: int
    followers_b: int
    overlap_count: int
    jaccard_index: float
    overlap_ratio_a: float  # overlap / followers_a
    overlap_ratio_b: float  # overlap / followers_b
    unique_to_a: int
    unique_to_b: int


class InterestExtractor:
    """兴趣标签提取器 (从bio和推文提取)"""

    INTEREST_KEYWORDS = {
        "tech": ["developer", "engineer", "coding", "programming", "software", "ai", "ml",
                 "data", "cloud", "devops", "frontend", "backend", "fullstack", "web3",
                 "blockchain", "crypto", "nft", "startup", "saas", "api"],
        "marketing": ["marketing", "seo", "growth", "brand", "content", "social media",
                      "digital", "advertising", "copywriter", "funnel", "conversion",
                      "influencer", "affiliate", "email marketing"],
        "ecommerce": ["ecommerce", "shopify", "amazon", "dropshipping", "dms", "seller",
                      "retail", "wholesale", "supply chain", "fba", "product"],
        "finance": ["finance", "investing", "trading", "stocks", "forex", "defi",
                    "fintech", "banking", "wealth", "portfolio", "hedge fund"],
        "design": ["designer", "ui", "ux", "figma", "creative", "illustration",
                   "graphic", "branding", "typography", "art director"],
        "creator": ["creator", "youtuber", "podcaster", "blogger", "writer",
                    "newsletter", "content creator", "streamer", "vlogger"],
        "business": ["founder", "ceo", "entrepreneur", "cto", "cmo", "vp",
                     "director", "consultant", "advisor", "angel investor", "vc"],
    }

    @classmethod
    def extract(cls, text: str) -> List[str]:
        """从文本提取兴趣标签"""
        if not text:
            return []
        text_lower = text.lower()
        interests = []
        for category, keywords in cls.INTEREST_KEYWORDS.items():
            for kw in keywords:
                if kw in text_lower:
                    interests.append(category)
                    break
        return list(set(interests))

    @classmethod
    def interest_similarity(cls, a: List[str], b: List[str]) -> float:
        """两个兴趣列表的相似度 (Jaccard)"""
        if not a and not b:
            return 0.0
        set_a, set_b = set(a), set(b)
        intersection = len(set_a & set_b)
        union = len(set_a | set_b)
        return intersection / union if union > 0 else 0.0


class SimilarityCalculator:
    """多种相似度计算"""

    @staticmethod
    def jaccard(set_a: Set, set_b: Set) -> float:
        if not set_a and not set_b:
            return 0.0
        intersection = len(set_a & set_b)
        union = len(set_a | set_b)
        return intersection / union if union > 0 else 0.0

    @staticmethod
    def cosine(vec_a: Dict[str, float], vec_b: Dict[str, float]) -> float:
        all_keys = set(vec_a.keys()) | set(vec_b.keys())
        if not all_keys:
            return 0.0
        dot = sum(vec_a.get(k, 0) * vec_b.get(k, 0) for k in all_keys)
        mag_a = sum(v ** 2 for v in vec_a.values()) ** 0.5
        mag_b = sum(v ** 2 for v in vec_b.values()) ** 0.5
        if mag_a == 0 or mag_b == 0:
            return 0.0
        return dot / (mag_a * mag_b)

    @staticmethod
    def overlap_coefficient(set_a: Set, set_b: Set) -> float:
        if not set_a or not set_b:
            return 0.0
        intersection = len(set_a & set_b)
        return intersection / min(len(set_a), len(set_b))

    @staticmethod
    def dice(set_a: Set, set_b: Set) -> float:
        if not set_a and not set_b:
            return 0.0
        intersection = len(set_a & set_b)
        return (2 * intersection) / (len(set_a) + len(set_b))

    @classmethod
    def calculate(cls, set_a: Set, set_b: Set,
                  metric: SimilarityMetric = SimilarityMetric.JACCARD) -> float:
        methods = {
            SimilarityMetric.JACCARD: cls.jaccard,
            SimilarityMetric.OVERLAP: cls.overlap_coefficient,
            SimilarityMetric.DICE: cls.dice,
        }
        fn = methods.get(metric, cls.jaccard)
        return fn(set_a, set_b)


class LookalikeStore:
    """Lookalike数据持久化"""

    def __init__(self, db_path: str = "lookalike.db"):
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
            CREATE TABLE IF NOT EXISTS seed_accounts (
                username TEXT PRIMARY KEY,
                display_name TEXT,
                followers_count INTEGER DEFAULT 0,
                added_at TEXT DEFAULT (datetime('now')),
                last_scanned TEXT
            );
            CREATE TABLE IF NOT EXISTS discovered_users (
                user_id TEXT PRIMARY KEY,
                username TEXT,
                display_name TEXT,
                bio TEXT,
                followers_count INTEGER DEFAULT 0,
                following_count INTEGER DEFAULT 0,
                tweet_count INTEGER DEFAULT 0,
                verified INTEGER DEFAULT 0,
                interests TEXT DEFAULT '[]',
                engagement_rate REAL DEFAULT 0,
                similarity_score REAL DEFAULT 0,
                segment TEXT,
                source_accounts TEXT DEFAULT '[]',
                discovered_at TEXT DEFAULT (datetime('now')),
                last_updated TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS overlap_cache (
                cache_key TEXT PRIMARY KEY,
                account_a TEXT,
                account_b TEXT,
                overlap_count INTEGER,
                jaccard_index REAL,
                computed_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_discovered_score
                ON discovered_users(similarity_score DESC);
            CREATE INDEX IF NOT EXISTS idx_discovered_segment
                ON discovered_users(segment);
        """)
        conn.commit()

    def save_user(self, user: UserProfile, score: float,
                  segment: AudienceSegment, sources: List[str]):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO discovered_users
            (user_id, username, display_name, bio, followers_count,
             following_count, tweet_count, verified, interests,
             engagement_rate, similarity_score, segment, source_accounts,
             last_updated)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'))
        """, (
            user.user_id, user.username, user.display_name, user.bio,
            user.followers_count, user.following_count, user.tweet_count,
            int(user.verified), json.dumps(user.interests),
            user.engagement_rate, score, segment.value,
            json.dumps(sources)
        ))
        conn.commit()

    def get_top_users(self, limit: int = 50,
                      segment: Optional[str] = None) -> List[Dict]:
        conn = self._get_conn()
        if segment:
            rows = conn.execute(
                "SELECT * FROM discovered_users WHERE segment=? "
                "ORDER BY similarity_score DESC LIMIT ?",
                (segment, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM discovered_users "
                "ORDER BY similarity_score DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def get_stats(self) -> Dict[str, Any]:
        conn = self._get_conn()
        total = conn.execute(
            "SELECT COUNT(*) FROM discovered_users"
        ).fetchone()[0]
        by_segment = {}
        for row in conn.execute(
            "SELECT segment, COUNT(*) as cnt FROM discovered_users GROUP BY segment"
        ).fetchall():
            by_segment[row["segment"]] = row["cnt"]
        avg_score = conn.execute(
            "SELECT AVG(similarity_score) FROM discovered_users"
        ).fetchone()[0] or 0
        return {
            "total_discovered": total,
            "by_segment": by_segment,
            "avg_similarity": round(avg_score, 4),
        }

    def save_overlap(self, analysis: OverlapAnalysis):
        conn = self._get_conn()
        key = f"{analysis.account_a}:{analysis.account_b}"
        conn.execute(
            "INSERT OR REPLACE INTO overlap_cache "
            "(cache_key, account_a, account_b, overlap_count, jaccard_index) "
            "VALUES (?,?,?,?,?)",
            (key, analysis.account_a, analysis.account_b,
             analysis.overlap_count, analysis.jaccard_index)
        )
        conn.commit()

    def add_seed(self, username: str, display_name: str = "",
                 followers_count: int = 0):
        conn = self._get_conn()
        conn.execute(
            "INSERT OR REPLACE INTO seed_accounts "
            "(username, display_name, followers_count) VALUES (?,?,?)",
            (username, display_name, followers_count)
        )
        conn.commit()

    def get_seeds(self) -> List[Dict]:
        conn = self._get_conn()
        return [dict(r) for r in
                conn.execute("SELECT * FROM seed_accounts").fetchall()]

    def close(self):
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None


class AudienceLookalike:
    """
    Lookalike受众发现引擎

    工作流:
    1. 添加种子账号 (竞品/行业KOL)
    2. 收集种子账号粉丝列表
    3. 分析粉丝重叠 → 找到活跃在多个种子账号的用户
    4. 兴趣+互动评分 → 相似度排名
    5. 分段+推荐行动
    """

    # 分段阈值
    SEGMENT_THRESHOLDS = {
        "high_value_score": 0.7,
        "warm_lead_score": 0.4,
        "min_engagement": 0.02,
        "min_followers": 100,
        "max_following_ratio": 10,  # following/followers > 10 = bot嫌疑
    }

    def __init__(self, store: Optional[LookalikeStore] = None):
        self.store = store or LookalikeStore()
        self._seed_followers: Dict[str, Set[str]] = {}  # username → {user_ids}
        self._user_profiles: Dict[str, UserProfile] = {}

    def add_seed_account(self, username: str, display_name: str = "",
                         followers_count: int = 0):
        """添加种子账号"""
        self.store.add_seed(username, display_name, followers_count)
        if username not in self._seed_followers:
            self._seed_followers[username] = set()

    def add_seed_followers(self, seed_username: str,
                           followers: List[UserProfile]):
        """添加种子账号的粉丝列表"""
        if seed_username not in self._seed_followers:
            self._seed_followers[seed_username] = set()
        for f in followers:
            self._seed_followers[seed_username].add(f.user_id)
            f.source_account = seed_username
            if not f.interests:
                f.interests = InterestExtractor.extract(f.bio)
            self._user_profiles[f.user_id] = f

    def analyze_overlap(self, account_a: str,
                        account_b: str) -> Optional[OverlapAnalysis]:
        """分析两个种子账号的粉丝重叠"""
        fa = self._seed_followers.get(account_a, set())
        fb = self._seed_followers.get(account_b, set())
        if not fa or not fb:
            return None
        overlap = fa & fb
        analysis = OverlapAnalysis(
            account_a=account_a,
            account_b=account_b,
            followers_a=len(fa),
            followers_b=len(fb),
            overlap_count=len(overlap),
            jaccard_index=SimilarityCalculator.jaccard(fa, fb),
            overlap_ratio_a=len(overlap) / len(fa) if fa else 0,
            overlap_ratio_b=len(overlap) / len(fb) if fb else 0,
            unique_to_a=len(fa - fb),
            unique_to_b=len(fb - fa),
        )
        self.store.save_overlap(analysis)
        return analysis

    def analyze_all_overlaps(self) -> List[OverlapAnalysis]:
        """分析所有种子账号间的粉丝重叠"""
        results = []
        seeds = list(self._seed_followers.keys())
        for i, a in enumerate(seeds):
            for b in seeds[i + 1:]:
                r = self.analyze_overlap(a, b)
                if r:
                    results.append(r)
        return results

    def _calculate_user_score(self, user: UserProfile,
                              source_seeds: List[str]) -> float:
        """计算用户的Lookalike相似度分数"""
        total_seeds = len(self._seed_followers)
        if total_seeds == 0:
            return 0.0

        # 1. 重叠因子: 在多少个种子账号中出现 (0-0.4)
        overlap_ratio = len(source_seeds) / total_seeds
        overlap_score = min(overlap_ratio * 2, 1.0) * 0.4

        # 2. 互动因子 (0-0.25)
        engagement_score = min(user.engagement_rate / 0.05, 1.0) * 0.25

        # 3. 账号质量因子 (0-0.2)
        quality_score = 0.0
        if user.followers_count >= 1000:
            quality_score += 0.1
        elif user.followers_count >= 100:
            quality_score += 0.05
        if user.verified:
            quality_score += 0.05
        if user.tweet_count >= 100:
            quality_score += 0.05

        # 4. 兴趣匹配因子 (0-0.15)
        all_interests = []
        for uid, p in self._user_profiles.items():
            all_interests.extend(p.interests)
        common_interests = Counter(all_interests).most_common(5)
        common_tags = {t for t, _ in common_interests}
        user_tags = set(user.interests)
        interest_score = 0.0
        if common_tags and user_tags:
            interest_score = len(common_tags & user_tags) / len(common_tags) * 0.15

        return min(overlap_score + engagement_score + quality_score + interest_score, 1.0)

    def _classify_segment(self, score: float, user: UserProfile,
                          source_count: int) -> AudienceSegment:
        """根据分数和特征分类受众"""
        th = self.SEGMENT_THRESHOLDS
        if score >= th["high_value_score"] and user.engagement_rate >= th["min_engagement"]:
            return AudienceSegment.HIGH_VALUE
        if score >= th["warm_lead_score"]:
            return AudienceSegment.WARM_LEAD
        if source_count >= 2:
            return AudienceSegment.COMPETITOR_FAN
        if user.followers_count >= 1000:
            return AudienceSegment.INDUSTRY_PEER
        return AudienceSegment.COLD_LEAD

    def _recommend_action(self, segment: AudienceSegment,
                          user: UserProfile) -> str:
        """根据分段推荐行动"""
        actions = {
            AudienceSegment.HIGH_VALUE: "engage_and_follow",
            AudienceSegment.WARM_LEAD: "follow_and_like",
            AudienceSegment.COMPETITOR_FAN: "engage_with_content",
            AudienceSegment.INDUSTRY_PEER: "follow",
            AudienceSegment.COLD_LEAD: "skip",
        }
        action = actions.get(segment, "skip")
        # 过滤bot嫌疑
        if (user.followers_count > 0 and
                user.following_count / user.followers_count >
                self.SEGMENT_THRESHOLDS["max_following_ratio"]):
            action = "skip"
        return action

    def _is_bot_suspect(self, user: UserProfile) -> bool:
        """检测可能的bot/spam账号"""
        if user.followers_count == 0:
            return True
        ratio = user.following_count / max(user.followers_count, 1)
        if ratio > self.SEGMENT_THRESHOLDS["max_following_ratio"]:
            return True
        if user.tweet_count == 0 and user.followers_count < 10:
            return True
        return False

    def discover_lookalikes(self, min_score: float = 0.1,
                            max_results: int = 100,
                            exclude_bots: bool = True) -> List[LookalikeResult]:
        """
        执行Lookalike发现

        Returns: 按相似度排序的结果列表
        """
        # 找到出现在多个种子粉丝列表中的用户
        user_sources: Dict[str, List[str]] = defaultdict(list)
        for seed, follower_ids in self._seed_followers.items():
            for uid in follower_ids:
                user_sources[uid].append(seed)

        results = []
        for uid, sources in user_sources.items():
            user = self._user_profiles.get(uid)
            if not user:
                continue
            if exclude_bots and self._is_bot_suspect(user):
                continue

            score = self._calculate_user_score(user, sources)
            if score < min_score:
                continue

            segment = self._classify_segment(score, user, len(sources))
            action = self._recommend_action(segment, user)
            confidence = min(len(sources) / max(len(self._seed_followers), 1), 1.0)

            result = LookalikeResult(
                user=user,
                similarity_score=round(score, 4),
                overlap_sources=sources,
                overlap_count=len(sources),
                segment=segment,
                recommended_action=action,
                confidence=round(confidence, 4),
            )
            results.append(result)
            self.store.save_user(user, score, segment, sources)

        results.sort(key=lambda r: r.similarity_score, reverse=True)
        return results[:max_results]

    def get_segment_summary(self) -> Dict[str, Any]:
        """获取各分段摘要统计"""
        return self.store.get_stats()

    def export_results(self, results: List[LookalikeResult],
                       format: str = "json") -> str:
        """导出结果为JSON/CSV"""
        if format == "csv":
            lines = ["user_id,username,score,segment,action,overlap_count,confidence"]
            for r in results:
                lines.append(
                    f"{r.user.user_id},{r.user.username},{r.similarity_score},"
                    f"{r.segment.value},{r.recommended_action},"
                    f"{r.overlap_count},{r.confidence}"
                )
            return "\n".join(lines)
        else:
            return json.dumps([r.to_dict() for r in results],
                              indent=2, ensure_ascii=False)
