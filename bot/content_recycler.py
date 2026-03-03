"""
Content Recycler v1.0
内容回收再利用引擎 — 识别高表现旧推 + 智能改写策略 + 自动再调度

Features:
- PerformanceScanner: 扫描历史推文找出高表现内容
- RecycleStrategy: 8种改写策略 (引用/更新/线程展开/问答转换...)
- FreshnessChecker: 内容时效性检查 (避免过时信息)
- RecycleScheduler: 智能再发布调度 (避免重复+间隔控制)
- PerformanceTracker: 追踪原版 vs 回收版表现对比
"""

import json
import logging
import math
import re
import sqlite3
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ── 数据模型 ──


class RecycleStrategy(Enum):
    """改写策略"""
    QUOTE = "quote"                 # 引用原推 + 新评论
    UPDATE = "update"               # 更新数据/信息后重发
    THREAD_EXPAND = "thread_expand"  # 单推展开为线程
    QA_CONVERT = "qa_convert"       # 转为问答形式
    LISTICLE = "listicle"           # 转为列表形式
    VISUAL = "visual"               # 添加图表/图片
    REVERSE = "reverse"             # 反向观点引发讨论
    SUMMARY = "summary"             # 系列内容汇总


class ContentCategory(Enum):
    """内容类型"""
    INSIGHT = "insight"         # 观点/洞察
    DATA = "data"               # 数据/统计
    HOW_TO = "how_to"           # 教程/指南
    OPINION = "opinion"         # 观点/评论
    NEWS = "news"               # 新闻/时事
    PROMOTION = "promotion"     # 推广/广告
    ENGAGEMENT = "engagement"   # 互动/问答
    THREAD = "thread"           # 线程/系列
    MEME = "meme"               # 梗/幽默


@dataclass
class TweetRecord:
    """推文记录"""
    tweet_id: str
    text: str
    likes: int = 0
    retweets: int = 0
    replies: int = 0
    quotes: int = 0
    impressions: int = 0
    created_at: str = ""
    category: str = ""
    hashtags: List[str] = field(default_factory=list)
    urls: List[str] = field(default_factory=list)
    has_media: bool = False

    @property
    def total_engagement(self) -> int:
        return self.likes + self.retweets + self.replies + self.quotes

    @property
    def engagement_rate(self) -> float:
        if self.impressions > 0:
            return self.total_engagement / self.impressions
        return 0.0

    @property
    def virality_score(self) -> float:
        """病毒传播分 (转推+引用的权重更高)"""
        return (self.retweets * 2 + self.quotes * 3 + self.likes + self.replies * 1.5)

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class RecycleCandidate:
    """回收候选"""
    original: TweetRecord
    performance_score: float = 0.0
    freshness_score: float = 0.0
    recycle_score: float = 0.0
    suggested_strategies: List[str] = field(default_factory=list)
    suggested_time: str = ""
    times_recycled: int = 0
    last_recycled: str = ""

    def to_dict(self) -> Dict:
        d = asdict(self)
        d["original"] = self.original.to_dict()
        return d


# ── 表现扫描器 ──


class PerformanceScanner:
    """历史推文表现扫描器"""

    def __init__(self, min_engagement: int = 10, min_age_days: int = 14):
        self.min_engagement = min_engagement
        self.min_age_days = min_age_days

    def scan(self, tweets: List[TweetRecord], top_pct: float = 0.2) -> List[TweetRecord]:
        """扫描高表现推文"""
        # 过滤: 最低互动 + 最低年龄
        now = datetime.now(timezone.utc)
        eligible = []
        for t in tweets:
            if t.total_engagement < self.min_engagement:
                continue
            try:
                created = datetime.fromisoformat(t.created_at.replace("Z", "+00:00"))
                age = (now - created).days
                if age < self.min_age_days:
                    continue
            except (ValueError, TypeError):
                pass
            eligible.append(t)

        if not eligible:
            return []

        # 按互动排序，取top%
        eligible.sort(key=lambda t: t.total_engagement, reverse=True)
        top_n = max(1, int(len(eligible) * top_pct))
        return eligible[:top_n]

    def score_performance(self, tweet: TweetRecord, all_tweets: List[TweetRecord]) -> float:
        """相对表现评分 (0-1)"""
        if not all_tweets:
            return 0.5
        engagements = [t.total_engagement for t in all_tweets]
        max_eng = max(engagements) if engagements else 1
        if max_eng == 0:
            return 0.5
        return min(1.0, tweet.total_engagement / max_eng)

    def find_evergreen(self, tweets: List[TweetRecord]) -> List[TweetRecord]:
        """找常青内容 (不受时效影响的好内容)"""
        evergreen = []
        time_sensitive_patterns = [
            r"\b(today|tonight|this week|this month|right now)\b",
            r"\b(breaking|just in|happening now|live)\b",
            r"\b(sale|discount|limited time|expires|ends)\b",
            r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b",  # 日期
        ]
        for t in tweets:
            text_lower = t.text.lower()
            is_time_sensitive = any(
                re.search(p, text_lower) for p in time_sensitive_patterns
            )
            if not is_time_sensitive and t.total_engagement >= self.min_engagement:
                evergreen.append(t)
        return evergreen


# ── 时效性检查 ──


class FreshnessChecker:
    """内容时效性检查"""

    # 不同类型的保鲜期(天)
    SHELF_LIFE = {
        ContentCategory.INSIGHT: 180,
        ContentCategory.DATA: 90,
        ContentCategory.HOW_TO: 365,
        ContentCategory.OPINION: 120,
        ContentCategory.NEWS: 7,
        ContentCategory.PROMOTION: 30,
        ContentCategory.ENGAGEMENT: 60,
        ContentCategory.THREAD: 180,
        ContentCategory.MEME: 30,
    }

    def check(self, tweet: TweetRecord) -> float:
        """时效性评分 (0=过期, 1=新鲜)"""
        try:
            created = datetime.fromisoformat(tweet.created_at.replace("Z", "+00:00"))
            age_days = (datetime.now(timezone.utc) - created).days
        except (ValueError, TypeError):
            return 0.5

        category = ContentCategory.INSIGHT  # 默认
        try:
            category = ContentCategory(tweet.category)
        except (ValueError, KeyError):
            pass

        shelf_life = self.SHELF_LIFE.get(category, 120)
        if age_days <= shelf_life * 0.5:
            return 1.0
        elif age_days <= shelf_life:
            return 0.5 + 0.5 * (1 - (age_days - shelf_life * 0.5) / (shelf_life * 0.5))
        else:
            # 过期，但衰减到0.1
            overage = (age_days - shelf_life) / shelf_life
            return max(0.1, 0.5 * math.exp(-overage))

    def categorize(self, text: str) -> ContentCategory:
        """自动分类推文类型"""
        text_lower = text.lower()

        patterns = {
            ContentCategory.HOW_TO: [r"\bhow to\b", r"\bstep \d\b", r"\btutorial\b", r"\bguide\b", r"\btip[s]?\b"],
            ContentCategory.DATA: [r"\b\d+%\b", r"\bstatistic\b", r"\bdata\b", r"\bstudy\b", r"\bresearch\b"],
            ContentCategory.NEWS: [r"\bbreaking\b", r"\bannounce\b", r"\blaunch\b", r"\brelease\b", r"\bjust\b"],
            ContentCategory.ENGAGEMENT: [r"\?$", r"\bwhat do you\b", r"\bthoughts\?\b", r"\bpoll\b", r"\bvote\b"],
            ContentCategory.PROMOTION: [r"\bcheck out\b", r"\blink in bio\b", r"\bdiscount\b", r"\bfree\b"],
            ContentCategory.MEME: [r"\blmao\b", r"\b😂\b", r"\blol\b", r"\bbruh\b"],
            ContentCategory.THREAD: [r"\bthread\b", r"\b🧵\b", r"\b1/\d\b"],
        }

        for category, pats in patterns.items():
            for p in pats:
                if re.search(p, text_lower):
                    return category

        # 默认: 观点/洞察
        if len(text) > 200:
            return ContentCategory.INSIGHT
        return ContentCategory.OPINION


# ── 改写策略建议 ──


class StrategySuggester:
    """改写策略推荐器"""

    def suggest(self, tweet: TweetRecord, category: ContentCategory) -> List[RecycleStrategy]:
        """推荐适合的改写策略"""
        strategies = []

        # 通用策略
        strategies.append(RecycleStrategy.QUOTE)

        # 按类型推荐
        if category == ContentCategory.DATA:
            strategies.extend([RecycleStrategy.UPDATE, RecycleStrategy.VISUAL])
        elif category == ContentCategory.HOW_TO:
            strategies.extend([RecycleStrategy.THREAD_EXPAND, RecycleStrategy.LISTICLE])
        elif category == ContentCategory.INSIGHT:
            strategies.extend([RecycleStrategy.QA_CONVERT, RecycleStrategy.REVERSE])
        elif category == ContentCategory.OPINION:
            strategies.extend([RecycleStrategy.REVERSE, RecycleStrategy.QA_CONVERT])
        elif category == ContentCategory.ENGAGEMENT:
            strategies.append(RecycleStrategy.UPDATE)
        elif category == ContentCategory.THREAD:
            strategies.append(RecycleStrategy.SUMMARY)

        # 短推文 → 展开
        if len(tweet.text) < 100:
            strategies.append(RecycleStrategy.THREAD_EXPAND)

        # 高互动 → 引用
        if tweet.total_engagement > 100:
            if RecycleStrategy.QUOTE not in strategies:
                strategies.append(RecycleStrategy.QUOTE)

        # 去重
        seen = set()
        unique = []
        for s in strategies:
            if s not in seen:
                seen.add(s)
                unique.append(s)

        return unique[:4]  # 最多4个建议

    def generate_prompt(self, tweet: TweetRecord, strategy: RecycleStrategy) -> str:
        """生成改写提示词"""
        prompts = {
            RecycleStrategy.QUOTE: f'Quote this tweet with a fresh take:\n"{tweet.text}"',
            RecycleStrategy.UPDATE: f'Update this tweet with latest data/info:\n"{tweet.text}"',
            RecycleStrategy.THREAD_EXPAND: f'Expand this into a 5-tweet thread:\n"{tweet.text}"',
            RecycleStrategy.QA_CONVERT: f'Convert to Q&A format:\n"{tweet.text}"',
            RecycleStrategy.LISTICLE: f'Rewrite as a numbered list:\n"{tweet.text}"',
            RecycleStrategy.VISUAL: f'Suggest a chart/infographic for:\n"{tweet.text}"',
            RecycleStrategy.REVERSE: f'Write a contrarian take on:\n"{tweet.text}"',
            RecycleStrategy.SUMMARY: f'Summarize key points from:\n"{tweet.text}"',
        }
        return prompts.get(strategy, f'Rewrite this tweet:\n"{tweet.text}"')


# ── 回收调度器 ──


class RecycleScheduler:
    """回收内容调度管理"""

    def __init__(self, db_path: str = "recycle_schedule.db"):
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
            CREATE TABLE IF NOT EXISTS recycle_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_id TEXT NOT NULL,
                recycled_id TEXT,
                strategy TEXT NOT NULL,
                original_text TEXT NOT NULL,
                recycled_text TEXT DEFAULT '',
                original_engagement INTEGER DEFAULT 0,
                recycled_engagement INTEGER DEFAULT 0,
                scheduled_at TEXT,
                published_at TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS recycle_cooldown (
                original_id TEXT PRIMARY KEY,
                last_recycled TEXT NOT NULL,
                times_recycled INTEGER DEFAULT 1,
                min_interval_days INTEGER DEFAULT 30
            );
            CREATE INDEX IF NOT EXISTS idx_rh_original ON recycle_history(original_id);
            CREATE INDEX IF NOT EXISTS idx_rh_status ON recycle_history(status);
            CREATE INDEX IF NOT EXISTS idx_rc_time ON recycle_cooldown(last_recycled);
        """)
        conn.commit()

    def can_recycle(self, tweet_id: str, min_interval_days: int = 30) -> Tuple[bool, str]:
        """检查是否可以回收"""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT last_recycled, times_recycled FROM recycle_cooldown WHERE original_id=?",
            (tweet_id,),
        ).fetchone()
        if not row:
            return True, "never_recycled"

        try:
            last = datetime.fromisoformat(row["last_recycled"])
            days_since = (datetime.now(timezone.utc) - last).days
            if days_since < min_interval_days:
                return False, f"cooldown:{min_interval_days - days_since}d_remaining"
        except (ValueError, TypeError):
            pass

        if row["times_recycled"] >= 5:
            return False, "max_recycled_reached"

        return True, f"recycled_{row['times_recycled']}_times"

    def schedule(
        self,
        original_id: str,
        original_text: str,
        strategy: str,
        scheduled_at: Optional[str] = None,
        recycled_text: str = "",
        original_engagement: int = 0,
    ) -> int:
        """调度回收"""
        conn = self._get_conn()
        cursor = conn.execute(
            """INSERT INTO recycle_history
               (original_id, strategy, original_text, recycled_text, original_engagement, scheduled_at, status)
               VALUES (?,?,?,?,?,?,?)""",
            (original_id, strategy, original_text, recycled_text, original_engagement,
             scheduled_at or datetime.now(timezone.utc).isoformat(), "scheduled"),
        )
        conn.commit()
        return cursor.lastrowid

    def mark_published(self, schedule_id: int, recycled_id: str = "", recycled_text: str = ""):
        """标记已发布"""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE recycle_history SET recycled_id=?, recycled_text=?, published_at=?, status='published' WHERE id=?",
            (recycled_id, recycled_text, now, schedule_id),
        )
        # 更新cooldown
        row = conn.execute(
            "SELECT original_id FROM recycle_history WHERE id=?", (schedule_id,)
        ).fetchone()
        if row:
            conn.execute(
                """INSERT OR REPLACE INTO recycle_cooldown(original_id, last_recycled, times_recycled)
                   VALUES(?, ?, COALESCE(
                       (SELECT times_recycled + 1 FROM recycle_cooldown WHERE original_id=?), 1
                   ))""",
                (row["original_id"], now, row["original_id"]),
            )
        conn.commit()

    def update_recycled_engagement(self, schedule_id: int, engagement: int):
        """更新回收版互动数"""
        conn = self._get_conn()
        conn.execute(
            "UPDATE recycle_history SET recycled_engagement=? WHERE id=?",
            (engagement, schedule_id),
        )
        conn.commit()

    def get_pending(self, limit: int = 20) -> List[Dict]:
        """获取待发布"""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        rows = conn.execute(
            "SELECT * FROM recycle_history WHERE status='scheduled' AND scheduled_at<=? ORDER BY scheduled_at LIMIT ?",
            (now, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_history(self, original_id: Optional[str] = None, limit: int = 50) -> List[Dict]:
        """获取回收历史"""
        conn = self._get_conn()
        if original_id:
            rows = conn.execute(
                "SELECT * FROM recycle_history WHERE original_id=? ORDER BY created_at DESC LIMIT ?",
                (original_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM recycle_history ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def performance_comparison(self) -> Dict:
        """原版 vs 回收版表现对比"""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT original_engagement, recycled_engagement FROM recycle_history WHERE status='published' AND recycled_engagement > 0"
        ).fetchall()
        if not rows:
            return {"comparisons": 0, "avg_retention": 0.0, "better_count": 0, "worse_count": 0}

        retentions = []
        better = 0
        worse = 0
        for r in rows:
            orig = r["original_engagement"]
            recycled = r["recycled_engagement"]
            if orig > 0:
                retention = recycled / orig
                retentions.append(retention)
                if recycled >= orig:
                    better += 1
                else:
                    worse += 1

        avg_retention = sum(retentions) / len(retentions) if retentions else 0.0
        return {
            "comparisons": len(rows),
            "avg_retention": round(avg_retention, 3),
            "better_count": better,
            "worse_count": worse,
            "equal_or_better_pct": round(better / len(rows) * 100, 1) if rows else 0.0,
        }

    def stats(self) -> Dict:
        """统计"""
        conn = self._get_conn()
        total = conn.execute("SELECT COUNT(*) FROM recycle_history").fetchone()[0]
        by_status = {}
        for row in conn.execute("SELECT status, COUNT(*) as cnt FROM recycle_history GROUP BY status"):
            by_status[row["status"]] = row["cnt"]
        by_strategy = {}
        for row in conn.execute("SELECT strategy, COUNT(*) as cnt FROM recycle_history GROUP BY strategy"):
            by_strategy[row["strategy"]] = row["cnt"]
        return {
            "total_recycled": total,
            "by_status": by_status,
            "by_strategy": by_strategy,
            "performance": self.performance_comparison(),
        }


# ── 组合接口 ──


class ContentRecycler:
    """内容回收引擎 — 统一入口"""

    def __init__(
        self,
        db_dir: str = ".",
        min_engagement: int = 10,
        min_age_days: int = 14,
        min_recycle_interval: int = 30,
    ):
        self.scanner = PerformanceScanner(min_engagement, min_age_days)
        self.freshness = FreshnessChecker()
        self.suggester = StrategySuggester()
        self.scheduler = RecycleScheduler(f"{db_dir}/recycle_schedule.db")
        self.min_recycle_interval = min_recycle_interval

    def find_candidates(
        self,
        tweets: List[TweetRecord],
        top_pct: float = 0.2,
        include_evergreen: bool = True,
    ) -> List[RecycleCandidate]:
        """发现回收候选"""
        # 高表现推文
        top_tweets = self.scanner.scan(tweets, top_pct)

        # 常青内容
        if include_evergreen:
            evergreen = self.scanner.find_evergreen(tweets)
            # 合并去重
            seen_ids = {t.tweet_id for t in top_tweets}
            for e in evergreen:
                if e.tweet_id not in seen_ids:
                    top_tweets.append(e)
                    seen_ids.add(e.tweet_id)

        candidates = []
        for tweet in top_tweets:
            # 检查冷却期
            can_recycle, reason = self.scheduler.can_recycle(
                tweet.tweet_id, self.min_recycle_interval
            )
            if not can_recycle:
                continue

            # 自动分类
            if not tweet.category:
                tweet.category = self.freshness.categorize(tweet.text).value

            # 评分
            perf_score = self.scanner.score_performance(tweet, tweets)
            fresh_score = self.freshness.check(tweet)
            category = ContentCategory(tweet.category)
            strategies = self.suggester.suggest(tweet, category)

            # 综合回收价值 = 表现 * 0.5 + 新鲜度 * 0.3 + 策略多样性 * 0.2
            strategy_bonus = min(1.0, len(strategies) / 4)
            recycle_score = (perf_score * 0.5 + fresh_score * 0.3 + strategy_bonus * 0.2)

            candidate = RecycleCandidate(
                original=tweet,
                performance_score=round(perf_score, 3),
                freshness_score=round(fresh_score, 3),
                recycle_score=round(recycle_score, 3),
                suggested_strategies=[s.value for s in strategies],
            )
            candidates.append(candidate)

        # 按回收价值排序
        candidates.sort(key=lambda c: c.recycle_score, reverse=True)
        return candidates

    def schedule_recycle(
        self,
        candidate: RecycleCandidate,
        strategy: Optional[str] = None,
        scheduled_at: Optional[str] = None,
    ) -> int:
        """调度回收"""
        strat = strategy or (candidate.suggested_strategies[0] if candidate.suggested_strategies else "quote")
        return self.scheduler.schedule(
            original_id=candidate.original.tweet_id,
            original_text=candidate.original.text,
            strategy=strat,
            scheduled_at=scheduled_at,
            original_engagement=candidate.original.total_engagement,
        )

    def get_prompts(self, candidate: RecycleCandidate) -> List[Dict]:
        """获取所有改写提示"""
        prompts = []
        for strat_name in candidate.suggested_strategies:
            try:
                strategy = RecycleStrategy(strat_name)
                prompt = self.suggester.generate_prompt(candidate.original, strategy)
                prompts.append({"strategy": strat_name, "prompt": prompt})
            except ValueError:
                continue
        return prompts

    def export_candidates(self, candidates: List[RecycleCandidate], format: str = "text") -> str:
        """导出候选列表"""
        if format == "json":
            return json.dumps([c.to_dict() for c in candidates], indent=2, default=str)
        elif format == "text":
            lines = [f"♻️ Content Recycle Candidates ({len(candidates)})", "=" * 50]
            for i, c in enumerate(candidates, 1):
                lines.append(
                    f"#{i} [Score:{c.recycle_score:.2f}] "
                    f"Perf:{c.performance_score:.2f} Fresh:{c.freshness_score:.2f}"
                )
                text_preview = c.original.text[:80] + ("..." if len(c.original.text) > 80 else "")
                lines.append(f"   📝 {text_preview}")
                lines.append(f"   📊 Eng:{c.original.total_engagement} | Strategies: {', '.join(c.suggested_strategies)}")
                lines.append("")
            return "\n".join(lines)
        return ""
