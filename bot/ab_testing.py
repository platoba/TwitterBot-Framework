"""
AB Testing Engine for Twitter/X
推文AB测试: 对比不同版本的文案/标签/发布时间效果
"""

import json
import math
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any


class ABTest:
    """AB测试数据模型"""

    def __init__(
        self,
        name: str,
        test_type: str = "copy",  # copy|hashtag|timing
        status: str = "draft",
        test_id: str = None,
        variants: list = None,
        winner_metric: str = "engagement_rate",
        min_sample_size: int = 100,
        confidence_level: float = 0.95,
        auto_select: bool = True,
        created_at: str = None,
    ):
        self.test_id = test_id or str(uuid.uuid4())
        self.name = name
        self.test_type = test_type
        self.status = status
        self.variants = variants or []
        self.winner_metric = winner_metric
        self.min_sample_size = min_sample_size
        self.confidence_level = confidence_level
        self.auto_select = auto_select
        self.created_at = created_at or datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        return {
            "test_id": self.test_id,
            "name": self.name,
            "test_type": self.test_type,
            "status": self.status,
            "variants": self.variants,
            "winner_metric": self.winner_metric,
            "min_sample_size": self.min_sample_size,
            "confidence_level": self.confidence_level,
            "auto_select": self.auto_select,
            "created_at": self.created_at,
        }


class ABTestVariant:
    """AB测试变体"""

    def __init__(
        self,
        variant_id: str = None,
        name: str = "A",
        content: str = "",
        hashtags: list = None,
        schedule_time: str = None,
        impressions: int = 0,
        likes: int = 0,
        retweets: int = 0,
        replies: int = 0,
        clicks: int = 0,
        tweet_id: str = None,
    ):
        self.variant_id = variant_id or str(uuid.uuid4())
        self.name = name
        self.content = content
        self.hashtags = hashtags or []
        self.schedule_time = schedule_time
        self.impressions = impressions
        self.likes = likes
        self.retweets = retweets
        self.replies = replies
        self.clicks = clicks
        self.tweet_id = tweet_id

    @property
    def engagement_rate(self) -> float:
        if self.impressions == 0:
            return 0.0
        total = self.likes + self.retweets + self.replies + self.clicks
        return round(total / self.impressions * 100, 4)

    @property
    def total_engagements(self) -> int:
        return self.likes + self.retweets + self.replies + self.clicks

    def to_dict(self) -> dict:
        return {
            "variant_id": self.variant_id,
            "name": self.name,
            "content": self.content,
            "hashtags": self.hashtags,
            "schedule_time": self.schedule_time,
            "impressions": self.impressions,
            "likes": self.likes,
            "retweets": self.retweets,
            "replies": self.replies,
            "clicks": self.clicks,
            "tweet_id": self.tweet_id,
            "engagement_rate": self.engagement_rate,
            "total_engagements": self.total_engagements,
        }


class ABTestEngine:
    """AB测试引擎: 创建/追踪/统计/择优"""

    def __init__(self, db_path: str = "twitterbot.db"):
        self.db_path = db_path
        self._init_tables()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_tables(self):
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS ab_tests (
                test_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                test_type TEXT DEFAULT 'copy',
                status TEXT DEFAULT 'draft',
                winner_metric TEXT DEFAULT 'engagement_rate',
                min_sample_size INTEGER DEFAULT 100,
                confidence_level REAL DEFAULT 0.95,
                auto_select INTEGER DEFAULT 1,
                winner_variant_id TEXT,
                created_at TEXT,
                completed_at TEXT
            );
            CREATE TABLE IF NOT EXISTS ab_variants (
                variant_id TEXT PRIMARY KEY,
                test_id TEXT NOT NULL,
                name TEXT DEFAULT 'A',
                content TEXT DEFAULT '',
                hashtags TEXT DEFAULT '[]',
                schedule_time TEXT,
                impressions INTEGER DEFAULT 0,
                likes INTEGER DEFAULT 0,
                retweets INTEGER DEFAULT 0,
                replies INTEGER DEFAULT 0,
                clicks INTEGER DEFAULT 0,
                tweet_id TEXT,
                FOREIGN KEY (test_id) REFERENCES ab_tests(test_id)
            );
        """)
        conn.commit()
        conn.close()

    def create_test(self, name: str, test_type: str = "copy", **kwargs) -> ABTest:
        """创建AB测试"""
        test = ABTest(name=name, test_type=test_type, **kwargs)
        conn = self._get_conn()
        conn.execute(
            "INSERT INTO ab_tests (test_id, name, test_type, status, winner_metric, "
            "min_sample_size, confidence_level, auto_select, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (test.test_id, test.name, test.test_type, test.status,
             test.winner_metric, test.min_sample_size, test.confidence_level,
             1 if test.auto_select else 0, test.created_at),
        )
        conn.commit()
        conn.close()
        return test

    def add_variant(self, test_id: str, name: str, content: str = "",
                    hashtags: list = None, schedule_time: str = None) -> ABTestVariant:
        """添加测试变体"""
        variant = ABTestVariant(
            name=name, content=content,
            hashtags=hashtags or [], schedule_time=schedule_time,
        )
        conn = self._get_conn()
        conn.execute(
            "INSERT INTO ab_variants (variant_id, test_id, name, content, hashtags, schedule_time) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (variant.variant_id, test_id, variant.name, variant.content,
             json.dumps(variant.hashtags), variant.schedule_time),
        )
        conn.commit()
        conn.close()
        return variant

    def update_metrics(self, variant_id: str, impressions: int = 0,
                       likes: int = 0, retweets: int = 0,
                       replies: int = 0, clicks: int = 0):
        """更新变体指标"""
        conn = self._get_conn()
        conn.execute(
            "UPDATE ab_variants SET impressions=?, likes=?, retweets=?, "
            "replies=?, clicks=? WHERE variant_id=?",
            (impressions, likes, retweets, replies, clicks, variant_id),
        )
        conn.commit()
        conn.close()

    def get_test(self, test_id: str) -> Optional[dict]:
        """获取测试详情"""
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM ab_tests WHERE test_id=?", (test_id,)).fetchone()
        if not row:
            conn.close()
            return None
        test = dict(row)
        variants = conn.execute(
            "SELECT * FROM ab_variants WHERE test_id=?", (test_id,)
        ).fetchall()
        test["variants"] = [dict(v) for v in variants]
        conn.close()
        return test

    def list_tests(self, status: str = None, limit: int = 50) -> List[dict]:
        """列出测试"""
        conn = self._get_conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM ab_tests WHERE status=? ORDER BY created_at DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM ab_tests ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def start_test(self, test_id: str) -> bool:
        """启动测试"""
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE ab_tests SET status='running' WHERE test_id=? AND status='draft'",
            (test_id,),
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return updated

    def stop_test(self, test_id: str) -> bool:
        """停止测试"""
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE ab_tests SET status='completed', completed_at=? "
            "WHERE test_id=? AND status='running'",
            (datetime.now(timezone.utc).isoformat(), test_id),
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return updated

    def z_test(self, p1: float, n1: int, p2: float, n2: int) -> dict:
        """Z-test for two proportions (engagement rates)."""
        if n1 == 0 or n2 == 0:
            return {"z_score": 0, "p_value": 1.0, "significant": False}

        p_pool = (p1 * n1 + p2 * n2) / (n1 + n2)
        if p_pool == 0 or p_pool == 1:
            return {"z_score": 0, "p_value": 1.0, "significant": False}

        se = math.sqrt(p_pool * (1 - p_pool) * (1/n1 + 1/n2))
        if se == 0:
            return {"z_score": 0, "p_value": 1.0, "significant": False}

        z = (p1 - p2) / se
        # Approximate p-value using error function
        p_value = 2 * (1 - self._normal_cdf(abs(z)))

        return {
            "z_score": round(z, 4),
            "p_value": round(p_value, 6),
            "significant": p_value < 0.05,
        }

    def _normal_cdf(self, x: float) -> float:
        """Standard normal CDF approximation."""
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))

    def analyze_results(self, test_id: str) -> dict:
        """分析AB测试结果"""
        test = self.get_test(test_id)
        if not test or len(test.get("variants", [])) < 2:
            return {"error": "Test not found or insufficient variants"}

        variants = test["variants"]
        results = []

        for v in variants:
            impressions = v.get("impressions", 0)
            total_eng = (v.get("likes", 0) + v.get("retweets", 0) +
                        v.get("replies", 0) + v.get("clicks", 0))
            eng_rate = total_eng / impressions if impressions > 0 else 0.0

            results.append({
                "variant_id": v["variant_id"],
                "name": v["name"],
                "impressions": impressions,
                "engagements": total_eng,
                "engagement_rate": round(eng_rate, 6),
                "likes": v.get("likes", 0),
                "retweets": v.get("retweets", 0),
                "replies": v.get("replies", 0),
                "clicks": v.get("clicks", 0),
            })

        # Sort by engagement rate
        results.sort(key=lambda x: x["engagement_rate"], reverse=True)

        # Statistical test between top 2
        stat_test = None
        if len(results) >= 2:
            r1, r2 = results[0], results[1]
            stat_test = self.z_test(
                r1["engagement_rate"], r1["impressions"],
                r2["engagement_rate"], r2["impressions"],
            )

        winner = results[0] if results else None

        return {
            "test_id": test_id,
            "test_name": test["name"],
            "status": test["status"],
            "variants": results,
            "winner": winner,
            "statistical_test": stat_test,
        }

    def auto_pick_winner(self, test_id: str) -> Optional[str]:
        """自动选择赢家 (达到显著性后)"""
        analysis = self.analyze_results(test_id)
        if "error" in analysis:
            return None

        stat = analysis.get("statistical_test")
        if stat and stat.get("significant"):
            winner = analysis["winner"]
            if winner:
                conn = self._get_conn()
                conn.execute(
                    "UPDATE ab_tests SET winner_variant_id=?, status='completed', completed_at=? WHERE test_id=?",
                    (winner["variant_id"], datetime.now(timezone.utc).isoformat(), test_id),
                )
                conn.commit()
                conn.close()
                return winner["variant_id"]
        return None
