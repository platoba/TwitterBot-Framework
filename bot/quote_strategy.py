"""
Quote Tweet Strategy Engine v1.0
引用推文策略: 自动发现/智能引用/效果追踪/策略优化

Features:
- Target tweet discovery (by keyword, author, engagement threshold)
- Quote tweet template engine (agree, disagree, add-value, humor, question)
- Performance tracking (quote vs original engagement ratio)
- Strategy optimization based on historical performance
- Anti-spam safeguards (rate limiting, cool-down periods)
- Author relationship scoring
- SQLite persistence
"""

import json
import sqlite3
import uuid
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple


class QuoteStyle:
    """引用推文风格"""
    AGREE = "agree"           # 赞同+补充
    DISAGREE = "disagree"     # 反驳(礼貌)
    ADD_VALUE = "add_value"   # 补充信息
    HUMOR = "humor"           # 幽默评论
    QUESTION = "question"     # 提出问题
    DATA = "data"             # 数据佐证
    THREAD_HOOK = "thread_hook"  # 引出自己的线程
    HOT_TAKE = "hot_take"     # 犀利观点

    ALL = [AGREE, DISAGREE, ADD_VALUE, HUMOR, QUESTION, DATA, THREAD_HOOK, HOT_TAKE]


class QuoteTweet:
    """引用推文数据模型"""

    def __init__(
        self,
        original_tweet_id: str,
        original_author: str,
        original_content: str = "",
        quote_content: str = "",
        quote_tweet_id: str = None,
        quote_id: str = None,
        style: str = "add_value",
        status: str = "draft",
        original_likes: int = 0,
        original_retweets: int = 0,
        quote_likes: int = 0,
        quote_retweets: int = 0,
        quote_replies: int = 0,
        quote_impressions: int = 0,
        created_at: str = None,
        posted_at: str = None,
    ):
        self.quote_id = quote_id or str(uuid.uuid4())[:12]
        self.original_tweet_id = original_tweet_id
        self.original_author = original_author
        self.original_content = original_content
        self.quote_content = quote_content
        self.quote_tweet_id = quote_tweet_id
        self.style = style
        self.status = status
        self.original_likes = original_likes
        self.original_retweets = original_retweets
        self.quote_likes = quote_likes
        self.quote_retweets = quote_retweets
        self.quote_replies = quote_replies
        self.quote_impressions = quote_impressions
        self.created_at = created_at or datetime.now(timezone.utc).isoformat()
        self.posted_at = posted_at

    @property
    def amplification_ratio(self) -> float:
        """引用互动 vs 原推互动比"""
        original_eng = self.original_likes + self.original_retweets
        quote_eng = self.quote_likes + self.quote_retweets + self.quote_replies
        if original_eng == 0:
            return float(quote_eng) if quote_eng > 0 else 0.0
        return round(quote_eng / original_eng, 4)

    @property
    def engagement_rate(self) -> float:
        """引用推文互动率"""
        if self.quote_impressions == 0:
            return 0.0
        total = self.quote_likes + self.quote_retweets + self.quote_replies
        return round(total / self.quote_impressions * 100, 4)

    def to_dict(self) -> dict:
        return {
            "quote_id": self.quote_id,
            "original_tweet_id": self.original_tweet_id,
            "original_author": self.original_author,
            "original_content": self.original_content,
            "quote_content": self.quote_content,
            "quote_tweet_id": self.quote_tweet_id,
            "style": self.style,
            "status": self.status,
            "original_likes": self.original_likes,
            "original_retweets": self.original_retweets,
            "quote_likes": self.quote_likes,
            "quote_retweets": self.quote_retweets,
            "quote_replies": self.quote_replies,
            "quote_impressions": self.quote_impressions,
            "amplification_ratio": self.amplification_ratio,
            "engagement_rate": self.engagement_rate,
            "created_at": self.created_at,
            "posted_at": self.posted_at,
        }


class QuoteStrategyEngine:
    """引用推文策略引擎"""

    # 模板 (占位符: {author}, {topic}, {opinion})
    TEMPLATES = {
        QuoteStyle.AGREE: [
            "This ☝️ {opinion}",
            "Absolutely. {opinion}",
            "Couldn't agree more. {opinion}",
            "100% this. Let me add: {opinion}",
        ],
        QuoteStyle.DISAGREE: [
            "Respectfully, I see it differently. {opinion}",
            "Interesting take, but {opinion}",
            "I'd push back on this: {opinion}",
        ],
        QuoteStyle.ADD_VALUE: [
            "Adding context: {opinion}",
            "To build on this: {opinion}",
            "Related data point: {opinion}",
            "Worth noting: {opinion}",
        ],
        QuoteStyle.HUMOR: [
            "Meanwhile: {opinion}",
            "The real question is: {opinion}",
        ],
        QuoteStyle.QUESTION: [
            "Genuine question: {opinion}",
            "But have you considered {opinion}",
            "What about {opinion}?",
        ],
        QuoteStyle.DATA: [
            "The data tells a different story: {opinion}",
            "Here are the numbers: {opinion}",
            "For reference: {opinion}",
        ],
        QuoteStyle.THREAD_HOOK: [
            "This reminds me of something I've been thinking about. Thread 🧵: {opinion}",
            "Great point. Here's a deeper dive: {opinion}",
        ],
        QuoteStyle.HOT_TAKE: [
            "Hot take: {opinion}",
            "Unpopular opinion: {opinion}",
            "Nobody asked but: {opinion}",
        ],
    }

    def __init__(self, db_path: str = "twitterbot.db",
                 cooldown_minutes: int = 30,
                 max_quotes_per_hour: int = 5):
        self.db_path = db_path
        self.cooldown_minutes = cooldown_minutes
        self.max_quotes_per_hour = max_quotes_per_hour
        self._init_tables()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_tables(self):
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS quote_tweets (
                quote_id TEXT PRIMARY KEY,
                original_tweet_id TEXT NOT NULL,
                original_author TEXT NOT NULL,
                original_content TEXT DEFAULT '',
                quote_content TEXT DEFAULT '',
                quote_tweet_id TEXT,
                style TEXT DEFAULT 'add_value',
                status TEXT DEFAULT 'draft',
                original_likes INTEGER DEFAULT 0,
                original_retweets INTEGER DEFAULT 0,
                quote_likes INTEGER DEFAULT 0,
                quote_retweets INTEGER DEFAULT 0,
                quote_replies INTEGER DEFAULT 0,
                quote_impressions INTEGER DEFAULT 0,
                created_at TEXT,
                posted_at TEXT
            );
            CREATE TABLE IF NOT EXISTS author_scores (
                username TEXT PRIMARY KEY,
                quote_count INTEGER DEFAULT 0,
                avg_amplification REAL DEFAULT 0,
                total_engagement INTEGER DEFAULT 0,
                last_quoted_at TEXT,
                relationship TEXT DEFAULT 'neutral',
                notes TEXT DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS quote_keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT NOT NULL UNIQUE,
                enabled INTEGER DEFAULT 1,
                min_likes INTEGER DEFAULT 10,
                min_retweets INTEGER DEFAULT 5,
                preferred_style TEXT DEFAULT 'add_value',
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)
        conn.commit()
        conn.close()

    # ── Quote CRUD ───────────────────────────────────────

    def create_quote(self, original_tweet_id: str, original_author: str,
                     quote_content: str, style: str = "add_value",
                     original_content: str = "",
                     original_likes: int = 0,
                     original_retweets: int = 0) -> QuoteTweet:
        """创建引用推文"""
        if style not in QuoteStyle.ALL:
            style = "add_value"

        qt = QuoteTweet(
            original_tweet_id=original_tweet_id,
            original_author=original_author,
            original_content=original_content,
            quote_content=quote_content,
            style=style,
            original_likes=original_likes,
            original_retweets=original_retweets,
        )

        conn = self._get_conn()
        conn.execute(
            """INSERT INTO quote_tweets (quote_id, original_tweet_id, original_author,
               original_content, quote_content, quote_tweet_id, style, status,
               original_likes, original_retweets, quote_likes, quote_retweets,
               quote_replies, quote_impressions, created_at, posted_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (qt.quote_id, qt.original_tweet_id, qt.original_author,
             qt.original_content, qt.quote_content, qt.quote_tweet_id,
             qt.style, qt.status, qt.original_likes, qt.original_retweets,
             qt.quote_likes, qt.quote_retweets, qt.quote_replies,
             qt.quote_impressions, qt.created_at, qt.posted_at),
        )
        conn.commit()
        conn.close()
        return qt

    def get_quote(self, quote_id: str) -> Optional[QuoteTweet]:
        """获取引用推文"""
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM quote_tweets WHERE quote_id=?", (quote_id,)).fetchone()
        conn.close()
        if not row:
            return None
        return QuoteTweet(**{k: row[k] for k in row.keys()})

    def list_quotes(self, status: str = None, style: str = None,
                    author: str = None, limit: int = 50) -> List[dict]:
        """列出引用推文"""
        conn = self._get_conn()
        query = "SELECT * FROM quote_tweets WHERE 1=1"
        params = []
        if status:
            query += " AND status=?"
            params.append(status)
        if style:
            query += " AND style=?"
            params.append(style)
        if author:
            query += " AND original_author=?"
            params.append(author)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def mark_posted(self, quote_id: str, quote_tweet_id: str = None) -> bool:
        """标记已发布"""
        now = datetime.now(timezone.utc).isoformat()
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE quote_tweets SET status='posted', posted_at=?, quote_tweet_id=? "
            "WHERE quote_id=? AND status='draft'",
            (now, quote_tweet_id, quote_id),
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()

        if updated:
            qt = self.get_quote(quote_id)
            if qt:
                self._update_author_score(qt.original_author)

        return updated

    def update_quote_metrics(self, quote_id: str, likes: int = 0,
                             retweets: int = 0, replies: int = 0,
                             impressions: int = 0) -> bool:
        """更新引用推文指标"""
        conn = self._get_conn()
        cursor = conn.execute(
            """UPDATE quote_tweets SET quote_likes=?, quote_retweets=?,
               quote_replies=?, quote_impressions=?
               WHERE quote_id=?""",
            (likes, retweets, replies, impressions, quote_id),
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        return updated

    def delete_quote(self, quote_id: str) -> bool:
        """删除引用推文"""
        conn = self._get_conn()
        cursor = conn.execute("DELETE FROM quote_tweets WHERE quote_id=?", (quote_id,))
        conn.commit()
        deleted = cursor.rowcount > 0
        conn.close()
        return deleted

    # ── Rate Limiting ────────────────────────────────────

    def can_quote(self, author: str = None) -> Tuple[bool, str]:
        """检查是否可以引用(防刷)"""
        conn = self._get_conn()

        # 1. 每小时配额检查
        one_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        count = conn.execute(
            "SELECT COUNT(*) as c FROM quote_tweets WHERE posted_at >= ?",
            (one_hour_ago,),
        ).fetchone()["c"]

        if count >= self.max_quotes_per_hour:
            conn.close()
            return False, f"Rate limit: {count}/{self.max_quotes_per_hour} quotes this hour"

        # 2. 同一作者冷却检查
        if author:
            cooldown_ago = (datetime.now(timezone.utc) - timedelta(minutes=self.cooldown_minutes)).isoformat()
            author_recent = conn.execute(
                "SELECT COUNT(*) as c FROM quote_tweets "
                "WHERE original_author=? AND posted_at >= ?",
                (author, cooldown_ago),
            ).fetchone()["c"]
            if author_recent > 0:
                conn.close()
                return False, f"Cooldown: Already quoted @{author} within {self.cooldown_minutes}min"

        conn.close()
        return True, "OK"

    # ── Author Scores ────────────────────────────────────

    def _update_author_score(self, username: str):
        """更新作者关系评分"""
        conn = self._get_conn()
        quotes = conn.execute(
            "SELECT * FROM quote_tweets WHERE original_author=? AND status='posted'",
            (username,),
        ).fetchall()

        if not quotes:
            conn.close()
            return

        count = len(quotes)
        total_eng = sum(
            r["quote_likes"] + r["quote_retweets"] + r["quote_replies"]
            for r in quotes
        )
        total_amp = 0
        for r in quotes:
            orig_eng = r["original_likes"] + r["original_retweets"]
            q_eng = r["quote_likes"] + r["quote_retweets"] + r["quote_replies"]
            if orig_eng > 0:
                total_amp += q_eng / orig_eng

        avg_amp = total_amp / count if count > 0 else 0
        now = datetime.now(timezone.utc).isoformat()

        conn.execute(
            """INSERT OR REPLACE INTO author_scores
               (username, quote_count, avg_amplification, total_engagement, last_quoted_at)
               VALUES (?, ?, ?, ?, ?)""",
            (username, count, round(avg_amp, 4), total_eng, now),
        )
        conn.commit()
        conn.close()

    def get_author_score(self, username: str) -> Optional[dict]:
        """获取作者评分"""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM author_scores WHERE username=?", (username,)
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_top_authors(self, limit: int = 20) -> List[dict]:
        """获取最佳引用目标作者"""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM author_scores ORDER BY avg_amplification DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def set_author_relationship(self, username: str,
                                relationship: str, notes: str = "") -> bool:
        """设置作者关系 (ally/neutral/competitor/avoid)"""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT username FROM author_scores WHERE username=?", (username,)
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE author_scores SET relationship=?, notes=? WHERE username=?",
                (relationship, notes, username),
            )
        else:
            conn.execute(
                "INSERT INTO author_scores (username, relationship, notes) VALUES (?, ?, ?)",
                (username, relationship, notes),
            )
        conn.commit()
        conn.close()
        return True

    # ── Keywords ─────────────────────────────────────────

    def add_keyword(self, keyword: str, min_likes: int = 10,
                    min_retweets: int = 5,
                    preferred_style: str = "add_value") -> bool:
        """添加监控关键词"""
        conn = self._get_conn()
        try:
            conn.execute(
                """INSERT INTO quote_keywords (keyword, min_likes, min_retweets, preferred_style)
                   VALUES (?, ?, ?, ?)""",
                (keyword.lower(), min_likes, min_retweets, preferred_style),
            )
            conn.commit()
            conn.close()
            return True
        except sqlite3.IntegrityError:
            conn.close()
            return False

    def remove_keyword(self, keyword: str) -> bool:
        """移除关键词"""
        conn = self._get_conn()
        cursor = conn.execute(
            "DELETE FROM quote_keywords WHERE keyword=?", (keyword.lower(),)
        )
        conn.commit()
        deleted = cursor.rowcount > 0
        conn.close()
        return deleted

    def get_keywords(self, enabled_only: bool = True) -> List[dict]:
        """获取关键词列表"""
        conn = self._get_conn()
        if enabled_only:
            rows = conn.execute(
                "SELECT * FROM quote_keywords WHERE enabled=1"
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM quote_keywords").fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def match_keywords(self, text: str) -> List[dict]:
        """匹配文本中的关键词"""
        keywords = self.get_keywords()
        matches = []
        text_lower = text.lower()
        for kw in keywords:
            if kw["keyword"] in text_lower:
                matches.append(kw)
        return matches

    # ── Templates ────────────────────────────────────────

    def get_template(self, style: str, index: int = 0) -> str:
        """获取引用模板"""
        templates = self.TEMPLATES.get(style, self.TEMPLATES[QuoteStyle.ADD_VALUE])
        return templates[index % len(templates)]

    def fill_template(self, style: str, opinion: str,
                      author: str = "", index: int = 0) -> str:
        """填充模板"""
        template = self.get_template(style, index)
        return template.format(
            opinion=opinion,
            author=author,
            topic="",
        )

    # ── Analytics ────────────────────────────────────────

    def get_style_performance(self) -> Dict[str, dict]:
        """按风格分析引用效果"""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM quote_tweets WHERE status='posted'"
        ).fetchall()
        conn.close()

        styles: Dict[str, dict] = {}
        for r in rows:
            s = r["style"]
            if s not in styles:
                styles[s] = {
                    "count": 0,
                    "total_likes": 0,
                    "total_retweets": 0,
                    "total_replies": 0,
                    "total_impressions": 0,
                    "total_amplification": 0.0,
                }
            styles[s]["count"] += 1
            styles[s]["total_likes"] += r["quote_likes"]
            styles[s]["total_retweets"] += r["quote_retweets"]
            styles[s]["total_replies"] += r["quote_replies"]
            styles[s]["total_impressions"] += r["quote_impressions"]

            orig_eng = r["original_likes"] + r["original_retweets"]
            q_eng = r["quote_likes"] + r["quote_retweets"] + r["quote_replies"]
            if orig_eng > 0:
                styles[s]["total_amplification"] += q_eng / orig_eng

        # Averages
        for s, d in styles.items():
            c = d["count"]
            d["avg_likes"] = round(d["total_likes"] / c, 1) if c > 0 else 0
            d["avg_retweets"] = round(d["total_retweets"] / c, 1) if c > 0 else 0
            d["avg_engagement_rate"] = (
                round((d["total_likes"] + d["total_retweets"] + d["total_replies"])
                      / d["total_impressions"] * 100, 4)
                if d["total_impressions"] > 0 else 0
            )
            d["avg_amplification"] = round(d["total_amplification"] / c, 4) if c > 0 else 0

        return styles

    def get_best_style(self) -> Optional[str]:
        """获取最佳引用风格"""
        perf = self.get_style_performance()
        if not perf:
            return None
        return max(perf.items(), key=lambda x: x[1]["avg_amplification"])[0]

    def generate_report(self) -> dict:
        """生成引用策略报告"""
        conn = self._get_conn()
        total = conn.execute("SELECT COUNT(*) as c FROM quote_tweets").fetchone()["c"]
        posted = conn.execute(
            "SELECT COUNT(*) as c FROM quote_tweets WHERE status='posted'"
        ).fetchone()["c"]
        conn.close()

        style_perf = self.get_style_performance()
        top_authors = self.get_top_authors(5)
        keywords = self.get_keywords()

        return {
            "total_quotes": total,
            "posted_quotes": posted,
            "draft_quotes": total - posted,
            "style_performance": style_perf,
            "best_style": self.get_best_style(),
            "top_authors": top_authors,
            "active_keywords": len(keywords),
            "cooldown_minutes": self.cooldown_minutes,
            "max_per_hour": self.max_quotes_per_hour,
        }
