"""
Sentiment Analysis Engine v1.0
推文情感分析 + 品牌舆情监控 + 情感趋势追踪
基于关键词+模式匹配的轻量级方案(无外部ML依赖)
"""

import logging
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from bot.database import Database

logger = logging.getLogger(__name__)


# ── 情感词典 ──

POSITIVE_WORDS = {
    # 英文
    "amazing", "awesome", "beautiful", "best", "brilliant", "celebrate",
    "congrats", "congratulations", "cool", "excellent", "excited",
    "fantastic", "glad", "good", "gorgeous", "great", "happy", "helpful",
    "incredible", "inspired", "insightful", "impressive", "joy", "kind",
    "launch", "legendary", "love", "magnificent", "nice", "outstanding",
    "perfect", "pleased", "powerful", "proud", "recommend", "remarkable",
    "success", "superb", "terrific", "thank", "thanks", "thrilled",
    "top", "useful", "valuable", "win", "wonderful", "wow",
    # 表情
    "🔥", "🚀", "💪", "👏", "❤️", "😍", "🎉", "✨", "💯", "👍",
    "🏆", "⭐", "🙏", "💕", "😊", "🥳", "💎", "👑", "🌟",
}

NEGATIVE_WORDS = {
    # 英文
    "awful", "bad", "boring", "broken", "bug", "complaint", "confusing",
    "crash", "dead", "disappointing", "disaster", "disgusting", "dislike",
    "error", "fail", "failure", "fraud", "frustrating", "garbage", "hate",
    "horrible", "issue", "lame", "lousy", "mediocre", "mess", "mistake",
    "negative", "nightmare", "overpriced", "pathetic", "poor", "problem",
    "ridiculous", "sad", "scam", "shame", "slow", "spam", "stupid",
    "suck", "sucks", "terrible", "trash", "ugly", "unfair", "unhappy",
    "useless", "waste", "worst", "wrong",
    # 表情
    "😡", "😤", "😠", "💩", "👎", "😢", "😭", "🤮", "💔", "🙄",
    "😒", "😞", "😫", "🤬", "😵",
}

INTENSIFIERS = {
    "very", "extremely", "incredibly", "absolutely", "totally",
    "completely", "really", "truly", "highly", "super",
}

NEGATIONS = {
    "not", "no", "never", "neither", "nor", "don't", "doesn't",
    "didn't", "won't", "wouldn't", "couldn't", "shouldn't", "isn't",
    "aren't", "wasn't", "weren't", "can't", "cannot", "hardly",
    "barely", "scarcely",
}


class SentimentLabel:
    VERY_POSITIVE = "very_positive"
    POSITIVE = "positive"
    NEUTRAL = "neutral"
    NEGATIVE = "negative"
    VERY_NEGATIVE = "very_negative"


@dataclass
class SentimentResult:
    """单条推文情感分析结果"""
    text: str
    score: float  # -1.0 到 1.0
    label: str
    confidence: float  # 0-1
    positive_words: List[str] = field(default_factory=list)
    negative_words: List[str] = field(default_factory=list)
    has_question: bool = False
    has_exclamation: bool = False
    word_count: int = 0

    @property
    def is_positive(self) -> bool:
        return self.score > 0.1

    @property
    def is_negative(self) -> bool:
        return self.score < -0.1

    @property
    def is_neutral(self) -> bool:
        return -0.1 <= self.score <= 0.1

    def to_dict(self) -> Dict:
        return {
            "text": self.text[:200],
            "score": round(self.score, 3),
            "label": self.label,
            "confidence": round(self.confidence, 3),
            "positive_words": self.positive_words,
            "negative_words": self.negative_words,
            "has_question": self.has_question,
            "has_exclamation": self.has_exclamation,
            "word_count": self.word_count,
        }


@dataclass
class SentimentSummary:
    """批量情感分析汇总"""
    total: int = 0
    positive_count: int = 0
    negative_count: int = 0
    neutral_count: int = 0
    avg_score: float = 0
    score_distribution: Dict[str, int] = field(default_factory=dict)
    top_positive_words: List[Tuple[str, int]] = field(default_factory=list)
    top_negative_words: List[Tuple[str, int]] = field(default_factory=list)
    sentiment_trend: List[Dict] = field(default_factory=list)

    @property
    def positive_pct(self) -> float:
        return round(self.positive_count / max(self.total, 1) * 100, 1)

    @property
    def negative_pct(self) -> float:
        return round(self.negative_count / max(self.total, 1) * 100, 1)

    @property
    def neutral_pct(self) -> float:
        return round(self.neutral_count / max(self.total, 1) * 100, 1)

    def to_dict(self) -> Dict:
        return {
            "total": self.total,
            "positive": {"count": self.positive_count, "pct": self.positive_pct},
            "negative": {"count": self.negative_count, "pct": self.negative_pct},
            "neutral": {"count": self.neutral_count, "pct": self.neutral_pct},
            "avg_score": round(self.avg_score, 3),
            "score_distribution": self.score_distribution,
            "top_positive_words": self.top_positive_words[:10],
            "top_negative_words": self.top_negative_words[:10],
            "sentiment_trend": self.sentiment_trend,
        }


class SentimentAnalyzer:
    """推文情感分析引擎"""

    def __init__(self, db: Database = None,
                 custom_positive: set = None,
                 custom_negative: set = None):
        self.db = db
        self.positive_words = POSITIVE_WORDS | (custom_positive or set())
        self.negative_words = NEGATIVE_WORDS | (custom_negative or set())
        self._ensure_table()

    def _ensure_table(self):
        """确保sentiment表存在"""
        if not self.db:
            return
        conn = self.db._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS sentiment_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tweet_id TEXT,
                username TEXT,
                text TEXT,
                score REAL,
                label TEXT,
                confidence REAL,
                analyzed_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_sentiment_user
                ON sentiment_log(username, analyzed_at);
            CREATE INDEX IF NOT EXISTS idx_sentiment_label
                ON sentiment_log(label);
        """)
        conn.commit()

    def analyze(self, text: str) -> SentimentResult:
        """分析单条文本情感"""
        if not text or not text.strip():
            return SentimentResult(
                text="", score=0, label=SentimentLabel.NEUTRAL,
                confidence=0, word_count=0
            )

        # 预处理
        clean_text = text.lower()
        words = re.findall(r'[\w\']+|[^\w\s]', clean_text)
        word_count = len(words)

        positive_found = []
        negative_found = []
        pos_score = 0
        neg_score = 0
        negation_window = 0

        for i, word in enumerate(words):
            # 否定词窗口
            if word in NEGATIONS:
                negation_window = 3
                continue

            multiplier = 1.0

            # 强化词
            if i > 0 and words[i - 1] in INTENSIFIERS:
                multiplier = 1.5

            # 否定翻转
            is_negated = negation_window > 0

            if word in self.positive_words:
                if is_negated:
                    neg_score += 1.0 * multiplier
                    negative_found.append(f"not {word}")
                else:
                    pos_score += 1.0 * multiplier
                    positive_found.append(word)
            elif word in self.negative_words:
                if is_negated:
                    pos_score += 0.5 * multiplier  # 否定负面词的正面性较弱
                    positive_found.append(f"not {word}")
                else:
                    neg_score += 1.0 * multiplier
                    negative_found.append(word)

            if negation_window > 0:
                negation_window -= 1

        # 检查emoji(直接在原文中搜索)
        for emoji in self.positive_words:
            if len(emoji) <= 2 and emoji in text:
                count = text.count(emoji)
                pos_score += count * 0.5
                positive_found.append(emoji)

        for emoji in self.negative_words:
            if len(emoji) <= 2 and emoji in text:
                count = text.count(emoji)
                neg_score += count * 0.5
                negative_found.append(emoji)

        # 计算最终分数 [-1, 1]
        total = pos_score + neg_score
        if total == 0:
            score = 0.0
        else:
            score = (pos_score - neg_score) / total

        # 基于匹配词数量调整置信度
        if total == 0:
            confidence = 0.2  # 低置信度-无情感词
        elif total <= 2:
            confidence = 0.5
        elif total <= 5:
            confidence = 0.7
        else:
            confidence = 0.9

        # 标签
        label = self._score_to_label(score)

        return SentimentResult(
            text=text,
            score=score,
            label=label,
            confidence=confidence,
            positive_words=list(set(positive_found)),
            negative_words=list(set(negative_found)),
            has_question="?" in text,
            has_exclamation="!" in text,
            word_count=word_count,
        )

    def analyze_batch(self, texts: List[str]) -> List[SentimentResult]:
        """批量分析"""
        return [self.analyze(text) for text in texts]

    def analyze_tweets(self, tweets: List[Dict]) -> List[SentimentResult]:
        """分析推文列表"""
        results = []
        for tweet in tweets:
            text = tweet.get("text", "")
            result = self.analyze(text)

            # 保存到数据库
            if self.db:
                self._save_result(
                    tweet_id=tweet.get("id", tweet.get("tweet_id", "")),
                    username=tweet.get("author_username", ""),
                    result=result,
                )

            results.append(result)
        return results

    def summarize(self, results: List[SentimentResult]) -> SentimentSummary:
        """汇总分析结果"""
        if not results:
            return SentimentSummary()

        summary = SentimentSummary(total=len(results))

        scores = []
        pos_words = Counter()
        neg_words = Counter()
        distribution = Counter()

        for r in results:
            scores.append(r.score)
            distribution[r.label] += 1

            if r.is_positive:
                summary.positive_count += 1
            elif r.is_negative:
                summary.negative_count += 1
            else:
                summary.neutral_count += 1

            for w in r.positive_words:
                pos_words[w] += 1
            for w in r.negative_words:
                neg_words[w] += 1

        summary.avg_score = sum(scores) / len(scores)
        summary.score_distribution = dict(distribution)
        summary.top_positive_words = pos_words.most_common(10)
        summary.top_negative_words = neg_words.most_common(10)

        return summary

    def analyze_mentions(self, username: str,
                          tweets: List[Dict] = None) -> SentimentSummary:
        """分析提及某用户的推文情感"""
        if tweets is None and self.db:
            tweets = self.db.get_tweet_history(limit=100)
            tweets = [t for t in tweets
                      if username.lower() in t.get("text", "").lower()]

        if not tweets:
            return SentimentSummary()

        results = self.analyze_tweets(tweets)
        return self.summarize(results)

    def detect_brand_crisis(self, results: List[SentimentResult],
                             threshold: float = 0.4) -> Dict:
        """品牌危机预警"""
        if not results:
            return {"crisis": False, "level": "none"}

        neg_count = sum(1 for r in results if r.is_negative)
        neg_pct = neg_count / len(results)
        avg_score = sum(r.score for r in results) / len(results)

        # 最近趋势(如果有时间序列)
        recent_neg = sum(1 for r in results[-10:] if r.is_negative)
        recent_neg_pct = recent_neg / min(len(results), 10)

        if neg_pct > 0.6 or avg_score < -0.5:
            level = "critical"
            crisis = True
        elif neg_pct > threshold or avg_score < -0.2:
            level = "warning"
            crisis = True
        elif recent_neg_pct > 0.5:
            level = "watch"
            crisis = True
        else:
            level = "normal"
            crisis = False

        # 负面关键词聚合
        neg_topics = Counter()
        for r in results:
            if r.is_negative:
                for w in r.negative_words:
                    neg_topics[w] += 1

        return {
            "crisis": crisis,
            "level": level,
            "negative_pct": round(neg_pct * 100, 1),
            "avg_score": round(avg_score, 3),
            "recent_negative_pct": round(recent_neg_pct * 100, 1),
            "top_complaints": neg_topics.most_common(5),
            "sample_size": len(results),
            "recommendation": self._crisis_recommendation(level),
        }

    def _crisis_recommendation(self, level: str) -> str:
        recommendations = {
            "critical": "🚨 立即响应! 大量负面情绪，建议: 1)识别核心问题 2)发布官方声明 3)一对一回复投诉",
            "warning": "⚠️ 关注中! 负面情绪上升，建议: 1)监控关键话题 2)增加正面内容 3)回复负面反馈",
            "watch": "👀 近期负面趋势，建议密切关注并准备应对方案",
            "normal": "✅ 舆情正常",
        }
        return recommendations.get(level, "")

    def _save_result(self, tweet_id: str, username: str,
                      result: SentimentResult):
        """保存分析结果到数据库"""
        if not self.db:
            return
        try:
            conn = self.db._get_conn()
            conn.execute("""
                INSERT INTO sentiment_log
                (tweet_id, username, text, score, label, confidence)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (tweet_id, username, result.text[:500],
                  result.score, result.label, result.confidence))
            conn.commit()
        except Exception as e:
            logger.warning(f"Failed to save sentiment: {e}")

    def get_sentiment_history(self, username: str = "",
                               days: int = 7) -> List[Dict]:
        """获取历史情感趋势"""
        if not self.db:
            return []
        conn = self.db._get_conn()
        if username:
            rows = conn.execute("""
                SELECT date(analyzed_at) as date,
                       AVG(score) as avg_score,
                       COUNT(*) as count,
                       SUM(CASE WHEN score > 0.1 THEN 1 ELSE 0 END) as positive,
                       SUM(CASE WHEN score < -0.1 THEN 1 ELSE 0 END) as negative
                FROM sentiment_log
                WHERE username = ? AND analyzed_at >= datetime('now', ?)
                GROUP BY date(analyzed_at)
                ORDER BY date
            """, (username, f"-{days} days")).fetchall()
        else:
            rows = conn.execute("""
                SELECT date(analyzed_at) as date,
                       AVG(score) as avg_score,
                       COUNT(*) as count,
                       SUM(CASE WHEN score > 0.1 THEN 1 ELSE 0 END) as positive,
                       SUM(CASE WHEN score < -0.1 THEN 1 ELSE 0 END) as negative
                FROM sentiment_log
                WHERE analyzed_at >= datetime('now', ?)
                GROUP BY date(analyzed_at)
                ORDER BY date
            """, (f"-{days} days",)).fetchall()
        return [dict(r) for r in rows]

    def _score_to_label(self, score: float) -> str:
        if score >= 0.5:
            return SentimentLabel.VERY_POSITIVE
        elif score > 0.1:
            return SentimentLabel.POSITIVE
        elif score <= -0.5:
            return SentimentLabel.VERY_NEGATIVE
        elif score < -0.1:
            return SentimentLabel.NEGATIVE
        return SentimentLabel.NEUTRAL

    def format_summary(self, summary: SentimentSummary) -> str:
        """格式化情感分析报告"""
        lines = [
            f"🧠 *情感分析报告* ({summary.total} 条)\n",
            f"😊 正面: {summary.positive_count} ({summary.positive_pct}%)",
            f"😐 中立: {summary.neutral_count} ({summary.neutral_pct}%)",
            f"😠 负面: {summary.negative_count} ({summary.negative_pct}%)",
            f"📊 平均得分: {summary.avg_score:.3f}\n",
        ]

        # 情感条
        total = max(summary.total, 1)
        pos_bar = "🟢" * max(1, round(summary.positive_count / total * 20))
        neu_bar = "🟡" * max(0, round(summary.neutral_count / total * 20))
        neg_bar = "🔴" * max(0, round(summary.negative_count / total * 20))
        lines.append(f"{pos_bar}{neu_bar}{neg_bar}\n")

        if summary.top_positive_words:
            lines.append("✅ *高频正面词*")
            for word, count in summary.top_positive_words[:5]:
                lines.append(f"  {word}: {count}")
            lines.append("")

        if summary.top_negative_words:
            lines.append("❌ *高频负面词*")
            for word, count in summary.top_negative_words[:5]:
                lines.append(f"  {word}: {count}")

        return "\n".join(lines)
