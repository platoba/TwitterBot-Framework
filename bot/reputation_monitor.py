"""
Reputation Monitor - 品牌声誉监控

Features:
- Brand mention sentiment tracking
- Negative mention alerts with severity
- Crisis detection (spike in negative sentiment)
- Daily/weekly reputation reports
- Response suggestions for negative mentions
"""

import re
import time
import sqlite3
import logging
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass, asdict
from collections import Counter, defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class Mention:
    """A brand mention"""
    tweet_id: str
    author: str
    text: str
    sentiment: str  # positive, negative, neutral
    sentiment_score: float  # -1.0 to 1.0
    severity: int  # 0=none, 1=low, 2=medium, 3=high
    detected_at: float
    needs_response: bool = False

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class ReputationReport:
    """Reputation summary report"""
    period: str
    total_mentions: int
    positive: int
    negative: int
    neutral: int
    sentiment_score: float
    crisis_detected: bool
    top_negative: List[Dict]
    trend: str  # improving, declining, stable

    def to_dict(self) -> Dict:
        return asdict(self)

    def format_report(self) -> str:
        emoji = {"improving": "📈", "declining": "📉", "stable": "➡️"}
        crisis = "🚨 CRISIS DETECTED" if self.crisis_detected else ""
        return (
            f"🛡️ Reputation Report ({self.period})\n"
            f"Total mentions: {self.total_mentions}\n"
            f"Positive: {self.positive} | Neutral: {self.neutral} | Negative: {self.negative}\n"
            f"Sentiment: {self.sentiment_score:+.2f} {emoji.get(self.trend, '')}\n"
            f"{crisis}"
        ).strip()


class ReputationMonitor:
    """
    Brand reputation monitoring and crisis detection.

    Usage:
        monitor = ReputationMonitor(db, brand_keywords=["myapp", "mycompany"])
        mentions = monitor.analyze_mentions(tweets)
        report = monitor.get_report("7d")
    """

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS reputation_mentions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tweet_id TEXT NOT NULL,
        author TEXT NOT NULL,
        text TEXT NOT NULL,
        sentiment TEXT NOT NULL,
        sentiment_score REAL NOT NULL,
        severity INTEGER DEFAULT 0,
        needs_response INTEGER DEFAULT 0,
        responded INTEGER DEFAULT 0,
        detected_at REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS reputation_daily (
        date TEXT PRIMARY KEY,
        total INTEGER DEFAULT 0,
        positive INTEGER DEFAULT 0,
        negative INTEGER DEFAULT 0,
        neutral INTEGER DEFAULT 0,
        avg_sentiment REAL DEFAULT 0.0
    );

    CREATE INDEX IF NOT EXISTS idx_rep_sentiment ON reputation_mentions(sentiment);
    CREATE INDEX IF NOT EXISTS idx_rep_time ON reputation_mentions(detected_at);
    """

    # Sentiment keywords
    POSITIVE_WORDS = {
        "love", "great", "awesome", "amazing", "excellent", "best", "fantastic",
        "wonderful", "brilliant", "perfect", "impressive", "outstanding",
        "helpful", "useful", "recommend", "thanks", "thank", "good", "nice",
    }

    NEGATIVE_WORDS = {
        "hate", "terrible", "awful", "worst", "horrible", "bad", "poor",
        "broken", "bug", "crash", "slow", "useless", "disappointing",
        "frustrated", "annoying", "sucks", "fails", "garbage", "trash",
        "scam", "fraud", "ripoff", "waste",
    }

    CRISIS_WORDS = {
        "data breach", "lawsuit", "hacked", "leaked", "scandal",
        "class action", "security vulnerability", "outage",
    }

    def __init__(self, db, brand_keywords: Optional[List[str]] = None):
        self.db = db
        self.brand_keywords = [k.lower() for k in (brand_keywords or [])]
        self._ensure_tables()

    def _ensure_tables(self):
        conn = self.db._get_conn()
        conn.executescript(self.SCHEMA)
        conn.commit()

    def analyze_sentiment(self, text: str) -> Tuple[str, float, int]:
        """
        Simple rule-based sentiment analysis.

        Returns:
            (sentiment, score, severity)
        """
        text_lower = text.lower()
        words = set(re.findall(r"\b\w+\b", text_lower))

        pos_count = len(words & self.POSITIVE_WORDS)
        neg_count = len(words & self.NEGATIVE_WORDS)

        # Crisis check
        severity = 0
        for phrase in self.CRISIS_WORDS:
            if phrase in text_lower:
                severity = 3
                neg_count += 3
                break

        # Score calculation
        total = pos_count + neg_count
        if total == 0:
            return "neutral", 0.0, 0

        score = (pos_count - neg_count) / total

        if score > 0.2:
            sentiment = "positive"
        elif score < -0.2:
            sentiment = "negative"
            if severity == 0:
                severity = 1 if neg_count <= 2 else 2
        else:
            sentiment = "neutral"

        return sentiment, round(score, 3), severity

    def analyze_mentions(self, tweets: List[Dict]) -> List[Mention]:
        """
        Analyze a batch of tweets for brand mentions and sentiment.

        Args:
            tweets: List of tweet dicts with text, author, tweet_id

        Returns:
            List of Mention objects
        """
        mentions = []
        conn = self.db._get_conn()
        now = time.time()

        for tweet in tweets:
            text = tweet.get("text", "")
            text_lower = text.lower()

            # Check if it's a brand mention
            is_mention = not self.brand_keywords  # if no keywords, analyze all
            for kw in self.brand_keywords:
                if kw in text_lower:
                    is_mention = True
                    break

            if not is_mention:
                continue

            sentiment, score, severity = self.analyze_sentiment(text)
            needs_response = severity >= 2 or (sentiment == "negative" and severity >= 1)

            mention = Mention(
                tweet_id=tweet.get("tweet_id", ""),
                author=tweet.get("author", tweet.get("author_username", "")),
                text=text,
                sentiment=sentiment,
                sentiment_score=score,
                severity=severity,
                detected_at=now,
                needs_response=needs_response,
            )
            mentions.append(mention)

            # Store in DB
            conn.execute(
                """INSERT OR IGNORE INTO reputation_mentions
                   (tweet_id, author, text, sentiment, sentiment_score, severity, needs_response, detected_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (mention.tweet_id, mention.author, mention.text,
                 mention.sentiment, mention.sentiment_score, mention.severity,
                 int(mention.needs_response), now),
            )

        # Update daily stats
        self._update_daily(conn, mentions)
        conn.commit()
        return mentions

    def _update_daily(self, conn, mentions: List[Mention]):
        """Update daily reputation stats"""
        date = datetime.now().strftime("%Y-%m-%d")
        pos = sum(1 for m in mentions if m.sentiment == "positive")
        neg = sum(1 for m in mentions if m.sentiment == "negative")
        neu = sum(1 for m in mentions if m.sentiment == "neutral")
        total = len(mentions)
        avg = sum(m.sentiment_score for m in mentions) / total if total else 0

        existing = conn.execute(
            "SELECT * FROM reputation_daily WHERE date = ?", (date,)
        ).fetchone()

        if existing:
            conn.execute(
                """UPDATE reputation_daily
                   SET total = total + ?, positive = positive + ?,
                       negative = negative + ?, neutral = neutral + ?,
                       avg_sentiment = (avg_sentiment * total + ? * ?) / (total + ?)
                   WHERE date = ?""",
                (total, pos, neg, neu, avg, total, total, date),
            )
        else:
            conn.execute(
                """INSERT INTO reputation_daily (date, total, positive, negative, neutral, avg_sentiment)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (date, total, pos, neg, neu, round(avg, 3)),
            )

    def get_report(self, period: str = "7d") -> ReputationReport:
        """Generate reputation report"""
        conn = self.db._get_conn()

        days = 7
        if period.endswith("d"):
            days = int(period[:-1])

        since = time.time() - (days * 86400)

        # Aggregate
        row = conn.execute(
            """SELECT COUNT(*) as total,
                      SUM(CASE WHEN sentiment='positive' THEN 1 ELSE 0 END) as pos,
                      SUM(CASE WHEN sentiment='negative' THEN 1 ELSE 0 END) as neg,
                      SUM(CASE WHEN sentiment='neutral' THEN 1 ELSE 0 END) as neu,
                      AVG(sentiment_score) as avg_score
               FROM reputation_mentions
               WHERE detected_at >= ?""",
            (since,),
        ).fetchone()

        total = row["total"] or 0
        pos = row["pos"] or 0
        neg = row["neg"] or 0
        neu = row["neu"] or 0
        avg_score = row["avg_score"] or 0

        # Top negative
        top_neg = conn.execute(
            """SELECT * FROM reputation_mentions
               WHERE sentiment='negative' AND detected_at >= ?
               ORDER BY severity DESC, sentiment_score ASC
               LIMIT 5""",
            (since,),
        ).fetchall()

        # Crisis detection: >30% negative in recent period
        crisis = neg > 0 and total > 0 and (neg / total) > 0.3

        # Trend: compare first half vs second half
        midpoint = since + (days * 86400 / 2)
        first_half = conn.execute(
            "SELECT AVG(sentiment_score) FROM reputation_mentions WHERE detected_at >= ? AND detected_at < ?",
            (since, midpoint),
        ).fetchone()[0] or 0
        second_half = conn.execute(
            "SELECT AVG(sentiment_score) FROM reputation_mentions WHERE detected_at >= ?",
            (midpoint,),
        ).fetchone()[0] or 0

        if second_half > first_half + 0.05:
            trend = "improving"
        elif second_half < first_half - 0.05:
            trend = "declining"
        else:
            trend = "stable"

        return ReputationReport(
            period=period,
            total_mentions=total,
            positive=pos,
            negative=neg,
            neutral=neu,
            sentiment_score=round(avg_score, 3),
            crisis_detected=crisis,
            top_negative=[dict(r) for r in top_neg],
            trend=trend,
        )

    def get_alerts(self, min_severity: int = 2) -> List[Dict]:
        """Get unresponded mentions needing attention"""
        conn = self.db._get_conn()
        rows = conn.execute(
            """SELECT * FROM reputation_mentions
               WHERE needs_response = 1 AND responded = 0 AND severity >= ?
               ORDER BY severity DESC, detected_at DESC
               LIMIT 20""",
            (min_severity,),
        ).fetchall()
        return [dict(r) for r in rows]

    def mark_responded(self, mention_id: int):
        """Mark a mention as responded to"""
        conn = self.db._get_conn()
        conn.execute(
            "UPDATE reputation_mentions SET responded = 1 WHERE id = ?",
            (mention_id,),
        )
        conn.commit()

    def suggest_response(self, text: str) -> str:
        """Generate a response suggestion for a negative mention"""
        text_lower = text.lower()

        if any(w in text_lower for w in ["bug", "crash", "broken", "error"]):
            return "We're sorry about this issue. Could you DM us the details? Our team will investigate ASAP."

        if any(w in text_lower for w in ["slow", "performance", "lag"]):
            return "Thanks for the feedback. We're continuously working on performance. Can you share more details?"

        if any(w in text_lower for w in ["scam", "fraud", "ripoff"]):
            return "We take these concerns seriously. Please reach out to our support team for a full review."

        if any(w in text_lower for w in ["disappointed", "frustrat", "annoying"]):
            return "We're sorry for the frustration. We'd love to hear more about your experience to improve."

        return "Thank you for your feedback. We value your input and are looking into this."
