"""
Content Performance Optimizer - 内容性能优化

Features:
- Analyze top-performing tweets by engagement
- Identify best posting times
- Content type performance analysis
- Hashtag ROI calculator
- Optimal tweet length analysis
"""

import time
import re
import logging
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass, asdict
from collections import Counter, defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class ContentInsight:
    """Performance insight for a content type"""
    content_type: str
    avg_engagement: float
    avg_likes: float
    avg_retweets: float
    avg_replies: float
    sample_count: int
    best_example: Optional[Dict] = None

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class TimeSlot:
    """Posting time performance"""
    hour: int
    day_of_week: int
    avg_engagement: float
    tweet_count: int

    @property
    def label(self) -> str:
        days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        return f"{days[self.day_of_week]} {self.hour:02d}:00"


@dataclass
class HashtagROI:
    """Hashtag performance metrics"""
    hashtag: str
    usage_count: int
    avg_engagement: float
    avg_impressions: float
    engagement_per_use: float

    def to_dict(self) -> Dict:
        return asdict(self)


class ContentOptimizer:
    """
    Analyze tweet performance and provide optimization suggestions.

    Usage:
        optimizer = ContentOptimizer(db)
        top = optimizer.get_top_tweets(limit=10)
        best_times = optimizer.find_best_posting_times()
        hashtag_roi = optimizer.calculate_hashtag_roi()
    """

    def __init__(self, db):
        self.db = db

    def _get_tweets(self, limit: int = 500, since_days: int = 30) -> List[Dict]:
        """Fetch tweet history from database"""
        conn = self.db._get_conn()
        since = time.time() - (since_days * 86400)

        rows = conn.execute(
            """SELECT * FROM tweet_history
               WHERE collected_at IS NOT NULL
               ORDER BY collected_at DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _engagement_score(tweet: Dict) -> float:
        """Calculate engagement score for a tweet"""
        likes = tweet.get("like_count", 0) or 0
        retweets = tweet.get("retweet_count", 0) or 0
        replies = tweet.get("reply_count", 0) or 0
        quotes = tweet.get("quote_count", 0) or 0
        return likes + retweets * 2 + replies * 1.5 + quotes * 2.5

    @staticmethod
    def _classify_content(text: str) -> str:
        """Classify tweet content type"""
        if not text:
            return "empty"

        text_lower = text.lower()

        # Thread indicator
        if re.search(r"(?:🧵|\bthread\b|1/\d)", text_lower):
            return "thread"

        # Question
        if "?" in text and len(text) < 280:
            return "question"

        # Poll-style
        if re.search(r"(?:vote|poll|which|option)", text_lower):
            return "poll"

        # Link share
        if re.search(r"https?://", text):
            return "link_share"

        # Hot take / opinion
        if re.search(r"(?:unpopular opinion|hot take|controversial|i think)", text_lower):
            return "opinion"

        # List / tips
        if re.search(r"(?:\d+[.)]|\b(?:tips|steps|ways|things)\b)", text_lower):
            return "listicle"

        # Short tweet
        if len(text) < 100:
            return "short"

        return "standard"

    def get_top_tweets(self, limit: int = 10, since_days: int = 30) -> List[Dict]:
        """Get top-performing tweets by engagement score"""
        tweets = self._get_tweets(limit=500, since_days=since_days)
        scored = []
        for t in tweets:
            score = self._engagement_score(t)
            t["engagement_score"] = score
            t["content_type"] = self._classify_content(t.get("text", ""))
            scored.append(t)

        scored.sort(key=lambda x: x["engagement_score"], reverse=True)
        return scored[:limit]

    def analyze_content_types(self, since_days: int = 30) -> List[ContentInsight]:
        """Analyze performance by content type"""
        tweets = self._get_tweets(limit=1000, since_days=since_days)

        by_type: Dict[str, List[Dict]] = defaultdict(list)
        for t in tweets:
            ctype = self._classify_content(t.get("text", ""))
            t["engagement_score"] = self._engagement_score(t)
            by_type[ctype].append(t)

        insights = []
        for ctype, group in by_type.items():
            if not group:
                continue
            n = len(group)
            avg_eng = sum(t["engagement_score"] for t in group) / n
            avg_likes = sum(t.get("like_count", 0) or 0 for t in group) / n
            avg_rts = sum(t.get("retweet_count", 0) or 0 for t in group) / n
            avg_replies = sum(t.get("reply_count", 0) or 0 for t in group) / n

            best = max(group, key=lambda x: x["engagement_score"])

            insights.append(ContentInsight(
                content_type=ctype,
                avg_engagement=round(avg_eng, 1),
                avg_likes=round(avg_likes, 1),
                avg_retweets=round(avg_rts, 1),
                avg_replies=round(avg_replies, 1),
                sample_count=n,
                best_example={"text": best.get("text", "")[:100], "score": best["engagement_score"]},
            ))

        insights.sort(key=lambda x: x.avg_engagement, reverse=True)
        return insights

    def find_best_posting_times(self, since_days: int = 30) -> List[TimeSlot]:
        """Find the best posting times based on engagement"""
        tweets = self._get_tweets(limit=1000, since_days=since_days)

        slots: Dict[Tuple[int, int], List[float]] = defaultdict(list)

        for t in tweets:
            created = t.get("created_at", "")
            if not created:
                continue

            try:
                if isinstance(created, str):
                    dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                else:
                    dt = datetime.fromtimestamp(float(created))
            except (ValueError, TypeError):
                continue

            key = (dt.weekday(), dt.hour)
            score = self._engagement_score(t)
            slots[key].append(score)

        results = []
        for (dow, hour), scores in slots.items():
            avg = sum(scores) / len(scores)
            results.append(TimeSlot(
                hour=hour,
                day_of_week=dow,
                avg_engagement=round(avg, 1),
                tweet_count=len(scores),
            ))

        results.sort(key=lambda x: x.avg_engagement, reverse=True)
        return results

    def calculate_hashtag_roi(self, since_days: int = 30) -> List[HashtagROI]:
        """Calculate ROI for each hashtag used"""
        tweets = self._get_tweets(limit=1000, since_days=since_days)

        hashtag_data: Dict[str, List[Dict]] = defaultdict(list)

        for t in tweets:
            text = t.get("text", "")
            tags = re.findall(r"#(\w+)", text)
            t["engagement_score"] = self._engagement_score(t)
            for tag in tags:
                hashtag_data[tag.lower()].append(t)

        results = []
        for tag, group in hashtag_data.items():
            n = len(group)
            avg_eng = sum(t["engagement_score"] for t in group) / n
            avg_imp = sum(t.get("impression_count", 0) or 0 for t in group) / n

            results.append(HashtagROI(
                hashtag=f"#{tag}",
                usage_count=n,
                avg_engagement=round(avg_eng, 1),
                avg_impressions=round(avg_imp, 1),
                engagement_per_use=round(avg_eng, 2),
            ))

        results.sort(key=lambda x: x.avg_engagement, reverse=True)
        return results

    def analyze_tweet_length(self, since_days: int = 30) -> List[Dict]:
        """Analyze performance by tweet length buckets"""
        tweets = self._get_tweets(limit=1000, since_days=since_days)

        buckets = {
            "short (1-50)": (1, 50),
            "medium (51-140)": (51, 140),
            "long (141-200)": (141, 200),
            "max (201-280)": (201, 280),
        }

        results = []
        for label, (lo, hi) in buckets.items():
            group = [
                t for t in tweets
                if lo <= len(t.get("text", "")) <= hi
            ]
            if not group:
                results.append({
                    "bucket": label,
                    "count": 0,
                    "avg_engagement": 0,
                })
                continue

            avg_eng = sum(self._engagement_score(t) for t in group) / len(group)
            results.append({
                "bucket": label,
                "count": len(group),
                "avg_engagement": round(avg_eng, 1),
            })

        return results

    def get_suggestions(self, since_days: int = 30) -> List[str]:
        """Generate content optimization suggestions"""
        suggestions = []

        # Content type analysis
        types = self.analyze_content_types(since_days)
        if types:
            best = types[0]
            suggestions.append(
                f"💡 '{best.content_type}' posts perform best (avg engagement: {best.avg_engagement}). Create more of these."
            )

        # Best times
        times = self.find_best_posting_times(since_days)
        if times:
            top3 = times[:3]
            time_labels = [t.label for t in top3]
            suggestions.append(
                f"⏰ Best posting times: {', '.join(time_labels)}"
            )

        # Hashtags
        tags = self.calculate_hashtag_roi(since_days)
        if tags:
            top_tags = [t.hashtag for t in tags[:3]]
            suggestions.append(
                f"#️⃣ Top hashtags: {' '.join(top_tags)}"
            )

        # Length
        lengths = self.analyze_tweet_length(since_days)
        best_bucket = max(lengths, key=lambda x: x["avg_engagement"]) if lengths else None
        if best_bucket and best_bucket["count"] > 0:
            suggestions.append(
                f"📏 {best_bucket['bucket']} tweets get the most engagement ({best_bucket['avg_engagement']})"
            )

        return suggestions
