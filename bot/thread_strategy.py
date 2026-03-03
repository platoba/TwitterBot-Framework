"""
Strategic Thread Publishing Engine for Twitter/X

Advanced thread planning and execution:
- Thread structure optimizer (hook → body → CTA flow)
- Optimal tweet count estimation based on content
- Engagement-driven structure (cliff-hangers, open loops)
- Auto-numbering with configurable formats
- Thread performance predictor (estimated engagement)
- Template library (story, tutorial, listicle, debate, case study)
- Readability scoring per tweet (Flesch-Kincaid adapted)
- Character budget optimizer (maximize info per tweet)
- Thread scheduling with drip-feed option
- Thread analytics (which tweet in chain performs best)
"""

import json
import re
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


class ThreadTemplate(str, Enum):
    """Pre-built thread structure templates."""
    STORY = "story"           # Hook → Setup → Conflict → Resolution → CTA
    TUTORIAL = "tutorial"     # Problem → Step-by-step → Summary → CTA
    LISTICLE = "listicle"     # Hook → N items → Bonus → CTA
    DEBATE = "debate"         # Thesis → Arguments → Counter → Conclusion
    CASE_STUDY = "case_study" # Context → Challenge → Solution → Results → Takeaway
    THREAD_BOMB = "thread_bomb"  # Hook → Rapid-fire facts → Summary
    PERSONAL = "personal"     # Hook → Backstory → Lesson → Ask


class NumberingStyle(str, Enum):
    """Tweet numbering formats."""
    SLASH = "slash"       # 1/10
    EMOJI = "emoji"       # 1️⃣
    ARROW = "arrow"       # → 1.
    BRACKET = "bracket"   # [1/10]
    DOT = "dot"           # 1.
    NONE = "none"


class ThreadStatus(str, Enum):
    DRAFT = "draft"
    SCHEDULED = "scheduled"
    PUBLISHING = "publishing"
    PUBLISHED = "published"
    PAUSED = "paused"
    FAILED = "failed"


class TweetRole(str, Enum):
    """Role of tweet in thread structure."""
    HOOK = "hook"
    SETUP = "setup"
    BODY = "body"
    CLIMAX = "climax"
    CTA = "cta"
    BRIDGE = "bridge"  # Cliff-hanger / transition


TEMPLATE_STRUCTURES = {
    ThreadTemplate.STORY: [
        TweetRole.HOOK, TweetRole.SETUP, TweetRole.BODY, TweetRole.BODY,
        TweetRole.CLIMAX, TweetRole.CTA,
    ],
    ThreadTemplate.TUTORIAL: [
        TweetRole.HOOK, TweetRole.SETUP, TweetRole.BODY, TweetRole.BODY,
        TweetRole.BODY, TweetRole.CTA,
    ],
    ThreadTemplate.LISTICLE: [
        TweetRole.HOOK, TweetRole.BODY, TweetRole.BODY, TweetRole.BODY,
        TweetRole.BODY, TweetRole.BODY, TweetRole.CTA,
    ],
    ThreadTemplate.DEBATE: [
        TweetRole.HOOK, TweetRole.SETUP, TweetRole.BODY, TweetRole.BODY,
        TweetRole.CLIMAX, TweetRole.CTA,
    ],
    ThreadTemplate.CASE_STUDY: [
        TweetRole.HOOK, TweetRole.SETUP, TweetRole.BODY, TweetRole.CLIMAX,
        TweetRole.CTA,
    ],
    ThreadTemplate.THREAD_BOMB: [
        TweetRole.HOOK, TweetRole.BODY, TweetRole.BODY, TweetRole.BODY,
        TweetRole.BODY, TweetRole.BODY, TweetRole.BODY, TweetRole.CTA,
    ],
    ThreadTemplate.PERSONAL: [
        TweetRole.HOOK, TweetRole.SETUP, TweetRole.BODY, TweetRole.CLIMAX,
        TweetRole.CTA,
    ],
}

# Engagement modifiers by position
POSITION_ENGAGEMENT_FACTORS = {
    0: 1.0,     # First tweet (hook)
    1: 0.85,    # Drop off
    2: 0.72,
    3: 0.65,
    4: 0.60,
    5: 0.56,
    6: 0.53,
    7: 0.50,
    8: 0.48,
    9: 0.46,
}

CLIFF_HANGER_PHRASES = [
    "But here's where it gets interesting...",
    "And then something unexpected happened:",
    "This is the part most people miss:",
    "Here's the twist nobody saw coming:",
    "But wait, it gets better:",
    "The real lesson? Keep reading 👇",
    "What happened next changed everything:",
    "But that's not even the best part...",
]

HOOK_PATTERNS = [
    "question",      # Start with a question
    "bold_claim",    # Make a controversial statement
    "statistic",     # Lead with a number
    "story_open",    # "I just..." / "Last week..."
    "contrast",      # "Everyone thinks X. They're wrong."
    "promise",       # "In this thread, I'll show you..."
]


@dataclass
class ThreadTweet:
    """Single tweet in a thread."""
    index: int
    text: str
    role: TweetRole = TweetRole.BODY
    char_count: int = 0
    word_count: int = 0
    readability_score: float = 0.0
    has_media: bool = False
    media_url: Optional[str] = None
    tweet_id: Optional[str] = None
    engagement: Dict[str, int] = field(default_factory=dict)
    published_at: Optional[str] = None

    def __post_init__(self):
        self.char_count = len(self.text)
        self.word_count = len(self.text.split())


@dataclass
class ThreadPlan:
    """Complete thread plan."""
    thread_id: str = ""
    title: str = ""
    template: ThreadTemplate = ThreadTemplate.STORY
    tweets: List[ThreadTweet] = field(default_factory=list)
    numbering: NumberingStyle = NumberingStyle.SLASH
    status: ThreadStatus = ThreadStatus.DRAFT
    created_at: str = ""
    scheduled_at: Optional[str] = None
    drip_interval_minutes: int = 0  # 0 = publish all at once
    estimated_engagement: float = 0.0
    total_chars: int = 0
    tags: List[str] = field(default_factory=list)

    def __post_init__(self):
        if not self.thread_id:
            self.thread_id = str(uuid.uuid4())[:12]
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()


class ReadabilityAnalyzer:
    """Adapted Flesch-Kincaid for tweets (short-form)."""

    @staticmethod
    def count_syllables(word: str) -> int:
        """Estimate syllable count for English word."""
        word = word.lower().strip(".,!?;:'\"")
        if not word:
            return 0
        if len(word) <= 2:
            return 1
        vowels = "aeiouy"
        count = 0
        prev_vowel = False
        for char in word:
            is_vowel = char in vowels
            if is_vowel and not prev_vowel:
                count += 1
            prev_vowel = is_vowel
        if word.endswith("e") and count > 1:
            count -= 1
        return max(count, 1)

    @classmethod
    def flesch_score(cls, text: str) -> float:
        """
        Calculate Flesch Reading Ease for text.
        Higher = easier to read. Tweets should aim for 60-80.
        """
        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        if not sentences:
            return 100.0

        words = text.split()
        if not words:
            return 100.0

        total_syllables = sum(cls.count_syllables(w) for w in words)
        avg_sentence_len = len(words) / len(sentences)
        avg_syllables = total_syllables / len(words)

        score = 206.835 - (1.015 * avg_sentence_len) - (84.6 * avg_syllables)
        return max(0.0, min(100.0, round(score, 1)))

    @classmethod
    def tweet_readability(cls, text: str) -> Dict[str, Any]:
        """Comprehensive readability analysis for a tweet."""
        words = text.split()
        chars = len(text)
        flesch = cls.flesch_score(text)

        # Grade classification
        if flesch >= 80:
            grade = "very_easy"
        elif flesch >= 60:
            grade = "easy"
        elif flesch >= 40:
            grade = "moderate"
        elif flesch >= 20:
            grade = "difficult"
        else:
            grade = "very_difficult"

        # Tweet-specific metrics
        has_emoji = bool(re.search(r'[\U0001F600-\U0001F9FF\U00002600-\U000027BF]', text))
        has_hashtag = '#' in text
        has_mention = '@' in text
        has_url = bool(re.search(r'https?://', text))
        line_breaks = text.count('\n')

        return {
            "flesch_score": flesch,
            "grade": grade,
            "word_count": len(words),
            "char_count": chars,
            "char_remaining": max(0, 280 - chars),
            "has_emoji": has_emoji,
            "has_hashtag": has_hashtag,
            "has_mention": has_mention,
            "has_url": has_url,
            "line_breaks": line_breaks,
            "optimal_for_twitter": 60 <= flesch <= 85 and chars <= 280,
        }


class CharacterBudgetOptimizer:
    """Maximize information density within 280 char limit."""

    MAX_CHARS = 280
    URL_CHARS = 23  # Twitter t.co shortens all URLs

    @classmethod
    def optimize_text(cls, text: str, max_chars: int = 280) -> str:
        """Optimize text to fit within character limit."""
        if len(text) <= max_chars:
            return text

        # Replace common long phrases with shorter versions
        replacements = [
            ("in order to", "to"),
            ("due to the fact that", "because"),
            ("at this point in time", "now"),
            ("in the event that", "if"),
            ("for the purpose of", "to"),
            ("with regard to", "about"),
            ("in spite of", "despite"),
            ("a large number of", "many"),
            ("the vast majority of", "most"),
            ("on a daily basis", "daily"),
            ("at the end of the day", "ultimately"),
            ("it is important to note that", "notably"),
            ("as a matter of fact", "in fact"),
        ]

        result = text
        for long, short in replacements:
            result = re.sub(re.escape(long), short, result, flags=re.IGNORECASE)

        # Remove trailing whitespace
        result = re.sub(r'\s+', ' ', result).strip()

        # If still too long, truncate with ellipsis
        if len(result) > max_chars:
            result = result[:max_chars - 1] + "…"

        return result

    @classmethod
    def split_content(cls, content: str, max_chars: int = 280,
                      numbering: NumberingStyle = NumberingStyle.NONE) -> List[str]:
        """Split long content into tweet-sized chunks."""
        if len(content) <= max_chars:
            return [content]

        # Reserve space for numbering
        numbering_reserve = 0
        if numbering != NumberingStyle.NONE:
            numbering_reserve = 8  # e.g., " [1/10]"

        effective_max = max_chars - numbering_reserve
        words = content.split()
        tweets = []
        current = []
        current_len = 0

        for word in words:
            word_len = len(word) + (1 if current else 0)
            if current_len + word_len > effective_max and current:
                tweets.append(' '.join(current))
                current = [word]
                current_len = len(word)
            else:
                current.append(word)
                current_len += word_len

        if current:
            tweets.append(' '.join(current))

        return tweets

    @classmethod
    def budget_report(cls, text: str) -> Dict[str, Any]:
        """Analyze character budget usage."""
        chars = len(text)
        urls = re.findall(r'https?://\S+', text)
        url_savings = sum(len(u) - cls.URL_CHARS for u in urls if len(u) > cls.URL_CHARS)

        effective_chars = chars - url_savings
        return {
            "raw_chars": chars,
            "effective_chars": effective_chars,
            "remaining": cls.MAX_CHARS - effective_chars,
            "utilization_pct": round(effective_chars / cls.MAX_CHARS * 100, 1),
            "url_count": len(urls),
            "url_char_savings": url_savings,
            "fits": effective_chars <= cls.MAX_CHARS,
        }


class EngagementPredictor:
    """Predict thread engagement based on structure and content signals."""

    # Base engagement weights by content signal
    SIGNAL_WEIGHTS = {
        "has_question": 1.15,
        "has_number": 1.12,
        "has_emoji": 1.08,
        "has_bold_claim": 1.20,
        "short_sentences": 1.10,
        "has_media": 1.35,
        "cliff_hanger": 1.18,
        "has_cta": 1.25,
        "optimal_length": 1.10,  # 5-12 tweets
    }

    @classmethod
    def predict_thread(cls, plan: ThreadPlan, base_followers: int = 1000) -> Dict[str, Any]:
        """Predict engagement metrics for a thread plan."""
        if not plan.tweets:
            return {"estimated_impressions": 0, "estimated_likes": 0,
                    "estimated_retweets": 0, "estimated_replies": 0,
                    "engagement_rate": 0.0, "score": 0.0}

        # Base impression rate (typically 10-30% of followers see tweets)
        base_impressions = base_followers * 0.15
        multiplier = 1.0

        hook = plan.tweets[0].text if plan.tweets else ""

        # Analyze hook quality
        if '?' in hook:
            multiplier *= cls.SIGNAL_WEIGHTS["has_question"]
        if re.search(r'\d+', hook):
            multiplier *= cls.SIGNAL_WEIGHTS["has_number"]
        if re.search(r'[\U0001F600-\U0001F9FF]', hook):
            multiplier *= cls.SIGNAL_WEIGHTS["has_emoji"]

        # Thread length factor
        n_tweets = len(plan.tweets)
        if 5 <= n_tweets <= 12:
            multiplier *= cls.SIGNAL_WEIGHTS["optimal_length"]
        elif n_tweets > 20:
            multiplier *= 0.85  # Too long, drop-off

        # Media boost
        media_tweets = sum(1 for t in plan.tweets if t.has_media)
        if media_tweets > 0:
            multiplier *= cls.SIGNAL_WEIGHTS["has_media"]

        # CTA present
        has_cta = any(t.role == TweetRole.CTA for t in plan.tweets)
        if has_cta:
            multiplier *= cls.SIGNAL_WEIGHTS["has_cta"]

        est_impressions = int(base_impressions * multiplier)
        est_likes = int(est_impressions * 0.025 * multiplier)
        est_retweets = int(est_impressions * 0.008 * multiplier)
        est_replies = int(est_impressions * 0.004 * multiplier)

        total_engagement = est_likes + est_retweets + est_replies
        eng_rate = round(total_engagement / max(est_impressions, 1) * 100, 2)

        return {
            "estimated_impressions": est_impressions,
            "estimated_likes": est_likes,
            "estimated_retweets": est_retweets,
            "estimated_replies": est_replies,
            "engagement_rate": eng_rate,
            "multiplier": round(multiplier, 3),
            "score": round(multiplier * 100, 1),
            "tweet_count": n_tweets,
            "media_count": media_tweets,
        }

    @classmethod
    def predict_tweet_position(cls, position: int, total: int) -> float:
        """Predict engagement retention for tweet at given position."""
        if total <= 0:
            return 0.0
        # Exponential decay with slight recovery at end (CTA effect)
        base = POSITION_ENGAGEMENT_FACTORS.get(position, max(0.3, 1.0 - position * 0.07))
        if position == total - 1 and total > 3:
            base *= 1.15  # CTA bump
        return round(min(base, 1.0), 3)


class ThreadStrategy:
    """
    Main thread strategy engine.
    Plans, optimizes, and manages thread publishing.
    """

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.readability = ReadabilityAnalyzer()
        self.budget = CharacterBudgetOptimizer()
        self.predictor = EngagementPredictor()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self):
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS threads (
                thread_id TEXT PRIMARY KEY,
                title TEXT,
                template TEXT,
                numbering TEXT DEFAULT 'slash',
                status TEXT DEFAULT 'draft',
                created_at TEXT,
                scheduled_at TEXT,
                published_at TEXT,
                drip_interval_minutes INTEGER DEFAULT 0,
                estimated_engagement REAL DEFAULT 0.0,
                total_chars INTEGER DEFAULT 0,
                tags TEXT DEFAULT '[]',
                metadata TEXT DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS thread_tweets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id TEXT NOT NULL,
                tweet_index INTEGER NOT NULL,
                text TEXT NOT NULL,
                role TEXT DEFAULT 'body',
                char_count INTEGER DEFAULT 0,
                word_count INTEGER DEFAULT 0,
                readability_score REAL DEFAULT 0.0,
                has_media INTEGER DEFAULT 0,
                media_url TEXT,
                tweet_id TEXT,
                engagement TEXT DEFAULT '{}',
                published_at TEXT,
                FOREIGN KEY (thread_id) REFERENCES threads(thread_id),
                UNIQUE(thread_id, tweet_index)
            );

            CREATE TABLE IF NOT EXISTS thread_analytics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id TEXT NOT NULL,
                snapshot_at TEXT DEFAULT (datetime('now')),
                total_impressions INTEGER DEFAULT 0,
                total_likes INTEGER DEFAULT 0,
                total_retweets INTEGER DEFAULT 0,
                total_replies INTEGER DEFAULT 0,
                best_tweet_index INTEGER,
                drop_off_tweet_index INTEGER,
                completion_rate REAL DEFAULT 0.0,
                FOREIGN KEY (thread_id) REFERENCES threads(thread_id)
            );

            CREATE INDEX IF NOT EXISTS idx_thread_tweets_thread
                ON thread_tweets(thread_id);
            CREATE INDEX IF NOT EXISTS idx_threads_status
                ON threads(status);
        """)
        conn.commit()
        conn.close()

    def create_thread(self, title: str, content_pieces: List[str],
                      template: ThreadTemplate = ThreadTemplate.STORY,
                      numbering: NumberingStyle = NumberingStyle.SLASH,
                      tags: Optional[List[str]] = None) -> ThreadPlan:
        """Create a new thread plan from content pieces."""
        structure = TEMPLATE_STRUCTURES.get(template, TEMPLATE_STRUCTURES[ThreadTemplate.STORY])

        tweets = []
        for i, text in enumerate(content_pieces):
            role = structure[i] if i < len(structure) else TweetRole.BODY
            readability = self.readability.tweet_readability(text)
            tweet = ThreadTweet(
                index=i,
                text=text,
                role=role,
                readability_score=readability["flesch_score"],
            )
            tweets.append(tweet)

        plan = ThreadPlan(
            title=title,
            template=template,
            tweets=tweets,
            numbering=numbering,
            tags=tags or [],
            total_chars=sum(len(t.text) for t in tweets),
        )

        # Predict engagement
        prediction = self.predictor.predict_thread(plan)
        plan.estimated_engagement = prediction["score"]

        # Save to DB
        self._save_thread(plan)
        return plan

    def _save_thread(self, plan: ThreadPlan):
        """Persist thread plan to database."""
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO threads
            (thread_id, title, template, numbering, status, created_at,
             scheduled_at, drip_interval_minutes, estimated_engagement,
             total_chars, tags)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            plan.thread_id, plan.title, plan.template.value,
            plan.numbering.value, plan.status.value, plan.created_at,
            plan.scheduled_at, plan.drip_interval_minutes,
            plan.estimated_engagement, plan.total_chars,
            json.dumps(plan.tags),
        ))

        for tweet in plan.tweets:
            conn.execute("""
                INSERT OR REPLACE INTO thread_tweets
                (thread_id, tweet_index, text, role, char_count, word_count,
                 readability_score, has_media, media_url, tweet_id, engagement, published_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                plan.thread_id, tweet.index, tweet.text, tweet.role.value,
                tweet.char_count, tweet.word_count, tweet.readability_score,
                int(tweet.has_media), tweet.media_url, tweet.tweet_id,
                json.dumps(tweet.engagement), tweet.published_at,
            ))

        conn.commit()
        conn.close()

    def get_thread(self, thread_id: str) -> Optional[ThreadPlan]:
        """Load thread plan from database."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM threads WHERE thread_id = ?", (thread_id,)
        ).fetchone()
        if not row:
            conn.close()
            return None

        tweet_rows = conn.execute(
            "SELECT * FROM thread_tweets WHERE thread_id = ? ORDER BY tweet_index",
            (thread_id,)
        ).fetchall()
        conn.close()

        tweets = []
        for tr in tweet_rows:
            tweet = ThreadTweet(
                index=tr["tweet_index"],
                text=tr["text"],
                role=TweetRole(tr["role"]),
                readability_score=tr["readability_score"],
                has_media=bool(tr["has_media"]),
                media_url=tr["media_url"],
                tweet_id=tr["tweet_id"],
                engagement=json.loads(tr["engagement"]) if tr["engagement"] else {},
                published_at=tr["published_at"],
            )
            tweets.append(tweet)

        plan = ThreadPlan(
            thread_id=row["thread_id"],
            title=row["title"],
            template=ThreadTemplate(row["template"]),
            tweets=tweets,
            numbering=NumberingStyle(row["numbering"]),
            status=ThreadStatus(row["status"]),
            created_at=row["created_at"],
            scheduled_at=row["scheduled_at"],
            drip_interval_minutes=row["drip_interval_minutes"],
            estimated_engagement=row["estimated_engagement"],
            total_chars=row["total_chars"],
            tags=json.loads(row["tags"]) if row["tags"] else [],
        )
        return plan

    def list_threads(self, status: Optional[ThreadStatus] = None,
                     limit: int = 50) -> List[Dict[str, Any]]:
        """List thread plans with optional status filter."""
        conn = self._get_conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM threads WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                (status.value, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM threads ORDER BY created_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
        conn.close()

        return [dict(r) for r in rows]

    def apply_numbering(self, plan: ThreadPlan) -> List[str]:
        """Apply numbering style to thread tweets."""
        total = len(plan.tweets)
        result = []

        for tweet in plan.tweets:
            text = tweet.text
            if plan.numbering == NumberingStyle.SLASH:
                prefix = f"{tweet.index + 1}/{total} "
            elif plan.numbering == NumberingStyle.BRACKET:
                prefix = f"[{tweet.index + 1}/{total}] "
            elif plan.numbering == NumberingStyle.DOT:
                prefix = f"{tweet.index + 1}. "
            elif plan.numbering == NumberingStyle.ARROW:
                prefix = f"→ {tweet.index + 1}. "
            elif plan.numbering == NumberingStyle.EMOJI:
                digits = str(tweet.index + 1)
                emoji_map = {
                    '0': '0️⃣', '1': '1️⃣', '2': '2️⃣', '3': '3️⃣',
                    '4': '4️⃣', '5': '5️⃣', '6': '6️⃣', '7': '7️⃣',
                    '8': '8️⃣', '9': '9️⃣',
                }
                prefix = ''.join(emoji_map.get(d, d) for d in digits) + ' '
            else:
                prefix = ""

            numbered = prefix + text
            # Trim if over limit
            if len(numbered) > 280:
                trim_to = 280 - len(prefix) - 1
                text = text[:trim_to] + "…"
                numbered = prefix + text

            result.append(numbered)

        return result

    def insert_cliff_hangers(self, plan: ThreadPlan,
                             every_n: int = 3) -> ThreadPlan:
        """Insert cliff-hanger bridges between tweets."""
        if len(plan.tweets) < 4:
            return plan

        new_tweets = []
        cliff_idx = 0
        for i, tweet in enumerate(plan.tweets):
            new_tweets.append(tweet)
            # Add cliff-hanger after every N body tweets
            if (i + 1) % every_n == 0 and i < len(plan.tweets) - 1:
                phrase = CLIFF_HANGER_PHRASES[cliff_idx % len(CLIFF_HANGER_PHRASES)]
                bridge = ThreadTweet(
                    index=len(new_tweets),
                    text=phrase,
                    role=TweetRole.BRIDGE,
                )
                new_tweets.append(bridge)
                cliff_idx += 1

        # Re-index
        for i, tweet in enumerate(new_tweets):
            tweet.index = i

        plan.tweets = new_tweets
        plan.total_chars = sum(len(t.text) for t in new_tweets)
        self._save_thread(plan)
        return plan

    def analyze_thread(self, plan: ThreadPlan) -> Dict[str, Any]:
        """Comprehensive thread analysis."""
        if not plan.tweets:
            return {"error": "empty thread"}

        readability_scores = [
            self.readability.tweet_readability(t.text) for t in plan.tweets
        ]

        avg_readability = sum(r["flesch_score"] for r in readability_scores) / len(readability_scores)
        over_limit = [i for i, t in enumerate(plan.tweets) if len(t.text) > 280]

        # Engagement prediction per position
        position_predictions = [
            {
                "index": i,
                "retention": self.predictor.predict_tweet_position(i, len(plan.tweets)),
                "role": plan.tweets[i].role.value,
                "chars": len(plan.tweets[i].text),
            }
            for i in range(len(plan.tweets))
        ]

        # Overall prediction
        overall = self.predictor.predict_thread(plan)

        return {
            "thread_id": plan.thread_id,
            "title": plan.title,
            "template": plan.template.value,
            "tweet_count": len(plan.tweets),
            "total_chars": plan.total_chars,
            "avg_readability": round(avg_readability, 1),
            "tweets_over_limit": over_limit,
            "position_analysis": position_predictions,
            "engagement_prediction": overall,
            "roles": {role.value: sum(1 for t in plan.tweets if t.role == role)
                      for role in TweetRole},
            "has_hook": any(t.role == TweetRole.HOOK for t in plan.tweets),
            "has_cta": any(t.role == TweetRole.CTA for t in plan.tweets),
            "readability_details": readability_scores,
        }

    def optimize_thread(self, plan: ThreadPlan) -> Dict[str, Any]:
        """Suggest optimizations for a thread."""
        analysis = self.analyze_thread(plan)
        suggestions = []

        if not analysis["has_hook"]:
            suggestions.append({
                "type": "structure",
                "severity": "high",
                "message": "Thread missing hook tweet. First tweet should grab attention.",
            })

        if not analysis["has_cta"]:
            suggestions.append({
                "type": "structure",
                "severity": "medium",
                "message": "Thread missing CTA. Add a call-to-action as the final tweet.",
            })

        if analysis["tweet_count"] > 15:
            suggestions.append({
                "type": "length",
                "severity": "medium",
                "message": f"Thread has {analysis['tweet_count']} tweets. Consider condensing to 8-12 for optimal engagement.",
            })

        if analysis["avg_readability"] < 50:
            suggestions.append({
                "type": "readability",
                "severity": "medium",
                "message": f"Average readability score {analysis['avg_readability']} is low. Simplify language.",
            })

        for idx in analysis["tweets_over_limit"]:
            suggestions.append({
                "type": "char_limit",
                "severity": "high",
                "message": f"Tweet {idx + 1} exceeds 280 characters ({len(plan.tweets[idx].text)} chars).",
            })

        # Check for consecutive long tweets
        for i in range(len(plan.tweets) - 1):
            if plan.tweets[i].word_count > 40 and plan.tweets[i + 1].word_count > 40:
                suggestions.append({
                    "type": "density",
                    "severity": "low",
                    "message": f"Tweets {i + 1} and {i + 2} are both text-heavy. Add a media or short tweet between.",
                })
                break

        return {
            "thread_id": plan.thread_id,
            "current_score": analysis["engagement_prediction"]["score"],
            "suggestions": suggestions,
            "suggestion_count": len(suggestions),
            "high_severity": sum(1 for s in suggestions if s["severity"] == "high"),
        }

    def schedule_thread(self, thread_id: str, scheduled_at: str,
                        drip_minutes: int = 0) -> bool:
        """Schedule a thread for publishing."""
        conn = self._get_conn()
        result = conn.execute("""
            UPDATE threads
            SET status = ?, scheduled_at = ?, drip_interval_minutes = ?
            WHERE thread_id = ?
        """, (ThreadStatus.SCHEDULED.value, scheduled_at, drip_minutes, thread_id))
        conn.commit()
        updated = result.rowcount > 0
        conn.close()
        return updated

    def update_status(self, thread_id: str, status: ThreadStatus) -> bool:
        """Update thread status."""
        conn = self._get_conn()
        result = conn.execute(
            "UPDATE threads SET status = ? WHERE thread_id = ?",
            (status.value, thread_id)
        )
        conn.commit()
        updated = result.rowcount > 0
        conn.close()
        return updated

    def record_analytics(self, thread_id: str,
                         tweet_engagement: Dict[int, Dict[str, int]]) -> Dict[str, Any]:
        """Record engagement data for thread tweets."""
        conn = self._get_conn()

        total_impressions = 0
        total_likes = 0
        total_retweets = 0
        total_replies = 0
        best_idx = 0
        best_engagement = 0
        worst_idx = 0
        worst_engagement = float('inf')

        for idx, eng in tweet_engagement.items():
            conn.execute("""
                UPDATE thread_tweets SET engagement = ?
                WHERE thread_id = ? AND tweet_index = ?
            """, (json.dumps(eng), thread_id, idx))

            impressions = eng.get("impressions", 0)
            likes = eng.get("likes", 0)
            retweets = eng.get("retweets", 0)
            replies = eng.get("replies", 0)

            total_impressions += impressions
            total_likes += likes
            total_retweets += retweets
            total_replies += replies

            tweet_total = likes + retweets + replies
            if tweet_total > best_engagement:
                best_engagement = tweet_total
                best_idx = idx
            if tweet_total < worst_engagement:
                worst_engagement = tweet_total
                worst_idx = idx

        n_tweets = len(tweet_engagement)
        completion = 1.0
        if n_tweets > 1:
            first_imp = tweet_engagement.get(0, {}).get("impressions", 1)
            last_imp = tweet_engagement.get(n_tweets - 1, {}).get("impressions", 0)
            completion = round(last_imp / max(first_imp, 1), 3) if first_imp else 0.0

        conn.execute("""
            INSERT INTO thread_analytics
            (thread_id, total_impressions, total_likes, total_retweets,
             total_replies, best_tweet_index, drop_off_tweet_index, completion_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (thread_id, total_impressions, total_likes, total_retweets,
              total_replies, best_idx, worst_idx, completion))

        conn.commit()
        conn.close()

        return {
            "thread_id": thread_id,
            "total_impressions": total_impressions,
            "total_likes": total_likes,
            "total_retweets": total_retweets,
            "total_replies": total_replies,
            "best_tweet_index": best_idx,
            "drop_off_tweet_index": worst_idx,
            "completion_rate": completion,
        }

    def delete_thread(self, thread_id: str) -> bool:
        """Delete a thread and its tweets."""
        conn = self._get_conn()
        conn.execute("DELETE FROM thread_tweets WHERE thread_id = ?", (thread_id,))
        conn.execute("DELETE FROM thread_analytics WHERE thread_id = ?", (thread_id,))
        result = conn.execute("DELETE FROM threads WHERE thread_id = ?", (thread_id,))
        conn.commit()
        deleted = result.rowcount > 0
        conn.close()
        return deleted

    def get_template_info(self, template: ThreadTemplate) -> Dict[str, Any]:
        """Get information about a thread template."""
        structure = TEMPLATE_STRUCTURES.get(template, [])
        return {
            "template": template.value,
            "structure": [r.value for r in structure],
            "tweet_count": len(structure),
            "has_hook": TweetRole.HOOK in structure,
            "has_cta": TweetRole.CTA in structure,
            "has_climax": TweetRole.CLIMAX in structure,
            "description": {
                ThreadTemplate.STORY: "Hook → Setup → Conflict → Resolution → CTA",
                ThreadTemplate.TUTORIAL: "Problem → Step-by-step → Summary → CTA",
                ThreadTemplate.LISTICLE: "Hook → N items → Bonus → CTA",
                ThreadTemplate.DEBATE: "Thesis → Arguments → Counter → Conclusion",
                ThreadTemplate.CASE_STUDY: "Context → Challenge → Solution → Results → Takeaway",
                ThreadTemplate.THREAD_BOMB: "Hook → Rapid-fire facts → Summary",
                ThreadTemplate.PERSONAL: "Hook → Backstory → Lesson → Ask",
            }.get(template, "Custom structure"),
        }

    def get_analytics_history(self, thread_id: str,
                              limit: int = 10) -> List[Dict[str, Any]]:
        """Get analytics history for a thread."""
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT * FROM thread_analytics
            WHERE thread_id = ?
            ORDER BY snapshot_at DESC LIMIT ?
        """, (thread_id, limit)).fetchall()
        conn.close()
        return [dict(r) for r in rows]
