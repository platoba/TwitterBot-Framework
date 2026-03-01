"""
Engagement Rules Engine v1.0
规则驱动的自动互动引擎 - 自动点赞/转发/关注/回复

Features:
- Declarative rule definitions (YAML-like dicts)
- Condition matching (keywords, follower count, engagement threshold)
- Action execution with rate limiting
- Rule priority & conflict resolution
- Safety guardrails (cooldown, daily limits, blocklist)
- Action logging & analytics
- Rule templates for common scenarios
"""

import time
import re
import sqlite3
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable


class ActionType(Enum):
    LIKE = "like"
    RETWEET = "retweet"
    REPLY = "reply"
    FOLLOW = "follow"
    BOOKMARK = "bookmark"
    MUTE = "mute"
    BLOCK = "block"
    NOTIFY = "notify"
    TAG = "tag"


class ConditionType(Enum):
    KEYWORD_MATCH = "keyword_match"
    KEYWORD_EXCLUDE = "keyword_exclude"
    HASHTAG_MATCH = "hashtag_match"
    FOLLOWER_MIN = "follower_min"
    FOLLOWER_MAX = "follower_max"
    ENGAGEMENT_MIN = "engagement_min"
    AUTHOR_IN_LIST = "author_in_list"
    AUTHOR_NOT_IN_LIST = "author_not_in_list"
    LANGUAGE = "language"
    IS_REPLY = "is_reply"
    IS_RETWEET = "is_retweet"
    HAS_MEDIA = "has_media"
    HAS_LINK = "has_link"
    TIME_WINDOW = "time_window"
    TWEET_LENGTH_MIN = "tweet_length_min"
    TWEET_LENGTH_MAX = "tweet_length_max"
    VERIFIED_ONLY = "verified_only"
    SENTIMENT = "sentiment"


class RulePriority(Enum):
    LOW = 1
    MEDIUM = 5
    HIGH = 10
    CRITICAL = 20


@dataclass
class RuleCondition:
    """A single condition within a rule."""
    condition_type: ConditionType
    value: any = None
    negate: bool = False

    def evaluate(self, tweet: dict) -> bool:
        """Evaluate condition against a tweet dict."""
        result = self._check(tweet)
        return not result if self.negate else result

    def _check(self, tweet: dict) -> bool:
        text = tweet.get("text", "").lower()
        author = tweet.get("author", {})

        if self.condition_type == ConditionType.KEYWORD_MATCH:
            keywords = self.value if isinstance(self.value, list) else [self.value]
            return any(kw.lower() in text for kw in keywords)

        elif self.condition_type == ConditionType.KEYWORD_EXCLUDE:
            keywords = self.value if isinstance(self.value, list) else [self.value]
            return not any(kw.lower() in text for kw in keywords)

        elif self.condition_type == ConditionType.HASHTAG_MATCH:
            hashtags = [h.lower() for h in tweet.get("hashtags", [])]
            targets = self.value if isinstance(self.value, list) else [self.value]
            return any(t.lower().lstrip("#") in hashtags for t in targets)

        elif self.condition_type == ConditionType.FOLLOWER_MIN:
            return author.get("followers_count", 0) >= self.value

        elif self.condition_type == ConditionType.FOLLOWER_MAX:
            return author.get("followers_count", 0) <= self.value

        elif self.condition_type == ConditionType.ENGAGEMENT_MIN:
            likes = tweet.get("like_count", 0)
            rts = tweet.get("retweet_count", 0)
            return (likes + rts) >= self.value

        elif self.condition_type == ConditionType.AUTHOR_IN_LIST:
            author_id = author.get("id", "")
            return author_id in (self.value or [])

        elif self.condition_type == ConditionType.AUTHOR_NOT_IN_LIST:
            author_id = author.get("id", "")
            return author_id not in (self.value or [])

        elif self.condition_type == ConditionType.LANGUAGE:
            return tweet.get("lang", "") == self.value

        elif self.condition_type == ConditionType.IS_REPLY:
            return bool(tweet.get("in_reply_to_id"))

        elif self.condition_type == ConditionType.IS_RETWEET:
            return text.startswith("rt @")

        elif self.condition_type == ConditionType.HAS_MEDIA:
            return bool(tweet.get("media", []))

        elif self.condition_type == ConditionType.HAS_LINK:
            return bool(re.search(r'https?://', tweet.get("text", "")))

        elif self.condition_type == ConditionType.TWEET_LENGTH_MIN:
            return len(tweet.get("text", "")) >= self.value

        elif self.condition_type == ConditionType.TWEET_LENGTH_MAX:
            return len(tweet.get("text", "")) <= self.value

        elif self.condition_type == ConditionType.VERIFIED_ONLY:
            return author.get("verified", False)

        elif self.condition_type == ConditionType.TIME_WINDOW:
            if isinstance(self.value, dict):
                now_hour = time.localtime().tm_hour
                start = self.value.get("start_hour", 0)
                end = self.value.get("end_hour", 24)
                return start <= now_hour < end
            return True

        return False


@dataclass
class RuleAction:
    """Action to take when rule matches."""
    action_type: ActionType
    params: dict = field(default_factory=dict)
    delay_sec: float = 0
    probability: float = 1.0

    def should_execute(self) -> bool:
        """Probabilistic execution check."""
        import random
        return random.random() < self.probability


@dataclass
class EngagementRule:
    """A complete engagement rule: conditions → actions."""
    rule_id: str
    name: str
    description: str = ""
    conditions: list[RuleCondition] = field(default_factory=list)
    actions: list[RuleAction] = field(default_factory=list)
    priority: RulePriority = RulePriority.MEDIUM
    enabled: bool = True
    match_all: bool = True  # True=AND, False=OR
    max_daily_triggers: int = 100
    cooldown_sec: float = 60
    created_at: float = field(default_factory=time.time)
    tags: list = field(default_factory=list)

    # Runtime state
    _trigger_count: int = field(default=0, repr=False)
    _last_triggered: float = field(default=0, repr=False)

    def matches(self, tweet: dict) -> bool:
        """Check if tweet matches this rule's conditions."""
        if not self.enabled:
            return False
        if self._trigger_count >= self.max_daily_triggers:
            return False
        if time.time() - self._last_triggered < self.cooldown_sec:
            return False
        if not self.conditions:
            return True

        if self.match_all:
            return all(c.evaluate(tweet) for c in self.conditions)
        else:
            return any(c.evaluate(tweet) for c in self.conditions)

    def trigger(self):
        """Record a trigger event."""
        self._trigger_count += 1
        self._last_triggered = time.time()

    def reset_daily(self):
        self._trigger_count = 0

    def summary(self) -> dict:
        return {
            "id": self.rule_id,
            "name": self.name,
            "priority": self.priority.value,
            "enabled": self.enabled,
            "conditions": len(self.conditions),
            "actions": [a.action_type.value for a in self.actions],
            "daily_triggers": f"{self._trigger_count}/{self.max_daily_triggers}",
            "tags": self.tags,
        }


class SafetyGuardrails:
    """Prevent abuse and maintain account safety."""

    def __init__(self):
        self.daily_like_limit: int = 500
        self.daily_retweet_limit: int = 100
        self.daily_follow_limit: int = 200
        self.daily_reply_limit: int = 100
        self.min_action_interval_sec: float = 5
        self.blocklist: set[str] = set()
        self.protected_authors: set[str] = set()

        # Counters
        self._counts: dict[str, int] = {
            "like": 0, "retweet": 0, "follow": 0, "reply": 0,
        }
        self._last_action_time: float = 0

    def can_act(self, action_type: ActionType, target_author_id: str = "") -> tuple[bool, str]:
        """Check if action is safe to perform."""
        if target_author_id in self.blocklist:
            return False, "Author is in blocklist"

        if target_author_id in self.protected_authors:
            return False, "Author is protected (no auto-actions)"

        now = time.time()
        if now - self._last_action_time < self.min_action_interval_sec:
            return False, f"Min interval not met ({self.min_action_interval_sec}s)"

        limits = {
            ActionType.LIKE: ("like", self.daily_like_limit),
            ActionType.RETWEET: ("retweet", self.daily_retweet_limit),
            ActionType.FOLLOW: ("follow", self.daily_follow_limit),
            ActionType.REPLY: ("reply", self.daily_reply_limit),
        }

        if action_type in limits:
            key, limit = limits[action_type]
            if self._counts.get(key, 0) >= limit:
                return False, f"Daily {key} limit reached ({limit})"

        return True, "OK"

    def record_action(self, action_type: ActionType):
        key = action_type.value
        self._counts[key] = self._counts.get(key, 0) + 1
        self._last_action_time = time.time()

    def reset_daily(self):
        self._counts = {k: 0 for k in self._counts}

    def add_to_blocklist(self, author_id: str):
        self.blocklist.add(author_id)

    def remove_from_blocklist(self, author_id: str):
        self.blocklist.discard(author_id)

    def stats(self) -> dict:
        return {
            "counts": dict(self._counts),
            "limits": {
                "like": self.daily_like_limit,
                "retweet": self.daily_retweet_limit,
                "follow": self.daily_follow_limit,
                "reply": self.daily_reply_limit,
            },
            "blocklist_size": len(self.blocklist),
            "protected_size": len(self.protected_authors),
        }


class EngagementRulesEngine:
    """
    Central engine for rule-based Twitter engagement automation.
    """

    def __init__(self, db_path: str = ":memory:"):
        self.rules: dict[str, EngagementRule] = {}
        self.guardrails = SafetyGuardrails()
        self.db_path = db_path
        self._init_db()
        self._action_history: list[dict] = []

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS engagement_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id TEXT NOT NULL,
                action_type TEXT NOT NULL,
                tweet_id TEXT,
                author_id TEXT,
                status TEXT DEFAULT 'executed',
                created_at REAL DEFAULT (strftime('%s','now'))
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rule_stats (
                rule_id TEXT PRIMARY KEY,
                total_triggers INTEGER DEFAULT 0,
                total_actions INTEGER DEFAULT 0,
                last_triggered REAL DEFAULT 0
            )
        """)
        conn.commit()
        conn.close()

    def add_rule(self, rule: EngagementRule) -> bool:
        if rule.rule_id in self.rules:
            return False
        self.rules[rule.rule_id] = rule
        return True

    def remove_rule(self, rule_id: str) -> bool:
        if rule_id not in self.rules:
            return False
        del self.rules[rule_id]
        return True

    def enable_rule(self, rule_id: str) -> bool:
        rule = self.rules.get(rule_id)
        if not rule:
            return False
        rule.enabled = True
        return True

    def disable_rule(self, rule_id: str) -> bool:
        rule = self.rules.get(rule_id)
        if not rule:
            return False
        rule.enabled = False
        return True

    def evaluate(self, tweet: dict) -> list[tuple[EngagementRule, list[RuleAction]]]:
        """Evaluate a tweet against all rules. Returns matched rules and their actions."""
        matches = []
        sorted_rules = sorted(
            self.rules.values(),
            key=lambda r: r.priority.value,
            reverse=True,
        )

        for rule in sorted_rules:
            if rule.matches(tweet):
                executable_actions = []
                author_id = tweet.get("author", {}).get("id", "")

                for action in rule.actions:
                    can, reason = self.guardrails.can_act(action.action_type, author_id)
                    if can and action.should_execute():
                        executable_actions.append(action)

                if executable_actions:
                    matches.append((rule, executable_actions))
                    rule.trigger()

        return matches

    def execute_actions(self, tweet: dict, matches: list[tuple[EngagementRule, list[RuleAction]]]) -> list[dict]:
        """Execute matched actions and log them."""
        results = []
        tweet_id = tweet.get("id", "")
        author_id = tweet.get("author", {}).get("id", "")

        for rule, actions in matches:
            for action in actions:
                result = {
                    "rule_id": rule.rule_id,
                    "rule_name": rule.name,
                    "action": action.action_type.value,
                    "tweet_id": tweet_id,
                    "author_id": author_id,
                    "status": "executed",
                    "ts": time.time(),
                }

                self.guardrails.record_action(action.action_type)
                self._log_action(rule.rule_id, action.action_type.value, tweet_id, author_id)
                results.append(result)

        self._action_history.extend(results)
        return results

    def process_tweet(self, tweet: dict) -> list[dict]:
        """Full pipeline: evaluate + execute."""
        matches = self.evaluate(tweet)
        if not matches:
            return []
        return self.execute_actions(tweet, matches)

    def get_rule(self, rule_id: str) -> Optional[EngagementRule]:
        return self.rules.get(rule_id)

    def list_rules(self, enabled_only: bool = False) -> list[dict]:
        rules = self.rules.values()
        if enabled_only:
            rules = [r for r in rules if r.enabled]
        return [r.summary() for r in sorted(rules, key=lambda r: r.priority.value, reverse=True)]

    def get_stats(self) -> dict:
        total_triggers = sum(r._trigger_count for r in self.rules.values())
        return {
            "total_rules": len(self.rules),
            "enabled_rules": sum(1 for r in self.rules.values() if r.enabled),
            "total_triggers_today": total_triggers,
            "total_actions": len(self._action_history),
            "guardrails": self.guardrails.stats(),
        }

    def reset_daily(self):
        """Daily reset for all counters."""
        for rule in self.rules.values():
            rule.reset_daily()
        self.guardrails.reset_daily()
        self._action_history.clear()

    def _log_action(self, rule_id: str, action_type: str, tweet_id: str, author_id: str):
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute(
                "INSERT INTO engagement_actions (rule_id, action_type, tweet_id, author_id) VALUES (?,?,?,?)",
                (rule_id, action_type, tweet_id, author_id),
            )
            conn.execute("""
                INSERT INTO rule_stats (rule_id, total_triggers, total_actions, last_triggered)
                VALUES (?, 1, 1, strftime('%s','now'))
                ON CONFLICT(rule_id) DO UPDATE SET
                    total_triggers = total_triggers + 1,
                    total_actions = total_actions + 1,
                    last_triggered = strftime('%s','now')
            """, (rule_id,))
            conn.commit()
            conn.close()
        except Exception:
            pass

    def get_action_history(self, rule_id: Optional[str] = None, limit: int = 50) -> list[dict]:
        try:
            conn = sqlite3.connect(self.db_path)
            if rule_id:
                rows = conn.execute(
                    "SELECT rule_id, action_type, tweet_id, author_id, status, created_at FROM engagement_actions WHERE rule_id=? ORDER BY id DESC LIMIT ?",
                    (rule_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT rule_id, action_type, tweet_id, author_id, status, created_at FROM engagement_actions ORDER BY id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            conn.close()
            return [{"rule_id": r[0], "action": r[1], "tweet_id": r[2], "author_id": r[3], "status": r[4], "ts": r[5]} for r in rows]
        except Exception:
            return self._action_history[-limit:]


# ──── Rule Templates ────

class RuleTemplates:
    """Pre-built rule templates for common scenarios."""

    @staticmethod
    def niche_engagement(rule_id: str, keywords: list[str], min_followers: int = 100) -> EngagementRule:
        """Auto-like tweets matching niche keywords from accounts with min followers."""
        return EngagementRule(
            rule_id=rule_id,
            name=f"Niche Engagement: {', '.join(keywords[:3])}",
            description="Auto-like niche-relevant tweets",
            conditions=[
                RuleCondition(ConditionType.KEYWORD_MATCH, keywords),
                RuleCondition(ConditionType.FOLLOWER_MIN, min_followers),
                RuleCondition(ConditionType.IS_RETWEET, negate=True),
            ],
            actions=[
                RuleAction(ActionType.LIKE, probability=0.8),
            ],
            priority=RulePriority.MEDIUM,
            max_daily_triggers=200,
            cooldown_sec=30,
        )

    @staticmethod
    def influencer_engage(rule_id: str, min_followers: int = 10000) -> EngagementRule:
        """Engage with influencer tweets (like + bookmark)."""
        return EngagementRule(
            rule_id=rule_id,
            name=f"Influencer Engage (>{min_followers} followers)",
            description="Like and bookmark influencer tweets",
            conditions=[
                RuleCondition(ConditionType.FOLLOWER_MIN, min_followers),
                RuleCondition(ConditionType.ENGAGEMENT_MIN, 50),
            ],
            actions=[
                RuleAction(ActionType.LIKE, probability=0.9),
                RuleAction(ActionType.BOOKMARK, probability=0.5),
            ],
            priority=RulePriority.HIGH,
            max_daily_triggers=50,
            cooldown_sec=120,
        )

    @staticmethod
    def follow_back(rule_id: str, min_followers: int = 50, max_followers: int = 5000) -> EngagementRule:
        """Auto follow-back accounts in a follower range."""
        return EngagementRule(
            rule_id=rule_id,
            name="Smart Follow-Back",
            description="Follow back accounts within follower range",
            conditions=[
                RuleCondition(ConditionType.FOLLOWER_MIN, min_followers),
                RuleCondition(ConditionType.FOLLOWER_MAX, max_followers),
            ],
            actions=[
                RuleAction(ActionType.FOLLOW, probability=0.7, delay_sec=10),
            ],
            priority=RulePriority.LOW,
            max_daily_triggers=100,
            cooldown_sec=300,
        )

    @staticmethod
    def viral_amplify(rule_id: str, min_engagement: int = 500) -> EngagementRule:
        """Retweet viral tweets for visibility."""
        return EngagementRule(
            rule_id=rule_id,
            name=f"Viral Amplify (>{min_engagement} engagement)",
            description="Retweet high-engagement tweets",
            conditions=[
                RuleCondition(ConditionType.ENGAGEMENT_MIN, min_engagement),
                RuleCondition(ConditionType.IS_RETWEET, negate=True),
                RuleCondition(ConditionType.HAS_MEDIA),
            ],
            actions=[
                RuleAction(ActionType.LIKE),
                RuleAction(ActionType.RETWEET, probability=0.6),
            ],
            priority=RulePriority.HIGH,
            max_daily_triggers=20,
            cooldown_sec=600,
        )

    @staticmethod
    def spam_filter(rule_id: str, spam_keywords: list[str] = None) -> EngagementRule:
        """Mute accounts posting spam content."""
        if spam_keywords is None:
            spam_keywords = ["buy followers", "free giveaway", "click here", "dm for promo"]
        return EngagementRule(
            rule_id=rule_id,
            name="Spam Filter",
            description="Auto-mute spam accounts",
            conditions=[
                RuleCondition(ConditionType.KEYWORD_MATCH, spam_keywords),
            ],
            actions=[
                RuleAction(ActionType.MUTE),
            ],
            priority=RulePriority.CRITICAL,
            max_daily_triggers=500,
            cooldown_sec=5,
        )

    @staticmethod
    def hashtag_engage(rule_id: str, hashtags: list[str]) -> EngagementRule:
        """Like tweets with specific hashtags."""
        return EngagementRule(
            rule_id=rule_id,
            name=f"Hashtag Engage: {', '.join(hashtags[:3])}",
            conditions=[
                RuleCondition(ConditionType.HASHTAG_MATCH, hashtags),
                RuleCondition(ConditionType.IS_RETWEET, negate=True),
            ],
            actions=[
                RuleAction(ActionType.LIKE, probability=0.7),
            ],
            priority=RulePriority.MEDIUM,
            max_daily_triggers=150,
            cooldown_sec=20,
        )
