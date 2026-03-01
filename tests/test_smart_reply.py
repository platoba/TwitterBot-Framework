"""Tests for SmartReplyEngine"""
import pytest
from bot.smart_reply import (
    SmartReplyEngine, ReplyTemplate, ReplyMatcher, ConversationTracker,
    MatchType, SentimentFilter, MatchResult,
)


@pytest.fixture
def engine():
    e = SmartReplyEngine(db_path=":memory:")
    yield e
    e.close()


@pytest.fixture
def sample_template():
    return ReplyTemplate(
        id="t1", name="greeting", pattern="hello,hi,hey",
        response_text="Hello! Thanks for reaching out!",
        match_type=MatchType.KEYWORD, priority=10,
        cooldown_seconds=0, max_uses_per_day=1000,
    )


@pytest.fixture
def regex_template():
    return ReplyTemplate(
        id="t2", name="question", pattern=r"\?$",
        response_text="Great question!",
        match_type=MatchType.REGEX, priority=5,
    )


class TestReplyTemplate:
    def test_create_template(self, sample_template):
        assert sample_template.id == "t1"
        assert sample_template.match_type == MatchType.KEYWORD
        assert sample_template.enabled is True

    def test_default_created_at(self):
        t = ReplyTemplate(id="x", name="x", pattern="x", response_text="x")
        assert t.created_at != ""

    def test_string_match_type(self):
        t = ReplyTemplate(id="x", name="x", pattern="x", response_text="x", match_type="regex")
        assert t.match_type == MatchType.REGEX

    def test_string_sentiment_filter(self):
        t = ReplyTemplate(id="x", name="x", pattern="x", response_text="x", sentiment_filter="positive")
        assert t.sentiment_filter == SentimentFilter.POSITIVE


class TestReplyMatcher:
    def test_keyword_match(self, sample_template):
        matcher = ReplyMatcher()
        result = matcher.match("hello world", sample_template)
        assert result is not None
        assert result.score > 0

    def test_keyword_no_match(self, sample_template):
        matcher = ReplyMatcher()
        result = matcher.match("goodbye world", sample_template)
        assert result is None

    def test_regex_match(self, regex_template):
        matcher = ReplyMatcher()
        result = matcher.match("What time is it?", regex_template)
        assert result is not None

    def test_regex_no_match(self, regex_template):
        matcher = ReplyMatcher()
        result = matcher.match("No question here", regex_template)
        assert result is None

    def test_exact_match(self):
        t = ReplyTemplate(id="e", name="e", pattern="exact text", response_text="ok", match_type=MatchType.EXACT)
        matcher = ReplyMatcher()
        assert matcher.match("exact text", t) is not None
        assert matcher.match("not exact text", t) is None

    def test_contains_match(self):
        t = ReplyTemplate(id="c", name="c", pattern="help", response_text="ok", match_type=MatchType.CONTAINS)
        matcher = ReplyMatcher()
        assert matcher.match("I need help please", t) is not None
        assert matcher.match("I need nothing", t) is None

    def test_disabled_template(self, sample_template):
        sample_template.enabled = False
        matcher = ReplyMatcher()
        assert matcher.match("hello", sample_template) is None

    def test_invalid_regex(self):
        t = ReplyTemplate(id="bad", name="bad", pattern="[invalid", response_text="x", match_type=MatchType.REGEX)
        matcher = ReplyMatcher()
        assert matcher.match("test", t) is None

    def test_keyword_multiple_match(self):
        t = ReplyTemplate(id="m", name="m", pattern="hello,world", response_text="ok")
        matcher = ReplyMatcher()
        result = matcher.match("hello world", t)
        assert result is not None
        assert result.score == 1.0  # both keywords matched


class TestConversationTracker:
    def test_no_reply_initially(self):
        tracker = ConversationTracker()
        assert tracker.has_replied("user1", "t1") is False

    def test_record_and_check(self):
        tracker = ConversationTracker()
        tracker.record_reply("user1", "t1")
        assert tracker.has_replied("user1", "t1") is True

    def test_different_user(self):
        tracker = ConversationTracker()
        tracker.record_reply("user1", "t1")
        assert tracker.has_replied("user2", "t1") is False

    def test_clear(self):
        tracker = ConversationTracker()
        tracker.record_reply("user1", "t1")
        tracker.clear()
        assert tracker.has_replied("user1", "t1") is False

    def test_cleanup_expired(self):
        tracker = ConversationTracker(dedup_window_seconds=0)
        tracker.record_reply("user1", "t1")
        tracker.cleanup_expired()
        assert tracker.has_replied("user1", "t1") is False


class TestSmartReplyEngine:
    def test_add_template(self, engine, sample_template):
        assert engine.add_template(sample_template) is True
        assert len(engine.list_templates()) == 1

    def test_remove_template(self, engine, sample_template):
        engine.add_template(sample_template)
        engine.remove_template("t1")
        assert len(engine.list_templates()) == 0

    def test_get_template(self, engine, sample_template):
        engine.add_template(sample_template)
        t = engine.get_template("t1")
        assert t is not None
        assert t.name == "greeting"

    def test_match_reply(self, engine, sample_template):
        engine.add_template(sample_template)
        result = engine.match_reply("hello world", "user1")
        assert result is not None
        assert result.template.id == "t1"

    def test_no_match(self, engine, sample_template):
        engine.add_template(sample_template)
        result = engine.match_reply("goodbye world", "user1")
        assert result is None

    def test_priority_ordering(self, engine):
        t1 = ReplyTemplate(id="low", name="low", pattern="help", response_text="low", priority=1, cooldown_seconds=0, max_uses_per_day=1000)
        t2 = ReplyTemplate(id="high", name="high", pattern="help", response_text="high", priority=10, cooldown_seconds=0, max_uses_per_day=1000)
        engine.add_template(t1)
        engine.add_template(t2)
        result = engine.match_reply("I need help", "user1")
        assert result.template.id == "high"

    def test_blacklist(self, engine, sample_template):
        engine.add_template(sample_template)
        engine.add_to_blacklist("blocked_user")
        result = engine.match_reply("hello", "blocked_user")
        assert result is None

    def test_remove_from_blacklist(self, engine, sample_template):
        engine.add_template(sample_template)
        engine.add_to_blacklist("user1")
        engine.remove_from_blacklist("user1")
        result = engine.match_reply("hello", "user1")
        assert result is not None

    def test_whitelist_mode(self, engine, sample_template):
        engine.add_template(sample_template)
        engine.set_whitelist_mode(True)
        assert engine.match_reply("hello", "user1") is None
        engine.add_to_whitelist("user1")
        assert engine.match_reply("hello", "user1") is not None

    def test_execute_reply(self, engine, sample_template):
        engine.add_template(sample_template)
        assert engine.execute_reply("tweet123", "user1", "t1", "Hello!") is True

    def test_reply_stats(self, engine, sample_template):
        engine.add_template(sample_template)
        engine.execute_reply("tweet1", "user1", "t1", "Hi!")
        engine.execute_reply("tweet2", "user2", "t1", "Hi!")
        stats = engine.get_reply_stats()
        assert stats["total_replies"] == 2
        assert stats["unique_users"] == 2

    def test_reply_history(self, engine, sample_template):
        engine.add_template(sample_template)
        engine.execute_reply("tweet1", "user1", "t1", "Hi!")
        history = engine.get_reply_history()
        assert len(history) == 1
        assert history[0]["tweet_id"] == "tweet1"

    def test_daily_limit(self, engine):
        t = ReplyTemplate(id="limited", name="limited", pattern="test", response_text="ok",
                          cooldown_seconds=0, max_uses_per_day=1)
        engine.add_template(t)
        engine.execute_reply("tw1", "u1", "limited", "ok")
        result = engine.match_reply("test again", "u2")
        assert result is None  # daily limit reached

    def test_sentiment_filter(self, engine):
        t = ReplyTemplate(id="pos", name="pos", pattern="great",
                          response_text="Thanks!", sentiment_filter=SentimentFilter.POSITIVE,
                          cooldown_seconds=0, max_uses_per_day=1000)
        engine.add_template(t)
        assert engine.match_reply("great job", "u1", sentiment="positive") is not None
        assert engine.match_reply("great job", "u2", sentiment="negative") is None
