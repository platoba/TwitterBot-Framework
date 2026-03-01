"""
Tests for Content Recycler v1.0
"""
import json
import tempfile
from datetime import datetime, timezone, timedelta

import pytest
from bot.content_recycler import (
    RecycleStrategy, ContentCategory, TweetRecord, RecycleCandidate,
    PerformanceScanner, FreshnessChecker, StrategySuggester,
    RecycleScheduler, ContentRecycler,
)


# ── Fixtures ──

@pytest.fixture
def old_date():
    return (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()

@pytest.fixture
def recent_date():
    return (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()

@pytest.fixture
def sample_tweets(old_date):
    return [
        TweetRecord(tweet_id="t1", text="How to build a startup from scratch #startup", likes=200, retweets=50, replies=30, impressions=10000, created_at=old_date, category="how_to"),
        TweetRecord(tweet_id="t2", text="Bitcoin just hit $100K! Breaking news!", likes=500, retweets=200, replies=100, impressions=50000, created_at=old_date, category="news"),
        TweetRecord(tweet_id="t3", text="10 tips for productivity in 2025", likes=150, retweets=30, replies=20, impressions=8000, created_at=old_date, category="how_to"),
        TweetRecord(tweet_id="t4", text="Nice weather today", likes=5, retweets=0, replies=1, impressions=500, created_at=old_date),
        TweetRecord(tweet_id="t5", text="What do you think about AI? Drop your thoughts below 👇", likes=100, retweets=15, replies=50, impressions=6000, created_at=old_date, category="engagement"),
    ]

@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


# ── TweetRecord Tests ──

class TestTweetRecord:
    def test_total_engagement(self):
        t = TweetRecord(tweet_id="t1", text="hi", likes=100, retweets=20, replies=10, quotes=5)
        assert t.total_engagement == 135

    def test_engagement_rate(self):
        t = TweetRecord(tweet_id="t1", text="hi", likes=100, retweets=0, replies=0, impressions=1000)
        assert t.engagement_rate == 0.1

    def test_engagement_rate_zero_impressions(self):
        t = TweetRecord(tweet_id="t1", text="hi", likes=100)
        assert t.engagement_rate == 0.0

    def test_virality_score(self):
        t = TweetRecord(tweet_id="t1", text="hi", likes=100, retweets=50, replies=20, quotes=10)
        expected = 50 * 2 + 10 * 3 + 100 + 20 * 1.5
        assert t.virality_score == expected

    def test_to_dict(self):
        t = TweetRecord(tweet_id="t1", text="hello", likes=10)
        d = t.to_dict()
        assert d["tweet_id"] == "t1"
        assert d["likes"] == 10


# ── PerformanceScanner Tests ──

class TestPerformanceScanner:
    def test_scan_top_tweets(self, sample_tweets):
        scanner = PerformanceScanner(min_engagement=10, min_age_days=14)
        top = scanner.scan(sample_tweets, top_pct=0.5)
        assert len(top) > 0
        # t2 (500 likes) should be first
        assert top[0].tweet_id == "t2"

    def test_scan_filters_low_engagement(self, sample_tweets):
        scanner = PerformanceScanner(min_engagement=100, min_age_days=14)
        top = scanner.scan(sample_tweets, top_pct=1.0)
        # t4 (5 likes) should be filtered out
        ids = [t.tweet_id for t in top]
        assert "t4" not in ids

    def test_scan_filters_recent(self, recent_date):
        scanner = PerformanceScanner(min_engagement=1, min_age_days=14)
        tweets = [TweetRecord(tweet_id="t1", text="hi", likes=100, created_at=recent_date)]
        top = scanner.scan(tweets)
        assert len(top) == 0

    def test_score_performance(self, sample_tweets):
        scanner = PerformanceScanner()
        score = scanner.score_performance(sample_tweets[1], sample_tweets)  # t2 highest
        assert score > 0.9

    def test_score_performance_lowest(self, sample_tweets):
        scanner = PerformanceScanner()
        score = scanner.score_performance(sample_tweets[3], sample_tweets)  # t4 lowest
        assert score < 0.1

    def test_find_evergreen(self, sample_tweets):
        scanner = PerformanceScanner(min_engagement=10)
        evergreen = scanner.find_evergreen(sample_tweets)
        # t2 has "breaking news" → should be filtered as time-sensitive
        ids = [t.tweet_id for t in evergreen]
        assert "t2" not in ids
        # t1 and t3 should be evergreen
        assert "t1" in ids or "t3" in ids

    def test_find_evergreen_filters_dates(self, old_date):
        scanner = PerformanceScanner(min_engagement=5)
        tweets = [
            TweetRecord(tweet_id="t1", text="Meeting on 2025-01-15", likes=50, created_at=old_date),
            TweetRecord(tweet_id="t2", text="Timeless wisdom here", likes=50, created_at=old_date),
        ]
        evergreen = scanner.find_evergreen(tweets)
        ids = [t.tweet_id for t in evergreen]
        assert "t1" not in ids
        assert "t2" in ids

    def test_scan_empty(self):
        scanner = PerformanceScanner()
        assert scanner.scan([]) == []


# ── FreshnessChecker Tests ──

class TestFreshnessChecker:
    def test_fresh_content(self):
        checker = FreshnessChecker()
        t = TweetRecord(
            tweet_id="t1", text="hi",
            created_at=(datetime.now(timezone.utc) - timedelta(days=10)).isoformat(),
            category="how_to",
        )
        score = checker.check(t)
        assert score == 1.0

    def test_stale_news(self):
        checker = FreshnessChecker()
        t = TweetRecord(
            tweet_id="t1", text="breaking news",
            created_at=(datetime.now(timezone.utc) - timedelta(days=30)).isoformat(),
            category="news",
        )
        score = checker.check(t)
        assert score < 0.5

    def test_evergreen_howto(self):
        checker = FreshnessChecker()
        t = TweetRecord(
            tweet_id="t1", text="how to cook",
            created_at=(datetime.now(timezone.utc) - timedelta(days=100)).isoformat(),
            category="how_to",
        )
        score = checker.check(t)
        assert score > 0.5

    def test_categorize_howto(self):
        checker = FreshnessChecker()
        assert checker.categorize("How to build a website step by step") == ContentCategory.HOW_TO

    def test_categorize_data(self):
        checker = FreshnessChecker()
        assert checker.categorize("New study shows 75% of users prefer dark mode") == ContentCategory.DATA

    def test_categorize_engagement(self):
        checker = FreshnessChecker()
        assert checker.categorize("What do you think about this?") == ContentCategory.ENGAGEMENT

    def test_categorize_thread(self):
        checker = FreshnessChecker()
        assert checker.categorize("🧵 Thread about startup lessons") == ContentCategory.THREAD

    def test_categorize_default_long(self):
        checker = FreshnessChecker()
        text = "A" * 250  # long text defaults to insight
        assert checker.categorize(text) == ContentCategory.INSIGHT

    def test_categorize_default_short(self):
        checker = FreshnessChecker()
        assert checker.categorize("A simple thought") == ContentCategory.OPINION


# ── StrategySuggester Tests ──

class TestStrategySuggester:
    def test_suggest_data(self):
        suggester = StrategySuggester()
        t = TweetRecord(tweet_id="t1", text="50% growth in Q4")
        strategies = suggester.suggest(t, ContentCategory.DATA)
        values = [s.value for s in strategies]
        assert "update" in values
        assert "visual" in values

    def test_suggest_howto(self):
        suggester = StrategySuggester()
        t = TweetRecord(tweet_id="t1", text="How to cook pasta perfectly")
        strategies = suggester.suggest(t, ContentCategory.HOW_TO)
        values = [s.value for s in strategies]
        assert "thread_expand" in values

    def test_suggest_short_tweet(self):
        suggester = StrategySuggester()
        t = TweetRecord(tweet_id="t1", text="AI is the future")
        strategies = suggester.suggest(t, ContentCategory.OPINION)
        values = [s.value for s in strategies]
        assert "thread_expand" in values

    def test_suggest_max_4(self):
        suggester = StrategySuggester()
        t = TweetRecord(tweet_id="t1", text="Short", likes=200)
        strategies = suggester.suggest(t, ContentCategory.INSIGHT)
        assert len(strategies) <= 4

    def test_generate_prompt(self):
        suggester = StrategySuggester()
        t = TweetRecord(tweet_id="t1", text="Bitcoin is digital gold")
        prompt = suggester.generate_prompt(t, RecycleStrategy.QUOTE)
        assert "Bitcoin is digital gold" in prompt

    def test_generate_prompt_all_strategies(self):
        suggester = StrategySuggester()
        t = TweetRecord(tweet_id="t1", text="Test tweet")
        for strategy in RecycleStrategy:
            prompt = suggester.generate_prompt(t, strategy)
            assert len(prompt) > 0


# ── RecycleScheduler Tests ──

class TestRecycleScheduler:
    def test_can_recycle_new(self, tmp_dir):
        sched = RecycleScheduler(f"{tmp_dir}/sched.db")
        can, reason = sched.can_recycle("t1")
        assert can is True
        assert reason == "never_recycled"

    def test_schedule_and_history(self, tmp_dir):
        sched = RecycleScheduler(f"{tmp_dir}/sched.db")
        sid = sched.schedule("t1", "original text", "quote", original_engagement=100)
        assert sid > 0
        history = sched.get_history("t1")
        assert len(history) == 1
        assert history[0]["strategy"] == "quote"

    def test_mark_published(self, tmp_dir):
        sched = RecycleScheduler(f"{tmp_dir}/sched.db")
        sid = sched.schedule("t1", "text", "quote")
        sched.mark_published(sid, "new_t1", "recycled text")
        history = sched.get_history("t1")
        assert history[0]["status"] == "published"
        assert history[0]["recycled_id"] == "new_t1"

    def test_cooldown_after_publish(self, tmp_dir):
        sched = RecycleScheduler(f"{tmp_dir}/sched.db")
        sid = sched.schedule("t1", "text", "quote")
        sched.mark_published(sid)
        can, reason = sched.can_recycle("t1", min_interval_days=30)
        assert can is False
        assert "cooldown" in reason

    def test_get_pending(self, tmp_dir):
        sched = RecycleScheduler(f"{tmp_dir}/sched.db")
        sched.schedule("t1", "text", "quote")
        pending = sched.get_pending()
        assert len(pending) >= 1

    def test_performance_comparison_empty(self, tmp_dir):
        sched = RecycleScheduler(f"{tmp_dir}/sched.db")
        result = sched.performance_comparison()
        assert result["comparisons"] == 0

    def test_performance_comparison(self, tmp_dir):
        sched = RecycleScheduler(f"{tmp_dir}/sched.db")
        sid = sched.schedule("t1", "text", "quote", original_engagement=100)
        sched.mark_published(sid, "rt1")
        sched.update_recycled_engagement(sid, 80)
        result = sched.performance_comparison()
        assert result["comparisons"] == 1
        assert result["avg_retention"] == 0.8

    def test_stats(self, tmp_dir):
        sched = RecycleScheduler(f"{tmp_dir}/sched.db")
        sched.schedule("t1", "text", "quote")
        stats = sched.stats()
        assert stats["total_recycled"] == 1
        assert "quote" in stats["by_strategy"]


# ── ContentRecycler Integration ──

class TestContentRecycler:
    def test_find_candidates(self, tmp_dir, sample_tweets):
        recycler = ContentRecycler(db_dir=tmp_dir, min_engagement=10, min_age_days=14)
        candidates = recycler.find_candidates(sample_tweets)
        assert len(candidates) > 0
        # Should be sorted by recycle_score
        if len(candidates) > 1:
            assert candidates[0].recycle_score >= candidates[-1].recycle_score

    def test_find_candidates_with_evergreen(self, tmp_dir, sample_tweets):
        recycler = ContentRecycler(db_dir=tmp_dir, min_engagement=10, min_age_days=14)
        candidates = recycler.find_candidates(sample_tweets, include_evergreen=True)
        assert len(candidates) > 0

    def test_schedule_recycle(self, tmp_dir, sample_tweets):
        recycler = ContentRecycler(db_dir=tmp_dir, min_engagement=10, min_age_days=14)
        candidates = recycler.find_candidates(sample_tweets)
        if candidates:
            sid = recycler.schedule_recycle(candidates[0])
            assert sid > 0

    def test_get_prompts(self, tmp_dir, sample_tweets):
        recycler = ContentRecycler(db_dir=tmp_dir, min_engagement=10, min_age_days=14)
        candidates = recycler.find_candidates(sample_tweets)
        if candidates:
            prompts = recycler.get_prompts(candidates[0])
            assert len(prompts) > 0
            assert "strategy" in prompts[0]
            assert "prompt" in prompts[0]

    def test_export_json(self, tmp_dir, sample_tweets):
        recycler = ContentRecycler(db_dir=tmp_dir, min_engagement=10, min_age_days=14)
        candidates = recycler.find_candidates(sample_tweets)
        report = recycler.export_candidates(candidates, format="json")
        data = json.loads(report)
        assert isinstance(data, list)

    def test_export_text(self, tmp_dir, sample_tweets):
        recycler = ContentRecycler(db_dir=tmp_dir, min_engagement=10, min_age_days=14)
        candidates = recycler.find_candidates(sample_tweets)
        report = recycler.export_candidates(candidates, format="text")
        assert "Content Recycle" in report

    def test_empty_tweets(self, tmp_dir):
        recycler = ContentRecycler(db_dir=tmp_dir)
        candidates = recycler.find_candidates([])
        assert len(candidates) == 0

    def test_all_recent_tweets(self, tmp_dir, recent_date):
        recycler = ContentRecycler(db_dir=tmp_dir, min_age_days=14)
        tweets = [TweetRecord(tweet_id="t1", text="hi", likes=500, created_at=recent_date)]
        candidates = recycler.find_candidates(tweets, include_evergreen=False)
        assert len(candidates) == 0
