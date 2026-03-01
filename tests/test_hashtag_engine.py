"""
Tests for Hashtag Research Engine
"""

import pytest
from bot.hashtag_engine import HashtagEngine, HashtagStats, HashtagSet
from bot.database import Database


@pytest.fixture
def db(tmp_path):
    return Database(str(tmp_path / "test.db"))


@pytest.fixture
def engine(db):
    return HashtagEngine(db)


class TestHashtagStats:
    def test_defaults(self):
        s = HashtagStats(tag="python")
        assert s.usage_count == 0
        assert s.avg_impressions == 0
        assert s.avg_engagements == 0
        assert s.trend == "stable"

    def test_averages(self):
        s = HashtagStats(
            tag="tech", usage_count=10,
            total_impressions=5000, total_engagements=250,
        )
        assert s.avg_impressions == 500
        assert s.avg_engagements == 25

    def test_score_rising(self):
        rising = HashtagStats(
            tag="ai", usage_count=20,
            avg_engagement_rate=5.0, trend="rising",
        )
        falling = HashtagStats(
            tag="old", usage_count=20,
            avg_engagement_rate=5.0, trend="falling",
        )
        assert rising.score > falling.score

    def test_to_dict(self):
        s = HashtagStats(tag="test", usage_count=5)
        d = s.to_dict()
        assert d["tag"] == "test"
        assert "score" in d


class TestHashtagSet:
    def test_tag_count(self):
        hs = HashtagSet(tags=["#ai", "#ml", "#python"])
        assert hs.tag_count == 3

    def test_to_dict(self):
        hs = HashtagSet(
            tags=["#tech"],
            predicted_reach=500,
            competition_level="low",
        )
        d = hs.to_dict()
        assert d["predicted_reach"] == 500
        assert d["competition_level"] == "low"


class TestExtractHashtags:
    def test_basic(self):
        tags = HashtagEngine.extract_hashtags("Hello #world #python")
        assert tags == ["world", "python"]

    def test_no_tags(self):
        tags = HashtagEngine.extract_hashtags("No tags here")
        assert tags == []

    def test_case_insensitive(self):
        tags = HashtagEngine.extract_hashtags("#AI #Python")
        assert tags == ["ai", "python"]

    def test_mixed_content(self):
        tags = HashtagEngine.extract_hashtags("Check out #React and @user #vue")
        assert tags == ["react", "vue"]


class TestRecordUsage:
    def test_record(self, engine):
        engine.record_usage(
            "Hello #python #coding", "tweet1",
            impressions=100, engagements=10,
        )
        stats = engine.get_stats("python")
        assert stats is not None
        assert stats.usage_count == 1
        assert stats.total_impressions == 100

    def test_record_multiple(self, engine):
        engine.record_usage("#ai trends", "t1", impressions=200, engagements=20)
        engine.record_usage("#ai again", "t2", impressions=300, engagements=30)
        stats = engine.get_stats("ai")
        assert stats.usage_count == 2
        assert stats.total_impressions == 500

    def test_co_tags(self, engine):
        engine.record_usage(
            "#python #ml #coding", "t1",
            impressions=100, engagements=10,
        )
        stats = engine.get_stats("python")
        assert "ml" in stats.co_tags or "coding" in stats.co_tags

    def test_no_hashtags(self, engine):
        engine.record_usage("No tags", "t1", impressions=100)
        stats = engine.get_stats("notag")
        assert stats is None


class TestGetStats:
    def test_nonexistent(self, engine):
        assert engine.get_stats("nonexistent") is None

    def test_strip_hash(self, engine):
        engine.record_usage("#python test", impressions=100, engagements=10)
        stats = engine.get_stats("#python")
        assert stats is not None
        assert stats.tag == "python"

    def test_trend_new(self, engine):
        engine.record_usage("#brand_new", impressions=50, engagements=5)
        stats = engine.get_stats("brand_new")
        assert stats.trend == "new"


class TestTopHashtags:
    def test_empty(self, engine):
        result = engine.top_hashtags()
        assert result == []

    def test_ranking(self, engine):
        # High engagement tag
        for i in range(5):
            engine.record_usage(f"#good post {i}", f"g{i}", impressions=100, engagements=20)
        # Low engagement tag
        for i in range(5):
            engine.record_usage(f"#bad post {i}", f"b{i}", impressions=100, engagements=2)

        top = engine.top_hashtags(limit=2, min_usage=3)
        assert len(top) > 0
        assert top[0].tag == "good"

    def test_category_filter(self, engine):
        for i in range(3):
            engine.record_usage(f"#python code {i}", f"t{i}", impressions=100, engagements=10)
        for i in range(3):
            engine.record_usage(f"#marketing tips {i}", f"m{i}", impressions=100, engagements=10)

        tech = engine.top_hashtags(category="tech", min_usage=1)
        assert all(s.tag in HashtagEngine.CATEGORIES["tech"] for s in tech)

    def test_min_usage(self, engine):
        engine.record_usage("#rare tag", impressions=100, engagements=50)
        result = engine.top_hashtags(min_usage=2)
        assert len(result) == 0


class TestSuggestHashtags:
    def test_basic_suggest(self, engine):
        for i in range(3):
            engine.record_usage(f"#python #ai topic {i}", f"t{i}", impressions=200, engagements=20)

        result = engine.suggest_hashtags("Python AI machine learning content")
        assert isinstance(result, HashtagSet)

    def test_empty_no_crash(self, engine):
        result = engine.suggest_hashtags("Random content without history")
        assert isinstance(result, HashtagSet)

    def test_respects_max_tags(self, engine):
        for i in range(10):
            engine.record_usage(f"#tag{i} content", f"t{i}", impressions=100, engagements=10)

        result = engine.suggest_hashtags("content here", max_tags=3)
        assert result.tag_count <= 3


class TestBlacklist:
    def test_blacklist(self, engine):
        engine.blacklist_tag("spam", "Too spammy")
        bl = engine._get_blacklist()
        assert "spam" in bl

    def test_remove_blacklist(self, engine):
        engine.blacklist_tag("test")
        engine.remove_blacklist("test")
        bl = engine._get_blacklist()
        assert "test" not in bl

    def test_suggest_excludes_blacklisted(self, engine):
        for i in range(3):
            engine.record_usage(f"#blocked topic {i}", f"t{i}", impressions=200, engagements=20)
        engine.blacklist_tag("blocked")

        result = engine.suggest_hashtags("topic related")
        assert all("#blocked" not in t for t in result.tags)


class TestPostingHours:
    def test_empty(self, engine):
        hours = engine.best_posting_hours()
        assert hours == []

    def test_with_data(self, engine):
        engine.record_usage(
            "#test morning", impressions=100, engagements=20,
            posted_at="2026-02-28T09:00:00+00:00",
        )
        engine.record_usage(
            "#test evening", impressions=100, engagements=5,
            posted_at="2026-02-28T22:00:00+00:00",
        )
        hours = engine.best_posting_hours(limit=2)
        assert len(hours) > 0
        assert "hour" in hours[0]


class TestFormatReport:
    def test_empty_report(self, engine):
        report = engine.format_report()
        assert "暂无" in report

    def test_with_data(self, engine):
        for i in range(5):
            engine.record_usage(
                f"#python tip #{i}", f"t{i}",
                impressions=200, engagements=20
            )
        report = engine.format_report()
        assert "Top Hashtags" in report
        assert "python" in report
