"""
Tests for Competitor Analysis Engine
"""

import pytest
import json
from unittest.mock import MagicMock, patch
from bot.competitor import (
    CompetitorAnalyzer, CompetitorProfile, CompetitorComparison,
)
from bot.database import Database
from bot.twitter_api import TwitterAPI


@pytest.fixture
def db(tmp_path):
    return Database(str(tmp_path / "test.db"))


@pytest.fixture
def mock_api():
    api = MagicMock(spec=TwitterAPI)
    api.is_configured = True
    return api


@pytest.fixture
def analyzer(mock_api, db):
    return CompetitorAnalyzer(mock_api, db)


# ── CompetitorProfile Tests ──

class TestCompetitorProfile:
    def test_defaults(self):
        p = CompetitorProfile(username="test")
        assert p.username == "test"
        assert p.followers == 0
        assert p.analyzed_at != ""

    def test_ff_ratio(self):
        p = CompetitorProfile(username="test", followers=1000, following=500)
        assert p.followers_following_ratio == 2.0

    def test_ff_ratio_zero(self):
        p = CompetitorProfile(username="test", followers=1000, following=0)
        assert p.followers_following_ratio == 0

    def test_to_dict(self):
        p = CompetitorProfile(
            username="test", user_id="123", followers=5000,
            following=200, engagement_rate=2.5, avg_likes=50,
            top_hashtags=[("python", 10), ("ai", 5)],
        )
        d = p.to_dict()
        assert d["username"] == "test"
        assert d["followers"] == 5000
        assert d["ff_ratio"] == 25.0
        assert len(d["top_hashtags"]) == 2


class TestCompetitorComparison:
    def test_defaults(self):
        c = CompetitorComparison(my_username="me", competitors=[])
        assert c.my_username == "me"
        assert c.generated_at != ""

    def test_to_dict(self):
        p1 = CompetitorProfile(username="comp1", followers=1000)
        c = CompetitorComparison(
            my_username="me",
            competitors=[p1],
            benchmarks={"test": 1},
            insights=["insight1"],
        )
        d = c.to_dict()
        assert d["my_username"] == "me"
        assert len(d["competitors"]) == 1
        assert len(d["insights"]) == 1


# ── CompetitorAnalyzer Tests ──

class TestCompetitorAnalyzer:
    def test_ensure_table(self, analyzer, db):
        analyzer._ensure_table()
        conn = db._get_conn()
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = [t["name"] for t in tables]
        assert "competitors" in table_names
        assert "competitor_snapshots" in table_names

    def test_add_competitor(self, analyzer, mock_api):
        mock_api.get_user.return_value = {
            "data": {
                "id": "123", "username": "rival",
                "name": "Rival Co", "description": "test",
                "public_metrics": {
                    "followers_count": 5000, "following_count": 200,
                    "tweet_count": 1000, "listed_count": 50,
                },
            }
        }
        mock_api.resolve_username.return_value = "123"
        mock_api.get_user_tweets.return_value = {"data": []}

        profile = analyzer.add_competitor("rival")
        assert profile is not None
        assert profile.username == "rival"
        assert profile.followers == 5000

    def test_add_competitor_fails(self, analyzer, mock_api):
        mock_api.get_user.return_value = None
        profile = analyzer.add_competitor("nonexistent")
        assert profile is None

    def test_remove_competitor(self, analyzer, mock_api):
        # First add
        mock_api.get_user.return_value = {
            "data": {
                "id": "1", "username": "rival",
                "public_metrics": {"followers_count": 100, "following_count": 50,
                                   "tweet_count": 10, "listed_count": 1},
            }
        }
        mock_api.get_user_tweets.return_value = {"data": []}
        analyzer.add_competitor("rival")

        result = analyzer.remove_competitor("rival")
        assert result is True

    def test_remove_nonexistent(self, analyzer):
        result = analyzer.remove_competitor("nobody")
        assert result is False

    def test_list_competitors(self, analyzer, mock_api):
        mock_api.get_user.return_value = {
            "data": {
                "id": "1", "username": "comp1",
                "public_metrics": {"followers_count": 100, "following_count": 50,
                                   "tweet_count": 10, "listed_count": 1},
            }
        }
        mock_api.get_user_tweets.return_value = {"data": []}
        analyzer.add_competitor("comp1")

        comps = analyzer.list_competitors()
        assert len(comps) >= 1
        assert comps[0]["username"] == "comp1"

    def test_analyze_account(self, analyzer, mock_api):
        mock_api.get_user.return_value = {
            "data": {
                "id": "456", "username": "target",
                "name": "Target", "description": "A test account",
                "public_metrics": {
                    "followers_count": 10000, "following_count": 500,
                    "tweet_count": 2000, "listed_count": 100,
                },
                "verified": True,
                "created_at": "2020-01-01T00:00:00Z",
            }
        }
        mock_api.resolve_username.return_value = "456"
        mock_api.get_user_tweets.return_value = {
            "data": [
                {
                    "text": "Check out #python #ai trends 🧵",
                    "public_metrics": {"like_count": 50, "retweet_count": 10, "reply_count": 5},
                    "created_at": "2026-02-27T14:00:00Z",
                },
                {
                    "text": "Great article https://example.com #python",
                    "public_metrics": {"like_count": 30, "retweet_count": 5, "reply_count": 2},
                    "created_at": "2026-02-26T10:00:00Z",
                },
            ]
        }

        profile = analyzer.analyze_account("target")
        assert profile is not None
        assert profile.username == "target"
        assert profile.followers == 10000
        assert profile.avg_likes > 0
        assert len(profile.top_hashtags) > 0
        assert "python" in [t for t, _ in profile.top_hashtags]


class TestTweetAnalysis:
    def test_analyze_tweets_content_types(self, analyzer):
        profile = CompetitorProfile(username="test", followers=1000)
        tweets = [
            {"text": "RT @other: something", "public_metrics": {"like_count": 0, "retweet_count": 0, "reply_count": 0},
             "created_at": "2026-02-27T10:00:00Z"},
            {"text": "@someone check this", "public_metrics": {"like_count": 5, "retweet_count": 0, "reply_count": 0},
             "created_at": "2026-02-27T12:00:00Z"},
            {"text": "Read this https://example.com", "public_metrics": {"like_count": 10, "retweet_count": 2, "reply_count": 1},
             "created_at": "2026-02-27T14:00:00Z"},
            {"text": "🧵 Thread about AI", "public_metrics": {"like_count": 20, "retweet_count": 5, "reply_count": 3},
             "created_at": "2026-02-27T16:00:00Z"},
            {"text": "What do you think?", "public_metrics": {"like_count": 15, "retweet_count": 1, "reply_count": 10},
             "created_at": "2026-02-27T18:00:00Z"},
            {"text": "Just sharing my thoughts", "public_metrics": {"like_count": 8, "retweet_count": 0, "reply_count": 2},
             "created_at": "2026-02-27T20:00:00Z"},
        ]

        analyzer._analyze_tweets(profile, tweets)
        assert "retweet" in profile.content_types
        assert "reply" in profile.content_types
        assert "link_share" in profile.content_types
        assert "thread" in profile.content_types
        assert "question" in profile.content_types
        assert profile.avg_likes > 0

    def test_analyze_empty_tweets(self, analyzer):
        profile = CompetitorProfile(username="test")
        analyzer._analyze_tweets(profile, [])
        assert profile.avg_likes == 0

    def test_posting_frequency(self, analyzer):
        profile = CompetitorProfile(username="test", followers=1000)
        tweets = [
            {"text": "Post 1", "public_metrics": {"like_count": 1, "retweet_count": 0, "reply_count": 0},
             "created_at": "2026-02-20T10:00:00Z"},
            {"text": "Post 2", "public_metrics": {"like_count": 1, "retweet_count": 0, "reply_count": 0},
             "created_at": "2026-02-25T10:00:00Z"},
            {"text": "Post 3", "public_metrics": {"like_count": 1, "retweet_count": 0, "reply_count": 0},
             "created_at": "2026-02-27T10:00:00Z"},
        ]
        analyzer._analyze_tweets(profile, tweets)
        assert profile.posting_frequency > 0


class TestComparison:
    def test_compare(self, analyzer, mock_api):
        # Setup mock for my account
        def mock_get_user(username):
            users = {
                "me": {
                    "data": {"id": "1", "username": "me",
                             "public_metrics": {"followers_count": 5000, "following_count": 300,
                                                "tweet_count": 500, "listed_count": 20}}
                },
                "comp1": {
                    "data": {"id": "2", "username": "comp1",
                             "public_metrics": {"followers_count": 10000, "following_count": 400,
                                                "tweet_count": 1000, "listed_count": 50}}
                },
            }
            return users.get(username)

        mock_api.get_user.side_effect = mock_get_user
        mock_api.resolve_username.return_value = "1"
        mock_api.get_user_tweets.return_value = {"data": []}

        comparison = analyzer.compare("me", ["comp1"])
        assert comparison.my_username == "me"
        assert len(comparison.competitors) == 1

    def test_benchmarks(self, analyzer):
        p1 = CompetitorProfile(username="c1", followers=5000, engagement_rate=2.0,
                               posting_frequency=1.5, avg_likes=30)
        p2 = CompetitorProfile(username="c2", followers=10000, engagement_rate=3.0,
                               posting_frequency=2.0, avg_likes=50)
        my = CompetitorProfile(username="me", followers=7000, engagement_rate=2.5,
                               posting_frequency=1.0, avg_likes=40)

        benchmarks = analyzer._compute_benchmarks(my, [p1, p2])
        assert benchmarks["followers"]["avg"] > 0
        assert benchmarks["engagement_rate"]["avg"] > 0
        assert "my_rank" in benchmarks

    def test_rank(self, analyzer):
        rank = analyzer._rank(50, [30, 70, 50, 90])
        assert rank["rank"] <= rank["total"]
        assert 0 <= rank["percentile"] <= 100

    def test_insights_low_followers(self, analyzer):
        my = CompetitorProfile(username="me", followers=100, engagement_rate=1.0,
                               posting_frequency=0.5)
        comps = [
            CompetitorProfile(username="c1", followers=5000, engagement_rate=2.0,
                              posting_frequency=2.0),
        ]
        benchmarks = analyzer._compute_benchmarks(my, comps)
        insights = analyzer._generate_insights(my, comps, benchmarks)
        assert len(insights) > 0

    def test_insights_high_engagement(self, analyzer):
        my = CompetitorProfile(username="me", followers=10000, engagement_rate=5.0,
                               posting_frequency=3.0)
        comps = [
            CompetitorProfile(username="c1", followers=5000, engagement_rate=1.0,
                              posting_frequency=1.0),
        ]
        benchmarks = analyzer._compute_benchmarks(my, comps)
        insights = analyzer._generate_insights(my, comps, benchmarks)
        # Should have positive insight about engagement
        assert any("互动率" in i for i in insights)


class TestContentGapAnalysis:
    def test_gap_analysis(self, analyzer, mock_api):
        # Pre-populate cache
        analyzer._competitor_cache["me"] = CompetitorProfile(
            username="me",
            top_hashtags=[("python", 5)],
            content_types={"original": 10},
            active_hours=[14, 15],
        )
        analyzer._competitor_cache["comp1"] = CompetitorProfile(
            username="comp1",
            top_hashtags=[("python", 5), ("ai", 10), ("ml", 8)],
            content_types={"original": 5, "thread": 8},
            active_hours=[10, 14, 18],
        )

        gaps = analyzer.content_gap_analysis("me", ["comp1"])
        assert len(gaps["missing_hashtags"]) > 0
        assert "thread" in gaps["missing_content_types"]


class TestFormatting:
    def test_format_comparison(self, analyzer):
        my = CompetitorProfile(username="me", followers=5000,
                               engagement_rate=2.5, posting_frequency=1.5,
                               avg_likes=30)
        c1 = CompetitorProfile(username="comp1", followers=10000,
                               engagement_rate=3.0, avg_likes=50)

        comparison = CompetitorComparison(
            my_username="me",
            my_profile=my,
            competitors=[c1],
            benchmarks={"followers": {"avg": 10000},
                        "engagement_rate": {"avg": 3.0},
                        "posting_frequency": {"avg": 2.0},
                        "my_rank": {"followers": {"rank": 2, "total": 2},
                                    "engagement": {"rank": 2, "total": 2}}},
            insights=["Test insight"],
        )

        text = analyzer.format_comparison(comparison)
        assert "竞品分析报告" in text
        assert "@me" in text
        assert "@comp1" in text
        assert "Test insight" in text


class TestGrowthHistory:
    def test_save_and_get_history(self, analyzer, mock_api, db):
        profile = CompetitorProfile(
            username="test", followers=1000, following=100,
            tweet_count=500, engagement_rate=2.0, avg_likes=20,
        )
        analyzer._ensure_table()
        # Insert directly into competitors table (FK constraint requires it)
        import json
        conn = db._get_conn()
        conn.execute(
            "INSERT OR REPLACE INTO competitors (username, user_id, profile_data) VALUES (?, ?, ?)",
            ("test", "", json.dumps(profile.to_dict())),
        )
        conn.commit()
        analyzer._save_snapshot(profile)

        history = analyzer.get_growth_history("test")
        assert len(history) == 1
        assert history[0]["followers"] == 1000
