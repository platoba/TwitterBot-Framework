"""
Tests for Audience Analyzer
"""

import pytest
from unittest.mock import MagicMock
from bot.audience import AudienceAnalyzer, AudienceProfile, AudienceSegment
from bot.database import Database
from bot.twitter_api import TwitterAPI


@pytest.fixture
def db(tmp_path):
    return Database(str(tmp_path / "test.db"))


@pytest.fixture
def mock_api():
    return MagicMock(spec=TwitterAPI)


@pytest.fixture
def analyzer(mock_api, db):
    return AudienceAnalyzer(mock_api, db)


SAMPLE_USERS = [
    {
        "id": "1", "username": "alice", "name": "Alice",
        "description": "Software developer and AI enthusiast",
        "public_metrics": {
            "followers_count": 500, "following_count": 200,
            "tweet_count": 1000, "listed_count": 5,
        },
        "verified": False,
        "created_at": "2022-01-01T00:00:00Z",
        "location": "San Francisco",
    },
    {
        "id": "2", "username": "bob", "name": "Bob",
        "description": "Entrepreneur and startup founder",
        "public_metrics": {
            "followers_count": 15000, "following_count": 500,
            "tweet_count": 5500, "listed_count": 50,
        },
        "verified": True,
        "created_at": "2018-06-15T00:00:00Z",
        "location": "New York",
    },
    {
        "id": "3", "username": "carol", "name": "Carol",
        "description": "Designer and content creator",
        "public_metrics": {
            "followers_count": 3000, "following_count": 800,
            "tweet_count": 2000, "listed_count": 15,
        },
        "verified": False,
        "created_at": "2020-03-20T00:00:00Z",
        "location": "San Francisco",
    },
]


class TestAudienceSegment:
    def test_basic(self):
        seg = AudienceSegment(name="test", description="Test segment",
                              user_ids=["1", "2", "3"])
        assert seg.size == 3

    def test_to_dict(self):
        seg = AudienceSegment(name="vip", description="VIP users",
                              user_ids=["1"], criteria={"min_followers": 10000})
        d = seg.to_dict()
        assert d["name"] == "vip"
        assert d["size"] == 1


class TestAudienceProfile:
    def test_defaults(self):
        p = AudienceProfile()
        assert p.total_analyzed == 0
        assert p.avg_followers == 0

    def test_to_dict(self):
        p = AudienceProfile(
            total_analyzed=100, verified_pct=5.0,
            avg_followers=1000, avg_following=200,
            median_followers=500,
        )
        d = p.to_dict()
        assert d["total_analyzed"] == 100
        assert d["avg_followers"] == 1000


class TestBuildProfile:
    def test_build_from_users(self, analyzer):
        profile = analyzer.build_profile(SAMPLE_USERS)
        assert profile.total_analyzed == 3
        assert profile.avg_followers > 0
        assert profile.median_followers > 0

    def test_verified_pct(self, analyzer):
        profile = analyzer.build_profile(SAMPLE_USERS)
        # 1 out of 3 is verified
        assert profile.verified_pct == pytest.approx(33.3, abs=0.1)

    def test_locations(self, analyzer):
        profile = analyzer.build_profile(SAMPLE_USERS)
        assert len(profile.top_locations) > 0
        # San Francisco appears twice
        sf_count = next(
            (count for loc, count in profile.top_locations
             if loc == "San Francisco"), 0
        )
        assert sf_count == 2

    def test_interests(self, analyzer):
        profile = analyzer.build_profile(SAMPLE_USERS)
        interest_names = [name for name, _ in profile.top_interests]
        # Alice has "developer", "AI" → tech
        assert "tech" in interest_names
        # Bob has "entrepreneur", "startup", "founder" → business
        assert "business" in interest_names

    def test_follower_tiers(self, analyzer):
        profile = analyzer.build_profile(SAMPLE_USERS)
        assert len(profile.follower_tiers) > 0
        # Alice(500) = nano, Bob(15000) = mid, Carol(3000) = micro
        assert profile.follower_tiers.get("nano", 0) >= 1
        assert profile.follower_tiers.get("mid", 0) >= 1

    def test_account_age(self, analyzer):
        profile = analyzer.build_profile(SAMPLE_USERS)
        assert len(profile.account_age_distribution) > 0

    def test_empty_users(self, analyzer):
        profile = analyzer.build_profile([])
        assert profile.total_analyzed == 0


class TestSegmentation:
    def test_segment_by_tier(self, analyzer):
        segments = analyzer.segment_audience(SAMPLE_USERS)
        tier_segments = [s for s in segments if s.name.startswith("tier_")]
        assert len(tier_segments) > 0

    def test_segment_by_interest(self, analyzer):
        segments = analyzer.segment_audience(SAMPLE_USERS)
        interest_segments = [s for s in segments if s.name.startswith("interest_")]
        assert len(interest_segments) > 0

    def test_high_activity_segment(self, analyzer):
        users = SAMPLE_USERS + [{
            "id": "4", "username": "power",
            "description": "",
            "public_metrics": {
                "followers_count": 100, "following_count": 100,
                "tweet_count": 10000, "listed_count": 0,
            },
        }]
        segments = analyzer.segment_audience(users)
        activity_seg = next(
            (s for s in segments if s.name == "high_activity"), None
        )
        assert activity_seg is not None
        assert activity_seg.size >= 1

    def test_verified_segment(self, analyzer):
        segments = analyzer.segment_audience(SAMPLE_USERS)
        verified_seg = next(
            (s for s in segments if s.name == "verified"), None
        )
        assert verified_seg is not None
        assert verified_seg.size == 1


class TestInfluencers:
    def test_find_influencers(self, analyzer):
        influencers = analyzer.find_influencers(SAMPLE_USERS, min_followers=10000)
        assert len(influencers) == 1
        assert influencers[0]["username"] == "bob"

    def test_influencer_scoring(self, analyzer):
        influencers = analyzer.find_influencers(SAMPLE_USERS, min_followers=0)
        assert len(influencers) > 0
        # Should be sorted by score
        scores = [i["influence_score"] for i in influencers]
        assert scores == sorted(scores, reverse=True)

    def test_top_n(self, analyzer):
        influencers = analyzer.find_influencers(SAMPLE_USERS, min_followers=0,
                                                 top_n=1)
        assert len(influencers) == 1


class TestOverlapAnalysis:
    def test_overlap(self, analyzer, mock_api):
        mock_api.resolve_username.side_effect = lambda u: f"id_{u}"
        mock_api.get_user_followers.side_effect = lambda uid, max_results: {
            "data": [{"id": f"f{i}"} for i in range(3)],
            "meta": {},
        }

        # Cache different follower sets
        analyzer._cache["followers:userA"] = [
            {"id": "f1"}, {"id": "f2"}, {"id": "f3"},
        ]
        analyzer._cache["followers:userB"] = [
            {"id": "f2"}, {"id": "f3"}, {"id": "f4"},
        ]

        result = analyzer.overlap_analysis("userA", "userB")
        assert result["overlap_count"] == 2  # f2, f3
        assert result["unique_a"] == 1  # f1
        assert result["unique_b"] == 1  # f4
        assert result["jaccard_index"] > 0


class TestGrowthForecast:
    def test_forecast(self, analyzer, db):
        for i in range(7):
            db.save_analytics_snapshot("testuser", {
                "followers_count": 1000 + i * 10,
                "following_count": 200,
                "tweet_count": 500 + i,
                "listed_count": 10,
            })

        forecast = analyzer.growth_forecast("testuser", days_ahead=30)
        assert forecast is not None
        assert forecast["current_followers"] > 0
        assert forecast["daily_growth"] > 0

    def test_forecast_insufficient_data(self, analyzer, db):
        db.save_analytics_snapshot("newuser", {
            "followers_count": 100,
            "following_count": 50,
            "tweet_count": 10,
            "listed_count": 0,
        })
        forecast = analyzer.growth_forecast("newuser")
        assert forecast is None


class TestFormatProfile:
    def test_format(self, analyzer):
        profile = AudienceProfile(
            total_analyzed=100,
            verified_pct=5.0,
            avg_followers=5000,
            avg_tweets=1000,
            median_followers=3000,
            follower_tiers={"nano": 30, "micro": 50, "mid": 15, "macro": 5},
            top_interests=[("tech", 40), ("business", 25)],
            top_locations=[("SF", 20), ("NY", 15)],
        )
        text = analyzer.format_profile(profile)
        assert "受众画像" in text
        assert "100" in text
        assert "粉丝层级" in text

    def test_format_empty(self, analyzer):
        profile = AudienceProfile()
        text = analyzer.format_profile(profile)
        assert "受众画像" in text


class TestCache:
    def test_clear_cache(self, analyzer):
        analyzer._cache["test"] = [1, 2, 3]
        analyzer.clear_cache()
        assert len(analyzer._cache) == 0
