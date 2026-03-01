"""
Tests for Influencer Finder v1.0
"""
import json
import os
import tempfile
import pytest
from bot.influencer_finder import (
    InfluencerTier, InfluencerProfile, EngagementSample, NicheConfig,
    NicheScorer, EngagementQualityAnalyzer, GrowthTracker,
    CooperationEstimator, InfluencerRanker, WatchList, InfluencerFinder,
)


# ── Fixtures ──

@pytest.fixture
def niche_config():
    return NicheConfig(
        name="crypto",
        keywords=["bitcoin", "ethereum", "defi", "web3", "blockchain"],
        hashtags=["#crypto", "#bitcoin", "#defi", "#web3"],
        seed_accounts=["elonmusk", "VitalikButerin"],
    )

@pytest.fixture
def sample_profile():
    return InfluencerProfile(
        user_id="123",
        username="crypto_guru",
        display_name="Crypto Guru",
        bio="Bitcoin maximalist | DeFi researcher | Web3 builder",
        followers=25000,
        following=1200,
        tweet_count=5000,
    )

@pytest.fixture
def sample_tweets():
    return [
        {"text": "Bitcoin is about to break ATH! 🚀 #bitcoin #crypto"},
        {"text": "New DeFi protocol launching on Ethereum mainnet"},
        {"text": "Web3 gaming is the future of entertainment"},
        {"text": "Just had a great lunch today"},
        {"text": "Blockchain scalability solved with ZK rollups"},
    ]

@pytest.fixture
def sample_engagement():
    return [
        EngagementSample(tweet_id="t1", likes=100, retweets=20, replies=15, quotes=5, impressions=5000),
        EngagementSample(tweet_id="t2", likes=50, retweets=10, replies=8, quotes=2, impressions=3000),
        EngagementSample(tweet_id="t3", likes=200, retweets=50, replies=30, quotes=10, impressions=10000),
        EngagementSample(tweet_id="t4", likes=30, retweets=5, replies=3, quotes=1, impressions=2000),
    ]

@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


# ── InfluencerTier Tests ──

class TestInfluencerTier:
    def test_nano(self):
        assert InfluencerTier.from_followers(5000) == InfluencerTier.NANO

    def test_micro(self):
        assert InfluencerTier.from_followers(25000) == InfluencerTier.MICRO

    def test_mid(self):
        assert InfluencerTier.from_followers(200000) == InfluencerTier.MID

    def test_macro(self):
        assert InfluencerTier.from_followers(800000) == InfluencerTier.MACRO

    def test_mega(self):
        assert InfluencerTier.from_followers(5000000) == InfluencerTier.MEGA

    def test_boundary_nano_micro(self):
        assert InfluencerTier.from_followers(9999) == InfluencerTier.NANO
        assert InfluencerTier.from_followers(10000) == InfluencerTier.MICRO

    def test_boundary_micro_mid(self):
        assert InfluencerTier.from_followers(49999) == InfluencerTier.MICRO
        assert InfluencerTier.from_followers(50000) == InfluencerTier.MID

    def test_zero_followers(self):
        assert InfluencerTier.from_followers(0) == InfluencerTier.NANO


# ── InfluencerProfile Tests ──

class TestInfluencerProfile:
    def test_to_dict(self, sample_profile):
        d = sample_profile.to_dict()
        assert d["username"] == "crypto_guru"
        assert d["followers"] == 25000

    def test_from_dict(self):
        d = {"user_id": "456", "username": "test", "followers": 1000}
        p = InfluencerProfile.from_dict(d)
        assert p.user_id == "456"
        assert p.followers == 1000

    def test_from_dict_extra_fields(self):
        d = {"user_id": "456", "username": "test", "unknown_field": "x"}
        p = InfluencerProfile.from_dict(d)
        assert p.user_id == "456"

    def test_default_values(self):
        p = InfluencerProfile(user_id="1", username="u")
        assert p.overall_score == 0.0
        assert p.tags == []


# ── EngagementSample Tests ──

class TestEngagementSample:
    def test_total_engagement(self):
        s = EngagementSample(tweet_id="t", likes=100, retweets=20, replies=10, quotes=5)
        assert s.total_engagement == 135

    def test_engagement_rate(self):
        s = EngagementSample(tweet_id="t", likes=100, retweets=20, replies=10, quotes=5, impressions=1000)
        assert s.engagement_rate == 0.135

    def test_engagement_rate_zero_impressions(self):
        s = EngagementSample(tweet_id="t", likes=100)
        assert s.engagement_rate == 0.0


# ── NicheScorer Tests ──

class TestNicheScorer:
    def test_score_bio_relevant(self, niche_config):
        scorer = NicheScorer(niche_config)
        score = scorer.score_bio("Bitcoin trader and DeFi explorer")
        assert score > 0.3

    def test_score_bio_irrelevant(self, niche_config):
        scorer = NicheScorer(niche_config)
        score = scorer.score_bio("I love cooking and gardening")
        assert score == 0.0

    def test_score_bio_empty(self, niche_config):
        scorer = NicheScorer(niche_config)
        assert scorer.score_bio("") == 0.0

    def test_score_tweets_all_relevant(self, niche_config, sample_tweets):
        scorer = NicheScorer(niche_config)
        score = scorer.score_tweets(sample_tweets)
        assert score > 0.5

    def test_score_tweets_empty(self, niche_config):
        scorer = NicheScorer(niche_config)
        assert scorer.score_tweets([]) == 0.0

    def test_combined_score(self, niche_config, sample_profile, sample_tweets):
        scorer = NicheScorer(niche_config)
        score = scorer.score(sample_profile, sample_tweets)
        assert 0.0 <= score <= 1.0
        assert score > 0.3

    def test_empty_config(self):
        scorer = NicheScorer(NicheConfig(name="empty"))
        assert scorer.score_bio("anything") == 0.0


# ── EngagementQualityAnalyzer Tests ──

class TestEngagementQuality:
    def test_good_quality(self, sample_engagement):
        analyzer = EngagementQualityAnalyzer()
        result = analyzer.analyze(sample_engagement)
        assert result["quality_score"] > 0.5
        assert isinstance(result["flags"], list)

    def test_empty_samples(self):
        analyzer = EngagementQualityAnalyzer()
        result = analyzer.analyze([])
        assert result["quality_score"] == 0.0
        assert "no_samples" in result["flags"]

    def test_suspicious_like_ratio(self):
        analyzer = EngagementQualityAnalyzer()
        samples = [
            EngagementSample(tweet_id="t1", likes=1000, retweets=0, replies=1, impressions=5000),
        ]
        result = analyzer.analyze(samples)
        assert "suspicious_like_ratio" in result["flags"]

    def test_zero_replies_with_likes(self):
        analyzer = EngagementQualityAnalyzer()
        samples = [
            EngagementSample(tweet_id="t1", likes=100, retweets=10, replies=0),
            EngagementSample(tweet_id="t2", likes=200, retweets=20, replies=0),
        ]
        result = analyzer.analyze(samples)
        assert "zero_replies_with_likes" in result["flags"]

    def test_uniform_engagement(self):
        analyzer = EngagementQualityAnalyzer()
        samples = [
            EngagementSample(tweet_id=f"t{i}", likes=100, retweets=10, replies=5)
            for i in range(10)
        ]
        result = analyzer.analyze(samples)
        assert "too_uniform_engagement" in result["flags"]

    def test_details_populated(self, sample_engagement):
        analyzer = EngagementQualityAnalyzer()
        result = analyzer.analyze(sample_engagement)
        assert "zero_reply_pct" in result["details"]


# ── GrowthTracker Tests ──

class TestGrowthTracker:
    def test_record_and_history(self, tmp_dir):
        gt = GrowthTracker(f"{tmp_dir}/growth.db")
        gt.record("u1", 1000, 500, 100)
        history = gt.get_history("u1")
        assert len(history) == 1
        assert history[0]["followers"] == 1000

    def test_growth_calculation_insufficient(self, tmp_dir):
        gt = GrowthTracker(f"{tmp_dir}/growth.db")
        gt.record("u1", 1000, 500)
        result = gt.calculate_growth("u1")
        assert result["trend"] == "insufficient_data"

    def test_growth_score_default(self, tmp_dir):
        gt = GrowthTracker(f"{tmp_dir}/growth.db")
        score = gt.growth_score("nonexistent")
        assert score == 0.5  # neutral for no data

    def test_growth_score_range(self, tmp_dir):
        gt = GrowthTracker(f"{tmp_dir}/growth.db")
        gt.record("u1", 1000, 500)
        score = gt.growth_score("u1")
        assert 0.0 <= score <= 1.0


# ── CooperationEstimator Tests ──

class TestCooperation:
    def test_estimate_micro(self, sample_profile):
        est = CooperationEstimator()
        result = est.estimate(sample_profile, avg_engagement=200, niche_score=0.8)
        assert "tier" in result
        assert result["tier"] == "micro"
        assert result["cooperation_value_score"] > 0

    def test_estimate_nano(self):
        est = CooperationEstimator()
        p = InfluencerProfile(user_id="1", username="small", followers=5000)
        result = est.estimate(p)
        assert result["tier"] == "nano"

    def test_estimate_mega(self):
        est = CooperationEstimator()
        p = InfluencerProfile(user_id="1", username="celeb", followers=5000000)
        result = est.estimate(p)
        assert result["tier"] == "mega"
        assert result["estimated_rate_usd"] > 10000

    def test_niche_multiplier(self, sample_profile):
        est = CooperationEstimator()
        r1 = est.estimate(sample_profile, niche_score=0.0)
        r2 = est.estimate(sample_profile, niche_score=1.0)
        assert r2["niche_multiplier"] > r1["niche_multiplier"]


# ── InfluencerRanker Tests ──

class TestRanker:
    def test_rank_order(self):
        ranker = InfluencerRanker()
        p1 = InfluencerProfile(user_id="1", username="a", overall_score=0.8)
        p2 = InfluencerProfile(user_id="2", username="b", overall_score=0.5)
        p3 = InfluencerProfile(user_id="3", username="c", overall_score=0.9)
        ranked = ranker.rank([p1, p2, p3])
        assert ranked[0].user_id == "3"
        assert ranked[1].user_id == "1"

    def test_calculate_overall(self):
        ranker = InfluencerRanker()
        score = ranker.calculate_overall(0.8, 0.7, 0.6, 50, 0.9)
        assert 0.0 <= score <= 1.0

    def test_custom_weights(self):
        ranker = InfluencerRanker({"niche": 1.0, "engagement_quality": 0, "growth": 0, "cooperation_value": 0, "authenticity": 0})
        score = ranker.calculate_overall(1.0, 0.0, 0.0, 0, 0.0)
        assert score > 0.9


# ── WatchList Tests ──

class TestWatchList:
    def test_add_and_get(self, tmp_dir, sample_profile):
        wl = WatchList(f"{tmp_dir}/wl.db")
        wl.add(sample_profile)
        result = wl.get(sample_profile.user_id)
        assert result is not None
        assert result["username"] == "crypto_guru"

    def test_remove(self, tmp_dir, sample_profile):
        wl = WatchList(f"{tmp_dir}/wl.db")
        wl.add(sample_profile)
        wl.remove(sample_profile.user_id)
        assert wl.get(sample_profile.user_id) is None

    def test_list_all(self, tmp_dir):
        wl = WatchList(f"{tmp_dir}/wl.db")
        for i in range(5):
            p = InfluencerProfile(user_id=str(i), username=f"u{i}", overall_score=i * 0.2)
            wl.add(p)
        result = wl.list_all()
        assert len(result) == 5
        # Should be ordered by score desc
        assert result[0]["overall_score"] >= result[-1]["overall_score"]

    def test_list_filter_min_score(self, tmp_dir):
        wl = WatchList(f"{tmp_dir}/wl.db")
        for i in range(5):
            p = InfluencerProfile(user_id=str(i), username=f"u{i}", overall_score=i * 0.2)
            wl.add(p)
        result = wl.list_all(min_score=0.5)
        assert all(r["overall_score"] >= 0.5 for r in result)

    def test_update_scores(self, tmp_dir, sample_profile):
        wl = WatchList(f"{tmp_dir}/wl.db")
        wl.add(sample_profile)
        wl.update_scores(sample_profile.user_id, overall_score=0.95)
        r = wl.get(sample_profile.user_id)
        assert r["overall_score"] == 0.95

    def test_events(self, tmp_dir, sample_profile):
        wl = WatchList(f"{tmp_dir}/wl.db")
        wl.add(sample_profile)
        events = wl.get_events(sample_profile.user_id)
        assert len(events) >= 1
        assert events[0]["event_type"] == "added"

    def test_stats(self, tmp_dir):
        wl = WatchList(f"{tmp_dir}/wl.db")
        p = InfluencerProfile(user_id="1", username="u", tier="micro", overall_score=0.7)
        wl.add(p)
        stats = wl.stats()
        assert stats["total"] == 1


# ── InfluencerFinder Integration ──

class TestInfluencerFinder:
    def test_evaluate(self, tmp_dir, niche_config, sample_profile, sample_tweets, sample_engagement):
        finder = InfluencerFinder(niche_config, db_dir=tmp_dir)
        result = finder.evaluate(sample_profile, sample_tweets, sample_engagement)
        assert result.tier == "micro"
        assert result.niche_score > 0
        assert result.quality_score > 0
        assert result.overall_score > 0

    def test_batch_evaluate(self, tmp_dir, niche_config):
        finder = InfluencerFinder(niche_config, db_dir=tmp_dir)
        profiles = [
            InfluencerProfile(user_id="1", username="a", bio="bitcoin trader", followers=5000),
            InfluencerProfile(user_id="2", username="b", bio="cook", followers=20000),
            InfluencerProfile(user_id="3", username="c", bio="defi degen ethereum", followers=15000),
        ]
        tweets_map = {
            "1": [{"text": "bitcoin going up"}, {"text": "defi yield farming"}],
            "3": [{"text": "ethereum merge was great"}, {"text": "new web3 project"}],
        }
        results = finder.batch_evaluate(profiles, tweets_map)
        assert len(results) == 3
        # First result should have highest score
        assert results[0].overall_score >= results[-1].overall_score

    def test_discover_from_seed(self, tmp_dir, niche_config):
        finder = InfluencerFinder(niche_config, db_dir=tmp_dir)
        candidates = finder.discover_from_seed(["user1", "user2"])
        assert len(candidates) >= 2

    def test_export_json(self, tmp_dir, niche_config, sample_profile):
        finder = InfluencerFinder(niche_config, db_dir=tmp_dir)
        finder.evaluate(sample_profile)
        report = finder.export_report([sample_profile], format="json")
        data = json.loads(report)
        assert len(data) == 1

    def test_export_csv(self, tmp_dir, niche_config, sample_profile):
        finder = InfluencerFinder(niche_config, db_dir=tmp_dir)
        finder.evaluate(sample_profile)
        report = finder.export_report([sample_profile], format="csv")
        lines = report.strip().split("\n")
        assert len(lines) == 2  # header + 1 profile

    def test_export_text(self, tmp_dir, niche_config, sample_profile):
        finder = InfluencerFinder(niche_config, db_dir=tmp_dir)
        finder.evaluate(sample_profile)
        report = finder.export_report([sample_profile], format="text")
        assert "Influencer Report" in report
        assert "crypto_guru" in report

    def test_watchlist_integration(self, tmp_dir, niche_config, sample_profile):
        finder = InfluencerFinder(niche_config, db_dir=tmp_dir)
        finder.evaluate(sample_profile)
        finder.watchlist.add(sample_profile)
        items = finder.watchlist.list_all()
        assert len(items) == 1
