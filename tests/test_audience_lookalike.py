"""Tests for audience_lookalike module"""

import json
import os
import tempfile
import pytest

from bot.audience_lookalike import (
    AudienceLookalike, AudienceSegment, InterestExtractor,
    LookalikeResult, LookalikeStore, OverlapAnalysis,
    SimilarityCalculator, SimilarityMetric,
    UserProfile,
)


# ── InterestExtractor ──

class TestInterestExtractor:
    def test_extract_tech(self):
        interests = InterestExtractor.extract("Full-stack developer | AI enthusiast")
        assert "tech" in interests

    def test_extract_marketing(self):
        interests = InterestExtractor.extract("Digital marketing specialist, SEO expert")
        assert "marketing" in interests

    def test_extract_multiple(self):
        interests = InterestExtractor.extract("Founder & CEO | Building AI SaaS products")
        assert "business" in interests
        assert "tech" in interests

    def test_extract_empty(self):
        assert InterestExtractor.extract("") == []
        assert InterestExtractor.extract(None) == []

    def test_extract_no_match(self):
        interests = InterestExtractor.extract("Just vibing 🌊")
        assert len(interests) == 0

    def test_extract_ecommerce(self):
        interests = InterestExtractor.extract("Shopify dropshipping expert")
        assert "ecommerce" in interests

    def test_extract_creator(self):
        interests = InterestExtractor.extract("YouTuber and podcaster")
        assert "creator" in interests

    def test_extract_finance(self):
        interests = InterestExtractor.extract("Forex trading | DeFi investor")
        assert "finance" in interests

    def test_extract_design(self):
        interests = InterestExtractor.extract("UI/UX designer at Figma")
        assert "design" in interests

    def test_interest_similarity_identical(self):
        a = ["tech", "business"]
        assert InterestExtractor.interest_similarity(a, a) == 1.0

    def test_interest_similarity_partial(self):
        a = ["tech", "business"]
        b = ["tech", "marketing"]
        score = InterestExtractor.interest_similarity(a, b)
        assert 0 < score < 1

    def test_interest_similarity_none(self):
        assert InterestExtractor.interest_similarity(["tech"], ["finance"]) == 0.0

    def test_interest_similarity_empty(self):
        assert InterestExtractor.interest_similarity([], []) == 0.0


# ── SimilarityCalculator ──

class TestSimilarityCalculator:
    def test_jaccard_identical(self):
        s = {1, 2, 3}
        assert SimilarityCalculator.jaccard(s, s) == 1.0

    def test_jaccard_disjoint(self):
        assert SimilarityCalculator.jaccard({1, 2}, {3, 4}) == 0.0

    def test_jaccard_partial(self):
        score = SimilarityCalculator.jaccard({1, 2, 3}, {2, 3, 4})
        assert abs(score - 0.5) < 0.01

    def test_jaccard_empty(self):
        assert SimilarityCalculator.jaccard(set(), set()) == 0.0

    def test_cosine_identical(self):
        v = {"a": 1.0, "b": 2.0}
        assert abs(SimilarityCalculator.cosine(v, v) - 1.0) < 0.01

    def test_cosine_orthogonal(self):
        assert SimilarityCalculator.cosine({"a": 1}, {"b": 1}) == 0.0

    def test_cosine_empty(self):
        assert SimilarityCalculator.cosine({}, {}) == 0.0

    def test_overlap_coefficient(self):
        score = SimilarityCalculator.overlap_coefficient({1, 2, 3}, {2, 3, 4, 5})
        assert abs(score - 2/3) < 0.01

    def test_overlap_empty(self):
        assert SimilarityCalculator.overlap_coefficient(set(), {1}) == 0.0

    def test_dice(self):
        score = SimilarityCalculator.dice({1, 2, 3}, {2, 3, 4})
        # 2*2 / (3+3) = 4/6
        assert abs(score - 4/6) < 0.01

    def test_dice_empty(self):
        assert SimilarityCalculator.dice(set(), set()) == 0.0

    def test_calculate_jaccard(self):
        score = SimilarityCalculator.calculate(
            {1, 2}, {1, 2, 3}, SimilarityMetric.JACCARD
        )
        assert abs(score - 2/3) < 0.01

    def test_calculate_overlap(self):
        score = SimilarityCalculator.calculate(
            {1, 2}, {1, 2, 3}, SimilarityMetric.OVERLAP
        )
        assert score == 1.0

    def test_calculate_dice(self):
        score = SimilarityCalculator.calculate(
            {1, 2}, {2, 3}, SimilarityMetric.DICE
        )
        # dice({1,2},{2,3}) = 2*1/(2+2) = 0.5
        assert abs(score - 0.5) < 0.01


# ── UserProfile ──

class TestUserProfile:
    def test_to_dict(self):
        p = UserProfile(user_id="123", username="test", followers_count=100)
        d = p.to_dict()
        assert d["user_id"] == "123"
        assert d["followers_count"] == 100

    def test_from_dict(self):
        d = {"user_id": "456", "username": "foo", "bio": "hello"}
        p = UserProfile.from_dict(d)
        assert p.user_id == "456"
        assert p.bio == "hello"

    def test_from_dict_extra_keys(self):
        d = {"user_id": "789", "username": "bar", "extra_field": True}
        p = UserProfile.from_dict(d)
        assert p.user_id == "789"


# ── LookalikeStore ──

class TestLookalikeStore:
    @pytest.fixture
    def store(self, tmp_path):
        db = str(tmp_path / "test_lookalike.db")
        s = LookalikeStore(db_path=db)
        yield s
        s.close()

    def test_add_seed(self, store):
        store.add_seed("elonmusk", "Elon Musk", 150_000_000)
        seeds = store.get_seeds()
        assert len(seeds) == 1
        assert seeds[0]["username"] == "elonmusk"

    def test_save_user(self, store):
        user = UserProfile(user_id="u1", username="testuser", bio="dev")
        store.save_user(user, 0.85, AudienceSegment.HIGH_VALUE, ["seed1"])
        top = store.get_top_users(limit=10)
        assert len(top) == 1
        assert top[0]["similarity_score"] == 0.85

    def test_get_top_users_by_segment(self, store):
        for i, seg in enumerate([AudienceSegment.HIGH_VALUE, AudienceSegment.COLD_LEAD]):
            user = UserProfile(user_id=f"u{i}", username=f"user{i}")
            store.save_user(user, 0.5 + i * 0.2, seg, ["s1"])
        results = store.get_top_users(segment="high_value")
        assert len(results) == 1

    def test_get_stats(self, store):
        user = UserProfile(user_id="u1", username="x")
        store.save_user(user, 0.5, AudienceSegment.WARM_LEAD, [])
        stats = store.get_stats()
        assert stats["total_discovered"] == 1
        assert "warm_lead" in stats["by_segment"]

    def test_save_overlap(self, store):
        analysis = OverlapAnalysis(
            account_a="a", account_b="b",
            followers_a=100, followers_b=200,
            overlap_count=50, jaccard_index=0.25,
            overlap_ratio_a=0.5, overlap_ratio_b=0.25,
            unique_to_a=50, unique_to_b=150,
        )
        store.save_overlap(analysis)  # 不应抛异常


# ── AudienceLookalike ──

class TestAudienceLookalike:
    @pytest.fixture
    def engine(self, tmp_path):
        db = str(tmp_path / "test_la.db")
        store = LookalikeStore(db_path=db)
        e = AudienceLookalike(store=store)
        yield e
        store.close()

    def _make_user(self, uid, username="user", bio="developer",
                   followers=500, engagement=0.03):
        return UserProfile(
            user_id=uid, username=username, bio=bio,
            followers_count=followers, engagement_rate=engagement,
            tweet_count=200,
        )

    def test_add_seed(self, engine):
        engine.add_seed_account("competitor1", "Comp 1", 10000)
        seeds = engine.store.get_seeds()
        assert len(seeds) == 1

    def test_add_followers(self, engine):
        engine.add_seed_account("seed1")
        followers = [self._make_user(f"u{i}") for i in range(5)]
        engine.add_seed_followers("seed1", followers)
        assert len(engine._seed_followers["seed1"]) == 5

    def test_analyze_overlap(self, engine):
        engine.add_seed_account("a")
        engine.add_seed_account("b")
        # 共享2个用户
        users_a = [self._make_user(f"u{i}") for i in range(5)]
        users_b = [self._make_user(f"u{i}") for i in range(3, 8)]
        engine.add_seed_followers("a", users_a)
        engine.add_seed_followers("b", users_b)
        overlap = engine.analyze_overlap("a", "b")
        assert overlap is not None
        assert overlap.overlap_count == 2  # u3, u4
        assert overlap.jaccard_index > 0

    def test_analyze_overlap_no_data(self, engine):
        result = engine.analyze_overlap("missing_a", "missing_b")
        assert result is None

    def test_analyze_all_overlaps(self, engine):
        for seed in ["a", "b", "c"]:
            engine.add_seed_account(seed)
            engine.add_seed_followers(seed, [self._make_user(f"u_{seed}_{i}") for i in range(3)])
        results = engine.analyze_all_overlaps()
        assert len(results) == 3  # C(3,2)

    def test_discover_lookalikes_basic(self, engine):
        engine.add_seed_account("s1")
        engine.add_seed_account("s2")
        # u0, u1在两个种子中都出现
        shared = [self._make_user(f"shared_{i}", engagement=0.05, followers=1000) for i in range(2)]
        unique_s1 = [self._make_user(f"only_s1_{i}") for i in range(3)]
        unique_s2 = [self._make_user(f"only_s2_{i}") for i in range(3)]
        engine.add_seed_followers("s1", shared + unique_s1)
        engine.add_seed_followers("s2", shared + unique_s2)
        results = engine.discover_lookalikes(min_score=0.0)
        assert len(results) > 0
        # 共享用户分数应更高
        shared_results = [r for r in results if "shared" in r.user.user_id]
        if shared_results:
            assert shared_results[0].overlap_count == 2

    def test_discover_excludes_bots(self, engine):
        engine.add_seed_account("s1")
        bot = UserProfile(
            user_id="bot1", username="bot",
            followers_count=5, following_count=5000,  # ratio > 10
            tweet_count=0,
        )
        real = self._make_user("real1", followers=500, engagement=0.04)
        engine.add_seed_followers("s1", [bot, real])
        results = engine.discover_lookalikes(exclude_bots=True)
        user_ids = {r.user.user_id for r in results}
        assert "bot1" not in user_ids

    def test_discover_includes_bots_when_disabled(self, engine):
        engine.add_seed_account("s1")
        bot = UserProfile(
            user_id="bot1", username="bot",
            followers_count=5, following_count=5000,
            tweet_count=0,
        )
        engine.add_seed_followers("s1", [bot])
        results = engine.discover_lookalikes(exclude_bots=False, min_score=0.0)
        user_ids = {r.user.user_id for r in results}
        assert "bot1" in user_ids

    def test_segment_classification(self, engine):
        engine.add_seed_account("s1")
        engine.add_seed_account("s2")
        high_val = self._make_user("hv", followers=2000, engagement=0.06)
        engine.add_seed_followers("s1", [high_val])
        engine.add_seed_followers("s2", [high_val])
        results = engine.discover_lookalikes(min_score=0.0)
        hv_results = [r for r in results if r.user.user_id == "hv"]
        if hv_results:
            assert hv_results[0].segment in (
                AudienceSegment.HIGH_VALUE, AudienceSegment.WARM_LEAD
            )

    def test_export_json(self, engine):
        engine.add_seed_account("s1")
        engine.add_seed_followers("s1", [self._make_user("u1")])
        results = engine.discover_lookalikes(min_score=0.0)
        output = engine.export_results(results, format="json")
        data = json.loads(output)
        assert isinstance(data, list)

    def test_export_csv(self, engine):
        engine.add_seed_account("s1")
        engine.add_seed_followers("s1", [self._make_user("u1")])
        results = engine.discover_lookalikes(min_score=0.0)
        output = engine.export_results(results, format="csv")
        lines = output.strip().split("\n")
        assert lines[0].startswith("user_id")

    def test_get_segment_summary(self, engine):
        summary = engine.get_segment_summary()
        assert "total_discovered" in summary

    def test_max_results(self, engine):
        engine.add_seed_account("s1")
        users = [self._make_user(f"u{i}") for i in range(20)]
        engine.add_seed_followers("s1", users)
        results = engine.discover_lookalikes(min_score=0.0, max_results=5)
        assert len(results) <= 5

    def test_is_bot_suspect_zero_followers(self, engine):
        user = UserProfile(user_id="x", username="x", followers_count=0)
        assert engine._is_bot_suspect(user) is True

    def test_is_bot_suspect_high_ratio(self, engine):
        user = UserProfile(user_id="x", username="x",
                          followers_count=10, following_count=200)
        assert engine._is_bot_suspect(user) is True

    def test_is_bot_suspect_normal(self, engine):
        user = UserProfile(user_id="x", username="x",
                          followers_count=500, following_count=300,
                          tweet_count=100)
        assert engine._is_bot_suspect(user) is False
