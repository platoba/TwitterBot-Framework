"""Tests for audience_insights module"""

import json
import os
import tempfile
import pytest
from datetime import datetime, timezone, timedelta

from bot.audience_insights import (
    AudienceSegment, InterestCategory, INTEREST_KEYWORDS,
    FollowerProfile, HeatmapCell, InterestClusterResult, LookalikeResult,
    AudienceInsightsDB, TimezoneAnalyzer, EngagementHeatmap,
    InterestAnalyzer, BotDetector, SegmentClassifier,
    LookalikeEngine, AudienceReportGenerator,
)


# --- Fixtures ---

@pytest.fixture
def sample_profiles():
    return [
        FollowerProfile(
            user_id="1", username="techdev1", display_name="Tech Dev",
            bio="Full-stack developer | Open source contributor | AI enthusiast",
            location="San Francisco, CA", followers_count=5000,
            following_count=800, tweet_count=3200, verified=True,
            engagement_score=0.08, interests=[InterestCategory.TECH, InterestCategory.AI_ML],
        ),
        FollowerProfile(
            user_id="2", username="marketer_pro", display_name="Marketing Pro",
            bio="Digital marketing | SEO expert | Growth hacking",
            location="New York", followers_count=12000,
            following_count=1200, tweet_count=8500,
            engagement_score=0.05, interests=[InterestCategory.MARKETING],
        ),
        FollowerProfile(
            user_id="3", username="cryptowhale99", display_name="Crypto Whale",
            bio="Bitcoin maximalist | DeFi | Web3 builder",
            location="Dubai", followers_count=45000,
            following_count=300, tweet_count=15000,
            engagement_score=0.12, interests=[InterestCategory.CRYPTO],
        ),
        FollowerProfile(
            user_id="4", username="bot12345678", display_name="User",
            bio="", location="", followers_count=2,
            following_count=9500, tweet_count=50000,
            engagement_score=0.0,
        ),
        FollowerProfile(
            user_id="5", username="casual_user", display_name="Just Me",
            bio="Love travel and food photography",
            location="London, UK", followers_count=350,
            following_count=500, tweet_count=120,
            engagement_score=0.02, interests=[InterestCategory.LIFESTYLE],
        ),
    ]


@pytest.fixture
def db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    database = AudienceInsightsDB(path)
    yield database
    database.close()
    os.unlink(path)


# --- FollowerProfile Tests ---

class TestFollowerProfile:
    def test_create(self):
        p = FollowerProfile(user_id="1", username="test")
        assert p.user_id == "1"
        assert p.username == "test"
        assert p.segment == AudienceSegment.CASUAL
        assert p.interests == []

    def test_to_dict(self):
        p = FollowerProfile(
            user_id="1", username="test",
            segment=AudienceSegment.SUPER_FAN,
            interests=[InterestCategory.TECH],
        )
        d = p.to_dict()
        assert d["segment"] == "super_fan"
        assert d["interests"] == ["tech"]

    def test_defaults(self):
        p = FollowerProfile(user_id="x", username="x")
        assert p.bio == ""
        assert p.location == ""
        assert p.bot_probability == 0.0
        assert p.language == "en"


# --- HeatmapCell Tests ---

class TestHeatmapCell:
    def test_intensity_no_tweets(self):
        cell = HeatmapCell(day=0, hour=0)
        assert cell.intensity == 0.0

    def test_intensity_with_data(self):
        cell = HeatmapCell(day=0, hour=0, tweet_count=10, engagement_rate=0.05)
        assert cell.intensity == 0.5

    def test_intensity_cap(self):
        cell = HeatmapCell(day=0, hour=0, tweet_count=1, engagement_rate=0.2)
        assert cell.intensity == 1.0


# --- AudienceInsightsDB Tests ---

class TestAudienceInsightsDB:
    def test_save_and_get_profile(self, db):
        p = FollowerProfile(
            user_id="1", username="test",
            bio="hello", engagement_score=0.5,
            segment=AudienceSegment.ACTIVE,
            interests=[InterestCategory.TECH],
        )
        db.save_profile(p)
        result = db.get_profile("1")
        assert result is not None
        assert result.username == "test"
        assert result.engagement_score == 0.5
        assert result.segment == AudienceSegment.ACTIVE
        assert InterestCategory.TECH in result.interests

    def test_save_profiles_batch(self, db, sample_profiles):
        db.save_profiles_batch(sample_profiles)
        assert db.count_profiles() == 5

    def test_get_profiles_by_segment(self, db):
        p1 = FollowerProfile(user_id="1", username="a", segment=AudienceSegment.ACTIVE)
        p2 = FollowerProfile(user_id="2", username="b", segment=AudienceSegment.ACTIVE)
        p3 = FollowerProfile(user_id="3", username="c", segment=AudienceSegment.LURKER)
        db.save_profiles_batch([p1, p2, p3])
        actives = db.get_profiles_by_segment(AudienceSegment.ACTIVE)
        assert len(actives) == 2

    def test_get_all_profiles(self, db, sample_profiles):
        db.save_profiles_batch(sample_profiles)
        all_p = db.get_all_profiles()
        assert len(all_p) == 5

    def test_profile_not_found(self, db):
        assert db.get_profile("nonexistent") is None

    def test_record_engagement(self, db):
        db.record_engagement("user1", "like", "tweet123")
        count = db.get_engagement_count("user1", days=1)
        assert count == 1

    def test_save_snapshot(self, db):
        db.save_snapshot("myaccount", 1000, {"active": 500}, {"tech": 300}, {"-8": 200})
        # No error = success

    def test_upsert_profile(self, db):
        p = FollowerProfile(user_id="1", username="old_name")
        db.save_profile(p)
        p.username = "new_name"
        db.save_profile(p)
        result = db.get_profile("1")
        assert result.username == "new_name"


# --- TimezoneAnalyzer Tests ---

class TestTimezoneAnalyzer:
    def test_infer_timezone_sf(self):
        assert TimezoneAnalyzer.infer_timezone("San Francisco, CA") == -8

    def test_infer_timezone_london(self):
        assert TimezoneAnalyzer.infer_timezone("London, UK") == 0

    def test_infer_timezone_tokyo(self):
        assert TimezoneAnalyzer.infer_timezone("Tokyo, Japan") == 9

    def test_infer_timezone_unknown(self):
        assert TimezoneAnalyzer.infer_timezone("Middle of Nowhere") is None

    def test_infer_timezone_empty(self):
        assert TimezoneAnalyzer.infer_timezone("") is None

    def test_analyze_distribution(self, sample_profiles):
        dist = TimezoneAnalyzer.analyze_distribution(sample_profiles)
        assert isinstance(dist, dict)
        assert -8 in dist  # SF
        assert -5 in dist  # NYC

    def test_optimal_posting_windows(self, sample_profiles):
        windows = TimezoneAnalyzer.optimal_posting_windows(sample_profiles)
        assert len(windows) <= 3
        assert all("utc_hour" in w for w in windows)

    def test_optimal_posting_empty(self):
        windows = TimezoneAnalyzer.optimal_posting_windows([])
        assert windows == []

    def test_coverage_report(self, sample_profiles):
        report = TimezoneAnalyzer.coverage_report(sample_profiles)
        assert "total_known" in report
        assert "regions" in report
        assert "dominant_region" in report
        assert "optimal_windows" in report

    def test_infer_shanghai(self):
        assert TimezoneAnalyzer.infer_timezone("Shanghai, China") == 8

    def test_infer_dubai(self):
        assert TimezoneAnalyzer.infer_timezone("Dubai, UAE") == 4

    def test_infer_case_insensitive(self):
        assert TimezoneAnalyzer.infer_timezone("NEW YORK CITY") == -5


# --- EngagementHeatmap Tests ---

class TestEngagementHeatmap:
    def test_initial_empty(self):
        hm = EngagementHeatmap()
        cell = hm.get_cell(0, 0)
        assert cell.engagement_count == 0

    def test_record(self):
        hm = EngagementHeatmap()
        hm.record(0, 10, engagements=5, impressions=100)
        cell = hm.get_cell(0, 10)
        assert cell.engagement_count == 5
        assert cell.impression_count == 100

    def test_record_accumulates(self):
        hm = EngagementHeatmap()
        hm.record(1, 14, engagements=3)
        hm.record(1, 14, engagements=7)
        cell = hm.get_cell(1, 14)
        assert cell.engagement_count == 10

    def test_record_from_datetime(self):
        hm = EngagementHeatmap()
        dt = datetime(2026, 3, 1, 14, 30)  # Saturday = 5
        hm.record_from_datetime(dt, engagements=10)
        cell = hm.get_cell(dt.weekday(), 14)
        assert cell.engagement_count == 10

    def test_get_peak_times(self):
        hm = EngagementHeatmap()
        hm.record(0, 10, engagements=50)
        hm.record(2, 14, engagements=100)
        hm.record(4, 18, engagements=30)
        peaks = hm.get_peak_times(top_n=2)
        assert len(peaks) == 2
        assert peaks[0].engagement_count == 100

    def test_get_peak_times_empty(self):
        hm = EngagementHeatmap()
        assert hm.get_peak_times() == []

    def test_get_dead_zones(self):
        hm = EngagementHeatmap()
        hm.record(0, 10, engagements=100)
        hm.record(0, 3, engagements=1)  # dead zone
        deads = hm.get_dead_zones()
        assert len(deads) >= 1

    def test_day_summary(self):
        hm = EngagementHeatmap()
        hm.record(0, 10, engagements=5)
        hm.record(0, 14, engagements=10)
        summary = hm.get_day_summary()
        assert summary["Monday"] == 15

    def test_hour_summary(self):
        hm = EngagementHeatmap()
        hm.record(0, 10, engagements=5)
        hm.record(3, 10, engagements=8)
        summary = hm.get_hour_summary()
        assert summary[10] == 13

    def test_to_matrix(self):
        hm = EngagementHeatmap()
        matrix = hm.to_matrix()
        assert len(matrix) == 7
        assert all(len(row) == 24 for row in matrix)

    def test_to_dict(self):
        hm = EngagementHeatmap()
        hm.record(0, 10, engagements=5)
        d = hm.to_dict()
        assert "matrix" in d
        assert "peak_times" in d
        assert "day_summary" in d

    def test_wrap_around(self):
        hm = EngagementHeatmap()
        hm.record(7, 25, engagements=5)  # wraps to day=0, hour=1
        cell = hm.get_cell(0, 1)
        assert cell.engagement_count == 5


# --- InterestAnalyzer Tests ---

class TestInterestAnalyzer:
    def test_classify_tech(self):
        interests = InterestAnalyzer.classify_interests("Full-stack developer, open source lover")
        assert InterestCategory.TECH in interests

    def test_classify_multiple(self):
        interests = InterestAnalyzer.classify_interests(
            "AI researcher and crypto trader, building DeFi"
        )
        assert InterestCategory.AI_ML in interests or InterestCategory.CRYPTO in interests

    def test_classify_empty(self):
        interests = InterestAnalyzer.classify_interests("")
        assert interests == []

    def test_classify_with_tweets(self):
        interests = InterestAnalyzer.classify_interests(
            "Just a person",
            tweets=["Love this new machine learning paper!", "AI is the future"]
        )
        assert InterestCategory.AI_ML in interests

    def test_classify_max_5(self):
        bio = " ".join(kw for kws in INTEREST_KEYWORDS.values() for kw in kws[:2])
        interests = InterestAnalyzer.classify_interests(bio)
        assert len(interests) <= 5

    def test_cluster_audience(self, sample_profiles):
        clusters = InterestAnalyzer.cluster_audience(sample_profiles)
        assert len(clusters) > 0
        assert all(isinstance(c, InterestClusterResult) for c in clusters)

    def test_cluster_empty(self):
        clusters = InterestAnalyzer.cluster_audience([])
        assert clusters == []

    def test_cluster_result_to_dict(self):
        cr = InterestClusterResult(
            category=InterestCategory.TECH,
            follower_count=100, percentage=50.0,
            top_keywords=["dev"], sample_users=["user1"],
        )
        d = cr.to_dict()
        assert d["category"] == "tech"
        assert d["percentage"] == 50.0

    def test_interest_overlap(self, sample_profiles):
        group_a = sample_profiles[:3]
        group_b = sample_profiles[2:]
        result = InterestAnalyzer.interest_overlap(group_a, group_b)
        assert "shared_interests" in result
        assert "overlap_ratio" in result


# --- BotDetector Tests ---

class TestBotDetector:
    def test_detect_likely_bot(self):
        bot = FollowerProfile(
            user_id="1", username="bot12345678",
            bio="", location="",
            followers_count=2, following_count=9500,
            tweet_count=50000,
        )
        prob = BotDetector.calculate_bot_probability(bot)
        assert prob > 0.3

    def test_detect_real_user(self):
        real = FollowerProfile(
            user_id="2", username="realuser",
            bio="Software engineer at Google. Love hiking.",
            location="Mountain View, CA",
            followers_count=1500, following_count=300,
            tweet_count=2000, profile_image_url="https://pbs.twimg.com/custom.jpg",
            created_at="2018-01-01T00:00:00+00:00",
        )
        prob = BotDetector.calculate_bot_probability(real)
        assert prob < 0.3

    def test_detect_bots_batch(self, sample_profiles):
        bots = BotDetector.detect_bots(sample_profiles, threshold=0.3)
        # bot12345678 should be detected
        bot_usernames = [b.username for b in bots]
        assert "bot12345678" in bot_usernames

    def test_probability_range(self, sample_profiles):
        for p in sample_profiles:
            prob = BotDetector.calculate_bot_probability(p)
            assert 0 <= prob <= 1.0

    def test_empty_profile(self):
        p = FollowerProfile(user_id="x", username="x")
        prob = BotDetector.calculate_bot_probability(p)
        assert 0 <= prob <= 1.0


# --- SegmentClassifier Tests ---

class TestSegmentClassifier:
    def test_super_fan(self):
        p = FollowerProfile(user_id="1", username="fan", engagement_score=0.5)
        seg = SegmentClassifier.classify(p, engagement_count_30d=25, avg_engagement=0.05)
        assert seg == AudienceSegment.SUPER_FAN

    def test_active(self):
        p = FollowerProfile(user_id="1", username="user", engagement_score=0.1)
        seg = SegmentClassifier.classify(p, engagement_count_30d=8, avg_engagement=0.05)
        assert seg == AudienceSegment.ACTIVE

    def test_casual(self):
        p = FollowerProfile(user_id="1", username="user", engagement_score=0.01)
        seg = SegmentClassifier.classify(p, engagement_count_30d=2, avg_engagement=0.05)
        assert seg == AudienceSegment.CASUAL

    def test_lurker(self):
        p = FollowerProfile(user_id="1", username="user", tweet_count=100, engagement_score=0.0)
        seg = SegmentClassifier.classify(p, engagement_count_30d=0, avg_engagement=0.05)
        assert seg == AudienceSegment.LURKER

    def test_inactive(self):
        p = FollowerProfile(user_id="1", username="user", tweet_count=0, engagement_score=0.0)
        seg = SegmentClassifier.classify(p, engagement_count_30d=0, avg_engagement=0.05)
        assert seg == AudienceSegment.INACTIVE

    def test_bot_suspect(self):
        p = FollowerProfile(user_id="1", username="user", bot_probability=0.8)
        seg = SegmentClassifier.classify(p, engagement_count_30d=100, avg_engagement=0.05)
        assert seg == AudienceSegment.BOT_SUSPECT

    def test_classify_batch(self, sample_profiles):
        result = SegmentClassifier.classify_batch(sample_profiles)
        assert isinstance(result, dict)
        total = sum(len(v) for v in result.values())
        assert total == len(sample_profiles)

    def test_classify_batch_empty(self):
        result = SegmentClassifier.classify_batch([])
        assert result == {}


# --- LookalikeEngine Tests ---

class TestLookalikeEngine:
    def test_calculate_similarity_full_overlap(self):
        profiles = [FollowerProfile(user_id="1", username="a")]
        result = LookalikeEngine.calculate_similarity(profiles, profiles, "acc1", "acc2")
        assert result.similarity_score == 1.0
        assert result.overlap_count == 1

    def test_calculate_similarity_no_overlap(self):
        a = [FollowerProfile(user_id="1", username="a")]
        b = [FollowerProfile(user_id="2", username="b")]
        result = LookalikeEngine.calculate_similarity(a, b, "acc1", "acc2")
        assert result.similarity_score == 0.0
        assert result.overlap_count == 0

    def test_calculate_similarity_partial(self):
        shared = FollowerProfile(user_id="1", username="shared")
        a = [shared, FollowerProfile(user_id="2", username="a_only")]
        b = [shared, FollowerProfile(user_id="3", username="b_only")]
        result = LookalikeEngine.calculate_similarity(a, b, "acc1", "acc2")
        assert 0 < result.similarity_score < 1
        assert result.overlap_count == 1

    def test_to_dict(self):
        result = LookalikeResult(
            source_account="a", target_account="b",
            overlap_count=5, overlap_percentage=10.0,
            shared_interests=[InterestCategory.TECH],
            similarity_score=0.5,
        )
        d = result.to_dict()
        assert d["shared_interests"] == ["tech"]

    def test_find_lookalikes(self, sample_profiles):
        source = sample_profiles[:2]
        candidates = {
            "group1": sample_profiles[1:4],
            "group2": [FollowerProfile(user_id="99", username="unrelated")],
        }
        results = LookalikeEngine.find_lookalikes(source, candidates, min_similarity=0.01)
        assert len(results) >= 1

    def test_recommended_action(self):
        a = [FollowerProfile(user_id=str(i), username=f"u{i}") for i in range(10)]
        b = a[:5] + [FollowerProfile(user_id=str(i+10), username=f"v{i}") for i in range(5)]
        result = LookalikeEngine.calculate_similarity(a, b, "a", "b")
        assert result.recommended_action != ""


# --- AudienceReportGenerator Tests ---

class TestAudienceReportGenerator:
    def test_generate_report(self, sample_profiles):
        report = AudienceReportGenerator.generate(sample_profiles, "testaccount")
        assert report["account"] == "testaccount"
        assert report["total_followers_analyzed"] == 5
        assert "segments" in report
        assert "top_interests" in report
        assert "timezone" in report
        assert "summary" in report

    def test_generate_report_empty(self):
        report = AudienceReportGenerator.generate([], "empty")
        assert "error" in report

    def test_generate_text(self, sample_profiles):
        report = AudienceReportGenerator.generate(sample_profiles, "testaccount")
        text = AudienceReportGenerator.generate_text(report)
        assert "Audience Insights Report" in text
        assert "@testaccount" in text

    def test_export_json(self, sample_profiles, tmp_path):
        report = AudienceReportGenerator.generate(sample_profiles, "test")
        filepath = str(tmp_path / "report.json")
        AudienceReportGenerator.export_json(report, filepath)
        with open(filepath) as f:
            loaded = json.load(f)
        assert loaded["account"] == "test"

    def test_export_csv(self, sample_profiles, tmp_path):
        filepath = str(tmp_path / "followers.csv")
        AudienceReportGenerator.export_csv(sample_profiles, filepath)
        with open(filepath) as f:
            lines = f.readlines()
        assert len(lines) == 6  # header + 5 profiles

    def test_report_with_heatmap(self, sample_profiles):
        hm = EngagementHeatmap()
        hm.record(0, 10, engagements=50)
        report = AudienceReportGenerator.generate(sample_profiles, "test", heatmap=hm)
        assert "engagement_heatmap" in report

    def test_summary_metrics(self, sample_profiles):
        report = AudienceReportGenerator.generate(sample_profiles, "test")
        summary = report["summary"]
        assert summary["avg_follower_count"] > 0
        assert 0 <= summary["verified_percentage"] <= 100
        assert 0 <= summary["bot_percentage"] <= 100


# --- InterestClusterResult Tests ---

class TestInterestClusterResult:
    def test_to_dict(self):
        r = InterestClusterResult(
            category=InterestCategory.ECOMMERCE,
            follower_count=50,
            percentage=25.5,
            top_keywords=["shopify", "amazon"],
            sample_users=["u1", "u2", "u3", "u4", "u5", "u6"],
            avg_engagement=0.05,
        )
        d = r.to_dict()
        assert d["category"] == "ecommerce"
        assert len(d["sample_users"]) <= 5  # capped at 5
        assert d["avg_engagement"] == 0.05


# --- Edge Cases ---

class TestEdgeCases:
    def test_all_segments_have_value(self):
        for seg in AudienceSegment:
            assert seg.value

    def test_all_interest_categories(self):
        for cat in InterestCategory:
            assert cat.value

    def test_interest_keywords_coverage(self):
        # All categories except OTHER should have keywords
        for cat in InterestCategory:
            if cat != InterestCategory.OTHER:
                assert cat in INTEREST_KEYWORDS

    def test_db_close_idempotent(self, db):
        db.close()
        db.close()  # Should not raise

    def test_timezone_map_coverage(self):
        # At least major cities
        assert TimezoneAnalyzer.infer_timezone("new york") == -5
        assert TimezoneAnalyzer.infer_timezone("london") == 0
        assert TimezoneAnalyzer.infer_timezone("tokyo") == 9
        assert TimezoneAnalyzer.infer_timezone("beijing") == 8
