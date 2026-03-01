"""
Tests for Competitive Benchmarking Engine
竞品对标引擎测试: 指标对比 + 策略检测 + engagement基准 + 增长轨迹 + 内容缺口 + SWOT
"""

import pytest
from bot.competitor_benchmark import (
    CompetitorBenchmark,
    CompetitorProfile,
    CompetitorMetrics,
    ContentPiece,
    ContentStrategy,
    GrowthPhase,
)


# ─── Fixtures ──────────────────────────────────────────────


def make_profile(handle, followers=10000, following=500, tweets=5000, category="tech"):
    return CompetitorProfile(
        handle=handle,
        display_name=handle.title(),
        followers=followers,
        following=following,
        tweet_count=tweets,
        category=category,
    )


def make_metrics(handle, date="2026-02-01", followers=10000, eng_rate=0.04, ppd=3.0,
                 likes=50, retweets=10, replies=5, thread_ratio=0.2, media_ratio=0.3):
    return CompetitorMetrics(
        handle=handle,
        snapshot_date=date,
        followers=followers,
        avg_likes=likes,
        avg_retweets=retweets,
        avg_replies=replies,
        avg_impressions=1000,
        engagement_rate=eng_rate,
        posts_per_day=ppd,
        thread_ratio=thread_ratio,
        media_ratio=media_ratio,
        top_hashtags=["tech", "ai", "python"],
        active_hours=[9, 13, 18],
    )


def make_content(handle, text="Check out this cool thing!", likes=50, retweets=10,
                 replies=5, impressions=1000, is_thread=False, has_media=False,
                 is_reply=False, hashtags=None):
    return ContentPiece(
        handle=handle,
        text=text,
        likes=likes,
        retweets=retweets,
        replies=replies,
        impressions=impressions,
        is_thread=is_thread,
        has_media=has_media,
        is_reply=is_reply,
        hashtags=hashtags or [],
    )


@pytest.fixture
def benchmark():
    return CompetitorBenchmark(my_handle="myaccount")


@pytest.fixture
def populated_benchmark():
    b = CompetitorBenchmark(my_handle="myaccount")

    # Add competitors
    for i, (handle, followers) in enumerate([
        ("competitor1", 50000),
        ("competitor2", 20000),
        ("competitor3", 100000),
        ("competitor4", 5000),
    ]):
        b.add_competitor(make_profile(handle, followers=followers))
        b.add_metrics(make_metrics(
            handle,
            date="2026-01-01",
            followers=followers - 2000,
            eng_rate=0.03 + i * 0.01,
        ))
        b.add_metrics(make_metrics(
            handle,
            date="2026-02-01",
            followers=followers,
            eng_rate=0.04 + i * 0.01,
        ))

        # Add content
        for j in range(5):
            b.add_content(make_content(
                handle,
                text=f"Post {j} from {handle}",
                likes=30 + j * 10,
                is_thread=(j % 3 == 0),
                has_media=(j % 2 == 0),
                hashtags=["tech", "startup"] if j % 2 == 0 else ["ai"],
            ))

    # Add my data
    b.add_my_metrics(make_metrics("myaccount", date="2026-01-01", followers=8000, eng_rate=0.045))
    b.add_my_metrics(make_metrics("myaccount", date="2026-02-01", followers=10000, eng_rate=0.05))

    for j in range(3):
        b.add_my_content(make_content(
            "myaccount",
            text=f"My post {j}",
            likes=40,
            hashtags=["tech"],
        ))

    return b


# ─── CompetitorProfile Tests ──────────────────────────────


class TestCompetitorProfile:
    def test_follower_ratio(self):
        p = make_profile("test", followers=10000, following=500)
        assert p.follower_ratio == 20.0

    def test_follower_ratio_zero_following(self):
        p = make_profile("test", followers=10000, following=0)
        assert p.follower_ratio == 10000.0

    def test_tweets_per_follower(self):
        p = make_profile("test", followers=10000, tweets=5000)
        assert p.tweets_per_follower == 0.5


# ─── ContentPiece Tests ──────────────────────────────────


class TestContentPiece:
    def test_engagement(self):
        c = make_content("test", likes=50, retweets=10, replies=5)
        assert c.engagement == 65

    def test_engagement_rate(self):
        c = make_content("test", likes=50, retweets=10, replies=5, impressions=1000)
        assert c.engagement_rate == 0.065

    def test_engagement_rate_zero_impressions(self):
        c = make_content("test", likes=0, retweets=0, replies=0, impressions=0)
        assert c.engagement_rate == 0  # 0 engagement / max(1, 0) = 0


# ─── CRUD Tests ──────────────────────────────────────────


class TestCRUD:
    def test_add_competitor(self, benchmark):
        benchmark.add_competitor(make_profile("comp1"))
        assert len(benchmark.list_competitors()) == 1

    def test_remove_competitor(self, benchmark):
        benchmark.add_competitor(make_profile("comp1"))
        assert benchmark.remove_competitor("comp1") is True
        assert len(benchmark.list_competitors()) == 0

    def test_remove_nonexistent(self, benchmark):
        assert benchmark.remove_competitor("fake") is False

    def test_list_competitors(self, populated_benchmark):
        comps = populated_benchmark.list_competitors()
        assert len(comps) == 4
        assert all("handle" in c for c in comps)

    def test_add_metrics(self, benchmark):
        benchmark.add_competitor(make_profile("comp1"))
        benchmark.add_metrics(make_metrics("comp1"))
        # metrics are stored internally


# ─── Metrics Comparison Tests ──────────────────────────────


class TestMetricsComparison:
    def test_basic_comparison(self, populated_benchmark):
        comp = populated_benchmark.compare_metrics()
        assert comp["total_compared"] == 5  # 4 competitors + me

    def test_rankings(self, populated_benchmark):
        comp = populated_benchmark.compare_metrics()
        assert "followers" in comp["rankings"]
        assert "engagement_rate" in comp["rankings"]

    def test_my_data_included(self, populated_benchmark):
        comp = populated_benchmark.compare_metrics()
        my_key = [k for k in comp["competitors"] if "(me)" in k]
        assert len(my_key) == 1

    def test_filter_handles(self, populated_benchmark):
        comp = populated_benchmark.compare_metrics(handles=["competitor1"])
        assert comp["total_compared"] == 2  # 1 competitor + me

    def test_empty_metrics(self, benchmark):
        comp = benchmark.compare_metrics()
        assert comp["total_compared"] == 0


# ─── Content Strategy Detection Tests ──────────────────


class TestStrategyDetection:
    def test_detect_thread_heavy(self, benchmark):
        benchmark.add_competitor(make_profile("thread_user"))
        for i in range(10):
            benchmark.add_content(make_content(
                "thread_user",
                text=f"Thread post {i}",
                is_thread=True,
            ))
        for i in range(3):
            benchmark.add_content(make_content(
                "thread_user",
                text=f"Normal post {i}",
            ))
        result = benchmark.detect_strategy("thread_user")
        assert result["strategy"] == ContentStrategy.THREAD_HEAVY.value

    def test_detect_media_first(self, benchmark):
        benchmark.add_competitor(make_profile("media_user"))
        for i in range(10):
            benchmark.add_content(make_content(
                "media_user",
                has_media=True,
            ))
        for i in range(3):
            benchmark.add_content(make_content("media_user"))
        result = benchmark.detect_strategy("media_user")
        assert result["strategy"] == ContentStrategy.MEDIA_FIRST.value

    def test_detect_engagement_bait(self, benchmark):
        benchmark.add_competitor(make_profile("bait_user"))
        bait_texts = [
            "What do you think about this?",
            "Agree? Quote tweet with your opinion",
            "Hot take: Python > JavaScript",
            "Reply with your favorite framework",
            "Unpopular opinion thread incoming",
        ]
        for text in bait_texts:
            benchmark.add_content(make_content("bait_user", text=text))
        for i in range(3):
            benchmark.add_content(make_content("bait_user", text=f"Normal {i}"))
        result = benchmark.detect_strategy("bait_user")
        assert result["engagement_bait_ratio"] > 0.3

    def test_detect_community(self, benchmark):
        benchmark.add_competitor(make_profile("comm_user"))
        for i in range(10):
            benchmark.add_content(make_content("comm_user", is_reply=True))
        for i in range(3):
            benchmark.add_content(make_content("comm_user"))
        result = benchmark.detect_strategy("comm_user")
        assert result["strategy"] == ContentStrategy.COMMUNITY.value

    def test_no_content(self, benchmark):
        benchmark.add_competitor(make_profile("empty"))
        result = benchmark.detect_strategy("empty")
        assert result["strategy"] == "unknown"

    def test_type_performance(self, populated_benchmark):
        result = populated_benchmark.detect_strategy("competitor1")
        assert "type_performance" in result
        assert result["total_analyzed"] == 5


# ─── Engagement Benchmark Tests ──────────────────────────


class TestEngagementBenchmark:
    def test_basic_benchmark(self, populated_benchmark):
        bench = populated_benchmark.engagement_benchmark()
        assert bench["overall_mean"] > 0
        assert bench["overall_median"] > 0
        assert bench["competitors_count"] == 4

    def test_my_percentile(self, populated_benchmark):
        bench = populated_benchmark.engagement_benchmark()
        assert bench["my_percentile"] is not None
        assert 0 <= bench["my_percentile"] <= 100

    def test_tier_benchmarks(self, populated_benchmark):
        bench = populated_benchmark.engagement_benchmark()
        assert len(bench["tier_benchmarks"]) > 0

    def test_empty_benchmark(self, benchmark):
        bench = benchmark.engagement_benchmark()
        assert bench["benchmark"] == 0


# ─── Growth Comparison Tests ──────────────────────────────


class TestGrowthComparison:
    def test_basic_growth(self, populated_benchmark):
        growth = populated_benchmark.growth_comparison()
        assert "growth_data" in growth
        assert growth["fastest_grower"] is not None

    def test_ranking(self, populated_benchmark):
        growth = populated_benchmark.growth_comparison()
        assert len(growth["ranking"]) > 0

    def test_my_growth_included(self, populated_benchmark):
        growth = populated_benchmark.growth_comparison()
        my_keys = [k for k in growth["growth_data"] if "(me)" in k]
        assert len(my_keys) == 1

    def test_growth_phases(self, populated_benchmark):
        growth = populated_benchmark.growth_comparison()
        for handle, data in growth["growth_data"].items():
            assert data["phase"] in [p.value for p in GrowthPhase]

    def test_insufficient_data(self, benchmark):
        benchmark.add_competitor(make_profile("lonely"))
        benchmark.add_metrics(make_metrics("lonely"))  # only 1 snapshot
        growth = benchmark.growth_comparison()
        assert "lonely" not in growth["growth_data"]


# ─── Content Gap Analysis Tests ──────────────────────────


class TestContentGapAnalysis:
    def test_basic_gap(self, populated_benchmark):
        gap = populated_benchmark.content_gap_analysis()
        assert "content_type_gaps" in gap
        assert "hashtag_gaps" in gap

    def test_hashtag_gaps(self, populated_benchmark):
        gap = populated_benchmark.content_gap_analysis()
        # competitors use "startup" hashtag, I don't
        startup_gap = [g for g in gap["hashtag_gaps"] if g["hashtag"] == "startup"]
        assert len(startup_gap) > 0

    def test_recommendation(self, populated_benchmark):
        gap = populated_benchmark.content_gap_analysis()
        assert isinstance(gap["recommendation"], str)

    def test_empty_gap(self, benchmark):
        gap = benchmark.content_gap_analysis()
        assert len(gap["content_type_gaps"]) == 0


# ─── Posting Frequency Tests ──────────────────────────────


class TestPostingFrequency:
    def test_basic_frequency(self, populated_benchmark):
        freq = populated_benchmark.posting_frequency_comparison()
        assert "frequency_data" in freq
        assert freq["most_active"] is not None

    def test_average(self, populated_benchmark):
        freq = populated_benchmark.posting_frequency_comparison()
        assert freq["average_posts_per_day"] > 0


# ─── Top Content Tests ──────────────────────────────────


class TestTopContent:
    def test_basic_top_content(self, populated_benchmark):
        top = populated_benchmark.top_content_analysis(top_n=5)
        assert len(top["top_content"]) <= 5

    def test_filter_by_handle(self, populated_benchmark):
        top = populated_benchmark.top_content_analysis(handle="competitor1", top_n=3)
        assert all(c["handle"] == "competitor1" for c in top["top_content"])

    def test_pattern_detection(self, populated_benchmark):
        top = populated_benchmark.top_content_analysis()
        assert isinstance(top["pattern"], str)

    def test_empty_content(self, benchmark):
        top = benchmark.top_content_analysis()
        assert top["note"] == "no data"


# ─── Full Report Tests ──────────────────────────────────


class TestFullReport:
    def test_full_report(self, populated_benchmark):
        report = populated_benchmark.full_benchmark_report()
        assert "my_handle" in report
        assert "competitors" in report
        assert "metrics_comparison" in report
        assert "engagement_benchmark" in report
        assert "growth_comparison" in report
        assert "content_gap" in report
        assert "strategies" in report

    def test_report_timestamp(self, populated_benchmark):
        report = populated_benchmark.full_benchmark_report()
        assert "generated_at" in report


# ─── SWOT Analysis Tests ──────────────────────────────────


class TestSWOT:
    def test_basic_swot(self, populated_benchmark):
        swot = populated_benchmark.swot_analysis()
        assert "strengths" in swot
        assert "weaknesses" in swot
        assert "opportunities" in swot
        assert "threats" in swot

    def test_swot_has_items(self, populated_benchmark):
        swot = populated_benchmark.swot_analysis()
        assert len(swot["strengths"]) >= 1
        assert len(swot["opportunities"]) >= 1

    def test_swot_empty(self, benchmark):
        swot = benchmark.swot_analysis()
        # Should still have default messages
        assert len(swot["strengths"]) >= 1


# ─── Helper Tests ──────────────────────────────────────────


class TestHelpers:
    def test_follower_tier_nano(self, benchmark):
        assert benchmark._follower_tier(500) == "nano(<1K)"

    def test_follower_tier_micro(self, benchmark):
        assert benchmark._follower_tier(5000) == "micro(1K-10K)"

    def test_follower_tier_mid(self, benchmark):
        assert benchmark._follower_tier(50000) == "mid(10K-100K)"

    def test_follower_tier_macro(self, benchmark):
        assert benchmark._follower_tier(500000) == "macro(100K-1M)"

    def test_follower_tier_mega(self, benchmark):
        assert benchmark._follower_tier(2000000) == "mega(1M+)"

    def test_growth_phase_explosive(self, benchmark):
        assert benchmark._growth_phase(0.6) == GrowthPhase.EXPLOSIVE

    def test_growth_phase_rapid(self, benchmark):
        assert benchmark._growth_phase(0.3) == GrowthPhase.RAPID

    def test_growth_phase_steady(self, benchmark):
        assert benchmark._growth_phase(0.1) == GrowthPhase.STEADY

    def test_growth_phase_stagnant(self, benchmark):
        assert benchmark._growth_phase(0.02) == GrowthPhase.STAGNANT

    def test_growth_phase_declining(self, benchmark):
        assert benchmark._growth_phase(-0.1) == GrowthPhase.DECLINING


# ─── Edge Cases ──────────────────────────────────────────


class TestEdgeCases:
    def test_competitor_with_no_metrics(self, benchmark):
        benchmark.add_competitor(make_profile("empty_comp"))
        comp = benchmark.compare_metrics()
        assert comp["total_compared"] == 0

    def test_metrics_to_dict(self):
        m = make_metrics("test")
        d = m.to_dict()
        assert d["handle"] == "test"
        assert "engagement_rate" in d

    def test_single_competitor(self, benchmark):
        benchmark.add_competitor(make_profile("solo"))
        benchmark.add_metrics(make_metrics("solo", date="2026-01-01", followers=5000))
        benchmark.add_metrics(make_metrics("solo", date="2026-02-01", followers=6000))
        growth = benchmark.growth_comparison()
        assert "solo" in growth["growth_data"]

    def test_all_same_engagement_rate(self, benchmark):
        for i in range(3):
            handle = f"comp{i}"
            benchmark.add_competitor(make_profile(handle))
            benchmark.add_metrics(make_metrics(handle, eng_rate=0.05))
        bench = benchmark.engagement_benchmark()
        assert bench["overall_mean"] == 0.05
