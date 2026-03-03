"""
Tests for Thread Analytics Engine
线程分析引擎测试: 完读率 + 互动衰减 + 最优长度 + 格式对比 + 位置热力图 + 推荐 + 导出
"""

import json
import pytest
from bot.thread_analytics import (
    ThreadAnalytics,
    ThreadRecord,
    TweetMetrics,
    ThreadFormat,
)


# ─── Fixtures ──────────────────────────────────────────────


def make_tweet(pos, impressions=1000, likes=50, retweets=10, replies=5, text=""):
    return TweetMetrics(
        position=pos,
        tweet_id=f"tw_{pos}",
        text=text or f"Tweet {pos}",
        impressions=impressions,
        likes=likes,
        retweets=retweets,
        replies=replies,
        quotes=2,
        bookmarks=3,
    )


def make_thread(
    thread_id="t1",
    length=5,
    fmt=ThreadFormat.NARRATIVE,
    author="user1",
    decay_factor=0.8,
    base_impressions=1000,
    base_likes=50,
    topic="tech",
    tags=None,
):
    tweets = []
    for i in range(1, length + 1):
        factor = decay_factor ** (i - 1)
        tweets.append(make_tweet(
            pos=i,
            impressions=int(base_impressions * factor),
            likes=int(base_likes * factor),
            retweets=int(10 * factor),
            replies=int(5 * factor),
        ))
    return ThreadRecord(
        thread_id=thread_id,
        author=author,
        format=fmt,
        tweets=tweets,
        tags=tags or ["tech"],
        topic=topic,
        created_at="2026-02-01T10:00:00+00:00",
    )


@pytest.fixture
def analytics():
    return ThreadAnalytics()


@pytest.fixture
def populated_analytics():
    ta = ThreadAnalytics()
    ta.add_thread(make_thread("t1", 5, ThreadFormat.NARRATIVE, decay_factor=0.8))
    ta.add_thread(make_thread("t2", 3, ThreadFormat.NUMBERED, decay_factor=0.9, base_likes=80))
    ta.add_thread(make_thread("t3", 10, ThreadFormat.TUTORIAL, decay_factor=0.7, base_likes=30))
    ta.add_thread(make_thread("t4", 7, ThreadFormat.QA, decay_factor=0.85, base_likes=60))
    ta.add_thread(make_thread("t5", 4, ThreadFormat.LISTICLE, decay_factor=0.75, base_likes=100))
    return ta


# ─── TweetMetrics Tests ──────────────────────────────────


class TestTweetMetrics:
    def test_engagement_total(self):
        t = make_tweet(1, likes=50, retweets=10, replies=5)
        assert t.engagement_total == 50 + 10 + 5 + 2 + 3  # +quotes+bookmarks

    def test_engagement_rate(self):
        t = make_tweet(1, impressions=1000, likes=50, retweets=10, replies=5)
        assert t.engagement_rate == t.engagement_total / 1000

    def test_engagement_rate_zero_impressions(self):
        t = make_tweet(1, impressions=0)
        assert t.engagement_rate == 0.0

    def test_to_dict(self):
        t = make_tweet(1)
        d = t.to_dict()
        assert "engagement_total" in d
        assert "engagement_rate" in d
        assert d["position"] == 1


# ─── ThreadRecord Tests ──────────────────────────────────


class TestThreadRecord:
    def test_length(self):
        thread = make_thread("t1", 5)
        assert thread.length == 5

    def test_total_impressions(self):
        thread = make_thread("t1", 3, decay_factor=1.0, base_impressions=100)
        assert thread.total_impressions == 300

    def test_total_engagement(self):
        thread = make_thread("t1", 1, decay_factor=1.0)
        assert thread.total_engagement > 0

    def test_avg_engagement_rate(self):
        thread = make_thread("t1", 3, decay_factor=1.0)
        rate = thread.avg_engagement_rate
        assert 0 < rate < 1


# ─── CRUD Tests ──────────────────────────────────────────


class TestCRUD:
    def test_add_thread(self, analytics):
        thread = make_thread("t1")
        analytics.add_thread(thread)
        assert analytics.get_thread("t1") is not None

    def test_add_thread_no_id(self, analytics):
        thread = ThreadRecord(thread_id="")
        with pytest.raises(ValueError):
            analytics.add_thread(thread)

    def test_get_thread_missing(self, analytics):
        assert analytics.get_thread("nonexistent") is None

    def test_remove_thread(self, analytics):
        analytics.add_thread(make_thread("t1"))
        assert analytics.remove_thread("t1") is True
        assert analytics.get_thread("t1") is None

    def test_remove_thread_missing(self, analytics):
        assert analytics.remove_thread("nonexistent") is False

    def test_list_threads_all(self, populated_analytics):
        threads = populated_analytics.list_threads()
        assert len(threads) == 5

    def test_list_threads_by_author(self, populated_analytics):
        threads = populated_analytics.list_threads(author="user1")
        assert len(threads) == 5

    def test_list_threads_by_format(self, populated_analytics):
        threads = populated_analytics.list_threads(fmt=ThreadFormat.NARRATIVE)
        assert len(threads) == 1

    def test_list_threads_by_length(self, populated_analytics):
        threads = populated_analytics.list_threads(min_length=5, max_length=8)
        assert all(5 <= t.length <= 8 for t in threads)

    def test_list_threads_by_topic(self, populated_analytics):
        threads = populated_analytics.list_threads(topic="tech")
        assert len(threads) == 5


# ─── Completion Rate Tests ──────────────────────────────


class TestCompletionRate:
    def test_basic_completion(self, analytics):
        analytics.add_thread(make_thread("t1", 5, decay_factor=0.8))
        cr = analytics.completion_rate("t1")
        assert cr["completion_rate"] > 0
        assert cr["completion_rate"] < 1

    def test_perfect_completion(self, analytics):
        analytics.add_thread(make_thread("t1", 5, decay_factor=1.0))
        cr = analytics.completion_rate("t1")
        assert cr["completion_rate"] == 1.0

    def test_completion_missing_thread(self, analytics):
        cr = analytics.completion_rate("nonexistent")
        assert "error" in cr

    def test_completion_pct_format(self, analytics):
        analytics.add_thread(make_thread("t1", 5, decay_factor=0.8))
        cr = analytics.completion_rate("t1")
        assert "%" in cr["completion_pct"]

    def test_max_drop_position(self, analytics):
        tweets = [
            make_tweet(1, impressions=1000),
            make_tweet(2, impressions=900),
            make_tweet(3, impressions=400),  # big drop
            make_tweet(4, impressions=350),
        ]
        thread = ThreadRecord(thread_id="t1", tweets=tweets)
        analytics.add_thread(thread)
        cr = analytics.completion_rate("t1")
        assert cr["max_drop_position"] == 3

    def test_position_retention(self, analytics):
        analytics.add_thread(make_thread("t1", 5, decay_factor=0.8))
        cr = analytics.completion_rate("t1")
        assert len(cr["position_retention"]) == 5
        assert cr["position_retention"][0]["retention"] == 1.0

    def test_completion_no_impressions(self, analytics):
        tweets = [make_tweet(1, impressions=0)]
        thread = ThreadRecord(thread_id="t1", tweets=tweets)
        analytics.add_thread(thread)
        cr = analytics.completion_rate("t1")
        assert "error" in cr

    def test_batch_completion_rates(self, populated_analytics):
        results = populated_analytics.batch_completion_rates()
        assert len(results) == 5
        # sorted descending
        rates = [r["completion_rate"] for r in results]
        assert rates == sorted(rates, reverse=True)


# ─── Engagement Decay Tests ──────────────────────────────


class TestEngagementDecay:
    def test_declining_decay(self, analytics):
        analytics.add_thread(make_thread("t1", 5, decay_factor=0.5))
        decay = analytics.engagement_decay("t1")
        assert decay["trend"] in ("declining", "cliff")

    def test_stable_thread(self, analytics):
        analytics.add_thread(make_thread("t1", 5, decay_factor=0.99))
        decay = analytics.engagement_decay("t1")
        assert decay["trend"] in ("stable", "declining")

    def test_half_life(self, analytics):
        analytics.add_thread(make_thread("t1", 10, decay_factor=0.7))
        decay = analytics.engagement_decay("t1")
        if decay["half_life_position"]:
            assert decay["half_life_position"] > 1

    def test_insufficient_data(self, analytics):
        tweets = [make_tweet(1)]
        thread = ThreadRecord(thread_id="t1", tweets=tweets)
        analytics.add_thread(thread)
        decay = analytics.engagement_decay("t1")
        assert decay["trend"] == "insufficient_data"

    def test_decay_points(self, analytics):
        analytics.add_thread(make_thread("t1", 5, decay_factor=0.8))
        decay = analytics.engagement_decay("t1")
        assert len(decay["decay_points"]) == 4

    def test_engagement_sequence(self, analytics):
        analytics.add_thread(make_thread("t1", 5, decay_factor=0.8))
        decay = analytics.engagement_decay("t1")
        assert len(decay["engagement_sequence"]) == 5

    def test_cliff_detection(self, analytics):
        tweets = [
            make_tweet(1, likes=100, retweets=50, replies=20),
            make_tweet(2, likes=5, retweets=2, replies=1),
            make_tweet(3, likes=3, retweets=1, replies=0),
        ]
        thread = ThreadRecord(thread_id="t1", tweets=tweets)
        analytics.add_thread(thread)
        decay = analytics.engagement_decay("t1")
        assert decay["trend"] in ("cliff", "declining")


# ─── Optimal Length Tests ──────────────────────────────


class TestOptimalLength:
    def test_basic_recommendation(self, populated_analytics):
        opt = populated_analytics.optimal_length(min_threads=2)
        assert opt["recommendation"] is not None
        assert opt["best_score"] > 0

    def test_insufficient_threads(self, analytics):
        analytics.add_thread(make_thread("t1"))
        opt = analytics.optimal_length(min_threads=5)
        assert opt["recommendation"] is None

    def test_buckets(self, populated_analytics):
        opt = populated_analytics.optimal_length(min_threads=2)
        assert "buckets" in opt

    def test_total_threads_analyzed(self, populated_analytics):
        opt = populated_analytics.optimal_length(min_threads=2)
        assert opt["total_threads_analyzed"] == 5


# ─── Format Comparison Tests ──────────────────────────


class TestFormatComparison:
    def test_basic_comparison(self, populated_analytics):
        comp = populated_analytics.format_comparison()
        assert "formats" in comp
        assert len(comp["formats"]) >= 3

    def test_ranking(self, populated_analytics):
        comp = populated_analytics.format_comparison()
        assert "ranking" in comp
        assert comp["best_format"] is not None

    def test_empty(self, analytics):
        comp = analytics.format_comparison()
        assert comp["best_format"] is None

    def test_format_count(self, populated_analytics):
        comp = populated_analytics.format_comparison()
        for fmt_data in comp["formats"].values():
            assert fmt_data["count"] >= 1


# ─── Heatmap Tests ──────────────────────────────────────


class TestPositionHeatmap:
    def test_basic_heatmap(self, analytics):
        analytics.add_thread(make_thread("t1", 5))
        hm = analytics.position_heatmap("t1")
        assert len(hm["heatmap"]) == 5

    def test_hotspots(self, analytics):
        analytics.add_thread(make_thread("t1", 5))
        hm = analytics.position_heatmap("t1")
        assert len(hm["hotspots"]) <= 3

    def test_cold_spots(self, analytics):
        analytics.add_thread(make_thread("t1", 5))
        hm = analytics.position_heatmap("t1")
        assert "cold_spots" in hm

    def test_intensity_range(self, analytics):
        analytics.add_thread(make_thread("t1", 5))
        hm = analytics.position_heatmap("t1")
        for point in hm["heatmap"]:
            assert 0 <= point["intensity"] <= 1

    def test_missing_thread(self, analytics):
        hm = analytics.position_heatmap("nonexistent")
        assert "error" in hm


# ─── Aggregate Position Tests ──────────────────────────


class TestAggregatePosition:
    def test_basic_aggregate(self, populated_analytics):
        agg = populated_analytics.aggregate_position_performance()
        assert len(agg["positions"]) > 0
        assert agg["best_position"] is not None

    def test_position_1_exists(self, populated_analytics):
        agg = populated_analytics.aggregate_position_performance()
        assert 1 in agg["positions"]

    def test_sample_count(self, populated_analytics):
        agg = populated_analytics.aggregate_position_performance()
        # position 1 should have all 5 threads
        assert agg["positions"][1]["sample_count"] == 5

    def test_max_position_limit(self, populated_analytics):
        agg = populated_analytics.aggregate_position_performance(max_position=3)
        assert all(pos <= 3 for pos in agg["positions"])


# ─── Performance Over Time Tests ──────────────────────


class TestPerformanceOverTime:
    def test_basic(self, populated_analytics):
        perf = populated_analytics.performance_over_time(days=365)
        assert perf["thread_count"] == 5

    def test_author_filter(self, populated_analytics):
        perf = populated_analytics.performance_over_time(author="user1", days=365)
        assert perf["thread_count"] == 5

    def test_trend(self, populated_analytics):
        perf = populated_analytics.performance_over_time(days=365)
        assert perf["trend"] in ("improving", "declining", "stable", "insufficient_data")


# ─── Recommendations Tests ──────────────────────────────


class TestRecommendations:
    def test_basic_recs(self, populated_analytics):
        recs = populated_analytics.recommendations()
        assert len(recs) > 0

    def test_empty_recs(self, analytics):
        recs = analytics.recommendations()
        assert len(recs) == 1
        assert recs[0]["type"] == "info"

    def test_rec_types(self, populated_analytics):
        recs = populated_analytics.recommendations()
        types = {r["type"] for r in recs}
        # should have at least length or format
        assert len(types) >= 1


# ─── Report Tests ──────────────────────────────────────


class TestReport:
    def test_generate_report(self, analytics):
        analytics.add_thread(make_thread("t1", 5))
        report = analytics.generate_report("t1")
        assert report["thread_id"] == "t1"
        assert "completion" in report
        assert "decay" in report
        assert "heatmap" in report

    def test_report_missing(self, analytics):
        report = analytics.generate_report("nonexistent")
        assert "error" in report

    def test_export_json(self, populated_analytics):
        exported = populated_analytics.export_all("json")
        data = json.loads(exported)
        assert len(data["threads"]) == 5
        assert data["summary"]["total_threads"] == 5

    def test_export_csv(self, populated_analytics):
        exported = populated_analytics.export_all("csv")
        lines = exported.strip().split("\n")
        assert len(lines) == 6  # header + 5 threads


# ─── Format Detection Tests ──────────────────────────────


class TestFormatDetection:
    def test_detect_numbered(self, analytics):
        tweets = [
            make_tweet(i, text=f"{i}/ Point number {i}") for i in range(1, 6)
        ]
        thread = ThreadRecord(thread_id="t1", tweets=tweets)
        analytics.add_thread(thread)
        assert thread.format == ThreadFormat.NUMBERED

    def test_detect_qa(self, analytics):
        tweets = [
            make_tweet(1, text="Q: What is Python?"),
            make_tweet(2, text="A: A programming language"),
            make_tweet(3, text="Q: Why use it?"),
            make_tweet(4, text="A: It's versatile"),
        ]
        thread = ThreadRecord(thread_id="t1", tweets=tweets)
        analytics.add_thread(thread)
        assert thread.format == ThreadFormat.QA

    def test_detect_listicle(self, analytics):
        tweets = [
            make_tweet(i, text=f"• Point {i}") for i in range(1, 6)
        ]
        thread = ThreadRecord(thread_id="t1", tweets=tweets)
        analytics.add_thread(thread)
        assert thread.format == ThreadFormat.LISTICLE

    def test_detect_tutorial(self, analytics):
        tweets = [
            make_tweet(1, text="Step 1: Install Python"),
            make_tweet(2, text="Step 2: Create a file"),
            make_tweet(3, text="Next, write your code"),
            make_tweet(4, text="Finally, run it"),
        ]
        thread = ThreadRecord(thread_id="t1", tweets=tweets)
        analytics.add_thread(thread)
        assert thread.format == ThreadFormat.TUTORIAL

    def test_detect_narrative(self, analytics):
        tweets = [
            make_tweet(i, text=f"I woke up and the sky was different on day {i}")
            for i in range(1, 6)
        ]
        thread = ThreadRecord(thread_id="t1", tweets=tweets)
        analytics.add_thread(thread)
        assert thread.format == ThreadFormat.NARRATIVE

    def test_skip_detection_if_set(self, analytics):
        tweets = [make_tweet(i, text=f"{i}/ Point {i}") for i in range(1, 4)]
        thread = ThreadRecord(thread_id="t1", tweets=tweets, format=ThreadFormat.DEBATE)
        analytics.add_thread(thread)
        assert thread.format == ThreadFormat.DEBATE


# ─── Edge Cases ──────────────────────────────────────────


class TestEdgeCases:
    def test_single_tweet_thread(self, analytics):
        thread = ThreadRecord(
            thread_id="t1",
            tweets=[make_tweet(1)],
        )
        analytics.add_thread(thread)
        cr = analytics.completion_rate("t1")
        assert cr["completion_rate"] == 1.0

    def test_all_zero_engagement(self, analytics):
        tweets = [
            TweetMetrics(position=i, impressions=100, likes=0, retweets=0, replies=0)
            for i in range(1, 4)
        ]
        thread = ThreadRecord(thread_id="t1", tweets=tweets)
        analytics.add_thread(thread)
        hm = analytics.position_heatmap("t1")
        assert all(p["level"] == "❄️" for p in hm["heatmap"])

    def test_resurgent_trend(self, analytics):
        tweets = [
            make_tweet(1, likes=100, retweets=50, replies=20),
            make_tweet(2, likes=10, retweets=5, replies=2),
            make_tweet(3, likes=80, retweets=40, replies=15),
        ]
        thread = ThreadRecord(thread_id="t1", tweets=tweets)
        analytics.add_thread(thread)
        decay = analytics.engagement_decay("t1")
        assert decay["bounce_back_position"] == 3
