"""Tests for Analytics Pipeline"""
import pytest
from datetime import datetime, timezone, timedelta
from bot.analytics_pipeline import (
    AnalyticsPipeline, TweetMetrics, EngagementCurve,
    PostingTimeAnalyzer, TrendDetector, TrendDirection,
)


@pytest.fixture
def pipeline():
    p = AnalyticsPipeline(db_path=":memory:")
    yield p
    p.close()


@pytest.fixture
def sample_metrics():
    return TweetMetrics(
        tweet_id="tw1", impressions=1000, engagements=50,
        likes=30, retweets=10, replies=5, quotes=3, bookmarks=2,
        author_id="auth1", text_preview="Test tweet",
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


class TestTweetMetrics:
    def test_engagement_rate(self, sample_metrics):
        assert sample_metrics.engagement_rate == 0.05

    def test_zero_impressions(self):
        m = TweetMetrics(tweet_id="x", impressions=0, engagements=10)
        assert m.engagement_rate == 0.0

    def test_auto_timestamp(self):
        m = TweetMetrics(tweet_id="x")
        assert m.timestamp != ""


class TestEngagementCurve:
    def test_add_and_get(self):
        curve = EngagementCurve()
        curve.add_point("tw1", 0.0, 0)
        curve.add_point("tw1", 1.0, 50)
        curve.add_point("tw1", 2.0, 80)
        data = curve.get_curve("tw1")
        assert len(data) == 3

    def test_empty_curve(self):
        curve = EngagementCurve()
        assert curve.get_curve("nonexistent") == []

    def test_peak_hour(self):
        curve = EngagementCurve()
        curve.add_point("tw1", 0, 0)
        curve.add_point("tw1", 1, 100)
        curve.add_point("tw1", 2, 120)
        curve.add_point("tw1", 3, 125)
        peak = curve.get_peak_hour("tw1")
        assert peak is not None
        assert peak == 1.0  # biggest jump is 0→100

    def test_peak_hour_insufficient_data(self):
        curve = EngagementCurve()
        curve.add_point("tw1", 0, 0)
        assert curve.get_peak_hour("tw1") is None

    def test_decay_rate(self):
        curve = EngagementCurve()
        for i in range(10):
            curve.add_point("tw1", float(i), max(0, 100 - i * 5))
        rate = curve.get_decay_rate("tw1")
        assert rate is not None

    def test_decay_rate_insufficient(self):
        curve = EngagementCurve()
        curve.add_point("tw1", 0, 100)
        assert curve.get_decay_rate("tw1") is None


class TestPostingTimeAnalyzer:
    def test_add_and_get_best_times(self):
        analyzer = PostingTimeAnalyzer()
        for _ in range(5):
            analyzer.add_data(10, 1, 0.08, 1000)
            analyzer.add_data(14, 3, 0.05, 800)
            analyzer.add_data(10, 1, 0.09, 1200)
        results = analyzer.get_best_times(2)
        assert len(results) > 0
        assert results[0].hour == 10  # better engagement

    def test_empty_data(self):
        analyzer = PostingTimeAnalyzer()
        assert analyzer.get_best_times() == []

    def test_hourly_heatmap(self):
        analyzer = PostingTimeAnalyzer()
        analyzer.add_data(9, 0, 0.05, 500)
        analyzer.add_data(14, 0, 0.08, 800)
        heatmap = analyzer.get_hourly_heatmap()
        assert 9 in heatmap
        assert 14 in heatmap

    def test_insufficient_samples_filtered(self):
        analyzer = PostingTimeAnalyzer()
        analyzer.add_data(9, 0, 0.05, 500)  # only 1 sample
        results = analyzer.get_best_times()
        assert len(results) == 0  # need at least 2 samples


class TestTrendDetector:
    def test_rising_trend(self):
        detector = TrendDetector()
        values = [10, 15, 20, 25, 30, 35, 40]
        result = detector.detect(values)
        assert result.direction == TrendDirection.RISING

    def test_falling_trend(self):
        detector = TrendDetector()
        values = [40, 35, 30, 25, 20, 15, 10]
        result = detector.detect(values)
        assert result.direction == TrendDirection.FALLING

    def test_stable_trend(self):
        detector = TrendDetector()
        values = [50, 50, 50, 50, 50]
        result = detector.detect(values)
        assert result.direction == TrendDirection.STABLE

    def test_insufficient_data(self):
        detector = TrendDetector()
        result = detector.detect([10, 20])
        assert result.direction == TrendDirection.STABLE

    def test_anomaly_detection(self):
        detector = TrendDetector()
        values = [10, 10, 10, 10, 100, 10, 10, 10]
        result = detector.detect(values)
        assert len(result.anomalies) > 0

    def test_no_anomalies(self):
        detector = TrendDetector()
        values = [10, 11, 10, 11, 10, 11]
        result = detector.detect(values)
        assert len(result.anomalies) == 0


class TestAnalyticsPipeline:
    def test_ingest(self, pipeline, sample_metrics):
        pipeline.ingest(sample_metrics)
        assert pipeline.get_total_metrics() == 1

    def test_multiple_ingest(self, pipeline):
        for i in range(10):
            m = TweetMetrics(
                tweet_id=f"tw{i}", impressions=1000, engagements=50 + i * 5,
                likes=20, retweets=5, replies=3,
                timestamp=(datetime.now(timezone.utc) - timedelta(hours=i)).isoformat(),
            )
            pipeline.ingest(m)
        assert pipeline.get_total_metrics() == 10

    def test_best_posting_times(self, pipeline):
        # Add data at different hours
        for hour in [9, 9, 9, 14, 14, 14]:
            m = TweetMetrics(
                tweet_id=f"tw_h{hour}_{hash(str(hour) + str(id(hour)))}",
                impressions=1000, engagements=50,
                likes=20, retweets=5, replies=3,
                timestamp=datetime(2026, 1, 15, hour, 0, tzinfo=timezone.utc).isoformat(),
            )
            pipeline.ingest(m)
        times = pipeline.get_best_posting_times()
        assert isinstance(times, list)

    def test_curve_tracking(self, pipeline):
        pipeline.add_curve_point("tw1", 0.0, 0)
        pipeline.add_curve_point("tw1", 1.0, 50)
        curve = pipeline.get_engagement_curve("tw1")
        assert len(curve) == 2

    def test_detect_trends_empty(self, pipeline):
        trends = pipeline.detect_trends()
        assert trends == {}

    def test_predict_performance_empty(self, pipeline):
        result = pipeline.predict_performance(100, 10, 1)
        assert result["confidence"] == 0

    def test_predict_performance_with_data(self, pipeline):
        for i in range(20):
            m = TweetMetrics(
                tweet_id=f"tw{i}", impressions=1000, engagements=50,
                timestamp=datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc).isoformat(),
            )
            pipeline.ingest(m)
        result = pipeline.predict_performance(100, 10, 1)
        assert result["predicted_impressions"] > 0

    def test_compare_periods(self, pipeline):
        for i in range(5):
            m = TweetMetrics(
                tweet_id=f"jan{i}", impressions=1000, engagements=50, likes=20,
                timestamp=f"2026-01-{10 + i:02d}T10:00:00+00:00",
            )
            pipeline.ingest(m)
        for i in range(5):
            m = TweetMetrics(
                tweet_id=f"feb{i}", impressions=2000, engagements=100, likes=50,
                timestamp=f"2026-02-{10 + i:02d}T10:00:00+00:00",
            )
            pipeline.ingest(m)
        result = pipeline.compare_periods(
            "2026-01-10", "2026-01-15", "2026-02-10", "2026-02-15"
        )
        assert "period1" in result
        assert "period2" in result
        assert "changes" in result

    def test_export_report_json(self, pipeline, sample_metrics):
        pipeline.ingest(sample_metrics)
        report = pipeline.export_report(format="json")
        import json
        data = json.loads(report)
        assert data["total_tweets"] == 1

    def test_export_report_text(self, pipeline, sample_metrics):
        pipeline.ingest(sample_metrics)
        report = pipeline.export_report(format="text")
        assert "Analytics Report" in report

    def test_export_empty(self, pipeline):
        report = pipeline.export_report()
        import json
        data = json.loads(report)
        assert data["total_tweets"] == 0
