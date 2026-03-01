"""
Tests for Performance Benchmarker
"""

import pytest
from datetime import datetime, timezone, timedelta
from bot.benchmarker import (
    Benchmarker, PeriodStats, PeriodComparison,
    HealthScore, GrowthTarget,
)
from bot.database import Database


@pytest.fixture
def db(tmp_path):
    return Database(str(tmp_path / "test.db"))


@pytest.fixture
def bench(db):
    return Benchmarker(db)


class TestPeriodStats:
    def test_defaults(self):
        ps = PeriodStats(period_name="Test", start_date="2026-01-01", end_date="2026-01-31")
        assert ps.tweet_count == 0
        assert ps.avg_impressions_per_tweet == 0

    def test_averages(self):
        ps = PeriodStats(
            period_name="Test", start_date="2026-01-01", end_date="2026-01-31",
            tweet_count=10, total_impressions=5000, total_engagements=250,
        )
        assert ps.avg_impressions_per_tweet == 500
        assert ps.avg_engagements_per_tweet == 25

    def test_like_to_retweet_ratio(self):
        ps = PeriodStats(
            period_name="Test", start_date="", end_date="",
            total_likes=300, total_retweets=100,
        )
        assert ps.like_to_retweet_ratio == 3.0

    def test_ratio_zero_retweets(self):
        ps = PeriodStats(
            period_name="Test", start_date="", end_date="",
            total_likes=100, total_retweets=0,
        )
        assert ps.like_to_retweet_ratio == 0

    def test_to_dict(self):
        ps = PeriodStats(period_name="Q1", start_date="2026-01-01", end_date="2026-03-31")
        d = ps.to_dict()
        assert d["period_name"] == "Q1"
        assert "avg_impressions_per_tweet" in d


class TestPeriodComparison:
    def test_growth(self):
        current = PeriodStats(
            period_name="Current", start_date="", end_date="",
            total_impressions=2000, total_engagements=200,
            avg_engagement_rate=5.0, posting_frequency=2.0,
        )
        previous = PeriodStats(
            period_name="Previous", start_date="", end_date="",
            total_impressions=1000, total_engagements=100,
            avg_engagement_rate=3.0, posting_frequency=1.5,
        )
        comp = PeriodComparison(current=current, previous=previous)
        assert comp.impressions_change_pct == 100.0
        assert comp.engagement_change_pct == 100.0
        assert comp.engagement_rate_change == 2.0
        assert comp.overall_trend in ["strong_growth", "growth"]

    def test_decline(self):
        current = PeriodStats(
            period_name="Current", start_date="", end_date="",
            total_impressions=500, total_engagements=30,
            avg_engagement_rate=1.0,
        )
        previous = PeriodStats(
            period_name="Previous", start_date="", end_date="",
            total_impressions=2000, total_engagements=200,
            avg_engagement_rate=5.0,
        )
        comp = PeriodComparison(current=current, previous=previous)
        assert comp.impressions_change_pct < 0
        assert comp.overall_trend in ["decline", "strong_decline"]

    def test_stable(self):
        stats = PeriodStats(
            period_name="Period", start_date="", end_date="",
            total_impressions=1000, total_engagements=100,
            avg_engagement_rate=3.0,
        )
        comp = PeriodComparison(current=stats, previous=stats)
        assert comp.overall_trend == "stable"
        assert comp.impressions_change_pct == 0

    def test_zero_previous(self):
        current = PeriodStats(
            period_name="Current", start_date="", end_date="",
            total_impressions=1000, total_engagements=100,
        )
        previous = PeriodStats(
            period_name="Previous", start_date="", end_date="",
            total_impressions=0, total_engagements=0,
        )
        comp = PeriodComparison(current=current, previous=previous)
        assert comp.impressions_change_pct == 0

    def test_to_dict(self):
        current = PeriodStats(period_name="C", start_date="", end_date="")
        previous = PeriodStats(period_name="P", start_date="", end_date="")
        comp = PeriodComparison(current=current, previous=previous)
        d = comp.to_dict()
        assert "current" in d
        assert "previous" in d
        assert "changes" in d


class TestHealthScore:
    def test_grade_s(self):
        h = HealthScore(overall=95)
        assert h.grade == "S"

    def test_grade_a(self):
        h = HealthScore(overall=80)
        assert h.grade == "A"

    def test_grade_b(self):
        h = HealthScore(overall=65)
        assert h.grade == "B"

    def test_grade_c(self):
        h = HealthScore(overall=45)
        assert h.grade == "C"

    def test_grade_d(self):
        h = HealthScore(overall=30)
        assert h.grade == "D"

    def test_to_dict(self):
        h = HealthScore(overall=75, consistency=80, engagement_quality=70)
        d = h.to_dict()
        assert d["grade"] == "A"
        assert d["consistency"] == 80


class TestGrowthTarget:
    def test_progress(self):
        t = GrowthTarget(metric="followers", target_value=1000, current_value=500)
        assert t.progress_pct == 50.0
        assert t.remaining == 500
        assert not t.is_achieved

    def test_achieved(self):
        t = GrowthTarget(metric="impressions", target_value=100, current_value=150)
        assert t.is_achieved
        assert t.progress_pct == 100.0

    def test_zero_target(self):
        t = GrowthTarget(metric="test", target_value=0)
        assert t.progress_pct == 0

    def test_days_remaining(self):
        future = (datetime.now(timezone.utc) + timedelta(days=10)).isoformat()
        t = GrowthTarget(metric="test", target_value=100, deadline=future)
        assert t.days_remaining is not None
        assert t.days_remaining >= 9

    def test_no_deadline(self):
        t = GrowthTarget(metric="test", target_value=100)
        assert t.days_remaining is None
        assert t.daily_pace_needed is None

    def test_daily_pace(self):
        future = (datetime.now(timezone.utc) + timedelta(days=10)).isoformat()
        t = GrowthTarget(
            metric="followers", target_value=200, current_value=100,
            deadline=future,
        )
        pace = t.daily_pace_needed
        assert pace is not None
        assert pace > 0

    def test_to_dict(self):
        t = GrowthTarget(metric="followers", target_value=1000, current_value=300)
        d = t.to_dict()
        assert d["progress_pct"] == 30.0
        assert d["remaining"] == 700


class TestLogTweet:
    def test_basic_log(self, bench):
        bench.log_tweet(
            tweet_id="t1", content="Hello world",
            impressions=100, likes=10, retweets=5, replies=2,
        )
        stats = bench.get_period_stats("2020-01-01", "2030-12-31")
        assert stats.tweet_count == 1

    def test_multiple_logs(self, bench):
        for i in range(5):
            bench.log_tweet(
                tweet_id=f"t{i}", content=f"Tweet {i}",
                impressions=100, likes=10, retweets=5, replies=2,
            )
        stats = bench.get_period_stats("2020-01-01", "2030-12-31")
        assert stats.tweet_count == 5
        assert stats.total_likes == 50


class TestLogFollowers:
    def test_log(self, bench):
        bench.log_followers(1000, 500)
        bench.log_followers(1050, 505)
        # Just verify no crash


class TestGetPeriodStats:
    def test_empty(self, bench):
        stats = bench.get_period_stats("2026-01-01", "2026-01-31")
        assert stats.tweet_count == 0

    def test_with_data(self, bench):
        bench.log_tweet(
            "t1", "Best tweet", 1000, 50, 20, 10,
            posted_at="2026-02-15T10:00:00",
        )
        bench.log_tweet(
            "t2", "Normal tweet", 500, 20, 5, 3,
            posted_at="2026-02-16T10:00:00",
        )
        stats = bench.get_period_stats("2026-02-01", "2026-02-28", "Feb 2026")
        assert stats.tweet_count == 2
        assert stats.total_impressions == 1500
        assert stats.best_tweet_id == "t1"

    def test_date_filter(self, bench):
        bench.log_tweet("t1", "Jan", 100, 10, 5, 2, posted_at="2026-01-15T10:00:00")
        bench.log_tweet("t2", "Feb", 200, 20, 10, 5, posted_at="2026-02-15T10:00:00")

        jan_stats = bench.get_period_stats("2026-01-01", "2026-01-31")
        assert jan_stats.tweet_count == 1
        assert jan_stats.total_impressions == 100


class TestComparePeriods:
    def test_compare(self, bench):
        bench.log_tweet("t1", "Old", 100, 5, 2, 1, posted_at="2026-01-15T10:00:00")
        bench.log_tweet("t2", "New", 500, 30, 10, 5, posted_at="2026-02-15T10:00:00")

        comp = bench.compare_periods(
            "2026-02-01", "2026-02-28",
            "2026-01-01", "2026-01-31",
        )
        assert comp.impressions_change_pct > 0
        assert isinstance(comp.overall_trend, str)


class TestHealthCheck:
    def test_empty(self, bench):
        health = bench.health_check(30)
        assert isinstance(health, HealthScore)
        assert 0 <= health.overall <= 100

    def test_with_data(self, bench):
        now = datetime.now(timezone.utc)
        for i in range(10):
            dt = (now - timedelta(days=i)).isoformat()
            bench.log_tweet(
                f"t{i}", f"Tweet {i} about #different #topics",
                impressions=200 + i * 50,
                likes=20 + i * 5,
                retweets=5 + i,
                replies=3 + i,
                posted_at=dt,
                tags=["topic" + str(i % 3)],
            )
        health = bench.health_check(30)
        assert health.overall > 0
        assert health.consistency > 0

    def test_health_recommendations(self, bench):
        health = bench.health_check(30)
        # Empty account should have recommendations
        assert isinstance(health.recommendations, list)


class TestGrowthTargets:
    def test_set_target(self, bench):
        target = bench.set_target("followers", 1000, "2026-12-31")
        assert target.metric == "followers"
        assert target.target_value == 1000

    def test_get_targets(self, bench):
        bench.set_target("followers", 1000)
        bench.set_target("impressions", 50000)
        targets = bench.get_targets()
        assert len(targets) == 2

    def test_update_target(self, bench):
        bench.set_target("followers", 100)
        bench.update_target("followers", 50)
        targets = bench.get_targets()
        assert any(t.current_value == 50 for t in targets)

    def test_auto_achieve(self, bench):
        bench.set_target("followers", 100)
        bench.update_target("followers", 150)
        active = bench.get_targets(include_achieved=False)
        all_targets = bench.get_targets(include_achieved=True)
        # Active should be empty (achieved)
        assert len(active) == 0


class TestFormatReports:
    def test_health_report(self, bench):
        report = bench.format_health_report(30)
        assert "Health" in report
        assert "Breakdown" in report

    def test_comparison_report(self, bench):
        current = PeriodStats(
            period_name="Current", start_date="", end_date="",
            total_impressions=2000, total_engagements=200,
            avg_engagement_rate=5.0, posting_frequency=1.5,
        )
        previous = PeriodStats(
            period_name="Previous", start_date="", end_date="",
            total_impressions=1000, total_engagements=100,
            avg_engagement_rate=3.0, posting_frequency=1.0,
        )
        comp = PeriodComparison(current=current, previous=previous)
        report = bench.format_comparison(comp)
        assert "Period Comparison" in report
        assert "Changes" in report

    def test_targets_report_empty(self, bench):
        report = bench.format_targets()
        assert "暂无" in report

    def test_targets_report(self, bench):
        bench.set_target("followers", 1000)
        report = bench.format_targets()
        assert "Growth Targets" in report
        assert "followers" in report
