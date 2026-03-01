"""
Tests for Smart Scheduling Engine
智能调度引擎测试: 最佳时段 + 互动预测 + 调度管理 + 自动排程 + 受众在线 + 节假日 + 频率
"""

import pytest
from datetime import datetime, timezone, timedelta
from bot.smart_scheduling import (
    SmartScheduler,
    PostRecord,
    ContentType,
    ScheduleEntry,
    TimeSlot,
    DayOfWeek,
)


# ─── Fixtures ──────────────────────────────────────────────


def make_record(
    post_id="p1",
    content_type=ContentType.TWEET,
    day=0,
    hour=14,
    impressions=1000,
    likes=50,
    retweets=10,
    replies=5,
    clicks=20,
    eng_rate=0.05,
):
    base = datetime(2026, 2, 24, hour, 0, tzinfo=timezone.utc)  # Monday
    target = base + timedelta(days=day)
    return PostRecord(
        post_id=post_id,
        content_type=content_type,
        posted_at=target.isoformat(),
        impressions=impressions,
        likes=likes,
        retweets=retweets,
        replies=replies,
        clicks=clicks,
        engagement_rate=eng_rate,
    )


def make_schedule_entry(entry_id="e1", hour=14, day_offset=1):
    dt = datetime(2026, 3, 1, hour, 0, tzinfo=timezone.utc) + timedelta(days=day_offset)
    return ScheduleEntry(
        entry_id=entry_id,
        content_type=ContentType.TWEET,
        scheduled_at=dt.isoformat(),
    )


@pytest.fixture
def scheduler():
    return SmartScheduler(audience_timezone=8)


@pytest.fixture
def populated_scheduler():
    s = SmartScheduler(audience_timezone=-5)
    # Add varied records across days/hours
    records = []
    for day in range(7):
        for hour in [9, 13, 18]:
            eng = 50 + (10 if hour == 13 else 0)  # peak at 13:00
            records.append(make_record(
                post_id=f"p_{day}_{hour}",
                day=day,
                hour=hour,
                likes=eng,
                impressions=1000 + day * 100,
                eng_rate=eng / 1000,
            ))
    for r in records:
        s.add_record(r)
    return s


# ─── PostRecord Tests ──────────────────────────────────────


class TestPostRecord:
    def test_total_engagement(self):
        r = make_record(likes=50, retweets=10, replies=5, clicks=20)
        assert r.total_engagement == 85

    def test_posted_datetime(self):
        r = make_record()
        dt = r.posted_datetime
        assert dt is not None
        assert dt.hour == 14

    def test_hour(self):
        r = make_record(hour=10)
        assert r.hour == 10

    def test_day_of_week(self):
        r = make_record(day=0)
        assert r.day_of_week is not None

    def test_invalid_datetime(self):
        r = PostRecord(post_id="x", content_type=ContentType.TWEET, posted_at="invalid")
        assert r.posted_datetime is None
        assert r.hour is None
        assert r.day_of_week is None


# ─── Best Times Tests ──────────────────────────────────────


class TestBestTimes:
    def test_analyze_with_data(self, populated_scheduler):
        best = populated_scheduler.analyze_best_times(top_n=5)
        assert len(best) <= 5
        assert all(isinstance(s, TimeSlot) for s in best)

    def test_analyze_empty(self, scheduler):
        best = scheduler.analyze_best_times()
        assert len(best) > 0  # default recommendations

    def test_analyze_by_content_type(self, populated_scheduler):
        best = populated_scheduler.analyze_best_times(content_type=ContentType.TWEET)
        assert len(best) > 0

    def test_sorted_by_score(self, populated_scheduler):
        best = populated_scheduler.analyze_best_times(top_n=10)
        scores = [s.score for s in best]
        assert scores == sorted(scores, reverse=True)

    def test_default_recommendations(self, scheduler):
        best = scheduler.analyze_best_times(content_type=ContentType.THREAD, top_n=5)
        assert len(best) <= 5
        assert all(s.confidence == "baseline" for s in best)


# ─── Engagement Prediction Tests ──────────────────────────


class TestEngagementPrediction:
    def test_predict_with_data(self, populated_scheduler):
        pred = populated_scheduler.predict_engagement(
            ContentType.TWEET, target_day=0, target_hour=13
        )
        assert pred["predicted_engagement"] > 0
        assert pred["method"] in ("exact_match", "nearby_slots", "content_type_avg")

    def test_predict_no_data(self, scheduler):
        pred = scheduler.predict_engagement(
            ContentType.TWEET, target_day=0, target_hour=3
        )
        assert pred["method"] == "no_data"

    def test_confidence_interval(self, populated_scheduler):
        # Add more data for same slot
        for i in range(5):
            populated_scheduler.add_record(make_record(
                post_id=f"extra_{i}",
                day=0,
                hour=13,
                likes=50 + i * 5,
            ))
        pred = populated_scheduler.predict_engagement(
            ContentType.TWEET, target_day=0, target_hour=13
        )
        ci = pred["confidence_interval"]
        assert len(ci) == 2
        assert ci[0] <= pred["predicted_engagement"] <= ci[1]

    def test_peak_hour_flag(self, populated_scheduler):
        pred = populated_scheduler.predict_engagement(
            ContentType.TWEET, target_day=0, target_hour=14
        )
        assert pred["is_peak_hour"] is True

    def test_avoid_hour_flag(self, populated_scheduler):
        pred = populated_scheduler.predict_engagement(
            ContentType.TWEET, target_day=0, target_hour=3
        )
        assert pred["is_avoid_hour"] is True


# ─── Schedule Management Tests ──────────────────────────


class TestScheduleManagement:
    def test_schedule_entry(self, scheduler):
        entry = make_schedule_entry("e1")
        result = scheduler.schedule(entry)
        assert result["status"] == "scheduled"

    def test_conflict_detection(self, scheduler):
        e1 = make_schedule_entry("e1", hour=14)
        e2 = make_schedule_entry("e2", hour=14)  # same hour
        scheduler.schedule(e1)
        result = scheduler.schedule(e2)
        assert len(result.get("conflicts", [])) > 0

    def test_unschedule(self, scheduler):
        scheduler.schedule(make_schedule_entry("e1"))
        assert scheduler.unschedule("e1") is True
        assert scheduler.unschedule("nonexistent") is False

    def test_get_schedule(self, scheduler):
        scheduler.schedule(make_schedule_entry("e1", day_offset=1))
        scheduler.schedule(make_schedule_entry("e2", day_offset=2))
        entries = scheduler.get_schedule(days_ahead=30)
        assert len(entries) == 2

    def test_get_schedule_filter(self, scheduler):
        e = ScheduleEntry(
            entry_id="e1",
            content_type=ContentType.THREAD,
            scheduled_at="2026-03-02T10:00:00+00:00",
        )
        scheduler.schedule(e)
        entries = scheduler.get_schedule(content_type=ContentType.TWEET)
        assert len(entries) == 0

    def test_reschedule(self, scheduler):
        scheduler.schedule(make_schedule_entry("e1"))
        result = scheduler.reschedule("e1", "2026-03-05T10:00:00+00:00")
        assert result["new_time"] == "2026-03-05T10:00:00+00:00"

    def test_reschedule_missing(self, scheduler):
        result = scheduler.reschedule("nonexistent", "2026-03-05T10:00:00+00:00")
        assert "error" in result


# ─── Auto Schedule Tests ──────────────────────────────────


class TestAutoSchedule:
    def test_auto_schedule(self, populated_scheduler):
        plan = populated_scheduler.auto_schedule(
            content_types=[ContentType.TWEET, ContentType.THREAD],
            days_ahead=3,
            posts_per_day=2,
        )
        assert len(plan) > 0
        assert len(plan) <= 6  # 3 days * 2 per day

    def test_auto_schedule_gap(self, populated_scheduler):
        plan = populated_scheduler.auto_schedule(
            content_types=[ContentType.TWEET],
            days_ahead=1,
            posts_per_day=3,
            min_gap_hours=3,
        )
        # Check gaps
        hours = [p["hour"] for p in plan]
        for i in range(1, len(hours)):
            assert abs(hours[i] - hours[i - 1]) >= 3

    def test_auto_schedule_empty_history(self, scheduler):
        plan = scheduler.auto_schedule(
            content_types=[ContentType.TWEET],
            days_ahead=2,
        )
        assert len(plan) > 0

    def test_blocked_slots(self, populated_scheduler):
        populated_scheduler.block_slot(0, 13)  # Block Monday 13:00
        plan = populated_scheduler.auto_schedule(
            content_types=[ContentType.TWEET],
            days_ahead=7,
        )
        monday_13 = [p for p in plan if p["day_name"] == "MONDAY" and p["hour"] == 13]
        assert len(monday_13) == 0


# ─── Audience Online Windows Tests ──────────────────────


class TestAudienceOnlineWindows:
    def test_with_data(self, populated_scheduler):
        windows = populated_scheduler.audience_online_windows()
        assert "online_windows" in windows
        assert "hourly_impressions" in windows

    def test_best_day(self, populated_scheduler):
        windows = populated_scheduler.audience_online_windows()
        assert windows.get("best_day") is not None

    def test_empty_data(self, scheduler):
        windows = scheduler.audience_online_windows()
        assert windows["note"] == "no data"


# ─── Holiday Tests ──────────────────────────────────────


class TestHolidays:
    def test_check_holiday(self, scheduler):
        result = scheduler._check_holiday("2026-12-25T10:00:00+00:00")
        assert result == "Christmas"

    def test_check_non_holiday(self, scheduler):
        result = scheduler._check_holiday("2026-03-15T10:00:00+00:00")
        assert result is None

    def test_upcoming_holidays(self, scheduler):
        holidays = scheduler.upcoming_holidays(days_ahead=365)
        assert len(holidays) > 0
        assert all("name" in h for h in holidays)

    def test_schedule_holiday_warning(self, scheduler):
        entry = ScheduleEntry(
            entry_id="xmas",
            content_type=ContentType.TWEET,
            scheduled_at="2026-12-25T10:00:00+00:00",
        )
        result = scheduler.schedule(entry)
        assert result.get("holiday_warning") == "Christmas"


# ─── Block Slot Tests ──────────────────────────────────


class TestBlockSlots:
    def test_block(self, scheduler):
        scheduler.block_slot(0, 9)
        assert len(scheduler.blocked_slots()) == 1

    def test_unblock(self, scheduler):
        scheduler.block_slot(0, 9)
        assert scheduler.unblock_slot(0, 9) is True
        assert len(scheduler.blocked_slots()) == 0

    def test_unblock_missing(self, scheduler):
        assert scheduler.unblock_slot(0, 9) is False


# ─── AB Schedule Test ──────────────────────────────────


class TestABScheduleTest:
    def test_ab_test(self, populated_scheduler):
        result = populated_scheduler.ab_schedule_test(
            ContentType.TWEET,
            slot_a=(0, 9),   # Monday 9
            slot_b=(0, 13),  # Monday 13
        )
        assert result["slot_a"]["count"] >= 0
        assert result["slot_b"]["count"] >= 0
        assert "winner" in result

    def test_ab_no_data(self, scheduler):
        result = scheduler.ab_schedule_test(
            ContentType.TWEET,
            slot_a=(0, 9),
            slot_b=(0, 13),
        )
        assert result["winner"] is None


# ─── Posting Frequency Tests ──────────────────────────


class TestPostingFrequency:
    def test_frequency_analysis(self, populated_scheduler):
        freq = populated_scheduler.posting_frequency_analysis()
        assert freq["avg_posts_per_day"] > 0
        assert "recommendation" in freq

    def test_frequency_empty(self, scheduler):
        freq = scheduler.posting_frequency_analysis()
        assert freq.get("note") == "no data"


# ─── Report Tests ──────────────────────────────────────


class TestScheduleReport:
    def test_generate_report(self, populated_scheduler):
        report = populated_scheduler.generate_schedule_report()
        assert "top_5_time_slots" in report
        assert "audience_windows" in report
        assert "posting_frequency" in report
        assert "upcoming_holidays" in report

    def test_report_structure(self, populated_scheduler):
        report = populated_scheduler.generate_schedule_report()
        assert len(report["top_5_time_slots"]) <= 5
        for slot in report["top_5_time_slots"]:
            assert "day" in slot
            assert "hour" in slot
            assert "score" in slot


# ─── Content Type Scheduling Tests ──────────────────────


class TestContentTypeScheduling:
    def test_thread_optimal_hours(self, scheduler):
        hours = SmartScheduler.CONTENT_OPTIMAL_HOURS[ContentType.THREAD]
        assert 8 in hours  # morning reading

    def test_poll_optimal_hours(self, scheduler):
        hours = SmartScheduler.CONTENT_OPTIMAL_HOURS[ContentType.POLL]
        assert 12 in hours or 13 in hours  # lunch engagement

    def test_space_optimal_hours(self, scheduler):
        hours = SmartScheduler.CONTENT_OPTIMAL_HOURS[ContentType.SPACE]
        assert 20 in hours or 21 in hours  # evening live


# ─── Edge Cases ──────────────────────────────────────────


class TestEdgeCases:
    def test_add_records_batch(self, scheduler):
        records = [make_record(post_id=f"p{i}", day=i) for i in range(5)]
        scheduler.add_records(records)
        best = scheduler.analyze_best_times()
        assert len(best) > 0

    def test_clear_history(self, populated_scheduler):
        populated_scheduler.clear_history()
        best = populated_scheduler.analyze_best_times()
        # Should fall back to defaults
        assert all(s.confidence == "baseline" for s in best)

    def test_multiple_content_types(self, scheduler):
        scheduler.add_record(make_record(post_id="t1", content_type=ContentType.THREAD))
        scheduler.add_record(make_record(post_id="p1", content_type=ContentType.POLL))
        scheduler.add_record(make_record(post_id="m1", content_type=ContentType.MEDIA))
        best = scheduler.analyze_best_times()
        assert len(best) > 0
