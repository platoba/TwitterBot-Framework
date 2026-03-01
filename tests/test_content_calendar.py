"""
tests/test_content_calendar.py - 内容日历测试
"""

import os
import pytest
from datetime import datetime, timedelta
from bot.content_calendar import ContentCalendar, CalendarEntry

TEST_DB = "/tmp/test_content_calendar.db"


@pytest.fixture(autouse=True)
def cleanup():
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


@pytest.fixture
def cal():
    return ContentCalendar(db_path=TEST_DB)


class TestCalendarEntry:
    def test_create(self):
        e = CalendarEntry(title="Test Post", content="Hello world")
        assert e.title == "Test Post"
        assert e.entry_id is not None

    def test_to_dict(self):
        e = CalendarEntry(title="Test", content="Content", category="thread")
        d = e.to_dict()
        assert d["title"] == "Test"
        assert "category" in d


class TestContentCalendar:
    def test_add_entry(self, cal):
        entry_id = cal.add_entry("My Post", "Hello world", category="original")
        assert entry_id is not None

    def test_get_entry(self, cal):
        entry_obj = cal.add_entry("Test", "Content")
        entry = cal.get_entry(entry_obj.entry_id)
        assert entry is not None
        assert entry["title"] == "Test"

    def test_get_nonexistent(self, cal):
        entry = cal.get_entry("nonexistent")
        assert entry is None

    def test_update_entry(self, cal):
        entry_obj = cal.add_entry("Old", "Old content")
        result = cal.update_entry(entry_obj.entry_id, title="New", content="New content")
        assert result
        entry = cal.get_entry(entry_obj.entry_id)
        assert entry["title"] == "New"

    def test_update_nonexistent(self, cal):
        result = cal.update_entry("fake", title="X")
        assert not result

    def test_delete_entry(self, cal):
        entry_obj = cal.add_entry("Delete me", "")
        result = cal.delete_entry(entry_obj.entry_id)
        assert result
        assert cal.get_entry(entry_obj.entry_id) is None

    def test_delete_nonexistent(self, cal):
        assert not cal.delete_entry("fake")

    def test_transition_status(self, cal):
        entry_obj = cal.add_entry("Draft Post", "Content")
        result = cal.transition_status(entry_obj.entry_id, "review")
        assert result

    def test_view_day(self, cal):
        today = datetime.now().strftime("%Y-%m-%d")
        cal.add_entry("Today's post", "Content", scheduled_at=today)
        entries = cal.view_day(today)
        assert len(entries) >= 1

    def test_view_day_default(self, cal):
        # Should not crash with no date
        entries = cal.view_day()
        assert isinstance(entries, list)

    def test_view_week(self, cal):
        entries = cal.view_week()
        assert isinstance(entries, list)

    def test_view_month(self, cal):
        now = datetime.now()
        entries = cal.view_month(now.year, now.month)
        assert isinstance(entries, list)

    def test_get_by_status(self, cal):
        cal.add_entry("Draft 1", "")
        cal.add_entry("Draft 2", "")
        entries = cal.get_by_status("draft")
        assert len(entries) >= 2

    def test_suggest_times(self, cal):
        times = cal.suggest_times()
        assert len(times) > 0

    def test_get_upcoming_events(self, cal):
        events = cal.get_upcoming_events(days=30)
        assert isinstance(events, list)

    def test_add_preset(self, cal):
        result = cal.add_preset("12-25", "Christmas Sale", "Holiday promo template")
        assert result

    def test_export_ical(self, cal):
        today = datetime.now().strftime("%Y-%m-%d")
        cal.add_entry("iCal Test", "Content", scheduled_at=today)
        ical = cal.export_ical(days=30)
        assert "BEGIN:VCALENDAR" in ical

    def test_stats(self, cal):
        cal.add_entry("Post 1", "A")
        cal.add_entry("Post 2", "B")
        stats = cal.stats()
        assert isinstance(stats, dict)
        assert stats.get("total", 0) >= 2

    def test_multiple_categories(self, cal):
        cal.add_entry("Thread", "", category="thread")
        cal.add_entry("Reply", "", category="reply")
        cal.add_entry("Original", "", category="original")
        stats = cal.stats()
        assert stats.get("total", 0) >= 3
