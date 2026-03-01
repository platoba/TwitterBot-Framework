"""Tests for Spaces Manager"""
import os
import tempfile
import pytest
from bot.spaces_manager import Space, SpaceStatus, ParticipantRole, SpacesManager


@pytest.fixture
def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    os.unlink(path)


@pytest.fixture
def manager(db_path):
    return SpacesManager(db_path=db_path)


class TestSpace:
    def test_create(self):
        s = Space(title="AI Talk")
        assert s.title == "AI Talk"
        assert s.status == "draft"
        assert s.max_speakers == 11
        assert s.language == "en"

    def test_to_dict(self):
        s = Space(title="T", topic="tech")
        d = s.to_dict()
        assert d["title"] == "T"
        assert d["topic"] == "tech"
        assert "space_id" in d

    def test_duration_not_started(self):
        s = Space(title="T")
        assert s.duration_minutes == 0.0

    def test_duration_with_times(self):
        s = Space(
            title="T",
            started_at="2026-03-01T10:00:00+00:00",
            ended_at="2026-03-01T11:30:00+00:00",
        )
        assert s.duration_minutes == 90.0

    def test_is_recurring(self):
        s = Space(title="Weekly AI", is_recurring=True, recurrence_rule="weekly")
        assert s.is_recurring is True
        assert s.recurrence_rule == "weekly"


class TestSpaceStatus:
    def test_values(self):
        assert SpaceStatus.DRAFT.value == "draft"
        assert SpaceStatus.LIVE.value == "live"
        assert SpaceStatus.ENDED.value == "ended"
        assert SpaceStatus.CANCELLED.value == "cancelled"


class TestSpacesManagerCRUD:
    def test_create_space(self, manager):
        space = manager.create_space("My Space", topic="tech")
        assert space.title == "My Space"
        assert space.topic == "tech"

    def test_get_space(self, manager):
        space = manager.create_space("Get me")
        result = manager.get_space(space.space_id)
        assert result is not None
        assert result["title"] == "Get me"
        assert "participants" in result

    def test_get_nonexistent(self, manager):
        assert manager.get_space("fake") is None

    def test_list_spaces(self, manager):
        manager.create_space("S1")
        manager.create_space("S2")
        spaces = manager.list_spaces()
        assert len(spaces) == 2

    def test_list_by_status(self, manager):
        s1 = manager.create_space("S1")
        manager.create_space("S2")
        manager.start_space(s1.space_id)
        drafts = manager.list_spaces(status="draft")
        live = manager.list_spaces(status="live")
        assert len(drafts) == 1
        assert len(live) == 1

    def test_schedule_space(self, manager):
        space = manager.create_space("Sched")
        assert manager.schedule_space(space.space_id, "2026-03-05T10:00:00Z")
        result = manager.get_space(space.space_id)
        assert result["status"] == "scheduled"

    def test_schedule_non_draft(self, manager):
        space = manager.create_space("X")
        manager.start_space(space.space_id)
        assert not manager.schedule_space(space.space_id, "2026-03-05T10:00:00Z")

    def test_start_space(self, manager):
        space = manager.create_space("Go live")
        assert manager.start_space(space.space_id)
        result = manager.get_space(space.space_id)
        assert result["status"] == "live"
        assert result["started_at"] is not None

    def test_end_space(self, manager):
        space = manager.create_space("End me")
        manager.start_space(space.space_id)
        assert manager.end_space(space.space_id)
        result = manager.get_space(space.space_id)
        assert result["status"] == "ended"

    def test_end_non_live(self, manager):
        space = manager.create_space("Draft")
        assert not manager.end_space(space.space_id)

    def test_cancel_space(self, manager):
        space = manager.create_space("Cancel me")
        assert manager.cancel_space(space.space_id)
        result = manager.get_space(space.space_id)
        assert result["status"] == "cancelled"

    def test_cancel_live(self, manager):
        space = manager.create_space("Live")
        manager.start_space(space.space_id)
        assert not manager.cancel_space(space.space_id)


class TestParticipants:
    def test_invite(self, manager):
        space = manager.create_space("Invite test")
        assert manager.invite_participant(space.space_id, "user1", "speaker")
        participants = manager.get_participants(space.space_id)
        assert len(participants) == 1
        assert participants[0]["role"] == "speaker"

    def test_invite_duplicate(self, manager):
        space = manager.create_space("Dup")
        manager.invite_participant(space.space_id, "user1")
        assert not manager.invite_participant(space.space_id, "user1")

    def test_join(self, manager):
        space = manager.create_space("Join")
        assert manager.join_participant(space.space_id, "user1", "listener")
        participants = manager.get_participants(space.space_id)
        assert len(participants) == 1

    def test_join_update_existing(self, manager):
        space = manager.create_space("Rejoin")
        manager.invite_participant(space.space_id, "user1", "speaker")
        manager.join_participant(space.space_id, "user1", "speaker")
        participants = manager.get_participants(space.space_id)
        assert len(participants) == 1
        assert participants[0]["joined_at"] is not None

    def test_leave(self, manager):
        space = manager.create_space("Leave")
        manager.join_participant(space.space_id, "user1")
        assert manager.leave_participant(space.space_id, "user1")
        participants = manager.get_participants(space.space_id)
        assert participants[0]["left_at"] is not None

    def test_leave_not_joined(self, manager):
        space = manager.create_space("X")
        assert not manager.leave_participant(space.space_id, "nobody")

    def test_promote_to_speaker(self, manager):
        space = manager.create_space("Promote")
        manager.join_participant(space.space_id, "user1", "listener")
        assert manager.promote_to_speaker(space.space_id, "user1")
        participants = manager.get_participants(space.space_id, role="speaker")
        assert len(participants) == 1

    def test_promote_max_speakers(self, manager):
        space = manager.create_space("Max", max_speakers=2)
        manager.join_participant(space.space_id, "host1", "host")
        manager.join_participant(space.space_id, "speaker1", "speaker")
        manager.join_participant(space.space_id, "listener1", "listener")
        assert not manager.promote_to_speaker(space.space_id, "listener1")

    def test_demote_to_listener(self, manager):
        space = manager.create_space("Demote")
        manager.join_participant(space.space_id, "user1", "speaker")
        assert manager.demote_to_listener(space.space_id, "user1")
        participants = manager.get_participants(space.space_id)
        assert participants[0]["role"] == "listener"

    def test_demote_non_speaker(self, manager):
        space = manager.create_space("X")
        manager.join_participant(space.space_id, "user1", "listener")
        assert not manager.demote_to_listener(space.space_id, "user1")

    def test_get_participants_by_role(self, manager):
        space = manager.create_space("Roles")
        manager.join_participant(space.space_id, "s1", "speaker")
        manager.join_participant(space.space_id, "s2", "speaker")
        manager.join_participant(space.space_id, "l1", "listener")
        speakers = manager.get_participants(space.space_id, role="speaker")
        listeners = manager.get_participants(space.space_id, role="listener")
        assert len(speakers) == 2
        assert len(listeners) == 1

    def test_update_speaking_time(self, manager):
        space = manager.create_space("Time")
        manager.join_participant(space.space_id, "user1", "speaker")
        assert manager.update_speaking_time(space.space_id, "user1", 300)
        participants = manager.get_participants(space.space_id)
        assert participants[0]["speaking_time_seconds"] == 300


class TestMetrics:
    def test_record_metric(self, manager):
        space = manager.create_space("Metrics")
        row_id = manager.record_metric(space.space_id, listener_count=100, speaker_count=5)
        assert row_id > 0

    def test_get_metrics(self, manager):
        space = manager.create_space("M")
        manager.record_metric(space.space_id, 100, 5)
        manager.record_metric(space.space_id, 150, 5)
        metrics = manager.get_metrics(space.space_id)
        assert len(metrics) == 2
        assert metrics[1]["listener_count"] == 150


class TestHighlights:
    def test_add_highlight(self, manager):
        space = manager.create_space("H")
        row_id = manager.add_highlight(space.space_id, 300, "Great insight", speaker="user1")
        assert row_id > 0

    def test_get_highlights(self, manager):
        space = manager.create_space("H")
        manager.add_highlight(space.space_id, 300, "Point 1")
        manager.add_highlight(space.space_id, 600, "Point 2")
        highlights = manager.get_highlights(space.space_id)
        assert len(highlights) == 2
        assert highlights[0]["timestamp_seconds"] == 300


class TestAnalytics:
    def test_space_analytics(self, manager):
        space = manager.create_space("Analytics",
                                     started_at="2026-03-01T10:00:00+00:00",
                                     ended_at="2026-03-01T11:00:00+00:00",
                                     status="ended")
        manager.join_participant(space.space_id, "s1", "speaker")
        manager.update_speaking_time(space.space_id, "s1", 600)
        manager.record_metric(space.space_id, 100, 3)
        manager.record_metric(space.space_id, 200, 5)
        manager.add_highlight(space.space_id, 300, "Key moment")

        analytics = manager.get_space_analytics(space.space_id)
        assert analytics["peak_listeners"] == 200
        assert analytics["total_speakers"] == 1
        assert analytics["total_speaking_time_seconds"] == 600
        assert analytics["highlights_count"] == 1
        assert analytics["duration_minutes"] == 60.0

    def test_analytics_nonexistent(self, manager):
        result = manager.get_space_analytics("fake")
        assert "error" in result

    def test_export_csv(self, manager):
        space = manager.create_space("CSV")
        manager.record_metric(space.space_id, 100, 5)
        csv = manager.export_analytics_csv(space.space_id)
        lines = csv.strip().split("\n")
        assert len(lines) == 2
        assert "listener_count" in lines[0]


class TestRecurringSeries:
    def test_create_series_next(self, manager):
        space = manager.create_space(
            "Weekly AI",
            is_recurring=True,
            recurrence_rule="weekly",
            scheduled_at="2026-03-01T10:00:00+00:00",
            topic="ai",
        )
        next_space = manager.create_series_next(space.space_id)
        assert next_space is not None
        assert next_space.title == "Weekly AI"
        assert "2026-03-08" in next_space.scheduled_at

    def test_biweekly_series(self, manager):
        space = manager.create_space(
            "Biweekly",
            is_recurring=True,
            recurrence_rule="biweekly",
            scheduled_at="2026-03-01T10:00:00+00:00",
        )
        next_space = manager.create_series_next(space.space_id)
        assert next_space is not None
        assert "2026-03-15" in next_space.scheduled_at

    def test_monthly_series(self, manager):
        space = manager.create_space(
            "Monthly",
            is_recurring=True,
            recurrence_rule="monthly",
            scheduled_at="2026-03-01T10:00:00+00:00",
        )
        next_space = manager.create_series_next(space.space_id)
        assert next_space is not None

    def test_non_recurring(self, manager):
        space = manager.create_space("One-off")
        assert manager.create_series_next(space.space_id) is None

    def test_invalid_rule(self, manager):
        space = manager.create_space(
            "Bad rule",
            is_recurring=True,
            recurrence_rule="daily_invalid",
            scheduled_at="2026-03-01T10:00:00+00:00",
        )
        assert manager.create_series_next(space.space_id) is None


class TestUpcoming:
    def test_get_upcoming_empty(self, manager):
        result = manager.get_upcoming()
        assert result == []


class TestTopics:
    def test_topics_list(self):
        assert "tech" in SpacesManager.TOPICS
        assert "ai" in SpacesManager.TOPICS
        assert len(SpacesManager.TOPICS) == 15
