"""Tests for Poll Engine"""
import os
import math
import tempfile
import pytest
from bot.poll_engine import PollOption, Poll, PollEngine


@pytest.fixture
def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    os.unlink(path)


@pytest.fixture
def engine(db_path):
    return PollEngine(db_path=db_path)


class TestPollOption:
    def test_create(self):
        o = PollOption(label="Yes")
        assert o.label == "Yes"
        assert o.votes == 0
        assert o.option_id

    def test_to_dict(self):
        o = PollOption(label="No", votes=42)
        d = o.to_dict()
        assert d["label"] == "No"
        assert d["votes"] == 42


class TestPoll:
    def test_create(self):
        p = Poll(question="Best lang?", options=[
            {"option_id": "a", "label": "Python", "votes": 100},
            {"option_id": "b", "label": "Rust", "votes": 50},
        ])
        assert p.question == "Best lang?"
        assert len(p.options) == 2
        assert p.status == "draft"

    def test_total_votes(self):
        p = Poll(question="Q", options=[
            {"option_id": "a", "label": "A", "votes": 100},
            {"option_id": "b", "label": "B", "votes": 50},
        ])
        assert p.total_votes == 150

    def test_vote_distribution(self):
        p = Poll(question="Q", options=[
            {"option_id": "a", "label": "A", "votes": 75},
            {"option_id": "b", "label": "B", "votes": 25},
        ])
        dist = p.vote_distribution
        assert len(dist) == 2
        assert dist[0]["percentage"] == 75.0
        assert dist[1]["percentage"] == 25.0

    def test_vote_distribution_zero(self):
        p = Poll(question="Q", options=[
            {"option_id": "a", "label": "A", "votes": 0},
        ])
        dist = p.vote_distribution
        assert dist[0]["percentage"] == 0.0

    def test_winner(self):
        p = Poll(question="Q", options=[
            {"option_id": "a", "label": "A", "votes": 100},
            {"option_id": "b", "label": "B", "votes": 200},
        ])
        w = p.winner
        assert w["label"] == "B"

    def test_winner_empty(self):
        p = Poll(question="Q")
        assert p.winner is None

    def test_duration_clamped(self):
        p1 = Poll(question="Q", duration_minutes=1)
        assert p1.duration_minutes == 5
        p2 = Poll(question="Q", duration_minutes=99999)
        assert p2.duration_minutes == 10080

    def test_to_dict(self):
        p = Poll(question="Q")
        d = p.to_dict()
        assert d["question"] == "Q"
        assert "poll_id" in d
        assert "total_votes" in d


class TestPollEngineCRUD:
    def test_create_poll(self, engine):
        poll = engine.create_poll("Best?", ["A", "B", "C"])
        assert poll.question == "Best?"
        assert len(poll.options) == 3

    def test_create_poll_too_few(self, engine):
        with pytest.raises(ValueError):
            engine.create_poll("Q", ["Only one"])

    def test_create_poll_too_many(self, engine):
        with pytest.raises(ValueError):
            engine.create_poll("Q", ["A", "B", "C", "D", "E"])

    def test_get_poll(self, engine):
        poll = engine.create_poll("Get?", ["Y", "N"])
        result = engine.get_poll(poll.poll_id)
        assert result is not None
        assert result.question == "Get?"

    def test_get_nonexistent(self, engine):
        assert engine.get_poll("fake") is None

    def test_list_polls(self, engine):
        engine.create_poll("Q1", ["A", "B"])
        engine.create_poll("Q2", ["A", "B"])
        polls = engine.list_polls()
        assert len(polls) == 2

    def test_list_by_status(self, engine):
        p1 = engine.create_poll("Q1", ["A", "B"])
        engine.create_poll("Q2", ["A", "B"])
        engine.start_poll(p1.poll_id)
        active = engine.list_polls(status="active")
        draft = engine.list_polls(status="draft")
        assert len(active) == 1
        assert len(draft) == 1

    def test_list_by_category(self, engine):
        engine.create_poll("Q1", ["A", "B"], category="tech")
        engine.create_poll("Q2", ["A", "B"], category="fun")
        tech = engine.list_polls(category="tech")
        assert len(tech) == 1

    def test_start_poll(self, engine):
        poll = engine.create_poll("Start?", ["Y", "N"])
        assert engine.start_poll(poll.poll_id)
        result = engine.get_poll(poll.poll_id)
        assert result.status == "active"

    def test_start_already_active(self, engine):
        poll = engine.create_poll("X", ["A", "B"])
        engine.start_poll(poll.poll_id)
        assert not engine.start_poll(poll.poll_id)

    def test_end_poll(self, engine):
        poll = engine.create_poll("End?", ["Y", "N"])
        engine.start_poll(poll.poll_id)
        assert engine.end_poll(poll.poll_id)
        result = engine.get_poll(poll.poll_id)
        assert result.status == "ended"

    def test_end_non_active(self, engine):
        poll = engine.create_poll("X", ["A", "B"])
        assert not engine.end_poll(poll.poll_id)

    def test_delete_poll(self, engine):
        poll = engine.create_poll("Delete", ["A", "B"])
        assert engine.delete_poll(poll.poll_id)
        assert engine.get_poll(poll.poll_id) is None

    def test_delete_nonexistent(self, engine):
        assert not engine.delete_poll("fake")


class TestVoteTracking:
    def test_update_votes(self, engine):
        poll = engine.create_poll("Vote?", ["A", "B"])
        opt_ids = {o["option_id"]: o["label"] for o in poll.options}
        votes = {oid: 50 if label == "A" else 30 for oid, label in opt_ids.items()}
        assert engine.update_votes(poll.poll_id, votes)
        result = engine.get_poll(poll.poll_id)
        assert result.total_votes == 80

    def test_update_nonexistent(self, engine):
        assert not engine.update_votes("fake", {})

    def test_record_snapshot(self, engine):
        poll = engine.create_poll("Snap?", ["A", "B"])
        row_id = engine.record_snapshot(poll.poll_id)
        assert row_id > 0

    def test_record_snapshot_nonexistent(self, engine):
        assert engine.record_snapshot("fake") == -1

    def test_get_snapshots(self, engine):
        poll = engine.create_poll("S?", ["A", "B"])
        engine.record_snapshot(poll.poll_id)
        engine.record_snapshot(poll.poll_id)
        snaps = engine.get_snapshots(poll.poll_id)
        assert len(snaps) == 2


class TestAnalytics:
    def test_analyze_poll(self, engine):
        poll = engine.create_poll("Analyze?", ["A", "B", "C", "D"])
        # Set some votes
        votes = {}
        for i, opt in enumerate(poll.options):
            votes[opt["option_id"]] = (i + 1) * 25
        engine.update_votes(poll.poll_id, votes)
        engine.start_poll(poll.poll_id)

        analysis = engine.analyze_poll(poll.poll_id)
        assert analysis["total_votes"] == 250  # 25+50+75+100
        assert analysis["winner"] is not None
        assert 0 <= analysis["dispersion"] <= 1
        assert analysis["option_count"] == 4

    def test_analyze_nonexistent(self, engine):
        result = engine.analyze_poll("fake")
        assert "error" in result

    def test_analyze_zero_votes(self, engine):
        poll = engine.create_poll("Empty?", ["A", "B"])
        analysis = engine.analyze_poll(poll.poll_id)
        assert analysis["total_votes"] == 0
        assert analysis["entropy"] == 0

    def test_competitiveness_tie(self, engine):
        poll = engine.create_poll("Tie?", ["A", "B"])
        votes = {poll.options[0]["option_id"]: 50, poll.options[1]["option_id"]: 50}
        engine.update_votes(poll.poll_id, votes)
        analysis = engine.analyze_poll(poll.poll_id)
        assert analysis["competitiveness"] == 100.0  # perfect tie

    def test_competitiveness_landslide(self, engine):
        poll = engine.create_poll("Landslide?", ["A", "B"])
        votes = {poll.options[0]["option_id"]: 100, poll.options[1]["option_id"]: 0}
        engine.update_votes(poll.poll_id, votes)
        analysis = engine.analyze_poll(poll.poll_id)
        assert analysis["competitiveness"] == 0.0

    def test_category_stats(self, engine):
        p1 = engine.create_poll("Q1", ["A", "B"], category="tech")
        votes = {p1.options[0]["option_id"]: 100, p1.options[1]["option_id"]: 50}
        engine.update_votes(p1.poll_id, votes)
        engine.start_poll(p1.poll_id)
        engine.end_poll(p1.poll_id)

        stats = engine.get_category_stats()
        assert "tech" in stats
        assert stats["tech"]["count"] == 1
        assert stats["tech"]["total_votes"] == 150


class TestStrategy:
    def test_suggest_duration(self, engine):
        assert engine.suggest_duration(goal="quick_engagement") == 60
        assert engine.suggest_duration(goal="max_reach") == 10080

    def test_suggest_duration_default(self, engine):
        assert engine.suggest_duration() == 1440

    def test_suggest_option_count(self, engine):
        assert engine.suggest_option_count("yes_no") == 2
        assert engine.suggest_option_count("opinion") == 4
        assert engine.suggest_option_count("this_or_that") == 2
        assert engine.suggest_option_count("unknown") == 4

    def test_generate_follow_up_ideas(self, engine):
        poll = engine.create_poll("Ideas?", ["A", "B"])
        votes = {poll.options[0]["option_id"]: 100, poll.options[1]["option_id"]: 50}
        engine.update_votes(poll.poll_id, votes)
        ideas = engine.generate_follow_up_ideas(poll.poll_id)
        assert len(ideas) > 0

    def test_follow_up_nonexistent(self, engine):
        ideas = engine.generate_follow_up_ideas("fake")
        assert ideas == []

    def test_follow_up_competitive(self, engine):
        poll = engine.create_poll("Close?", ["A", "B"])
        votes = {poll.options[0]["option_id"]: 51, poll.options[1]["option_id"]: 49}
        engine.update_votes(poll.poll_id, votes)
        ideas = engine.generate_follow_up_ideas(poll.poll_id)
        assert any("split" in idea.lower() or "debate" in idea.lower() for idea in ideas)

    def test_export_results_text(self, engine):
        poll = engine.create_poll("Export?", ["Yes", "No"])
        votes = {poll.options[0]["option_id"]: 75, poll.options[1]["option_id"]: 25}
        engine.update_votes(poll.poll_id, votes)
        text = engine.export_results_text(poll.poll_id)
        assert "Export?" in text
        assert "75" in text

    def test_export_nonexistent(self, engine):
        assert "not found" in engine.export_results_text("fake").lower()


class TestConstants:
    def test_categories(self):
        assert "general" in PollEngine.CATEGORIES
        assert "tech" in PollEngine.CATEGORIES
        assert len(PollEngine.CATEGORIES) == 9

    def test_optimal_durations(self):
        assert PollEngine.OPTIMAL_DURATIONS["quick_engagement"] == 60
        assert PollEngine.OPTIMAL_DURATIONS["max_reach"] == 10080
