"""Tests for Growth Engine"""
import os
import json
import tempfile
import pytest
from bot.growth_engine import GrowthExperiment, GrowthEngine


@pytest.fixture
def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    os.unlink(path)


@pytest.fixture
def engine(db_path):
    return GrowthEngine(db_path=db_path)


class TestGrowthExperiment:
    def test_create(self):
        exp = GrowthExperiment(name="KOL test", strategy="kol_engagement")
        assert exp.name == "KOL test"
        assert exp.strategy == "kol_engagement"
        assert exp.status == "active"

    def test_to_dict(self):
        exp = GrowthExperiment(name="T", strategy="keyword_targeting")
        d = exp.to_dict()
        assert d["name"] == "T"
        assert d["strategy"] == "keyword_targeting"
        assert "experiment_id" in d

    def test_metrics(self):
        exp = GrowthExperiment(
            name="M", strategy="hashtag_riding",
            metrics_before={"followers": 100},
            metrics_after={"followers": 150},
        )
        assert exp.metrics_before["followers"] == 100
        assert exp.metrics_after["followers"] == 150


class TestGrowthEngineSnapshots:
    def test_record_snapshot(self, engine):
        row_id = engine.record_snapshot(followers=1000, following=500, tweets=200)
        assert row_id > 0

    def test_get_snapshots(self, engine):
        engine.record_snapshot(followers=1000, snapshot_date="2026-02-25")
        engine.record_snapshot(followers=1050, snapshot_date="2026-02-26")
        engine.record_snapshot(followers=1100, snapshot_date="2026-02-27")
        snapshots = engine.get_snapshots(days=30)
        assert len(snapshots) == 3

    def test_get_snapshots_empty(self, engine):
        snapshots = engine.get_snapshots()
        assert snapshots == []

    def test_snapshot_with_details(self, engine):
        engine.record_snapshot(
            followers=1000, following=500, tweets=200,
            impressions=50000, profile_visits=300,
            mentions=20, new_followers=50, lost_followers=5,
        )
        snapshots = engine.get_snapshots()
        s = snapshots[0]
        assert s["followers"] == 1000
        assert s["impressions"] == 50000
        assert s["new_followers"] == 50


class TestGrowthRate:
    def test_growth_rate(self, engine):
        engine.record_snapshot(followers=1000, snapshot_date="2026-02-20")
        engine.record_snapshot(followers=1100, snapshot_date="2026-02-27")
        rate = engine.get_growth_rate(days=30)
        assert rate["net_growth"] == 100
        assert rate["growth_rate"] == 10.0

    def test_growth_rate_empty(self, engine):
        rate = engine.get_growth_rate()
        assert rate["net_growth"] == 0
        assert rate["growth_rate"] == 0.0

    def test_growth_rate_single_snapshot(self, engine):
        engine.record_snapshot(followers=500, snapshot_date="2026-02-27")
        rate = engine.get_growth_rate()
        assert rate["net_growth"] == 0

    def test_negative_growth(self, engine):
        engine.record_snapshot(followers=1000, snapshot_date="2026-02-20")
        engine.record_snapshot(followers=900, snapshot_date="2026-02-27")
        rate = engine.get_growth_rate(days=30)
        assert rate["net_growth"] == -100
        assert rate["growth_rate"] == -10.0


class TestRetention:
    def test_retention(self, engine):
        engine.record_snapshot(followers=1000, new_followers=50, lost_followers=10, snapshot_date="2026-02-25")
        engine.record_snapshot(followers=1040, new_followers=40, lost_followers=5, snapshot_date="2026-02-26")
        result = engine.retention_analysis(days=30)
        assert result["total_new"] == 90
        assert result["total_lost"] == 15
        assert result["net"] == 75
        assert result["retention_rate"] > 0

    def test_retention_empty(self, engine):
        result = engine.retention_analysis()
        assert result["total_new"] == 0
        assert result["retention_rate"] == 0.0

    def test_retention_all_lost(self, engine):
        engine.record_snapshot(followers=1000, new_followers=10, lost_followers=10, snapshot_date="2026-02-27")
        result = engine.retention_analysis(days=30)
        assert result["net"] == 0
        assert result["retention_rate"] == 0.0


class TestExperiments:
    def test_create_experiment(self, engine):
        exp = engine.create_experiment("Test1", "keyword_targeting")
        assert exp.name == "Test1"
        assert exp.strategy == "keyword_targeting"

    def test_complete_experiment(self, engine):
        exp = engine.create_experiment("T", "kol_engagement",
                                       metrics_before={"followers": 100})
        success = engine.complete_experiment(
            exp.experiment_id,
            metrics_after={"followers": 150},
        )
        assert success
        result = engine.get_experiment(exp.experiment_id)
        assert result["status"] == "completed"
        assert result["metrics_after"]["followers"] == 150

    def test_complete_already_completed(self, engine):
        exp = engine.create_experiment("T", "hashtag_riding")
        engine.complete_experiment(exp.experiment_id)
        assert not engine.complete_experiment(exp.experiment_id)

    def test_list_experiments(self, engine):
        engine.create_experiment("E1", "keyword_targeting")
        engine.create_experiment("E2", "kol_engagement")
        exps = engine.list_experiments()
        assert len(exps) == 2

    def test_list_by_status(self, engine):
        e1 = engine.create_experiment("E1", "keyword_targeting")
        engine.create_experiment("E2", "kol_engagement")
        engine.complete_experiment(e1.experiment_id)
        active = engine.list_experiments(status="active")
        completed = engine.list_experiments(status="completed")
        assert len(active) == 1
        assert len(completed) == 1

    def test_get_experiment(self, engine):
        exp = engine.create_experiment("Get me", "cross_promotion")
        result = engine.get_experiment(exp.experiment_id)
        assert result["name"] == "Get me"

    def test_get_nonexistent(self, engine):
        assert engine.get_experiment("fake") is None

    def test_experiment_with_notes(self, engine):
        exp = engine.create_experiment("T", "content_calendar", notes="testing notes")
        result = engine.get_experiment(exp.experiment_id)
        assert result["notes"] == "testing notes"


class TestTargets:
    def test_add_target(self, engine):
        row_id = engine.add_target("user1", category="prospect")
        assert row_id > 0

    def test_get_targets(self, engine):
        engine.add_target("user1")
        engine.add_target("user2", category="kol")
        targets = engine.get_targets()
        assert len(targets) == 2

    def test_get_by_category(self, engine):
        engine.add_target("user1", category="prospect")
        engine.add_target("user2", category="kol")
        prospects = engine.get_targets(category="prospect")
        kols = engine.get_targets(category="kol")
        assert len(prospects) == 1
        assert len(kols) == 1

    def test_mark_engaged(self, engine):
        engine.add_target("user1")
        assert engine.mark_engaged("user1")
        targets = engine.get_targets()
        assert targets[0]["engaged"] == 1

    def test_mark_nonexistent(self, engine):
        assert not engine.mark_engaged("nobody")


class TestReport:
    def test_generate_report(self, engine):
        engine.record_snapshot(followers=1000, new_followers=50, lost_followers=5, snapshot_date="2026-02-20")
        engine.record_snapshot(followers=1100, new_followers=60, lost_followers=10, snapshot_date="2026-02-27")
        engine.create_experiment("E1", "keyword_targeting")
        report = engine.generate_report(days=30)
        assert "growth" in report
        assert "retention" in report
        assert report["total_snapshots"] == 2
        assert report["recent_experiments"] == 1

    def test_report_best_day(self, engine):
        engine.record_snapshot(followers=1000, new_followers=10, snapshot_date="2026-02-25")
        engine.record_snapshot(followers=1050, new_followers=50, snapshot_date="2026-02-26")
        engine.record_snapshot(followers=1060, new_followers=10, snapshot_date="2026-02-27")
        report = engine.generate_report()
        assert report["best_day"]["new_followers"] == 50

    def test_report_empty(self, engine):
        report = engine.generate_report()
        assert report["total_snapshots"] == 0

    def test_export_csv(self, engine):
        engine.record_snapshot(followers=1000, following=500, snapshot_date="2026-02-27")
        csv = engine.export_csv()
        lines = csv.strip().split("\n")
        assert len(lines) == 2
        assert "followers" in lines[0]
        assert "1000" in lines[1]


class TestStrategies:
    def test_strategies_list(self):
        assert "keyword_targeting" in GrowthEngine.STRATEGIES
        assert "kol_engagement" in GrowthEngine.STRATEGIES
        assert len(GrowthEngine.STRATEGIES) == 8
