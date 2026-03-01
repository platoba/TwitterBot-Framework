"""Tests for AB Testing Engine"""
import os
import json
import math
import sqlite3
import tempfile
import pytest
from bot.ab_testing import ABTest, ABTestVariant, ABTestEngine


@pytest.fixture
def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    os.unlink(path)


@pytest.fixture
def engine(db_path):
    return ABTestEngine(db_path=db_path)


class TestABTest:
    def test_create(self):
        t = ABTest(name="Test copy A vs B")
        assert t.name == "Test copy A vs B"
        assert t.test_type == "copy"
        assert t.status == "draft"
        assert t.confidence_level == 0.95
        assert t.min_sample_size == 100
        assert t.auto_select is True
        assert t.test_id

    def test_to_dict(self):
        t = ABTest(name="X", test_type="hashtag")
        d = t.to_dict()
        assert d["name"] == "X"
        assert d["test_type"] == "hashtag"
        assert "test_id" in d

    def test_custom_params(self):
        t = ABTest(name="T", confidence_level=0.99, min_sample_size=500, auto_select=False)
        assert t.confidence_level == 0.99
        assert t.min_sample_size == 500
        assert t.auto_select is False


class TestABTestVariant:
    def test_create(self):
        v = ABTestVariant(name="A", content="Hello world")
        assert v.name == "A"
        assert v.content == "Hello world"
        assert v.impressions == 0
        assert v.engagement_rate == 0.0

    def test_engagement_rate(self):
        v = ABTestVariant(impressions=1000, likes=50, retweets=20, replies=10, clicks=20)
        rate = v.engagement_rate
        assert rate == 10.0  # 100/1000 * 100

    def test_total_engagements(self):
        v = ABTestVariant(likes=10, retweets=5, replies=3, clicks=2)
        assert v.total_engagements == 20

    def test_zero_impressions(self):
        v = ABTestVariant(likes=10)
        assert v.engagement_rate == 0.0

    def test_to_dict(self):
        v = ABTestVariant(name="B", content="Test", likes=5, impressions=100)
        d = v.to_dict()
        assert d["name"] == "B"
        assert d["engagement_rate"] == 5.0
        assert d["total_engagements"] == 5


class TestABTestEngine:
    def test_create_test(self, engine):
        test = engine.create_test("Copy test")
        assert test.name == "Copy test"
        assert test.test_type == "copy"
        assert test.status == "draft"

    def test_add_variant(self, engine):
        test = engine.create_test("V test")
        v = engine.add_variant(test.test_id, "A", content="Version A")
        assert v.name == "A"
        assert v.content == "Version A"

    def test_get_test(self, engine):
        test = engine.create_test("Get test")
        engine.add_variant(test.test_id, "A", content="AA")
        engine.add_variant(test.test_id, "B", content="BB")
        result = engine.get_test(test.test_id)
        assert result is not None
        assert result["name"] == "Get test"
        assert len(result["variants"]) == 2

    def test_get_nonexistent(self, engine):
        assert engine.get_test("nope") is None

    def test_list_tests(self, engine):
        engine.create_test("T1")
        engine.create_test("T2")
        tests = engine.list_tests()
        assert len(tests) == 2

    def test_list_by_status(self, engine):
        t = engine.create_test("T1")
        engine.create_test("T2")
        engine.start_test(t.test_id)
        drafts = engine.list_tests(status="draft")
        running = engine.list_tests(status="running")
        assert len(drafts) == 1
        assert len(running) == 1

    def test_start_test(self, engine):
        test = engine.create_test("Start me")
        assert engine.start_test(test.test_id)
        result = engine.get_test(test.test_id)
        assert result["status"] == "running"

    def test_start_already_running(self, engine):
        test = engine.create_test("X")
        engine.start_test(test.test_id)
        assert not engine.start_test(test.test_id)

    def test_stop_test(self, engine):
        test = engine.create_test("Stop me")
        engine.start_test(test.test_id)
        assert engine.stop_test(test.test_id)
        result = engine.get_test(test.test_id)
        assert result["status"] == "completed"

    def test_stop_draft(self, engine):
        test = engine.create_test("Draft")
        assert not engine.stop_test(test.test_id)

    def test_update_metrics(self, engine):
        test = engine.create_test("Metrics")
        v = engine.add_variant(test.test_id, "A")
        engine.update_metrics(v.variant_id, impressions=1000, likes=50, retweets=20, replies=10, clicks=5)
        result = engine.get_test(test.test_id)
        variant = result["variants"][0]
        assert variant["impressions"] == 1000
        assert variant["likes"] == 50

    def test_z_test_significant(self, engine):
        result = engine.z_test(0.10, 1000, 0.05, 1000)
        assert result["significant"] is True
        assert result["z_score"] > 0

    def test_z_test_not_significant(self, engine):
        result = engine.z_test(0.05, 100, 0.048, 100)
        assert result["significant"] is False

    def test_z_test_zero_samples(self, engine):
        result = engine.z_test(0.1, 0, 0.2, 100)
        assert result["p_value"] == 1.0
        assert result["significant"] is False

    def test_z_test_same_rate(self, engine):
        result = engine.z_test(0.0, 100, 0.0, 100)
        assert result["significant"] is False

    def test_analyze_results(self, engine):
        test = engine.create_test("Analyze")
        va = engine.add_variant(test.test_id, "A")
        vb = engine.add_variant(test.test_id, "B")
        engine.update_metrics(va.variant_id, impressions=1000, likes=80, retweets=20, replies=10, clicks=5)
        engine.update_metrics(vb.variant_id, impressions=1000, likes=30, retweets=10, replies=5, clicks=2)
        result = engine.analyze_results(test.test_id)
        assert result["test_id"] == test.test_id
        assert result["winner"]["name"] == "A"
        assert result["statistical_test"] is not None

    def test_analyze_no_variants(self, engine):
        test = engine.create_test("Empty")
        result = engine.analyze_results(test.test_id)
        assert "error" in result

    def test_analyze_nonexistent(self, engine):
        result = engine.analyze_results("fake")
        assert "error" in result

    def test_auto_pick_winner(self, engine):
        test = engine.create_test("Auto pick")
        va = engine.add_variant(test.test_id, "A")
        vb = engine.add_variant(test.test_id, "B")
        engine.update_metrics(va.variant_id, impressions=5000, likes=500, retweets=100, replies=50, clicks=30)
        engine.update_metrics(vb.variant_id, impressions=5000, likes=50, retweets=10, replies=5, clicks=3)
        winner = engine.auto_pick_winner(test.test_id)
        if winner:
            assert winner == va.variant_id

    def test_auto_pick_no_significance(self, engine):
        test = engine.create_test("Close")
        va = engine.add_variant(test.test_id, "A")
        vb = engine.add_variant(test.test_id, "B")
        engine.update_metrics(va.variant_id, impressions=10, likes=1)
        engine.update_metrics(vb.variant_id, impressions=10, likes=1)
        winner = engine.auto_pick_winner(test.test_id)
        assert winner is None

    def test_hashtag_test_type(self, engine):
        test = engine.create_test("Hashtag AB", test_type="hashtag")
        va = engine.add_variant(test.test_id, "A", hashtags=["#python", "#ai"])
        vb = engine.add_variant(test.test_id, "B", hashtags=["#coding", "#dev"])
        result = engine.get_test(test.test_id)
        assert result["test_type"] == "hashtag"
        assert len(result["variants"]) == 2

    def test_normal_cdf(self, engine):
        assert abs(engine._normal_cdf(0) - 0.5) < 0.001
        assert engine._normal_cdf(3) > 0.99
        assert engine._normal_cdf(-3) < 0.01
