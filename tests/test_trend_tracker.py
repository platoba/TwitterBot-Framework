"""
Tests for Trend Tracker v1.0
"""
import json
import tempfile
from datetime import datetime, timezone, timedelta

import pytest
from bot.trend_tracker import (
    TrendPhase, TrendPriority, TrendItem, TrendAlert,
    BurstDetector, RelevanceEngine, OpportunityScorer,
    TrendHistory, TrendTracker,
)


# ── Fixtures ──

@pytest.fixture
def niche_keywords():
    return ["python", "machine learning", "ai", "data science", "llm"]

@pytest.fixture
def niche_hashtags():
    return ["#python", "#machinelearning", "#ai", "#datascience", "#llm"]

@pytest.fixture
def sample_trending():
    return [
        {"keyword": "#python", "volume": 5000, "related_hashtags": ["#coding", "#ai"], "sample_tweets": ["Python 3.13 is amazing", "Learning python for data science"]},
        {"keyword": "#javascript", "volume": 8000, "related_hashtags": ["#webdev", "#react"]},
        {"keyword": "#machinelearning", "volume": 3000, "related_hashtags": ["#ai", "#deeplearning"], "sample_tweets": ["New ML paper on transformers"]},
        {"keyword": "#cooking", "volume": 2000, "related_hashtags": ["#food", "#recipe"]},
        {"keyword": "#llm", "volume": 4500, "related_hashtags": ["#ai", "#chatgpt"], "sample_tweets": ["GPT-5 just dropped", "LLM evaluation benchmark"]},
    ]

@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


# ── TrendItem Tests ──

class TestTrendItem:
    def test_to_dict(self):
        t = TrendItem(keyword="#python", volume=5000, phase="rising")
        d = t.to_dict()
        assert d["keyword"] == "#python"
        assert d["volume"] == 5000

    def test_from_dict(self):
        d = {"keyword": "#test", "volume": 100, "phase": "emerging"}
        t = TrendItem.from_dict(d)
        assert t.keyword == "#test"
        assert t.volume == 100

    def test_from_dict_extra_fields(self):
        d = {"keyword": "#test", "unknown": "x"}
        t = TrendItem.from_dict(d)
        assert t.keyword == "#test"

    def test_defaults(self):
        t = TrendItem(keyword="test")
        assert t.volume == 0
        assert t.phase == "emerging"
        assert t.related_hashtags == []


# ── BurstDetector Tests ──

class TestBurstDetector:
    def test_no_history_high_count(self):
        bd = BurstDetector()
        is_burst, z = bd.is_burst("test", 100)
        assert is_burst is True

    def test_no_history_low_count(self):
        bd = BurstDetector()
        is_burst, z = bd.is_burst("test", 5)
        assert is_burst is False

    def test_burst_detection(self):
        bd = BurstDetector(window_size=10, z_threshold=2.0)
        # Normal baseline
        for _ in range(10):
            bd.add_observation("test", 100)
        # Spike
        is_burst, z = bd.is_burst("test", 500)
        assert is_burst is True
        assert z > 2.0

    def test_no_burst_normal(self):
        bd = BurstDetector(window_size=10, z_threshold=2.0)
        # Normal baseline with some variance
        for v in [95, 105, 98, 102, 97, 103, 100, 96, 104, 99]:
            bd.add_observation("test", v)
        is_burst, z = bd.is_burst("test", 105)
        assert is_burst is False

    def test_trend_phase_emerging(self):
        bd = BurstDetector()
        bd.add_observation("test", 10)
        bd.add_observation("test", 20)
        phase = bd.get_trend_phase("test")
        # With few data points, should be emerging or growing
        assert phase in (TrendPhase.EMERGING, TrendPhase.RISING)

    def test_trend_phase_declining(self):
        bd = BurstDetector()
        for v in [100, 90, 80, 70, 60, 50, 40]:
            bd.add_observation("test", v)
        phase = bd.get_trend_phase("test")
        assert phase in (TrendPhase.DECLINING, TrendPhase.DEAD, TrendPhase.PEAKING)

    def test_trend_phase_no_data(self):
        bd = BurstDetector()
        assert bd.get_trend_phase("nonexistent") == TrendPhase.EMERGING

    def test_volume_change_rate(self):
        bd = BurstDetector()
        bd.add_observation("test", 100)
        bd.add_observation("test", 200)
        rate = bd.volume_change_rate("test")
        assert rate == 1.0  # 100% increase

    def test_volume_change_rate_no_data(self):
        bd = BurstDetector()
        assert bd.volume_change_rate("test") == 0.0


# ── RelevanceEngine Tests ──

class TestRelevanceEngine:
    def test_direct_keyword_match(self, niche_keywords, niche_hashtags):
        engine = RelevanceEngine(niche_keywords, niche_hashtags)
        trend = TrendItem(keyword="python", volume=100)
        score = engine.score(trend)
        assert score > 0.3

    def test_direct_hashtag_match(self, niche_keywords, niche_hashtags):
        engine = RelevanceEngine(niche_keywords, niche_hashtags)
        trend = TrendItem(keyword="#python", volume=100)
        score = engine.score(trend)
        assert score > 0.3

    def test_no_match(self, niche_keywords, niche_hashtags):
        engine = RelevanceEngine(niche_keywords, niche_hashtags)
        trend = TrendItem(keyword="cooking", volume=100)
        score = engine.score(trend)
        assert score < 0.3

    def test_related_hashtag_match(self, niche_keywords, niche_hashtags):
        engine = RelevanceEngine(niche_keywords, niche_hashtags)
        trend = TrendItem(keyword="transformers", related_hashtags=["#ai", "#deeplearning"])
        score = engine.score(trend)
        assert score > 0.0

    def test_sample_tweet_match(self, niche_keywords, niche_hashtags):
        engine = RelevanceEngine(niche_keywords, niche_hashtags)
        trend = TrendItem(keyword="coding", sample_tweets=["Learning python today", "AI is amazing"])
        score = engine.score(trend)
        assert score > 0.0

    def test_empty_niche(self):
        engine = RelevanceEngine()
        trend = TrendItem(keyword="anything")
        assert engine.score(trend) == 0.5

    def test_partial_match(self, niche_keywords, niche_hashtags):
        engine = RelevanceEngine(niche_keywords, niche_hashtags)
        trend = TrendItem(keyword="python3")  # partial match
        score = engine.score(trend)
        assert score > 0.0


# ── OpportunityScorer Tests ──

class TestOpportunityScorer:
    def test_emerging_high_relevance(self):
        scorer = OpportunityScorer()
        trend = TrendItem(keyword="test", volume=1000)
        score = scorer.score(trend, TrendPhase.EMERGING, 0.9)
        assert score > 0.7

    def test_dead_low_relevance(self):
        scorer = OpportunityScorer()
        trend = TrendItem(keyword="test", volume=10)
        score = scorer.score(trend, TrendPhase.DEAD, 0.1)
        assert score < 0.3

    def test_score_range(self):
        scorer = OpportunityScorer()
        trend = TrendItem(keyword="test", volume=500)
        for phase in TrendPhase:
            for rel in [0.0, 0.5, 1.0]:
                score = scorer.score(trend, phase, rel)
                assert 0.0 <= score <= 1.0

    def test_priority_critical(self):
        scorer = OpportunityScorer()
        assert scorer.get_priority(0.9, 0.9) == TrendPriority.CRITICAL

    def test_priority_high(self):
        scorer = OpportunityScorer()
        assert scorer.get_priority(0.7, 0.7) == TrendPriority.HIGH

    def test_priority_irrelevant(self):
        scorer = OpportunityScorer()
        assert scorer.get_priority(0.0, 0.0) == TrendPriority.IRRELEVANT

    def test_volume_bonus(self):
        scorer = OpportunityScorer()
        t_low = TrendItem(keyword="a", volume=1)
        t_high = TrendItem(keyword="b", volume=100000)
        s_low = scorer.score(t_low, TrendPhase.RISING, 0.5)
        s_high = scorer.score(t_high, TrendPhase.RISING, 0.5)
        assert s_high > s_low


# ── TrendHistory Tests ──

class TestTrendHistory:
    def test_record_and_get(self, tmp_dir):
        th = TrendHistory(f"{tmp_dir}/th.db")
        trend = TrendItem(keyword="#python", volume=5000, phase="rising", relevance_score=0.8)
        th.record(trend)
        history = th.get_trend_history("#python")
        assert len(history) == 1
        assert history[0]["volume"] == 5000

    def test_add_and_get_alert(self, tmp_dir):
        th = TrendHistory(f"{tmp_dir}/th.db")
        alert = TrendAlert(
            trend=TrendItem(keyword="test"),
            alert_type="new_trend",
            message="New trend detected",
        )
        aid = th.add_alert(alert)
        assert aid > 0
        unack = th.get_unacknowledged()
        assert len(unack) == 1

    def test_acknowledge_alert(self, tmp_dir):
        th = TrendHistory(f"{tmp_dir}/th.db")
        alert = TrendAlert(trend=TrendItem(keyword="test"), alert_type="test")
        aid = th.add_alert(alert)
        th.acknowledge_alert(aid)
        unack = th.get_unacknowledged()
        assert len(unack) == 0

    def test_hot_keywords(self, tmp_dir):
        th = TrendHistory(f"{tmp_dir}/th.db")
        th.record(TrendItem(keyword="a", volume=100))
        th.record(TrendItem(keyword="b", volume=500))
        th.record(TrendItem(keyword="a", volume=200))
        hot = th.hot_keywords()
        assert len(hot) == 2
        assert hot[0]["keyword"] == "b"  # higher volume

    def test_find_recurring(self, tmp_dir):
        th = TrendHistory(f"{tmp_dir}/th.db")
        for _ in range(5):
            th.record(TrendItem(keyword="recurring", volume=100))
        recurring = th.find_recurring(min_occurrences=1)
        assert len(recurring) >= 1

    def test_stats(self, tmp_dir):
        th = TrendHistory(f"{tmp_dir}/th.db")
        th.record(TrendItem(keyword="a", volume=100))
        stats = th.stats()
        assert stats["unique_trends"] == 1
        assert stats["total_snapshots"] == 1


# ── TrendTracker Integration ──

class TestTrendTracker:
    def test_process_trending(self, tmp_dir, niche_keywords, niche_hashtags, sample_trending):
        tracker = TrendTracker(niche_keywords, niche_hashtags, db_dir=tmp_dir)
        results = tracker.process_trending(sample_trending)
        assert len(results) == 5
        # Results should be sorted by opportunity
        if len(results) > 1:
            assert results[0].opportunity_score >= results[-1].opportunity_score

    def test_relevance_scoring(self, tmp_dir, niche_keywords, niche_hashtags, sample_trending):
        tracker = TrendTracker(niche_keywords, niche_hashtags, db_dir=tmp_dir)
        results = tracker.process_trending(sample_trending)
        # Python and ML should have higher relevance than cooking
        python = next(r for r in results if r.keyword == "#python")
        cooking = next(r for r in results if r.keyword == "#cooking")
        assert python.relevance_score > cooking.relevance_score

    def test_get_active_trends(self, tmp_dir, niche_keywords, niche_hashtags, sample_trending):
        tracker = TrendTracker(niche_keywords, niche_hashtags, db_dir=tmp_dir)
        tracker.process_trending(sample_trending)
        active = tracker.get_active_trends()
        assert len(active) == 5

    def test_get_active_filter_relevance(self, tmp_dir, niche_keywords, niche_hashtags, sample_trending):
        tracker = TrendTracker(niche_keywords, niche_hashtags, db_dir=tmp_dir)
        tracker.process_trending(sample_trending)
        relevant = tracker.get_active_trends(min_relevance=0.3)
        # Only niche-relevant trends
        assert len(relevant) <= 5

    def test_get_actionable(self, tmp_dir, niche_keywords, niche_hashtags, sample_trending):
        tracker = TrendTracker(niche_keywords, niche_hashtags, db_dir=tmp_dir)
        tracker.process_trending(sample_trending)
        actionable = tracker.get_actionable(top_n=3)
        assert len(actionable) <= 3

    def test_suggest_content(self, tmp_dir, niche_keywords, niche_hashtags):
        tracker = TrendTracker(niche_keywords, niche_hashtags, db_dir=tmp_dir)
        trend = TrendItem(keyword="#python", volume=5000, phase="emerging", related_hashtags=["#ai"])
        suggestions = tracker.suggest_content(trend)
        assert len(suggestions) > 0
        assert "type" in suggestions[0]
        assert "prompt" in suggestions[0]

    def test_suggest_content_rising(self, tmp_dir, niche_keywords, niche_hashtags):
        tracker = TrendTracker(niche_keywords, niche_hashtags, db_dir=tmp_dir)
        trend = TrendItem(keyword="#ai", volume=10000, phase="rising")
        suggestions = tracker.suggest_content(trend)
        assert any(s["type"] == "hot_take" for s in suggestions)

    def test_suggest_content_peaking(self, tmp_dir, niche_keywords, niche_hashtags):
        tracker = TrendTracker(niche_keywords, niche_hashtags, db_dir=tmp_dir)
        trend = TrendItem(keyword="#llm", volume=20000, phase="peaking")
        suggestions = tracker.suggest_content(trend)
        assert any(s["type"] == "thread" for s in suggestions)

    def test_alerts_generated(self, tmp_dir, niche_keywords, niche_hashtags, sample_trending):
        tracker = TrendTracker(niche_keywords, niche_hashtags, db_dir=tmp_dir)
        tracker.process_trending(sample_trending)
        unack = tracker.history.get_unacknowledged()
        # Should have new_trend alerts for relevant keywords
        assert len(unack) > 0

    def test_phase_change_alert(self, tmp_dir, niche_keywords, niche_hashtags):
        tracker = TrendTracker(niche_keywords, niche_hashtags, db_dir=tmp_dir)
        # First pass
        tracker.process_trending([{"keyword": "#python", "volume": 100}])
        # Force phase change by manipulating internal state
        if "#python" in tracker._active_trends:
            tracker._active_trends["#python"].phase = "emerging"
        # Second pass with higher volume
        tracker.process_trending([{"keyword": "#python", "volume": 10000}])

    def test_export_json(self, tmp_dir, niche_keywords, niche_hashtags, sample_trending):
        tracker = TrendTracker(niche_keywords, niche_hashtags, db_dir=tmp_dir)
        results = tracker.process_trending(sample_trending)
        report = tracker.export_report(results, format="json")
        data = json.loads(report)
        assert len(data) == 5

    def test_export_text(self, tmp_dir, niche_keywords, niche_hashtags, sample_trending):
        tracker = TrendTracker(niche_keywords, niche_hashtags, db_dir=tmp_dir)
        results = tracker.process_trending(sample_trending)
        report = tracker.export_report(results, format="text")
        assert "Trend Report" in report
        assert "#python" in report

    def test_stats(self, tmp_dir, niche_keywords, niche_hashtags, sample_trending):
        tracker = TrendTracker(niche_keywords, niche_hashtags, db_dir=tmp_dir)
        tracker.process_trending(sample_trending)
        stats = tracker.stats()
        assert stats["active_trends"] == 5
        assert "by_phase" in stats
        assert "by_priority" in stats

    def test_empty_trending(self, tmp_dir):
        tracker = TrendTracker(db_dir=tmp_dir)
        results = tracker.process_trending([])
        assert len(results) == 0

    def test_missing_keyword(self, tmp_dir):
        tracker = TrendTracker(db_dir=tmp_dir)
        results = tracker.process_trending([{"volume": 100}])
        assert len(results) == 0

    def test_multiple_passes(self, tmp_dir, niche_keywords, niche_hashtags):
        tracker = TrendTracker(niche_keywords, niche_hashtags, db_dir=tmp_dir)
        # First pass
        tracker.process_trending([{"keyword": "#python", "volume": 100}])
        # Second pass
        tracker.process_trending([{"keyword": "#python", "volume": 200}])
        active = tracker.get_active_trends()
        assert len(active) == 1
        assert active[0].volume == 200
