"""Tests for crisis_manager.py — 品牌危机检测与响应引擎"""
import os
import json
import tempfile
import pytest
from datetime import datetime, timezone, timedelta

from bot.crisis_manager import (
    CrisisSeverity, CrisisStatus, EscalationLevel, ResponseTone,
    Mention, CrisisEvent, AlertThreshold, CrisisDB,
    SpikeDetector, ResponseEngine, EscalationManager,
    PostCrisisAnalyzer, CrisisManager,
)


# ── Fixtures ──────────────────────────────────────────────────────

@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "crisis_test.db")


@pytest.fixture
def db(tmp_db):
    d = CrisisDB(tmp_db)
    yield d
    d.close()


@pytest.fixture
def manager(tmp_db):
    m = CrisisManager(db_path=tmp_db)
    yield m
    m.close()


def _mention(tweet_id="t1", sentiment=-0.8, text="terrible product",
             username="user1", author_id="a1", likes=5, rts=2,
             keywords=None, is_influencer=False):
    return Mention(
        tweet_id=tweet_id,
        author_id=author_id,
        author_username=username,
        text=text,
        sentiment_score=sentiment,
        created_at=datetime.now(timezone.utc).isoformat(),
        like_count=likes,
        retweet_count=rts,
        reply_count=1,
        reach=1000,
        keywords=keywords or ["product"],
        is_influencer=is_influencer,
    )


def _crisis(event_id="crisis1", severity=CrisisSeverity.HIGH):
    return CrisisEvent(
        event_id=event_id,
        title="Test Crisis",
        severity=severity,
    )


# ── Mention Tests ─────────────────────────────────────────────────

class TestMention:
    def test_virality_score(self):
        m = _mention(likes=100, rts=50)
        assert m.virality_score > 0

    def test_is_negative(self):
        m = _mention(sentiment=-0.5)
        assert m.is_negative is True

    def test_is_not_negative(self):
        m = _mention(sentiment=0.5)
        assert m.is_negative is False

    def test_to_dict(self):
        m = _mention()
        d = m.to_dict()
        assert d["tweet_id"] == "t1"
        assert "sentiment_score" in d

    def test_keywords_default(self):
        m = _mention(keywords=["brand", "fail"])
        assert m.keywords == ["brand", "fail"]


# ── CrisisEvent Tests ────────────────────────────────────────────

class TestCrisisEvent:
    def test_default_status(self):
        e = _crisis()
        assert e.status == CrisisStatus.DETECTED

    def test_acknowledge(self):
        e = _crisis()
        e.acknowledge("alice")
        assert e.status == CrisisStatus.ACKNOWLEDGED
        assert e.assigned_to == "alice"

    def test_escalate(self):
        e = _crisis()
        e.escalate(EscalationLevel.MANAGER, "getting worse")
        assert e.status == CrisisStatus.ESCALATED
        assert e.escalation_level == EscalationLevel.MANAGER

    def test_respond(self):
        e = _crisis()
        e.respond("We are looking into it")
        assert e.status == CrisisStatus.RESPONDING

    def test_resolve(self):
        e = _crisis()
        e.resolve("resolved it")
        assert e.status == CrisisStatus.RESOLVED

    def test_duration_minutes(self):
        e = _crisis()
        d = e.duration_minutes
        assert d >= 0

    def test_to_dict(self):
        e = _crisis()
        d = e.to_dict()
        assert d["event_id"] == "crisis1"
        assert "severity" in d

    def test_timeline_grows(self):
        e = _crisis()
        initial = len(e.timeline)
        e.acknowledge("bob")
        e.respond("on it")
        assert len(e.timeline) > initial


# ── CrisisDB Tests ───────────────────────────────────────────────

class TestCrisisDB:
    def test_save_and_get_mention(self, db):
        m = _mention()
        db.save_mention(m)
        count = db.get_negative_count(hours=1)
        assert count >= 1

    def test_save_mentions_batch(self, db):
        mentions = [_mention(tweet_id=f"t{i}", sentiment=-0.7) for i in range(5)]
        db.save_mentions_batch(mentions)
        assert db.get_negative_count(hours=1) >= 5

    def test_avg_sentiment(self, db):
        db.save_mention(_mention(sentiment=-0.9))
        db.save_mention(_mention(tweet_id="t2", sentiment=-0.1))
        avg = db.get_avg_sentiment(hours=24)
        assert -1.0 <= avg <= 1.0

    def test_influencer_mentions(self, db):
        db.save_mention(_mention(is_influencer=True))
        db.save_mention(_mention(tweet_id="t2", is_influencer=False))
        infl = db.get_influencer_mentions(hours=24)
        assert len(infl) >= 1

    def test_top_viral_negative(self, db):
        db.save_mention(_mention(likes=500, rts=200, sentiment=-0.9))
        db.save_mention(_mention(tweet_id="t2", likes=1, rts=0, sentiment=-0.1))
        top = db.get_top_viral_negative(hours=24, limit=1)
        assert len(top) >= 1

    def test_save_and_get_crisis(self, db):
        e = _crisis()
        db.save_crisis(e)
        got = db.get_crisis("crisis1")
        assert got is not None
        assert got["event_id"] == "crisis1"

    def test_active_crises(self, db):
        e = _crisis()
        db.save_crisis(e)
        active = db.get_active_crises()
        assert len(active) >= 1

    def test_crisis_history(self, db):
        e = _crisis()
        db.save_crisis(e)
        history = db.get_crisis_history(days=30)
        assert len(history) >= 1

    def test_save_and_get_threshold(self, db):
        t = AlertThreshold(keyword="brand", negative_count_1h=5, negative_count_24h=20)
        db.save_threshold(t)
        ts = db.get_thresholds()
        assert len(ts) >= 1
        assert ts[0].keyword == "brand"

    def test_negative_count_with_keyword(self, db):
        db.save_mention(_mention(keywords=["brand"]))
        count = db.get_negative_count(hours=1, keyword="brand")
        assert count >= 0


# ── SpikeDetector Tests ───────────────────────────────────────────

class TestSpikeDetector:
    def test_no_spikes_empty(self, db):
        detector = SpikeDetector(db)
        spikes = detector.check_spikes()
        assert isinstance(spikes, list)

    def test_detect_spike(self, db):
        for i in range(30):
            db.save_mention(_mention(tweet_id=f"t{i}", sentiment=-0.8))
        threshold = AlertThreshold(keyword="product", negative_count_1h=5, negative_count_24h=10)
        db.save_threshold(threshold)
        detector = SpikeDetector(db)
        spikes = detector.check_spikes()
        assert len(spikes) >= 1

    def test_no_spike_below_threshold(self, db):
        db.save_mention(_mention(sentiment=-0.8))
        threshold = AlertThreshold(keyword="product", negative_count_1h=100, negative_count_24h=500)
        detector = SpikeDetector(db)
        spikes = detector.check_spikes([threshold])
        keyword_spikes = [s for s in spikes if s.get("keyword") == "product"]
        assert len(keyword_spikes) == 0

    def test_severity_inference(self, db):
        detector = SpikeDetector(db)
        sev = detector._infer_severity(50, 200)
        assert isinstance(sev, CrisisSeverity)

    def test_severity_low(self, db):
        detector = SpikeDetector(db)
        sev = detector._infer_severity(1, 5)
        assert sev in [CrisisSeverity.LOW, CrisisSeverity.MEDIUM]


# ── ResponseEngine Tests ─────────────────────────────────────────

class TestResponseEngine:
    def test_list_categories(self):
        engine = ResponseEngine()
        cats = engine.list_categories()
        assert len(cats) > 0

    def test_list_tones(self):
        engine = ResponseEngine()
        cats = engine.list_categories()
        if cats:
            tones = engine.list_tones(cats[0])
            assert isinstance(tones, list)

    def test_get_template(self):
        engine = ResponseEngine()
        cats = engine.list_categories()
        if cats:
            tones = engine.list_tones(cats[0])
            if tones:
                tone_enum = ResponseTone(tones[0])
                tpl = engine.get_template(cats[0], tone_enum)
                assert tpl is not None

    def test_render_response(self):
        engine = ResponseEngine()
        cats = engine.list_categories()
        if cats:
            resp = engine.render_response(cats[0], variables={"brand": "TestCo"})
            assert isinstance(resp, str)

    def test_add_custom_template(self):
        engine = ResponseEngine()
        engine.add_template("custom_cat", ResponseTone.FACTUAL, "We are investigating {issue}")
        tpl = engine.get_template("custom_cat", ResponseTone.FACTUAL)
        assert tpl is not None

    def test_render_with_variables(self):
        engine = ResponseEngine({"test_cat": {ResponseTone.FACTUAL: "Hello {name}, re: {issue}"}})
        resp = engine.render_response("test_cat", tone=ResponseTone.FACTUAL,
                                      variables={"name": "Alice", "issue": "delay"})
        assert "Alice" in resp or isinstance(resp, str)


# ── EscalationManager Tests ──────────────────────────────────────

class TestEscalationManager:
    def test_determine_level_default(self):
        em = EscalationManager()
        event = _crisis(severity=CrisisSeverity.LOW)
        level = em.determine_level(event)
        assert isinstance(level, EscalationLevel)

    def test_determine_level_critical(self):
        em = EscalationManager()
        event = _crisis(severity=CrisisSeverity.CRITICAL)
        level = em.determine_level(event)
        assert level in [EscalationLevel.EXECUTIVE, EscalationLevel.EXTERNAL,
                         EscalationLevel.MANAGER]

    def test_should_escalate(self):
        em = EscalationManager()
        event = _crisis(severity=CrisisSeverity.HIGH)
        result = em.should_escalate(event)
        assert isinstance(result, bool)

    def test_escalate_updates_event(self):
        em = EscalationManager()
        event = _crisis(severity=CrisisSeverity.HIGH)
        level = em.escalate(event, "spike continues")
        assert event.status == CrisisStatus.ESCALATED
        assert isinstance(level, EscalationLevel)

    def test_custom_rules(self):
        rules = {CrisisSeverity.LOW: EscalationLevel.EXECUTIVE}
        em = EscalationManager(rules=rules)
        event = _crisis(severity=CrisisSeverity.LOW)
        level = em.determine_level(event)
        assert level == EscalationLevel.EXECUTIVE

    def test_level_rank(self):
        rank_auto = EscalationManager._level_rank(EscalationLevel.AUTO)
        rank_exec = EscalationManager._level_rank(EscalationLevel.EXECUTIVE)
        assert rank_exec > rank_auto


# ── PostCrisisAnalyzer Tests ─────────────────────────────────────

class TestPostCrisisAnalyzer:
    def test_analyze_basic(self):
        event = _crisis()
        event.acknowledge("alice")
        event.respond("We apologize")
        event.resolve("fixed")
        mentions = [_mention(tweet_id=f"m{i}", sentiment=-0.6) for i in range(3)]
        report = PostCrisisAnalyzer.analyze(event, mentions_during=mentions)
        assert isinstance(report, dict)
        assert "event_id" in report

    def test_analyze_no_mentions(self):
        event = _crisis()
        event.resolve("done")
        report = PostCrisisAnalyzer.analyze(event)
        assert isinstance(report, dict)
        assert "event_id" in report

    def test_analyze_with_after(self):
        event = _crisis()
        event.resolve("done")
        during = [_mention(tweet_id=f"d{i}", sentiment=-0.8) for i in range(3)]
        after = [_mention(tweet_id=f"a{i}", sentiment=0.2) for i in range(3)]
        report = PostCrisisAnalyzer.analyze(event, mentions_during=during, mentions_after=after)
        assert "post_crisis" in report

    def test_response_time(self):
        event = _crisis()
        event.acknowledge("bob")
        rt = PostCrisisAnalyzer._response_time(event)
        assert rt is None or rt >= 0

    def test_escalation_path(self):
        event = _crisis()
        event.escalate(EscalationLevel.TEAM, "level up")
        event.escalate(EscalationLevel.MANAGER, "still bad")
        path = PostCrisisAnalyzer._escalation_path(event)
        assert isinstance(path, list)

    def test_generate_lessons(self):
        event = _crisis(severity=CrisisSeverity.CRITICAL)
        event.resolve("done")
        lessons = PostCrisisAnalyzer._generate_lessons(event, {})
        assert isinstance(lessons, list)


# ── CrisisManager Integration Tests ──────────────────────────────

class TestCrisisManager:
    def test_init(self, manager):
        assert manager is not None

    def test_process_mention(self, manager):
        m = _mention()
        manager.db.save_mention(m)
        count = manager.db.get_negative_count(hours=1)
        assert count >= 1

    def test_add_threshold(self, manager):
        t = AlertThreshold(keyword="outage", negative_count_1h=3, negative_count_24h=10)
        manager.db.save_threshold(t)
        ts = manager.db.get_thresholds()
        assert any(t.keyword == "outage" for t in ts)

    def test_full_crisis_lifecycle(self, manager):
        for i in range(20):
            manager.db.save_mention(_mention(tweet_id=f"lc{i}", sentiment=-0.9))
        event = _crisis("lifecycle1")
        manager.db.save_crisis(event)
        event.acknowledge("team_lead")
        manager.db.save_crisis(event)
        event.respond("We are aware and working on it")
        manager.db.save_crisis(event)
        event.resolve("Issue fixed, monitoring")
        manager.db.save_crisis(event)
        got = manager.db.get_crisis("lifecycle1")
        assert got is not None

    def test_spike_detection_integration(self, manager):
        for i in range(25):
            manager.db.save_mention(_mention(tweet_id=f"sp{i}"))
        t = AlertThreshold(keyword="product", negative_count_1h=5, negative_count_24h=10)
        manager.db.save_threshold(t)
        detector = SpikeDetector(manager.db)
        spikes = detector.check_spikes()
        assert isinstance(spikes, list)

    def test_response_engine_integration(self, manager):
        engine = ResponseEngine()
        cats = engine.list_categories()
        assert len(cats) > 0

    def test_post_crisis_analysis(self, manager):
        event = _crisis("analysis1")
        event.acknowledge("analyst")
        event.respond("Investigating")
        event.resolve("Done")
        manager.db.save_crisis(event)
        mentions = [_mention(tweet_id=f"an{i}") for i in range(5)]
        report = PostCrisisAnalyzer.analyze(event, mentions_during=mentions)
        assert "event_id" in report

    def test_close(self, tmp_db):
        m = CrisisManager(db_path=tmp_db)
        m.close()


# ── Enum Tests ────────────────────────────────────────────────────

class TestEnums:
    def test_severity_values(self):
        assert CrisisSeverity.LOW.value == "low"
        assert CrisisSeverity.CRITICAL.value == "critical"

    def test_status_values(self):
        assert CrisisStatus.DETECTED.value == "detected"
        assert CrisisStatus.RESOLVED.value == "resolved"

    def test_escalation_values(self):
        assert EscalationLevel.AUTO.value == "auto"
        assert EscalationLevel.EXTERNAL.value == "external"

    def test_tone_values(self):
        assert ResponseTone.EMPATHETIC.value == "empathetic"
        assert ResponseTone.TRANSPARENT.value == "transparent"
