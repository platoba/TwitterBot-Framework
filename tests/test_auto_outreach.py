"""Tests for auto_outreach.py — 自动化外展引擎"""
import os
import json
import pytest
from datetime import datetime, timezone

from bot.auto_outreach import (
    ProspectStatus, SequenceStepType, ResponseCategory, OutreachChannel,
    Prospect, OutreachMessage, SequenceStep, Sequence,
    ProspectScorer, TemplateEngine, OutreachDB, ResponseClassifier,
    SequenceExecutor, OutreachAnalytics, AutoOutreach,
)


# ── Fixtures ──────────────────────────────────────────────────────

@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "outreach_test.db")


@pytest.fixture
def db(tmp_db):
    d = OutreachDB(tmp_db)
    yield d
    d.close()


@pytest.fixture
def outreach(tmp_db):
    o = AutoOutreach(db_path=tmp_db, niche_keywords=["crypto", "web3", "defi"])
    yield o
    o.close()


def _prospect(pid="p1", username="alice", followers=5000, following=500,
              tweets=1000, bio="crypto enthusiast"):
    return Prospect(
        prospect_id=pid,
        username=username,
        display_name=f"Display {username}",
        bio=bio,
        follower_count=followers,
        following_count=following,
        tweet_count=tweets,
        source="search",
    )


# ── Enum Tests ────────────────────────────────────────────────────

class TestEnums:
    def test_prospect_status(self):
        assert ProspectStatus.NEW.value == "new"
        assert ProspectStatus.CONVERTED.value == "converted"
        assert ProspectStatus.UNSUBSCRIBED.value == "unsubscribed"

    def test_sequence_step_type(self):
        assert SequenceStepType.MESSAGE.value == "message"
        assert SequenceStepType.DELAY.value == "delay"

    def test_response_category(self):
        assert ResponseCategory.POSITIVE.value == "positive"
        assert ResponseCategory.UNSUBSCRIBE.value == "unsubscribe"

    def test_outreach_channel(self):
        assert OutreachChannel.DM.value == "dm"
        assert OutreachChannel.QUOTE.value == "quote"


# ── Prospect Tests ────────────────────────────────────────────────

class TestProspect:
    def test_engagement_rate(self):
        p = _prospect(followers=5000, tweets=500)
        assert p.engagement_rate > 0

    def test_engagement_rate_zero_followers(self):
        p = _prospect(followers=0, tweets=100)
        assert p.engagement_rate == 0.0

    def test_to_dict(self):
        p = _prospect()
        d = p.to_dict()
        assert d["username"] == "alice"
        assert d["status"] == "new"

    def test_default_created_at(self):
        p = _prospect()
        assert p.created_at != ""


# ── OutreachMessage Tests ─────────────────────────────────────────

class TestOutreachMessage:
    def test_to_dict(self):
        m = OutreachMessage(
            message_id="m1", prospect_id="p1",
            sequence_id="s1", step_index=0,
            channel=OutreachChannel.DM, content="Hello!",
        )
        d = m.to_dict()
        assert d["channel"] == "dm"
        assert d["content"] == "Hello!"

    def test_default_sent_at(self):
        m = OutreachMessage(
            message_id="m1", prospect_id="p1",
            sequence_id="s1", step_index=0,
            channel=OutreachChannel.DM, content="Hi",
        )
        assert m.sent_at != ""


# ── SequenceStep Tests ────────────────────────────────────────────

class TestSequenceStep:
    def test_to_dict(self):
        s = SequenceStep(
            step_type=SequenceStepType.MESSAGE,
            content="Hello {{name}}!",
            channel=OutreachChannel.DM,
        )
        d = s.to_dict()
        assert d["step_type"] == "message"
        assert d["channel"] == "dm"

    def test_delay_step(self):
        s = SequenceStep(
            step_type=SequenceStepType.DELAY,
            delay_hours=24,
        )
        assert s.delay_hours == 24


# ── Sequence Tests ────────────────────────────────────────────────

class TestSequence:
    def test_add_step(self):
        seq = Sequence(sequence_id="s1", name="Cold DM")
        seq.add_step(SequenceStep(step_type=SequenceStepType.MESSAGE, content="Hi!"))
        assert len(seq.steps) == 1

    def test_to_dict(self):
        seq = Sequence(sequence_id="s1", name="Test")
        seq.add_step(SequenceStep(step_type=SequenceStepType.MESSAGE, content="Hey"))
        d = seq.to_dict()
        assert d["sequence_id"] == "s1"
        assert len(d["steps"]) == 1

    def test_default_created_at(self):
        seq = Sequence(sequence_id="s1", name="Test")
        assert seq.created_at != ""


# ── ProspectScorer Tests ──────────────────────────────────────────

class TestProspectScorer:
    def test_score_basic(self):
        scorer = ProspectScorer(niche_keywords=["crypto"])
        p = _prospect(followers=10000, tweets=5000, bio="crypto trader")
        score = scorer.score(p)
        assert 0 <= score <= 100

    def test_score_high_follower(self):
        scorer = ProspectScorer()
        p = _prospect(followers=100000)
        score = scorer.score(p)
        assert score > 30

    def test_score_low_follower(self):
        scorer = ProspectScorer()
        p = _prospect(followers=10)
        score = scorer.score(p)
        assert score < 80

    def test_score_detailed(self):
        scorer = ProspectScorer(niche_keywords=["web3"])
        p = _prospect(bio="web3 builder")
        result = scorer.score_detailed(p)
        assert "raw_scores" in result
        assert "weighted_scores" in result
        assert "total" in result
        assert "grade" in result

    def test_grade_s(self):
        assert ProspectScorer._grade(95) == "S"

    def test_grade_a(self):
        assert ProspectScorer._grade(85) == "A"

    def test_grade_f(self):
        assert ProspectScorer._grade(20) == "F"

    def test_bio_relevance_with_keywords(self):
        scorer = ProspectScorer(niche_keywords=["crypto", "web3", "defi"])
        p = _prospect(bio="crypto and web3 enthusiast")
        detail = scorer.score_detailed(p)
        assert detail["raw_scores"]["bio_relevance"] > 10

    def test_bio_relevance_no_keywords(self):
        scorer = ProspectScorer(niche_keywords=["crypto"])
        p = _prospect(bio="cooking recipes daily")
        detail = scorer.score_detailed(p)
        assert detail["raw_scores"]["bio_relevance"] <= 50

    def test_ratio_score_high(self):
        scorer = ProspectScorer()
        p = _prospect(followers=50000, following=100)
        detail = scorer.score_detailed(p)
        assert detail["raw_scores"]["ratio_score"] >= 80

    def test_custom_weights(self):
        weights = {"follower_score": 1.0, "engagement_score": 0, "bio_relevance": 0,
                   "activity_score": 0, "ratio_score": 0, "completeness": 0}
        scorer = ProspectScorer(weights=weights)
        p = _prospect(followers=50000)
        score = scorer.score(p)
        assert score > 0


# ── TemplateEngine Tests ─────────────────────────────────────────

class TestTemplateEngine:
    def test_render(self):
        engine = TemplateEngine()
        result = engine.render("cold_dm", {"name": "Alice", "niche": "crypto", "hook": "Nice!"})
        assert "Alice" in result
        assert "crypto" in result

    def test_render_missing_var(self):
        engine = TemplateEngine()
        result = engine.render("cold_dm", {"name": "Bob"})
        assert "Bob" in result
        assert "{{" not in result  # unreplaced vars removed

    def test_render_raw(self):
        engine = TemplateEngine()
        result = engine.render_raw("Hello {{name}}!", {"name": "Charlie"})
        assert result == "Hello Charlie!"

    def test_add_template(self):
        engine = TemplateEngine()
        engine.add_template("custom", "Hey {{name}}, check {{link}}")
        result = engine.render("custom", {"name": "Dave", "link": "example.com"})
        assert "Dave" in result

    def test_get_template(self):
        engine = TemplateEngine()
        tpl = engine.get_template("cold_dm")
        assert tpl is not None
        assert "{{name}}" in tpl

    def test_list_templates(self):
        engine = TemplateEngine()
        templates = engine.list_templates()
        assert "cold_dm" in templates
        assert len(templates) >= 8

    def test_get_variables(self):
        engine = TemplateEngine()
        vars = engine.get_variables("cold_dm")
        assert "name" in vars
        assert "niche" in vars

    def test_preview(self):
        engine = TemplateEngine()
        preview = engine.preview("cold_dm")
        assert "{{name}}" in preview

    def test_custom_templates_init(self):
        engine = TemplateEngine({"hello": "Hi {{name}}!"})
        result = engine.render("hello", {"name": "Eve"})
        assert result == "Hi Eve!"

    def test_nonexistent_template(self):
        engine = TemplateEngine()
        result = engine.render("nonexistent", {})
        assert result == ""


# ── OutreachDB Tests ──────────────────────────────────────────────

class TestOutreachDB:
    def test_save_and_get_prospect(self, db):
        p = _prospect()
        db.save_prospect(p)
        got = db.get_prospect("p1")
        assert got is not None
        assert got["username"] == "alice"

    def test_save_prospects_batch(self, db):
        prospects = [_prospect(pid=f"p{i}", username=f"user{i}") for i in range(5)]
        db.save_prospects_batch(prospects)
        for i in range(5):
            assert db.get_prospect(f"p{i}") is not None

    def test_get_prospects_by_status(self, db):
        p = _prospect()
        db.save_prospect(p)
        results = db.get_prospects_by_status(ProspectStatus.NEW)
        assert len(results) >= 1

    def test_get_top_prospects(self, db):
        for i in range(10):
            p = _prospect(pid=f"p{i}", username=f"user{i}")
            p.score = float(i * 10)
            db.save_prospect(p)
        top = db.get_top_prospects(limit=5)
        assert len(top) == 5
        assert top[0]["score"] >= top[-1]["score"]

    def test_update_prospect_status(self, db):
        p = _prospect()
        db.save_prospect(p)
        db.update_prospect_status("p1", ProspectStatus.CONTACTED)
        got = db.get_prospect("p1")
        assert got["status"] == "contacted"

    def test_count_prospects(self, db):
        db.save_prospect(_prospect(pid="p1"))
        p2 = _prospect(pid="p2", username="bob")
        p2.status = ProspectStatus.CONTACTED
        db.save_prospect(p2)
        counts = db.count_prospects()
        assert "new" in counts

    def test_save_and_get_sequence(self, db):
        seq = Sequence(sequence_id="s1", name="Cold DM")
        seq.add_step(SequenceStep(step_type=SequenceStepType.MESSAGE, content="Hi {{name}}"))
        db.save_sequence(seq)
        got = db.get_sequence("s1")
        assert got is not None
        assert got["name"] == "Cold DM"

    def test_get_all_sequences(self, db):
        db.save_sequence(Sequence(sequence_id="s1", name="Seq 1"))
        db.save_sequence(Sequence(sequence_id="s2", name="Seq 2"))
        all_seq = db.get_all_sequences()
        assert len(all_seq) >= 2

    def test_save_and_get_message(self, db):
        msg = OutreachMessage(
            message_id="m1", prospect_id="p1",
            sequence_id="s1", step_index=0,
            channel=OutreachChannel.DM, content="Hello!",
            delivered=True,
        )
        db.save_message(msg)
        msgs = db.get_prospect_messages("p1")
        assert len(msgs) >= 1

    def test_message_stats(self, db):
        for i in range(3):
            msg = OutreachMessage(
                message_id=f"m{i}", prospect_id=f"p{i}",
                sequence_id="s1", step_index=0,
                channel=OutreachChannel.DM, content=f"Hi {i}",
                delivered=True, replied=(i == 0),
            )
            db.save_message(msg)
        stats = db.get_message_stats()
        assert stats["total_sent"] == 3
        assert stats["delivered"] == 3
        assert stats["replied"] == 1

    def test_enroll_prospect(self, db):
        db.save_prospect(_prospect())
        db.save_sequence(Sequence(sequence_id="s1", name="Test"))
        db.enroll_prospect("p1", "s1")
        enrollments = db.get_active_enrollments("s1")
        assert len(enrollments) >= 1

    def test_advance_enrollment(self, db):
        db.save_prospect(_prospect())
        db.save_sequence(Sequence(sequence_id="s1", name="Test"))
        db.enroll_prospect("p1", "s1")
        db.advance_enrollment("p1", "s1")
        enrollments = db.get_active_enrollments("s1")
        assert enrollments[0]["current_step"] == 1

    def test_complete_enrollment(self, db):
        db.save_prospect(_prospect())
        db.save_sequence(Sequence(sequence_id="s1", name="Test"))
        db.enroll_prospect("p1", "s1")
        db.complete_enrollment("p1", "s1")
        enrollments = db.get_active_enrollments("s1")
        assert len(enrollments) == 0

    def test_pause_enrollment(self, db):
        db.save_prospect(_prospect())
        db.save_sequence(Sequence(sequence_id="s1", name="Test"))
        db.enroll_prospect("p1", "s1")
        db.pause_enrollment("p1", "s1")
        enrollments = db.get_active_enrollments("s1")
        assert len(enrollments) == 0


# ── ResponseClassifier Tests ─────────────────────────────────────

class TestResponseClassifier:
    def test_positive(self):
        c = ResponseClassifier()
        assert c.classify("Yes, I'm interested!") == ResponseCategory.POSITIVE

    def test_negative(self):
        c = ResponseClassifier()
        assert c.classify("No thanks, not interested") == ResponseCategory.NEGATIVE

    def test_unsubscribe(self):
        c = ResponseClassifier()
        assert c.classify("Please stop contacting me, unsubscribe") == ResponseCategory.UNSUBSCRIBE

    def test_ooo(self):
        c = ResponseClassifier()
        assert c.classify("I'm out of office until Monday") == ResponseCategory.OUT_OF_OFFICE

    def test_question(self):
        c = ResponseClassifier()
        assert c.classify("How much does it cost?") == ResponseCategory.QUESTION

    def test_neutral(self):
        c = ResponseClassifier()
        assert c.classify("okay") == ResponseCategory.NEUTRAL

    def test_empty(self):
        c = ResponseClassifier()
        assert c.classify("") == ResponseCategory.NEUTRAL

    def test_chinese_positive(self):
        c = ResponseClassifier()
        result = c.classify("可以，我们聊聊合作")
        assert result in [ResponseCategory.POSITIVE, ResponseCategory.NEUTRAL]

    def test_chinese_negative(self):
        c = ResponseClassifier()
        result = c.classify("不用了，不需要")
        assert result == ResponseCategory.NEGATIVE

    def test_classify_with_confidence(self):
        c = ResponseClassifier()
        cat, conf = c.classify_with_confidence("Yes, absolutely interested!")
        assert cat == ResponseCategory.POSITIVE
        assert 0 <= conf <= 1.0

    def test_confidence_neutral(self):
        c = ResponseClassifier()
        cat, conf = c.classify_with_confidence("")
        assert cat == ResponseCategory.NEUTRAL
        assert conf == 0.5


# ── SequenceExecutor Tests ────────────────────────────────────────

class TestSequenceExecutor:
    def test_enroll(self, db):
        db.save_prospect(_prospect())
        seq = Sequence(sequence_id="s1", name="Test")
        seq.add_step(SequenceStep(step_type=SequenceStepType.MESSAGE, content="Hi {{name}}"))
        db.save_sequence(seq)
        executor = SequenceExecutor(db, TemplateEngine())
        executor.enroll("p1", "s1")
        enrollments = db.get_active_enrollments("s1")
        assert len(enrollments) >= 1

    def test_get_next_messages(self, db):
        db.save_prospect(_prospect())
        seq = Sequence(sequence_id="s1", name="Cold DM")
        seq.add_step(SequenceStep(
            step_type=SequenceStepType.MESSAGE,
            content="Hi {{name}}, interested in crypto?",
        ))
        db.save_sequence(seq)
        executor = SequenceExecutor(db, TemplateEngine())
        executor.enroll("p1", "s1")
        msgs = executor.get_next_messages("s1")
        assert len(msgs) >= 1
        assert "alice" in msgs[0]["content"].lower() or "Display" in msgs[0]["content"]

    def test_record_sent(self, db):
        db.save_prospect(_prospect())
        seq = Sequence(sequence_id="s1", name="Test")
        seq.add_step(SequenceStep(step_type=SequenceStepType.MESSAGE, content="Hi"))
        db.save_sequence(seq)
        executor = SequenceExecutor(db, TemplateEngine())
        executor.enroll("p1", "s1")
        executor.record_sent("p1", "s1", 0, "Hi!")
        got = db.get_prospect("p1")
        assert got["status"] == "contacted"

    def test_record_reply_positive(self, db):
        db.save_prospect(_prospect())
        msg = OutreachMessage(
            message_id="m1", prospect_id="p1",
            sequence_id="s1", step_index=0,
            channel=OutreachChannel.DM, content="Hi!",
        )
        db.save_message(msg)
        executor = SequenceExecutor(db, TemplateEngine())
        executor.record_reply("p1", "Yes, I'm interested!")
        got = db.get_prospect("p1")
        assert got["status"] == "interested"

    def test_record_reply_negative_pauses(self, db):
        db.save_prospect(_prospect())
        seq = Sequence(sequence_id="s1", name="Test")
        db.save_sequence(seq)
        db.enroll_prospect("p1", "s1")
        msg = OutreachMessage(
            message_id="m1", prospect_id="p1",
            sequence_id="s1", step_index=0,
            channel=OutreachChannel.DM, content="Hi!",
        )
        db.save_message(msg)
        executor = SequenceExecutor(db, TemplateEngine())
        executor.record_reply("p1", "No thanks, not interested")
        got = db.get_prospect("p1")
        assert got["status"] == "not_interested"
        enrollments = db.get_active_enrollments("s1")
        assert len(enrollments) == 0

    def test_nonexistent_sequence(self, db):
        executor = SequenceExecutor(db, TemplateEngine())
        msgs = executor.get_next_messages("nonexistent")
        assert msgs == []


# ── OutreachAnalytics Tests ──────────────────────────────────────

class TestOutreachAnalytics:
    def test_funnel_report(self, db):
        db.save_prospect(_prospect(pid="p1"))
        p2 = _prospect(pid="p2", username="bob")
        p2.status = ProspectStatus.CONTACTED
        db.save_prospect(p2)
        analytics = OutreachAnalytics(db)
        report = analytics.funnel_report()
        assert report["total_prospects"] >= 2
        assert "rates" in report

    def test_message_report(self, db):
        msg = OutreachMessage(
            message_id="m1", prospect_id="p1",
            sequence_id="s1", step_index=0,
            channel=OutreachChannel.DM, content="Hi",
            delivered=True,
        )
        db.save_message(msg)
        analytics = OutreachAnalytics(db)
        report = analytics.message_report()
        assert report["total_sent"] >= 1

    def test_sequence_report(self, db):
        db.save_prospect(_prospect())
        seq = Sequence(sequence_id="s1", name="Test")
        db.save_sequence(seq)
        db.enroll_prospect("p1", "s1")
        analytics = OutreachAnalytics(db)
        report = analytics.sequence_report("s1")
        assert report["total_enrolled"] >= 1
        assert report["sequence_id"] == "s1"

    def test_channel_report(self, db):
        for i in range(3):
            msg = OutreachMessage(
                message_id=f"m{i}", prospect_id=f"p{i}",
                sequence_id="s1", step_index=0,
                channel=OutreachChannel.DM, content=f"Hi {i}",
                delivered=True,
            )
            db.save_message(msg)
        analytics = OutreachAnalytics(db)
        report = analytics.channel_report()
        assert "dm" in report

    def test_generate_text_report(self, db):
        db.save_prospect(_prospect())
        analytics = OutreachAnalytics(db)
        text = analytics.generate_text_report()
        assert "Outreach Analytics Report" in text

    def test_export_json(self, db, tmp_path):
        db.save_prospect(_prospect())
        analytics = OutreachAnalytics(db)
        path = str(tmp_path / "analytics.json")
        analytics.export_json(path)
        assert os.path.exists(path)
        with open(path) as f:
            data = json.load(f)
        assert "funnel" in data


# ── AutoOutreach Integration Tests ───────────────────────────────

class TestAutoOutreach:
    def test_add_prospect(self, outreach):
        p = outreach.add_prospect("alice", display_name="Alice",
                                   bio="crypto web3 builder",
                                   follower_count=10000, tweet_count=5000)
        assert p.score > 0
        assert p.username == "alice"

    def test_add_prospects_batch(self, outreach):
        data = [
            {"username": f"user{i}", "follower_count": 1000 * i,
             "bio": "crypto", "tweet_count": 500}
            for i in range(5)
        ]
        prospects = outreach.add_prospects_batch(data)
        assert len(prospects) == 5

    def test_create_sequence(self, outreach):
        seq = outreach.create_sequence("s1", "Cold DM", steps=[
            {"step_type": "message", "content": "Hi {{name}}!"},
            {"step_type": "delay", "delay_hours": 24},
            {"step_type": "message", "content": "Following up, {{name}}!"},
        ])
        assert len(seq.steps) == 3

    def test_full_workflow(self, outreach):
        # 1. Add prospects
        p = outreach.add_prospect("testuser", display_name="Test User",
                                   bio="defi trader", follower_count=8000,
                                   tweet_count=3000)
        # 2. Create sequence
        seq = outreach.create_sequence("s1", "Cold DM", steps=[
            {"step_type": "message", "content": "Hi {{name}}, love your work!"},
        ])
        # 3. Enroll
        outreach.enroll(p.prospect_id, "s1")
        # 4. Get pending messages
        msgs = outreach.get_pending_messages("s1")
        assert len(msgs) >= 1
        # 5. Mark sent
        outreach.mark_sent(p.prospect_id, "s1", 0, msgs[0]["content"])
        # 6. Process reply
        outreach.process_reply(p.prospect_id, "Sounds great, tell me more!")
        # 7. Check funnel
        funnel = outreach.get_funnel()
        assert funnel["total_prospects"] >= 1

    def test_get_report(self, outreach):
        outreach.add_prospect("reporter", follower_count=5000)
        report = outreach.get_report()
        assert isinstance(report, str)
        assert "Outreach" in report

    def test_get_top_prospects(self, outreach):
        for i in range(10):
            outreach.add_prospect(f"user{i}", follower_count=1000 * (i + 1),
                                   bio="crypto web3", tweet_count=500)
        top = outreach.get_top_prospects(limit=5)
        assert len(top) == 5
        assert top[0]["score"] >= top[-1]["score"]

    def test_close(self, tmp_db):
        o = AutoOutreach(db_path=tmp_db)
        o.close()
