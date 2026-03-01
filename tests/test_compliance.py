"""Tests for Compliance & Safety Module"""
import pytest
from bot.compliance import (
    ComplianceChecker, ContentPolicy, SpamScorer,
    ViolationType, Severity, Violation, ComplianceResult,
)


@pytest.fixture
def checker():
    c = ComplianceChecker(db_path=":memory:")
    yield c
    c.close()


@pytest.fixture
def policy():
    return ContentPolicy()


@pytest.fixture
def scorer():
    return SpamScorer()


class TestContentPolicy:
    def test_default_banned_words(self, policy):
        assert policy.banned_count > 0

    def test_check_clean(self, policy):
        violations = policy.check("This is a normal tweet about coding")
        assert len(violations) == 0

    def test_check_banned(self, policy):
        violations = policy.check("This is a scam, free money for all")
        assert len(violations) > 0
        assert any(v.type == ViolationType.BANNED_WORD for v in violations)

    def test_add_banned_words(self, policy):
        policy.add_banned_words(["badword"])
        violations = policy.check("This contains badword")
        assert len(violations) > 0

    def test_remove_banned_words(self, policy):
        policy.remove_banned_words(["scam"])
        violations = policy.check("This is a scam")
        assert not any(v.detail == "scam" for v in violations)

    def test_safe_words(self, policy):
        policy.add_safe_words(["scam alert"])
        violations = policy.check("This is a scam alert service")
        # "scam" is in "scam alert", so it should be safe
        assert not any(v.detail == "scam" for v in violations)

    def test_banned_pattern(self, policy):
        policy.add_banned_pattern(r"\b\d{4}-\d{4}-\d{4}\b")
        violations = policy.check("Call 1234-5678-9012 now")
        assert len(violations) > 0

    def test_safe_count(self, policy):
        assert policy.safe_count == 0
        policy.add_safe_words(["ok"])
        assert policy.safe_count == 1


class TestSpamScorer:
    def test_clean_text(self, scorer):
        score, violations = scorer.score("A well-written tweet about programming")
        assert score < 50

    def test_empty_text(self, scorer):
        score, _ = scorer.score("")
        assert score == 100.0

    def test_hashtag_spam(self, scorer):
        text = "Buy now #tag1 #tag2 #tag3 #tag4 #tag5 #tag6 #tag7 #tag8"
        score, violations = scorer.score(text)
        assert any(v.type == ViolationType.HASHTAG_SPAM for v in violations)

    def test_mention_spam(self, scorer):
        text = "Hey @a @b @c @d @e @f @g @h check this!"
        score, violations = scorer.score(text)
        assert any(v.type == ViolationType.MENTION_SPAM for v in violations)

    def test_link_density(self, scorer):
        text = "Visit https://a.com https://b.com https://c.com"
        score, violations = scorer.score(text)
        assert any(v.type == ViolationType.LINK_DENSITY for v in violations)

    def test_caps_abuse(self, scorer):
        text = "THIS IS ALL CAPS AND VERY LOUD CONTENT HERE"
        score, violations = scorer.score(text)
        assert any(v.type == ViolationType.CAPS_ABUSE for v in violations)

    def test_short_content(self, scorer):
        text = "ok"
        score, violations = scorer.score(text)
        assert any(v.type == ViolationType.SHORT_CONTENT for v in violations)

    def test_normal_hashtags_ok(self, scorer):
        text = "Great article on #Python #Coding"
        score, violations = scorer.score(text)
        assert not any(v.type == ViolationType.HASHTAG_SPAM for v in violations)

    def test_score_capped_at_100(self, scorer):
        text = "#a #b #c #d #e #f #g #h #i @x @y @z @w @v @u @t https://x.com https://y.com https://z.com THIS IS SPAM"
        score, _ = scorer.score(text)
        assert score <= 100.0


class TestComplianceChecker:
    def test_check_content_clean(self, checker):
        result = checker.check_content("A normal tweet about technology")
        assert result.passed is True
        assert result.score > 50

    def test_check_content_violation(self, checker):
        result = checker.check_content("This is a scam, buy now!")
        assert result.score < 100

    def test_record_action(self, checker):
        checker.record_action("acc1", "tweet")
        # Should not raise

    def test_rate_compliance_ok(self, checker):
        for _ in range(5):
            checker.record_action("acc1", "tweet")
        result = checker.check_rate_compliance("acc1")
        assert result.passed is True

    def test_rate_compliance_exceeded(self, checker):
        for _ in range(60):
            checker.record_action("acc2", "tweet")
        result = checker.check_rate_compliance("acc2")
        assert result.passed is False
        assert any(v.type == ViolationType.RATE_LIMIT for v in result.violations)

    def test_following_ratio_ok(self, checker):
        result = checker.check_following_ratio(followers=1000, following=800)
        assert result.passed is True

    def test_following_ratio_bad(self, checker):
        result = checker.check_following_ratio(followers=100, following=500)
        assert result.passed is False

    def test_following_ratio_high_following(self, checker):
        result = checker.check_following_ratio(followers=100, following=6000)
        assert any(v.type == ViolationType.FOLLOWING_RATIO for v in result.violations)

    def test_add_banned_words(self, checker):
        checker.add_banned_words(["customban"])
        result = checker.check_content("This has customban in it")
        assert result.score < 100

    def test_add_safe_words(self, checker):
        checker.add_safe_words(["safe scam"])
        # Should not crash

    def test_record_violation(self, checker):
        v = Violation(type=ViolationType.SPAM, severity=Severity.HIGH, description="test")
        checker.record_violation("acc1", v)
        history = checker.get_violation_history("acc1")
        assert len(history) == 1

    def test_violation_history_all(self, checker):
        v = Violation(type=ViolationType.SPAM, severity=Severity.LOW, description="test")
        checker.record_violation("acc1", v)
        checker.record_violation("acc2", v)
        history = checker.get_violation_history()
        assert len(history) == 2

    def test_compliance_report_text(self, checker):
        v = Violation(type=ViolationType.SPAM, severity=Severity.HIGH, description="test", score_impact=10)
        checker.record_violation("acc1", v)
        report = checker.generate_compliance_report("acc1", format="text")
        assert "acc1" in report

    def test_compliance_report_json(self, checker):
        v = Violation(type=ViolationType.SPAM, severity=Severity.HIGH, description="test", score_impact=10)
        checker.record_violation("acc1", v)
        report = checker.generate_compliance_report("acc1", format="json")
        import json
        data = json.loads(report)
        assert data["total_violations"] == 1

    def test_compliance_result_auto_timestamp(self):
        r = ComplianceResult(passed=True, score=100)
        assert r.checked_at != ""

    def test_zero_followers_ratio(self, checker):
        result = checker.check_following_ratio(followers=0, following=50)
        # Should handle division by zero
        assert isinstance(result, ComplianceResult)
