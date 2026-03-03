"""Tests for content_moderation module"""

import pytest

from bot.content_moderation import (
    BrandSafetyChecker, ContentModerator, LinkSafetyChecker,
    ModerationFlag, ModerationResult, ModerationStore,
    PIIDetector, PlatformComplianceChecker, RiskLevel,
    SpamDetector, ToxicityDetector, ViolationType,
)


# ── ToxicityDetector ──

class TestToxicityDetector:
    def test_clean_text(self):
        flags = ToxicityDetector.scan("Hello, this is a great product!")
        assert len(flags) == 0

    def test_detect_medium_toxicity(self):
        flags = ToxicityDetector.scan("That's a stupid idea from an idiot")
        assert len(flags) > 0
        assert any(f.risk_level == RiskLevel.MEDIUM for f in flags)

    def test_detect_low_toxicity(self):
        flags = ToxicityDetector.scan("This really sucks man")
        assert any(f.risk_level == RiskLevel.LOW for f in flags)

    def test_multiple_violations(self):
        flags = ToxicityDetector.scan("You stupid idiot, shut up")
        assert len(flags) >= 2

    def test_case_insensitive(self):
        flags = ToxicityDetector.scan("SHUT UP you MORON")
        assert len(flags) > 0

    def test_violation_type(self):
        flags = ToxicityDetector.scan("That idiot is pathetic")
        for f in flags:
            assert f.violation_type == ViolationType.TOXICITY


# ── PIIDetector ──

class TestPIIDetector:
    def test_detect_email(self):
        flags = PIIDetector.scan("Contact me at user@example.com")
        assert len(flags) > 0
        assert any(f.violation_type == ViolationType.PII_LEAK for f in flags)

    def test_detect_phone_us(self):
        flags = PIIDetector.scan("Call me at 555-123-4567")
        assert len(flags) > 0

    def test_detect_phone_cn(self):
        flags = PIIDetector.scan("我的手机号是 13812345678 请联系")
        assert len(flags) > 0

    def test_detect_ssn(self):
        flags = PIIDetector.scan("SSN: 123-45-6789")
        assert any(f.risk_level == RiskLevel.CRITICAL for f in flags)

    def test_detect_credit_card(self):
        flags = PIIDetector.scan("Card: 4111111111111111")
        assert any(f.risk_level == RiskLevel.CRITICAL for f in flags)

    def test_detect_ip_address(self):
        flags = PIIDetector.scan("Server IP: 192.168.1.100")
        assert len(flags) > 0

    def test_no_pii(self):
        flags = PIIDetector.scan("Check out our amazing product!")
        assert len(flags) == 0

    def test_mask_short(self):
        assert PIIDetector._mask("abc") == "***"

    def test_mask_long(self):
        result = PIIDetector._mask("user@example.com")
        assert result.startswith("us")
        assert result.endswith("om")
        assert "*" in result

    def test_redact_email(self):
        result = PIIDetector.redact("Contact user@example.com now")
        assert "user@example.com" not in result
        assert "EMAIL_REDACTED" in result

    def test_redact_phone(self):
        result = PIIDetector.redact("Call 13812345678 now")
        assert "13812345678" not in result

    def test_redact_preserves_clean(self):
        text = "No PII here, just normal text."
        assert PIIDetector.redact(text) == text


# ── BrandSafetyChecker ──

class TestBrandSafetyChecker:
    def test_clean_content(self):
        checker = BrandSafetyChecker()
        flags = checker.scan("Great new product launch!")
        assert len(flags) == 0

    def test_detect_high_risk(self):
        checker = BrandSafetyChecker()
        flags = checker.scan("cocaine is awesome")
        assert any(f.risk_level == RiskLevel.HIGH for f in flags)

    def test_detect_medium_risk(self):
        checker = BrandSafetyChecker()
        flags = checker.scan("Win big at the casino tonight!")
        assert any(f.risk_level == RiskLevel.MEDIUM for f in flags)

    def test_detect_low_risk(self):
        checker = BrandSafetyChecker()
        flags = checker.scan("Enjoying a glass of whiskey")
        assert any(f.risk_level == RiskLevel.LOW for f in flags)

    def test_custom_keywords(self):
        checker = BrandSafetyChecker(custom_keywords={
            "high": ["competitor_brand"]
        })
        flags = checker.scan("competitor_brand is better")
        assert len(flags) > 0

    def test_violation_type(self):
        checker = BrandSafetyChecker()
        flags = checker.scan("gambling addiction")
        for f in flags:
            assert f.violation_type == ViolationType.BRAND_UNSAFE


# ── SpamDetector ──

class TestSpamDetector:
    def test_clean_content(self):
        flags = SpamDetector.scan("Here's a helpful tip about coding!")
        assert len(flags) == 0

    def test_detect_excessive_hashtags(self):
        text = "#a #b #c #d #e #f #g"
        flags = SpamDetector.scan(text)
        assert any("hashtag" in f.description.lower() for f in flags)

    def test_detect_money_bait(self):
        flags = SpamDetector.scan("$5000 free just for signing up!")
        assert any(f.risk_level == RiskLevel.HIGH for f in flags)

    def test_detect_follow_beg(self):
        flags = SpamDetector.scan("Follow me back for great content!")
        assert len(flags) > 0

    def test_detect_dm_bait(self):
        flags = SpamDetector.scan("DM me for free crypto secrets!")
        assert any(f.risk_level == RiskLevel.HIGH for f in flags)

    def test_violation_type(self):
        flags = SpamDetector.scan("$1000 earn free money now")
        for f in flags:
            assert f.violation_type == ViolationType.SPAM


# ── PlatformComplianceChecker ──

class TestPlatformComplianceChecker:
    def test_valid_tweet(self):
        flags = PlatformComplianceChecker.scan("Hello world!")
        assert len(flags) == 0

    def test_too_long(self):
        text = "x" * 300
        flags = PlatformComplianceChecker.scan(text)
        assert any("exceed" in f.description.lower() for f in flags)

    def test_exact_280(self):
        text = "x" * 280
        flags = PlatformComplianceChecker.scan(text)
        length_flags = [f for f in flags if "char" in f.description.lower()]
        assert len(length_flags) == 0

    def test_too_many_hashtags(self):
        text = " ".join(f"#tag{i}" for i in range(15))
        flags = PlatformComplianceChecker.scan(text)
        assert any("hashtag" in f.description.lower() for f in flags)

    def test_too_many_mentions(self):
        text = " ".join(f"@user{i}" for i in range(15))
        flags = PlatformComplianceChecker.scan(text)
        assert any("mention" in f.description.lower() for f in flags)


# ── LinkSafetyChecker ──

class TestLinkSafetyChecker:
    def test_safe_link(self):
        flags = LinkSafetyChecker.scan("Visit https://google.com")
        assert len(flags) == 0

    def test_suspicious_tld(self):
        flags = LinkSafetyChecker.scan("Visit https://scam.xyz")
        assert any(f.violation_type == ViolationType.LINK_UNSAFE for f in flags)

    def test_phishing_pattern(self):
        flags = LinkSafetyChecker.scan("https://paypal-login.malicious.com")
        assert any(f.risk_level == RiskLevel.HIGH for f in flags)

    def test_no_links(self):
        flags = LinkSafetyChecker.scan("No links here!")
        assert len(flags) == 0

    def test_multiple_suspicious(self):
        text = "Check https://evil.tk and https://phish.xyz"
        flags = LinkSafetyChecker.scan(text)
        assert len(flags) >= 2


# ── ModerationStore ──

class TestModerationStore:
    @pytest.fixture
    def store(self, tmp_path):
        db = str(tmp_path / "test_mod.db")
        s = ModerationStore(db_path=db)
        yield s
        s.close()

    def test_log_result(self, store):
        result = ModerationResult(
            content="test content",
            approved=True,
            overall_risk=RiskLevel.SAFE,
            score=1.0,
            reviewed_at="2025-01-01T00:00:00Z",
        )
        store.log_result(result)
        stats = store.get_stats()
        assert stats["total"] >= 1

    def test_stats_approval_rate(self, store):
        for approved in [True, True, False]:
            result = ModerationResult(
                content=f"test_{approved}",
                approved=approved,
                overall_risk=RiskLevel.SAFE if approved else RiskLevel.HIGH,
                score=1.0 if approved else 0.2,
            )
            store.log_result(result)
        stats = store.get_stats()
        assert 0 < stats["approval_rate"] < 1


# ── ContentModerator ──

class TestContentModerator:
    @pytest.fixture
    def moderator(self, tmp_path):
        db = str(tmp_path / "test_cm.db")
        store = ModerationStore(db_path=db)
        return ContentModerator(store=store)

    def test_approve_clean(self, moderator):
        result = moderator.moderate("Great product announcement! 🚀")
        assert result.approved is True
        assert result.overall_risk == RiskLevel.SAFE
        assert result.score == 1.0

    def test_reject_toxic(self, moderator):
        result = moderator.moderate("You stupid idiot shut up moron loser")
        assert result.approved is False or result.score < 0.5

    def test_reject_pii(self, moderator):
        result = moderator.moderate("Email me at secret@private.com, SSN 123-45-6789")
        assert len(result.flags) > 0
        assert any(f.violation_type == ViolationType.PII_LEAK for f in result.flags)

    def test_flag_brand_unsafe(self, moderator):
        result = moderator.moderate("Try cocaine for more energy!")
        assert any(f.violation_type == ViolationType.BRAND_UNSAFE for f in result.flags)

    def test_flag_spam(self, moderator):
        result = moderator.moderate("$5000 earn free money now #a #b #c #d #e #f #g")
        assert any(f.violation_type == ViolationType.SPAM for f in result.flags)

    def test_flag_platform_violation(self, moderator):
        result = moderator.moderate("x" * 300)
        assert any(f.violation_type == ViolationType.PLATFORM_VIOLATION for f in result.flags)

    def test_flag_unsafe_link(self, moderator):
        result = moderator.moderate("Check https://paypal-login.evil.xyz")
        assert any(f.violation_type == ViolationType.LINK_UNSAFE for f in result.flags)

    def test_auto_fix_pii(self, tmp_path):
        db = str(tmp_path / "fix.db")
        store = ModerationStore(db_path=db)
        moderator = ContentModerator(store=store, auto_fix=True)
        result = moderator.moderate("Contact user@example.com for details")
        if result.auto_fix:
            assert "user@example.com" not in result.auto_fix

    def test_batch_moderate(self, moderator):
        contents = ["Good tweet", "bad idiot tweet", "Email: a@b.com"]
        results = moderator.moderate_batch(contents)
        assert len(results) == 3

    def test_score_calculation(self, moderator):
        result = moderator.moderate("Normal safe content")
        assert result.score == 1.0

    def test_score_decreases_with_flags(self, moderator):
        result = moderator.moderate("You stupid moron, check cocaine at casino gambling")
        assert result.score < 1.0

    def test_get_stats(self, moderator):
        moderator.moderate("test content")
        stats = moderator.get_stats()
        assert "total" in stats
        assert "approval_rate" in stats

    def test_result_to_dict(self, moderator):
        result = moderator.moderate("test")
        d = result.to_dict()
        assert "approved" in d
        assert "overall_risk" in d
        assert "flags" in d

    def test_moderation_flag_to_dict(self):
        flag = ModerationFlag(
            violation_type=ViolationType.TOXICITY,
            risk_level=RiskLevel.HIGH,
            description="test",
        )
        d = flag.to_dict()
        assert d["violation_type"] == "toxicity"
        assert d["risk_level"] == "high"

    def test_combined_multiple_violations(self, moderator):
        text = "Stupid idiots email me at test@hack.xyz, $5000 free gambling"
        result = moderator.moderate(text)
        types = {f.violation_type for f in result.flags}
        assert len(types) >= 2  # At least 2 different violation types
