"""Tests for Profile Optimizer"""

import pytest
from bot.profile_optimizer import (
    ProfileData, ProfileGrade, ScoreBreakdown,
    BioAnalyzer, ProfileScorer, ProfileComparator,
    BioGenerator, ProfileOptimizer,
    POWER_WORDS, CTA_PATTERNS, SOCIAL_PROOF_PATTERNS,
)


# ─── BioAnalyzer ───

class TestBioAnalyzer:
    def test_word_count(self):
        assert BioAnalyzer.word_count("Hello world") == 2
        assert BioAnalyzer.word_count("") == 0

    def test_char_count(self):
        assert BioAnalyzer.char_count("Hello") == 5
        assert BioAnalyzer.char_count("") == 0

    def test_has_emoji(self):
        assert BioAnalyzer.has_emoji("Hello 🚀") is True
        assert BioAnalyzer.has_emoji("Hello") is False
        assert BioAnalyzer.has_emoji("") is False

    def test_emoji_count(self):
        assert BioAnalyzer.emoji_count("🚀🔥💯") >= 1
        assert BioAnalyzer.emoji_count("No emoji") == 0
        assert BioAnalyzer.emoji_count("") == 0

    def test_has_url(self):
        assert BioAnalyzer.has_url("Visit https://example.com") is True
        assert BioAnalyzer.has_url("No url here") is False

    def test_has_hashtag(self):
        assert BioAnalyzer.has_hashtag("#Python #AI") is True
        assert BioAnalyzer.has_hashtag("No hashtags") is False

    def test_hashtag_count(self):
        assert BioAnalyzer.hashtag_count("#a #b #c") == 3
        assert BioAnalyzer.hashtag_count("") == 0

    def test_has_mention(self):
        assert BioAnalyzer.has_mention("Follow @test") is True
        assert BioAnalyzer.has_mention("No mention") is False

    def test_power_word_count(self):
        assert BioAnalyzer.power_word_count("Founder and CEO of X") >= 2
        assert BioAnalyzer.power_word_count("Hello world") == 0
        assert BioAnalyzer.power_word_count("") == 0

    def test_cta_count(self):
        assert BioAnalyzer.cta_count("DM me for details") >= 1
        assert BioAnalyzer.cta_count("Link below 👇") >= 1
        assert BioAnalyzer.cta_count("Just hanging out") == 0

    def test_social_proof_count(self):
        assert BioAnalyzer.social_proof_count("10k+ followers") >= 1
        assert BioAnalyzer.social_proof_count("Featured in Forbes") >= 1
        assert BioAnalyzer.social_proof_count("Just me") == 0
        assert BioAnalyzer.social_proof_count("") == 0

    def test_line_count(self):
        assert BioAnalyzer.line_count("Line 1\nLine 2\nLine 3") == 3
        assert BioAnalyzer.line_count("Single line") == 1
        assert BioAnalyzer.line_count("") == 0

    def test_readability_score(self):
        score = BioAnalyzer.readability_score("Building tools for developers worldwide")
        assert 0 <= score <= 100
        assert BioAnalyzer.readability_score("") == 0.0

    def test_readability_multiline(self):
        bio = "Line one\nLine two\nLine three\nLine four"
        score = BioAnalyzer.readability_score(bio)
        assert score > 50  # Multiple lines boost variety


# ─── ProfileScorer ───

class TestProfileScorer:
    def setup_method(self):
        self.scorer = ProfileScorer()

    # Bio length
    def test_score_bio_empty(self):
        r = self.scorer.score_bio_length("")
        assert r.score == 0
        assert len(r.suggestions) > 0

    def test_score_bio_short(self):
        r = self.scorer.score_bio_length("Hi")
        assert r.score < 10

    def test_score_bio_optimal(self):
        bio = "Building AI tools for developers | Founder @startup | 🚀 DM me for collabs | Python • TypeScript • React"
        r = self.scorer.score_bio_length(bio)
        assert r.score >= 12

    def test_score_bio_max_chars(self):
        bio = "x" * 160
        r = self.scorer.score_bio_length(bio)
        assert r.score == 15

    # Power words
    def test_score_power_words_none(self):
        r = self.scorer.score_power_words("Hello world")
        assert r.score == 0

    def test_score_power_words_one(self):
        r = self.scorer.score_power_words("Founder of something")
        assert r.score > 0

    def test_score_power_words_optimal(self):
        r = self.scorer.score_power_words("Founder and developer, building new things")
        assert r.score >= 12

    # CTA
    def test_score_cta_none(self):
        r = self.scorer.score_cta("Just existing")
        assert r.score == 0

    def test_score_cta_one(self):
        r = self.scorer.score_cta("DM me for pricing")
        assert r.score == 10

    def test_score_cta_multiple(self):
        r = self.scorer.score_cta("DM me | Subscribe now | Link below")
        assert r.score > 0

    # Social proof
    def test_score_social_proof_none(self):
        r = self.scorer.score_social_proof("Regular person")
        assert r.score == 0

    def test_score_social_proof_has(self):
        r = self.scorer.score_social_proof("50k+ followers | Featured in TechCrunch")
        assert r.score >= 8

    # Emoji
    def test_score_emoji_none(self):
        r = self.scorer.score_emoji("Plain text bio")
        assert r.score == 3

    def test_score_emoji_optimal(self):
        r = self.scorer.score_emoji("🚀 Builder | 💡 Creator")
        assert r.score >= 7

    def test_score_emoji_too_many(self):
        # Many scattered emojis across the text
        r = self.scorer.score_emoji("🚀 a 🔥 b 💯 c 🎯 d 🏆 e ✨ f 🌟 g ⭐ h")
        assert r.score <= 7

    # Formatting
    def test_score_formatting_single_line(self):
        r = self.scorer.score_formatting("Just one line")
        assert r.score < 5

    def test_score_formatting_pipes(self):
        r = self.scorer.score_formatting("A | B | C")
        assert r.score >= 3

    def test_score_formatting_bullets(self):
        r = self.scorer.score_formatting("A • B • C")
        assert r.score >= 3

    def test_score_formatting_multiline(self):
        r = self.scorer.score_formatting("Line 1\nLine 2\nLine 3")
        assert r.score >= 5

    # Completeness
    def test_score_completeness_full(self):
        p = ProfileData(
            bio="Test bio", display_name="Test", location="NYC",
            website="https://test.com", pinned_tweet="t1", banner_url="https://banner.jpg"
        )
        r = self.scorer.score_completeness(p)
        assert r.score == 15

    def test_score_completeness_empty(self):
        p = ProfileData()
        r = self.scorer.score_completeness(p)
        assert r.score == 0
        assert len(r.suggestions) > 0

    def test_score_completeness_partial(self):
        p = ProfileData(bio="Test", display_name="Name")
        r = self.scorer.score_completeness(p)
        assert 0 < r.score < 15

    # Engagement ratio
    def test_score_ratio_excellent(self):
        p = ProfileData(followers_count=10000, following_count=500)
        r = self.scorer.score_engagement_ratio(p)
        assert r.score >= 12

    def test_score_ratio_poor(self):
        p = ProfileData(followers_count=100, following_count=5000)
        r = self.scorer.score_engagement_ratio(p)
        assert r.score <= 5

    def test_score_ratio_no_followers(self):
        p = ProfileData(followers_count=0)
        r = self.scorer.score_engagement_ratio(p)
        assert r.score == 5

    # Full score
    def test_full_score_complete_profile(self):
        p = ProfileData(
            username="testuser",
            display_name="Test User",
            bio="🚀 Founder of TestCo | Building AI tools for developers | 50k+ followers | DM me 📩",
            location="San Francisco",
            website="https://testco.com",
            followers_count=50000,
            following_count=1000,
            pinned_tweet="t1",
            banner_url="https://banner.jpg",
        )
        result = self.scorer.full_score(p)
        assert result["username"] == "testuser"
        assert result["total_score"] > 0
        assert result["max_score"] == 100
        assert result["grade"] in [g.value for g in ProfileGrade]
        assert len(result["breakdowns"]) == 8

    def test_full_score_empty_profile(self):
        p = ProfileData(username="empty")
        result = self.scorer.full_score(p)
        assert result["grade"] in ["F", "D"]
        assert len(result["all_suggestions"]) > 0

    def test_grade_s(self):
        p = ProfileData(
            username="perfect",
            display_name="Perfect User",
            bio="🚀 Founder & CEO of BigCo | Building AI tools for 50k+ developers | Ex-Google engineer | DM me for collabs 📩",
            location="NYC",
            website="https://bigco.com",
            followers_count=100000,
            following_count=500,
            pinned_tweet="pin",
            banner_url="https://banner.jpg",
        )
        result = self.scorer.full_score(p)
        assert result["percentage"] >= 80  # High score


# ─── ProfileComparator ───

class TestProfileComparator:
    def setup_method(self):
        self.comparator = ProfileComparator()

    def test_compare_empty(self):
        result = self.comparator.compare([])
        assert result["profiles"] == []
        assert result["ranking"] == []

    def test_compare_single(self):
        p = ProfileData(username="solo", bio="Testing")
        result = self.comparator.compare([p])
        assert len(result["ranking"]) == 1
        assert result["ranking"][0]["rank"] == 1

    def test_compare_multiple(self):
        p1 = ProfileData(
            username="good",
            bio="🚀 Founder | Building tools | DM me",
            display_name="Good", location="NYC",
            website="https://x.com", pinned_tweet="t",
            banner_url="b", followers_count=10000, following_count=100,
        )
        p2 = ProfileData(username="basic", bio="Hi")
        result = self.comparator.compare([p1, p2])
        assert len(result["ranking"]) == 2
        assert result["ranking"][0]["username"] == "good"
        assert result["ranking"][0]["rank"] == 1

    def test_compare_insights(self):
        p1 = ProfileData(username="a", bio="🚀 Founder | Expert | DM me",
                         display_name="A", location="NY", website="x.com",
                         pinned_tweet="t", banner_url="b",
                         followers_count=10000, following_count=100)
        p2 = ProfileData(username="b", bio="Hi")
        result = self.comparator.compare([p1, p2])
        assert len(result["insights"]) > 0


# ─── BioGenerator ───

class TestBioGenerator:
    def test_suggest_bios(self):
        bios = BioGenerator.suggest_bios(role="Developer", niche="AI")
        assert len(bios) > 0
        assert all(len(b) <= 160 for b in bios)

    def test_suggest_bios_with_all_params(self):
        bios = BioGenerator.suggest_bios(
            role="Founder",
            niche="SaaS",
            project="MyApp",
            achievement="$1M ARR",
            cta="Link below 👇",
        )
        assert len(bios) > 0

    def test_optimize_length_short(self):
        bio = "Short bio"
        assert BioGenerator.optimize_length(bio) == bio

    def test_optimize_length_truncate(self):
        bio = "x " * 200
        result = BioGenerator.optimize_length(bio, max_chars=50)
        assert len(result) <= 50

    def test_optimize_length_exact(self):
        bio = "a" * 160
        assert BioGenerator.optimize_length(bio) == bio

    def test_optimize_length_strips(self):
        bio = "  short bio  "
        result = BioGenerator.optimize_length(bio)
        assert result.strip() == "short bio"


# ─── ProfileOptimizer ───

class TestProfileOptimizer:
    def setup_method(self):
        self.opt = ProfileOptimizer()

    def test_analyze(self):
        p = ProfileData(username="test", bio="🚀 Builder of things")
        result = self.opt.analyze(p)
        assert "total_score" in result
        assert "grade" in result

    def test_compare(self):
        profiles = [
            ProfileData(username="a", bio="Good bio here with content"),
            ProfileData(username="b", bio=""),
        ]
        result = self.opt.compare(profiles)
        assert len(result["ranking"]) == 2

    def test_suggest_bios(self):
        bios = self.opt.suggest_bios(role="Engineer")
        assert isinstance(bios, list)

    def test_optimize_bio(self):
        long = "word " * 50
        result = self.opt.optimize_bio(long)
        assert len(result) <= 160

    def test_text_report(self):
        p = ProfileData(
            username="reporter",
            bio="🚀 Founder | Building cool things | DM me",
            display_name="Reporter",
        )
        text = self.opt.text_report(p)
        assert "@reporter" in text
        assert "Overall:" in text
        assert "Breakdown:" in text

    def test_text_report_with_suggestions(self):
        p = ProfileData(username="empty")
        text = self.opt.text_report(p)
        assert "Suggestions" in text


# ─── ScoreBreakdown ───

class TestScoreBreakdown:
    def test_to_dict(self):
        sb = ScoreBreakdown("Test", 8, 10, "Good", ["Do X"])
        d = sb.to_dict()
        assert d["category"] == "Test"
        assert d["score"] == 8
        assert d["max_score"] == 10
        assert d["percentage"] == 80.0
        assert d["suggestions"] == ["Do X"]

    def test_to_dict_zero_max(self):
        sb = ScoreBreakdown("Test", 0, 0, "N/A")
        d = sb.to_dict()
        assert d["percentage"] == 0


# ─── Constants ───

class TestConstants:
    def test_power_words_exist(self):
        assert len(POWER_WORDS) >= 20

    def test_cta_patterns_exist(self):
        assert len(CTA_PATTERNS) >= 5

    def test_social_proof_patterns(self):
        assert len(SOCIAL_PROOF_PATTERNS) >= 4
