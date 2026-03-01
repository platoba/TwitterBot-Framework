"""
Tests for Viral Score Predictor
"""

import pytest
from bot.viral_predictor import (
    ViralPredictor, ViralPrediction, ContentFeatures,
    FEATURE_WEIGHTS, CTA_PATTERNS,
)
from bot.database import Database


@pytest.fixture
def db(tmp_path):
    return Database(str(tmp_path / "test.db"))


@pytest.fixture
def predictor(db):
    return ViralPredictor(db)


class TestContentFeatures:
    def test_defaults(self):
        f = ContentFeatures()
        assert f.has_hashtags is False
        assert f.char_count == 0

    def test_to_dict(self):
        f = ContentFeatures(has_emoji=True, emoji_count=3)
        d = f.to_dict()
        assert d["has_emoji"] is True
        assert d["emoji_count"] == 3


class TestViralPrediction:
    def test_grade_s(self):
        p = ViralPrediction(content="", viral_score=85)
        assert p.grade == "S"

    def test_grade_a(self):
        p = ViralPrediction(content="", viral_score=65)
        assert p.grade == "A"

    def test_grade_b(self):
        p = ViralPrediction(content="", viral_score=45)
        assert p.grade == "B"

    def test_grade_c(self):
        p = ViralPrediction(content="", viral_score=25)
        assert p.grade == "C"

    def test_grade_d(self):
        p = ViralPrediction(content="", viral_score=10)
        assert p.grade == "D"

    def test_to_dict(self):
        p = ViralPrediction(
            content="Hello world",
            viral_score=50,
            category="potential",
        )
        d = p.to_dict()
        assert d["grade"] == "B"
        assert d["category"] == "potential"
        assert "content_preview" in d


class TestExtractFeatures:
    def test_hashtags(self, predictor):
        f = predictor.extract_features("Hello #world #python")
        assert f.has_hashtags is True
        assert f.hashtag_count == 2

    def test_mentions(self, predictor):
        f = predictor.extract_features("Hey @elonmusk @openai")
        assert f.has_mention is True
        assert f.mention_count == 2

    def test_url(self, predictor):
        f = predictor.extract_features("Check https://example.com")
        assert f.has_url is True

    def test_emoji(self, predictor):
        f = predictor.extract_features("Great work! 🔥🚀")
        assert f.has_emoji is True

    def test_question(self, predictor):
        f = predictor.extract_features("What do you think?")
        assert f.has_question is True

    def test_number(self, predictor):
        f = predictor.extract_features("Top 10 Python tips")
        assert f.has_number is True

    def test_thread_hook(self, predictor):
        f = predictor.extract_features("🧵 Here's what most people don't know about AI:")
        assert f.has_thread_hook is True

    def test_cta(self, predictor):
        f = predictor.extract_features("Follow me for more Python tips!")
        assert f.has_cta is True

    def test_media_hint(self, predictor):
        f = predictor.extract_features("Check out this screenshot 📸")
        assert f.has_media_hint is True

    def test_optimal_length(self, predictor):
        content = "A" * 150  # 150 chars
        f = predictor.extract_features(content)
        assert f.optimal_length is True

    def test_too_short(self, predictor):
        f = predictor.extract_features("Hi")
        assert f.optimal_length is False

    def test_char_word_count(self, predictor):
        f = predictor.extract_features("Hello world test")
        assert f.char_count == 16
        assert f.word_count == 3

    def test_engagement_triggers(self, predictor):
        f = predictor.extract_features("Agree or disagree? This is the best framework")
        assert f.engagement_trigger_count >= 2

    def test_plain_text(self, predictor):
        f = predictor.extract_features("Just a plain text message")
        assert f.has_hashtags is False
        assert f.has_mention is False
        assert f.has_url is False


class TestPredict:
    def test_basic_prediction(self, predictor):
        p = predictor.predict("Hello world")
        assert isinstance(p, ViralPrediction)
        assert 0 <= p.viral_score <= 100

    def test_rich_content_scores_higher(self, predictor):
        plain = predictor.predict("Just a plain message without anything special")
        rich = predictor.predict(
            "🧵 Here's why you should learn Python in 2026?\n\n"
            "Reply with your thoughts! 🔥\n\n"
            "#Python #Coding #Dev"
        )
        assert rich.viral_score > plain.viral_score

    def test_too_many_hashtags_penalty(self, predictor):
        few_tags = predictor.predict("Good content #ai #ml #python")
        many_tags = predictor.predict(
            "Spam #a #b #c #d #e #f #g #h tags"
        )
        assert few_tags.viral_score >= many_tags.viral_score

    def test_short_content_penalty(self, predictor):
        short = predictor.predict("Hi")
        normal = predictor.predict("This is a normal length tweet that says something useful about technology")
        assert normal.viral_score > short.viral_score

    def test_prediction_has_suggestions(self, predictor):
        p = predictor.predict("Plain text without any features")
        assert len(p.suggestions) > 0

    def test_category_assignment(self, predictor):
        p = predictor.predict("test")
        assert p.category in ["low", "normal", "potential", "viral"]

    def test_predictions_not_negative(self, predictor):
        p = predictor.predict("x")
        assert p.predicted_impressions >= 0
        assert p.predicted_engagements >= 0
        assert p.predicted_engagement_rate >= 0

    def test_confidence_range(self, predictor):
        p = predictor.predict("test")
        assert 0 <= p.confidence <= 1

    def test_viral_content(self, predictor):
        p = predictor.predict(
            "🧵 THREAD: Here's what most people don't know about AI\n\n"
            "What do you think? Reply with your hot take! 🔥\n\n"
            "Check out this screenshot 📸\n"
            "Follow for more! #AI #ML\n"
            "Top 10 facts that will blow your mind"
        )
        assert p.viral_score > 40  # Should get a decent score


class TestSuggestions:
    def test_suggest_emoji(self, predictor):
        p = predictor.predict("Plain text without emoji")
        assert any("emoji" in s.lower() for s in p.suggestions)

    def test_suggest_question(self, predictor):
        p = predictor.predict("Statement without any question")
        assert any("提问" in s or "问" in s or "互动" in s for s in p.suggestions)

    def test_suggest_cta(self, predictor):
        p = predictor.predict("Just info. No call to action 🔥")
        assert any("CTA" in s or "号召" in s for s in p.suggestions)

    def test_suggest_hashtags(self, predictor):
        p = predictor.predict("No tags at all, just content")
        assert any("标签" in s for s in p.suggestions)

    def test_max_five_suggestions(self, predictor):
        p = predictor.predict("x")  # Minimal content
        assert len(p.suggestions) <= 5


class TestRecordActual:
    def test_record(self, predictor):
        predictor.record_actual(
            "Test content #ai",
            tweet_id="t1",
            impressions=500,
            engagements=50,
        )
        accuracy = predictor.model_accuracy()
        assert accuracy["samples"] == 1

    def test_multiple_records(self, predictor):
        for i in range(5):
            predictor.record_actual(
                f"Content {i} #test",
                tweet_id=f"t{i}",
                impressions=100 + i * 50,
                engagements=10 + i * 5,
            )
        accuracy = predictor.model_accuracy()
        assert accuracy["samples"] == 5

    def test_baseline_updates(self, predictor):
        old_baseline = predictor._baseline.copy()
        predictor.record_actual(
            "Big hit! #viral 🔥", "t1",
            impressions=10000, engagements=1000,
        )
        new_baseline = predictor._baseline
        assert new_baseline["avg_impressions"] > 0


class TestBatchPredict:
    def test_batch(self, predictor):
        contents = [
            "Plain text",
            "🔥 Hot take! What do you think? #AI",
            "🧵 Thread about Python and why you should learn it",
        ]
        results = predictor.batch_predict(contents)
        assert len(results) == 3
        # Sorted by score descending
        assert results[0].viral_score >= results[-1].viral_score


class TestModelAccuracy:
    def test_empty(self, predictor):
        acc = predictor.model_accuracy()
        assert acc["samples"] == 0
        assert acc["calibrated"] is False

    def test_not_calibrated(self, predictor):
        predictor.record_actual("test", impressions=100, engagements=10)
        acc = predictor.model_accuracy()
        assert acc["calibrated"] is False  # Need 20+ samples


class TestFormatPrediction:
    def test_format(self, predictor):
        p = predictor.predict("Test tweet with #hashtag and 🔥 emoji")
        text = predictor.format_prediction(p)
        assert "Viral Score" in text
        assert "Predictions" in text
        assert "Content Analysis" in text

    def test_format_with_suggestions(self, predictor):
        p = predictor.predict("Plain text")
        text = predictor.format_prediction(p)
        assert "Optimization Tips" in text
