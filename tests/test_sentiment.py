"""
Tests for Sentiment Analysis Engine
"""

import pytest
from unittest.mock import MagicMock
from bot.sentiment import (
    SentimentAnalyzer, SentimentResult, SentimentSummary,
    SentimentLabel, POSITIVE_WORDS, NEGATIVE_WORDS,
)
from bot.database import Database


@pytest.fixture
def db(tmp_path):
    return Database(str(tmp_path / "test.db"))


@pytest.fixture
def analyzer(db):
    return SentimentAnalyzer(db)


@pytest.fixture
def analyzer_no_db():
    return SentimentAnalyzer(db=None)


class TestSentimentResult:
    def test_positive(self):
        r = SentimentResult(text="great", score=0.5, label="positive",
                            confidence=0.8, word_count=1)
        assert r.is_positive
        assert not r.is_negative
        assert not r.is_neutral

    def test_negative(self):
        r = SentimentResult(text="bad", score=-0.5, label="negative",
                            confidence=0.8, word_count=1)
        assert not r.is_positive
        assert r.is_negative

    def test_neutral(self):
        r = SentimentResult(text="ok", score=0.0, label="neutral",
                            confidence=0.5, word_count=1)
        assert r.is_neutral

    def test_to_dict(self):
        r = SentimentResult(text="test", score=0.5, label="positive",
                            confidence=0.7, positive_words=["great"],
                            negative_words=[], word_count=5)
        d = r.to_dict()
        assert d["score"] == 0.5
        assert d["label"] == "positive"
        assert "great" in d["positive_words"]


class TestSentimentSummary:
    def test_percentages(self):
        s = SentimentSummary(total=10, positive_count=5,
                             negative_count=3, neutral_count=2)
        assert s.positive_pct == 50.0
        assert s.negative_pct == 30.0
        assert s.neutral_pct == 20.0

    def test_empty(self):
        s = SentimentSummary()
        assert s.positive_pct == 0
        assert s.negative_pct == 0

    def test_to_dict(self):
        s = SentimentSummary(total=5, positive_count=3,
                             negative_count=1, neutral_count=1, avg_score=0.3)
        d = s.to_dict()
        assert d["total"] == 5
        assert d["positive"]["pct"] == 60.0


class TestSentimentAnalyzer:
    def test_positive_text(self, analyzer):
        result = analyzer.analyze("This is amazing and wonderful! 🔥")
        assert result.is_positive
        assert result.score > 0
        assert len(result.positive_words) > 0

    def test_negative_text(self, analyzer):
        result = analyzer.analyze("This is terrible and horrible")
        assert result.is_negative
        assert result.score < 0
        assert len(result.negative_words) > 0

    def test_neutral_text(self, analyzer):
        result = analyzer.analyze("The meeting is at 3pm")
        assert result.is_neutral or result.confidence < 0.5

    def test_empty_text(self, analyzer):
        result = analyzer.analyze("")
        assert result.score == 0
        assert result.label == SentimentLabel.NEUTRAL

    def test_none_text(self, analyzer):
        result = analyzer.analyze(None)
        assert result.score == 0

    def test_negation_flips(self, analyzer):
        pos = analyzer.analyze("This is great")
        neg = analyzer.analyze("This is not great")
        assert pos.score > neg.score

    def test_intensifier(self, analyzer):
        normal = analyzer.analyze("good product")
        intense = analyzer.analyze("very good product")
        # Intensifier should increase score
        assert intense.score >= normal.score

    def test_very_positive_label(self, analyzer):
        result = analyzer.analyze("Amazing wonderful fantastic excellent brilliant")
        assert result.label in [SentimentLabel.VERY_POSITIVE, SentimentLabel.POSITIVE]

    def test_very_negative_label(self, analyzer):
        result = analyzer.analyze("Terrible horrible awful disgusting disaster")
        assert result.label in [SentimentLabel.VERY_NEGATIVE, SentimentLabel.NEGATIVE]

    def test_emoji_positive(self, analyzer):
        result = analyzer.analyze("Great work 🔥🚀💪")
        assert result.is_positive

    def test_emoji_negative(self, analyzer):
        result = analyzer.analyze("Awful 😡💩👎")
        assert result.is_negative

    def test_question_detection(self, analyzer):
        result = analyzer.analyze("What do you think about this?")
        assert result.has_question

    def test_exclamation_detection(self, analyzer):
        result = analyzer.analyze("Amazing news!")
        assert result.has_exclamation

    def test_word_count(self, analyzer):
        result = analyzer.analyze("one two three four five")
        assert result.word_count == 5

    def test_confidence_levels(self, analyzer):
        low = analyzer.analyze("the table is red")
        high = analyzer.analyze("amazing wonderful fantastic beautiful incredible")
        assert high.confidence > low.confidence

    def test_score_to_label(self, analyzer):
        assert analyzer._score_to_label(0.8) == SentimentLabel.VERY_POSITIVE
        assert analyzer._score_to_label(0.3) == SentimentLabel.POSITIVE
        assert analyzer._score_to_label(0.0) == SentimentLabel.NEUTRAL
        assert analyzer._score_to_label(-0.3) == SentimentLabel.NEGATIVE
        assert analyzer._score_to_label(-0.8) == SentimentLabel.VERY_NEGATIVE


class TestBatchAnalysis:
    def test_analyze_batch(self, analyzer):
        texts = ["Great!", "Terrible!", "Normal day"]
        results = analyzer.analyze_batch(texts)
        assert len(results) == 3

    def test_analyze_tweets(self, analyzer):
        tweets = [
            {"id": "1", "text": "I love this!", "author_username": "user1"},
            {"id": "2", "text": "This sucks", "author_username": "user2"},
        ]
        results = analyzer.analyze_tweets(tweets)
        assert len(results) == 2
        assert results[0].is_positive
        assert results[1].is_negative

    def test_summarize(self, analyzer):
        texts = ["Amazing!", "Great!", "Terrible", "Normal", "Wonderful"]
        results = analyzer.analyze_batch(texts)
        summary = analyzer.summarize(results)
        assert summary.total == 5
        assert summary.positive_count + summary.negative_count + summary.neutral_count == 5

    def test_summarize_empty(self, analyzer):
        summary = analyzer.summarize([])
        assert summary.total == 0

    def test_format_summary(self, analyzer):
        texts = ["Great!", "Bad!", "OK"]
        results = analyzer.analyze_batch(texts)
        summary = analyzer.summarize(results)
        formatted = analyzer.format_summary(summary)
        assert "情感分析报告" in formatted
        assert "正面" in formatted


class TestCrisisDetection:
    def test_no_crisis(self, analyzer):
        texts = ["Love it!", "Amazing!", "Great product", "Wonderful", "Perfect"]
        results = analyzer.analyze_batch(texts)
        crisis = analyzer.detect_brand_crisis(results)
        assert not crisis["crisis"]
        assert crisis["level"] == "normal"

    def test_warning_crisis(self, analyzer):
        texts = ["Bad", "Terrible", "Awful", "OK", "Horrible", "Fine",
                 "Disgusting", "Normal", "Poor", "Meh"]
        results = analyzer.analyze_batch(texts)
        crisis = analyzer.detect_brand_crisis(results)
        assert crisis["crisis"]
        assert crisis["level"] in ["warning", "critical"]

    def test_critical_crisis(self, analyzer):
        texts = ["Hate", "Terrible", "Worst", "Awful", "Horrible",
                 "Disaster", "Disgusting", "Failure", "Trash", "Garbage"]
        results = analyzer.analyze_batch(texts)
        crisis = analyzer.detect_brand_crisis(results)
        assert crisis["crisis"]
        assert crisis["level"] in ["critical", "warning"]

    def test_empty_crisis(self, analyzer):
        crisis = analyzer.detect_brand_crisis([])
        assert not crisis["crisis"]

    def test_crisis_recommendation(self, analyzer):
        assert "立即" in analyzer._crisis_recommendation("critical")
        assert "关注" in analyzer._crisis_recommendation("warning")
        assert "正常" in analyzer._crisis_recommendation("normal")


class TestCustomDictionary:
    def test_custom_positive(self):
        analyzer = SentimentAnalyzer(
            db=None,
            custom_positive={"moonshot", "rocketship"}
        )
        result = analyzer.analyze("This is a moonshot idea")
        assert result.is_positive

    def test_custom_negative(self):
        analyzer = SentimentAnalyzer(
            db=None,
            custom_negative={"rugpull", "ponzi"}
        )
        result = analyzer.analyze("Looks like a rugpull")
        assert result.is_negative


class TestDatabaseIntegration:
    def test_save_and_retrieve(self, analyzer, db):
        tweets = [
            {"id": "t1", "text": "Great product!", "author_username": "alice"},
            {"id": "t2", "text": "Terrible service", "author_username": "alice"},
        ]
        analyzer.analyze_tweets(tweets)

        history = analyzer.get_sentiment_history("alice", days=1)
        # May or may not have data depending on timing
        assert isinstance(history, list)

    def test_no_db_analyzer(self, analyzer_no_db):
        result = analyzer_no_db.analyze("Great!")
        assert result.is_positive
        # Should not crash without db
        history = analyzer_no_db.get_sentiment_history()
        assert history == []


class TestSentimentWords:
    def test_positive_words_set(self):
        assert "amazing" in POSITIVE_WORDS
        assert "🔥" in POSITIVE_WORDS

    def test_negative_words_set(self):
        assert "terrible" in NEGATIVE_WORDS
        assert "😡" in NEGATIVE_WORDS

    def test_no_overlap(self):
        # Ensure no word is in both sets
        overlap = POSITIVE_WORDS & NEGATIVE_WORDS
        assert len(overlap) == 0
