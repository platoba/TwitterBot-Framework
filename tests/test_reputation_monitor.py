"""Tests for bot/reputation_monitor.py"""

import time
import pytest
from bot.reputation_monitor import ReputationMonitor, Mention, ReputationReport
from bot.database import Database
import tempfile, os


@pytest.fixture
def db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    d = Database(path)
    yield d
    os.unlink(path)


@pytest.fixture
def monitor(db):
    return ReputationMonitor(db, brand_keywords=["myapp", "myproduct"])


class TestReputationMonitor:
    def test_positive_sentiment(self, monitor):
        sentiment, score, severity = monitor.analyze_sentiment("I love myapp, it's amazing and wonderful!")
        assert sentiment == "positive"
        assert score > 0
        assert severity == 0

    def test_negative_sentiment(self, monitor):
        sentiment, score, severity = monitor.analyze_sentiment("myapp is terrible and broken, worst experience ever")
        assert sentiment == "negative"
        assert score < 0
        assert severity >= 1

    def test_neutral_sentiment(self, monitor):
        sentiment, score, severity = monitor.analyze_sentiment("I used myapp today for work")
        assert sentiment == "neutral"
        assert severity == 0

    def test_crisis_detection(self, monitor):
        sentiment, score, severity = monitor.analyze_sentiment("myapp data breach exposed user passwords")
        assert severity == 3

    def test_analyze_mentions(self, monitor):
        tweets = [
            {"tweet_id": "t1", "author": "alice", "text": "I love myapp it's great!"},
            {"tweet_id": "t2", "author": "bob", "text": "myapp is terrible and broken"},
            {"tweet_id": "t3", "author": "carol", "text": "Just used myapp today"},
        ]
        mentions = monitor.analyze_mentions(tweets)
        assert len(mentions) == 3
        sentiments = [m.sentiment for m in mentions]
        assert "positive" in sentiments
        assert "negative" in sentiments

    def test_filter_non_mentions(self, monitor):
        tweets = [
            {"tweet_id": "t1", "author": "alice", "text": "Nice weather today"},
            {"tweet_id": "t2", "author": "bob", "text": "myapp is cool"},
        ]
        mentions = monitor.analyze_mentions(tweets)
        assert len(mentions) == 1
        assert mentions[0].author == "bob"

    def test_no_keywords_analyze_all(self, db):
        monitor = ReputationMonitor(db, brand_keywords=[])
        tweets = [
            {"tweet_id": "t1", "author": "alice", "text": "Random text"},
        ]
        mentions = monitor.analyze_mentions(tweets)
        assert len(mentions) == 1

    def test_get_report(self, monitor):
        tweets = [
            {"tweet_id": "t1", "author": "a", "text": "myapp is great and awesome!"},
            {"tweet_id": "t2", "author": "b", "text": "myapp is terrible awful garbage"},
            {"tweet_id": "t3", "author": "c", "text": "myapp works for me"},
        ]
        monitor.analyze_mentions(tweets)
        report = monitor.get_report("7d")
        assert isinstance(report, ReputationReport)
        assert report.total_mentions == 3
        assert report.positive >= 1
        assert report.negative >= 1

    def test_report_format(self, monitor):
        tweets = [{"tweet_id": "t1", "author": "a", "text": "myapp love it great"}]
        monitor.analyze_mentions(tweets)
        report = monitor.get_report("7d")
        text = report.format_report()
        assert "Reputation Report" in text

    def test_alerts(self, monitor):
        tweets = [
            {"tweet_id": "t1", "author": "angry", "text": "myapp is terrible broken horrible awful worst"},
        ]
        monitor.analyze_mentions(tweets)
        alerts = monitor.get_alerts(min_severity=1)
        assert len(alerts) >= 1

    def test_mark_responded(self, monitor):
        tweets = [{"tweet_id": "t1", "author": "a", "text": "myapp terrible awful broken worst"}]
        monitor.analyze_mentions(tweets)
        alerts = monitor.get_alerts(min_severity=1)
        if alerts:
            monitor.mark_responded(alerts[0]["id"])
            remaining = monitor.get_alerts(min_severity=1)
            assert len(remaining) < len(alerts)

    def test_suggest_response_bug(self, monitor):
        resp = monitor.suggest_response("myapp has a bug that crashes everything")
        assert "sorry" in resp.lower() or "issue" in resp.lower()

    def test_suggest_response_slow(self, monitor):
        resp = monitor.suggest_response("myapp is so slow and laggy")
        assert "performance" in resp.lower() or "feedback" in resp.lower()

    def test_suggest_response_scam(self, monitor):
        resp = monitor.suggest_response("myapp is a total scam")
        assert "concerns" in resp.lower() or "seriously" in resp.lower()

    def test_suggest_response_generic(self, monitor):
        resp = monitor.suggest_response("I don't like myapp")
        assert len(resp) > 10

    def test_mention_to_dict(self):
        m = Mention(
            tweet_id="t1", author="alice", text="test",
            sentiment="positive", sentiment_score=0.5,
            severity=0, detected_at=time.time(),
        )
        d = m.to_dict()
        assert d["tweet_id"] == "t1"

    def test_report_to_dict(self, monitor):
        tweets = [{"tweet_id": "t1", "author": "a", "text": "myapp great"}]
        monitor.analyze_mentions(tweets)
        report = monitor.get_report("7d")
        d = report.to_dict()
        assert "total_mentions" in d
        assert "trend" in d
