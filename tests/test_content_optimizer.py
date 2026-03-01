"""Tests for bot/content_optimizer.py"""

import pytest
from bot.content_optimizer import ContentOptimizer, ContentInsight, TimeSlot, HashtagROI
from bot.database import Database
import tempfile, os, time


@pytest.fixture
def db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    d = Database(path)
    yield d
    os.unlink(path)


@pytest.fixture
def db_with_tweets(db):
    conn = db._get_conn()
    tweets = [
        ("t1", "user1", "user1", "Check out this 🧵 thread about Python tips #python #coding", 50, 20, 10, 5, 5000, "2026-02-28T10:00:00"),
        ("t2", "user1", "user1", "What is the best programming language? #dev", 30, 5, 25, 2, 3000, "2026-02-28T14:00:00"),
        ("t3", "user1", "user1", "Short tweet", 10, 2, 1, 0, 1000, "2026-02-27T09:00:00"),
        ("t4", "user1", "user1", "1) Learn Python\n2) Build projects\n3) Get hired\n#python #career #tips", 100, 50, 30, 10, 10000, "2026-02-26T18:00:00"),
        ("t5", "user1", "user1", "Unpopular opinion: JavaScript is better than Python for everything", 200, 80, 100, 30, 20000, "2026-02-25T12:00:00"),
        ("t6", "user1", "user1", "https://example.com check this article", 5, 1, 0, 0, 500, "2026-02-24T08:00:00"),
        ("t7", "user1", "user1", "Nice day today", 3, 0, 0, 0, 200, "2026-02-23T16:00:00"),
    ]
    for t in tweets:
        conn.execute(
            """INSERT INTO tweet_history
               (tweet_id, author_id, author_username, text, like_count, retweet_count,
                reply_count, quote_count, impression_count, created_at, collected_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
            t,
        )
    conn.commit()
    return db


@pytest.fixture
def optimizer(db_with_tweets):
    return ContentOptimizer(db_with_tweets)


class TestContentOptimizer:
    def test_engagement_score(self):
        tweet = {"like_count": 10, "retweet_count": 5, "reply_count": 3, "quote_count": 2}
        score = ContentOptimizer._engagement_score(tweet)
        assert score == 10 + 5*2 + 3*1.5 + 2*2.5  # 29.5

    def test_classify_thread(self):
        assert ContentOptimizer._classify_content("🧵 Let me explain") == "thread"

    def test_classify_question(self):
        assert ContentOptimizer._classify_content("What is AI?") == "question"

    def test_classify_link(self):
        assert ContentOptimizer._classify_content("Check https://x.com/post") == "link_share"

    def test_classify_opinion(self):
        assert ContentOptimizer._classify_content("Unpopular opinion: Python is easy") == "opinion"

    def test_classify_listicle(self):
        assert ContentOptimizer._classify_content("5 tips for better code") == "listicle"

    def test_classify_short(self):
        assert ContentOptimizer._classify_content("Nice") == "short"

    def test_classify_empty(self):
        assert ContentOptimizer._classify_content("") == "empty"

    def test_get_top_tweets(self, optimizer):
        top = optimizer.get_top_tweets(limit=3)
        assert len(top) == 3
        assert top[0]["engagement_score"] >= top[1]["engagement_score"]

    def test_content_types(self, optimizer):
        insights = optimizer.analyze_content_types()
        assert len(insights) > 0
        assert all(isinstance(i, ContentInsight) for i in insights)
        # Opinion tweet has highest engagement
        types = [i.content_type for i in insights]
        assert "opinion" in types

    def test_best_posting_times(self, optimizer):
        times = optimizer.find_best_posting_times()
        assert isinstance(times, list)
        if times:
            assert all(isinstance(t, TimeSlot) for t in times)
            assert times[0].avg_engagement >= times[-1].avg_engagement

    def test_hashtag_roi(self, optimizer):
        roi = optimizer.calculate_hashtag_roi()
        assert len(roi) > 0
        tags = [r.hashtag for r in roi]
        assert "#python" in tags

    def test_tweet_length(self, optimizer):
        analysis = optimizer.analyze_tweet_length()
        assert len(analysis) == 4
        assert all("bucket" in a for a in analysis)

    def test_suggestions(self, optimizer):
        suggestions = optimizer.get_suggestions()
        assert len(suggestions) > 0
        assert all(isinstance(s, str) for s in suggestions)

    def test_content_insight_to_dict(self):
        ci = ContentInsight(content_type="thread", avg_engagement=50.0, avg_likes=20.0,
                           avg_retweets=10.0, avg_replies=5.0, sample_count=10)
        d = ci.to_dict()
        assert d["content_type"] == "thread"

    def test_time_slot_label(self):
        ts = TimeSlot(hour=14, day_of_week=0, avg_engagement=50, tweet_count=5)
        assert ts.label == "Mon 14:00"

    def test_hashtag_roi_to_dict(self):
        hr = HashtagROI(hashtag="#ai", usage_count=5, avg_engagement=100,
                       avg_impressions=5000, engagement_per_use=100)
        d = hr.to_dict()
        assert d["hashtag"] == "#ai"
