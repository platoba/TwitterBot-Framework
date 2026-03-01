"""
test_database.py - 数据库层测试
"""

import json
from datetime import datetime, timezone, timedelta

import pytest


class TestTweetHistory:
    def test_save_tweet(self, tmp_db, sample_tweet):
        assert tmp_db.save_tweet(sample_tweet, "test query") is True

    def test_save_tweet_duplicate(self, tmp_db, sample_tweet):
        tmp_db.save_tweet(sample_tweet, "q1")
        assert tmp_db.save_tweet(sample_tweet, "q2") is True  # REPLACE

    def test_get_tweet_history(self, tmp_db, sample_tweets):
        for t in sample_tweets:
            tmp_db.save_tweet(t)
        history = tmp_db.get_tweet_history(limit=50)
        assert len(history) == 7

    def test_get_tweet_history_by_user(self, tmp_db, sample_tweets):
        for t in sample_tweets:
            tmp_db.save_tweet(t)
        history = tmp_db.get_tweet_history("testuser3", limit=50)
        assert len(history) == 1
        assert history[0]["author_username"] == "testuser3"

    def test_save_tweets_batch(self, tmp_db, sample_tweets):
        saved = tmp_db.save_tweets_batch(sample_tweets, "batch_query")
        assert saved == 7

    def test_get_top_tweets(self, tmp_db, sample_tweets):
        tmp_db.save_tweets_batch(sample_tweets)
        top = tmp_db.get_top_tweets(limit=3, metric="like_count")
        assert len(top) == 3
        assert top[0]["like_count"] >= top[1]["like_count"]

    def test_get_top_tweets_invalid_metric(self, tmp_db, sample_tweets):
        tmp_db.save_tweets_batch(sample_tweets)
        top = tmp_db.get_top_tweets(limit=3, metric="invalid_metric")
        assert len(top) == 3  # falls back to like_count


class TestAnalyticsSnapshots:
    def test_save_snapshot(self, tmp_db):
        assert tmp_db.save_analytics_snapshot("testuser", {
            "followers_count": 5000,
            "following_count": 300,
            "tweet_count": 1200,
            "listed_count": 50,
        }) is True

    def test_get_analytics_history(self, tmp_db):
        for i in range(5):
            tmp_db.save_analytics_snapshot("testuser", {
                "followers_count": 5000 + i * 10,
                "following_count": 300,
                "tweet_count": 1200 + i,
                "listed_count": 50,
            })
        history = tmp_db.get_analytics_history("testuser", limit=30)
        assert len(history) == 5

    def test_get_follower_growth(self, tmp_db):
        tmp_db.save_analytics_snapshot("testuser", {"followers_count": 5000})
        tmp_db.save_analytics_snapshot("testuser", {"followers_count": 5100})
        growth = tmp_db.get_follower_growth("testuser", days=7)
        assert growth is not None
        assert growth["growth"] == 100
        assert growth["current"] == 5100

    def test_get_follower_growth_insufficient(self, tmp_db):
        tmp_db.save_analytics_snapshot("testuser", {"followers_count": 5000})
        growth = tmp_db.get_follower_growth("testuser", days=7)
        assert growth is None


class TestScheduleQueue:
    def test_add_scheduled_tweet(self, tmp_db):
        sid = tmp_db.add_scheduled_tweet("Hello world!", "2026-03-01T10:00:00")
        assert sid > 0

    def test_get_pending_tweets(self, tmp_db):
        future = "2099-01-01T00:00:00"
        past = "2020-01-01T00:00:00"
        tmp_db.add_scheduled_tweet("future tweet", future)
        tmp_db.add_scheduled_tweet("past tweet", past)
        pending = tmp_db.get_pending_tweets()
        assert len(pending) == 1
        assert pending[0]["content"] == "past tweet"

    def test_update_schedule_status(self, tmp_db):
        sid = tmp_db.add_scheduled_tweet("test", "2020-01-01T00:00:00")
        tmp_db.update_schedule_status(sid, "sent", tweet_id="tw123")
        queue = tmp_db.get_schedule_queue("sent")
        assert len(queue) == 1
        assert queue[0]["tweet_id"] == "tw123"

    def test_get_schedule_queue_all(self, tmp_db):
        tmp_db.add_scheduled_tweet("t1", "2026-03-01T10:00:00")
        tmp_db.add_scheduled_tweet("t2", "2026-03-02T10:00:00")
        queue = tmp_db.get_schedule_queue()
        assert len(queue) == 2


class TestMonitors:
    def test_add_monitor(self, tmp_db):
        mid = tmp_db.add_monitor("python", "chat123", "keyword")
        assert mid > 0

    def test_get_active_monitors(self, tmp_db):
        tmp_db.add_monitor("python", "chat1", "keyword")
        tmp_db.add_monitor("elonmusk", "chat2", "competitor")
        monitors = tmp_db.get_active_monitors()
        assert len(monitors) == 2

    def test_get_active_monitors_by_type(self, tmp_db):
        tmp_db.add_monitor("python", "chat1", "keyword")
        tmp_db.add_monitor("elonmusk", "chat2", "competitor")
        kw = tmp_db.get_active_monitors("keyword")
        assert len(kw) == 1
        assert kw[0]["keyword"] == "python"

    def test_deactivate_monitor(self, tmp_db):
        tmp_db.add_monitor("python", "chat1")
        assert tmp_db.deactivate_monitor("python") is True
        assert len(tmp_db.get_active_monitors()) == 0

    def test_deactivate_nonexistent(self, tmp_db):
        assert tmp_db.deactivate_monitor("nonexistent") is False

    def test_update_monitor(self, tmp_db):
        mid = tmp_db.add_monitor("python", "chat1")
        tmp_db.update_monitor(mid, "tweet999")
        monitors = tmp_db.get_active_monitors()
        assert monitors[0]["last_tweet_id"] == "tweet999"


class TestEngagementLog:
    def test_log_engagement(self, tmp_db):
        tmp_db.log_engagement("like", "tw123", "testuser")
        stats = tmp_db.get_engagement_stats(7)
        assert stats.get("like") == 1

    def test_engagement_stats_multiple(self, tmp_db):
        tmp_db.log_engagement("like", "tw1", "user1")
        tmp_db.log_engagement("like", "tw2", "user2")
        tmp_db.log_engagement("reply", "tw3", "user3", "nice!")
        stats = tmp_db.get_engagement_stats(7)
        assert stats["like"] == 2
        assert stats["reply"] == 1


class TestABTests:
    def test_create_ab_test(self, tmp_db):
        tid = tmp_db.create_ab_test("Test 1", "variant A text", "variant B text")
        assert tid > 0

    def test_get_ab_tests(self, tmp_db):
        tmp_db.create_ab_test("T1", "A", "B")
        tmp_db.create_ab_test("T2", "C", "D")
        tests = tmp_db.get_ab_tests()
        assert len(tests) == 2

    def test_get_ab_tests_by_status(self, tmp_db):
        tid = tmp_db.create_ab_test("T1", "A", "B")
        tmp_db.update_ab_test(tid, status="completed", winner="A")
        pending = tmp_db.get_ab_tests("pending")
        assert len(pending) == 0
        completed = tmp_db.get_ab_tests("completed")
        assert len(completed) == 1
        assert completed[0]["winner"] == "A"

    def test_update_ab_test(self, tmp_db):
        tid = tmp_db.create_ab_test("T1", "A", "B")
        tmp_db.update_ab_test(tid,
                              variant_a_tweet_id="tw1",
                              variant_b_tweet_id="tw2",
                              status="running")
        tests = tmp_db.get_ab_tests("running")
        assert len(tests) == 1
        assert tests[0]["variant_a_tweet_id"] == "tw1"


class TestDatabaseClose:
    def test_close(self, tmp_db):
        tmp_db.close()
        # should not raise on double close
        tmp_db.close()
