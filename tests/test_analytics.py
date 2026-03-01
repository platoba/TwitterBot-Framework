"""
test_analytics.py - 分析策略引擎测试
"""

import pytest
from unittest.mock import patch, MagicMock

from bot.strategies.analytics import AnalyticsStrategy


class TestEngagementRate:
    def test_calculate_with_impressions(self, analytics, tmp_db, sample_tweets):
        tmp_db.save_tweets_batch(sample_tweets)
        result = analytics.calculate_engagement_rate("", sample_tweets)
        assert result["engagement_rate"] > 0
        assert result["total_tweets"] == 7
        assert "avg_likes" in result
        assert "avg_retweets" in result

    def test_calculate_empty(self, analytics):
        result = analytics.calculate_engagement_rate("nobody")
        assert result["engagement_rate"] == 0
        assert result["total_tweets"] == 0

    def test_calculate_no_impressions(self, analytics):
        tweets = [{"like_count": 10, "retweet_count": 5, "reply_count": 2,
                    "quote_count": 1, "impression_count": 0}]
        result = analytics.calculate_engagement_rate("user", tweets)
        assert result["total_tweets"] == 1


class TestFollowerGrowth:
    def test_get_growth(self, analytics, tmp_db):
        tmp_db.save_analytics_snapshot("testuser", {"followers_count": 1000})
        tmp_db.save_analytics_snapshot("testuser", {"followers_count": 1100})
        growth = analytics.get_follower_growth("testuser")
        assert growth is not None
        assert growth["growth"] == 100

    def test_track_user(self, analytics):
        with patch.object(analytics.api, "get_user", return_value={
            "data": {"public_metrics": {
                "followers_count": 5000,
                "following_count": 300,
                "tweet_count": 1200,
                "listed_count": 50,
            }}
        }):
            assert analytics.track_user("testuser") is True

    def test_track_user_fail(self, analytics):
        with patch.object(analytics.api, "get_user", return_value=None):
            assert analytics.track_user("nobody") is False


class TestBestPostingTimes:
    def test_with_tweets(self, analytics, sample_tweets):
        result = analytics.best_posting_times("", sample_tweets)
        assert result["total_analyzed"] == 7
        assert isinstance(result["best_hours"], list)
        assert isinstance(result["best_days"], list)

    def test_empty_tweets(self, analytics):
        result = analytics.best_posting_times("nobody")
        assert result["total_analyzed"] == 0
        assert result["best_hours"] == []


class TestTopTweets:
    def test_top_tweets(self, analytics, tmp_db, sample_tweets):
        tmp_db.save_tweets_batch(sample_tweets)
        top = analytics.top_tweets(limit=3)
        assert len(top) == 3


class TestContentAnalysis:
    def test_analyze_top_content(self, analytics, tmp_db, sample_tweets):
        tmp_db.save_tweets_batch(sample_tweets)
        result = analytics.analyze_top_content(limit=7)
        assert result["total_analyzed"] == 7
        assert "avg_length" in result
        assert "emoji_pct" in result
        assert isinstance(result["top_hashtags"], list)

    def test_analyze_empty(self, analytics):
        result = analytics.analyze_top_content("nobody")
        assert result["patterns"] == []
        assert result["avg_length"] == 0

    def test_identify_patterns(self, analytics):
        tweets = [
            {"text": "🧵 Thread about AI", "like_count": 100},
            {"text": "🧵 Another thread", "like_count": 80},
            {"text": "🧵 Thread 3", "like_count": 90},
            {"text": "Short tweet?", "like_count": 50},
        ]
        patterns = analytics._identify_patterns(tweets)
        assert isinstance(patterns, list)


class TestReport:
    def test_generate_report(self, analytics, tmp_db, sample_tweets):
        tmp_db.save_tweets_batch(sample_tweets)
        with patch.object(analytics.api, "get_user", return_value={
            "data": {"public_metrics": {
                "followers_count": 5000,
                "following_count": 300,
                "tweet_count": 1200,
                "listed_count": 50,
            }}
        }):
            report = analytics.generate_report("testuser")
            assert "分析报告" in report
            assert "互动率" in report

    def test_format_engagement_summary(self, analytics):
        stats = {"reply": 10, "like": 25, "retweet": 5}
        result = analytics.format_engagement_summary(stats)
        assert "reply" in result
        assert "10" in result

    def test_format_engagement_summary_empty(self, analytics):
        result = analytics.format_engagement_summary({})
        assert "暂无" in result
