"""
test_monitor.py - 监控策略测试
"""

import pytest
from unittest.mock import patch

from bot.strategies.monitor import MonitorStrategy


class TestMonitorManagement:
    def test_add_keyword_monitor(self, monitor):
        mid = monitor.add_keyword_monitor("python", "chat123")
        assert mid > 0

    def test_add_competitor_monitor(self, monitor):
        mid = monitor.add_competitor_monitor("elonmusk", "chat123")
        assert mid > 0

    def test_remove_monitor(self, monitor):
        monitor.add_keyword_monitor("python", "chat1")
        assert monitor.remove_monitor("python") is True

    def test_get_active_monitors(self, monitor):
        monitor.add_keyword_monitor("python", "chat1")
        monitor.add_competitor_monitor("elonmusk", "chat2")
        monitors = monitor.get_active_monitors()
        assert len(monitors) == 2

    def test_get_monitors_by_type(self, monitor):
        monitor.add_keyword_monitor("python", "chat1")
        monitor.add_competitor_monitor("elonmusk", "chat2")
        kw = monitor.get_active_monitors("keyword")
        assert len(kw) == 1


class TestCheckKeyword:
    def test_check_keyword_with_results(self, monitor, tmp_db):
        mid = monitor.add_keyword_monitor("python", "chat1")
        with patch.object(monitor.api, "search_recent", return_value={
            "data": [
                {"id": "100", "text": "Python is great", "author_id": "u1"}
            ],
            "includes": {"users": [{"id": "u1", "username": "pydev"}]}
        }):
            monitors = monitor.get_active_monitors("keyword")
            tweets = monitor.check_keyword(monitors[0])
            assert len(tweets) == 1
            assert tweets[0]["author_username"] == "pydev"

    def test_check_keyword_no_results(self, monitor, tmp_db):
        mid = monitor.add_keyword_monitor("obscure_term", "chat1")
        with patch.object(monitor.api, "search_recent", return_value=None):
            monitors = monitor.get_active_monitors("keyword")
            tweets = monitor.check_keyword(monitors[0])
            assert tweets == []


class TestCheckCompetitor:
    def test_check_competitor(self, monitor, tmp_db):
        mid = monitor.add_competitor_monitor("competitor", "chat1")
        with patch.object(monitor.api, "resolve_username", return_value="u99"):
            with patch.object(monitor.api, "get_user_tweets", return_value={
                "data": [
                    {"id": "200", "text": "Competitor tweet", "created_at": "2026-02-28T10:00:00Z"}
                ]
            }):
                monitors = monitor.get_active_monitors("competitor")
                tweets = monitor.check_competitor(monitors[0])
                assert len(tweets) == 1

    def test_check_competitor_user_not_found(self, monitor, tmp_db):
        mid = monitor.add_competitor_monitor("nobody", "chat1")
        with patch.object(monitor.api, "resolve_username", return_value=None):
            monitors = monitor.get_active_monitors("competitor")
            tweets = monitor.check_competitor(monitors[0])
            assert tweets == []


class TestCheckAll:
    def test_check_all_mixed(self, monitor, tmp_db):
        monitor.add_keyword_monitor("python", "chat1")
        monitor.add_competitor_monitor("competitor", "chat2")
        with patch.object(monitor, "check_keyword", return_value=[
            {"id": "1", "text": "test"}
        ]):
            with patch.object(monitor, "check_competitor", return_value=[]):
                results = monitor.check_all()
                assert "python" in results
                assert len(results["python"]) == 1

    def test_check_all_empty(self, monitor):
        results = monitor.check_all()
        assert results == {}


class TestCompareCompetitors:
    def test_compare(self, monitor):
        with patch.object(monitor.api, "get_user", side_effect=[
            {"data": {"id": "1", "username": "comp1", "description": "d1",
                       "public_metrics": {"followers_count": 5000, "following_count": 100, "tweet_count": 500}}},
            {"data": {"id": "2", "username": "comp2", "description": "d2",
                       "public_metrics": {"followers_count": 3000, "following_count": 200, "tweet_count": 300}}},
        ]):
            with patch.object(monitor.api, "get_user_tweets", return_value={
                "data": [
                    {"public_metrics": {"like_count": 10, "retweet_count": 5}}
                ]
            }):
                results = monitor.compare_competitors(["comp1", "comp2"])
                assert len(results) == 2
                assert results[0]["followers"] >= results[1]["followers"]

    def test_format_comparison(self, monitor):
        data = [
            {"username": "comp1", "followers": 5000, "tweets": 500, "avg_engagement": 15.0},
            {"username": "comp2", "followers": 3000, "tweets": 300, "avg_engagement": 10.0},
        ]
        result = monitor.format_comparison(data)
        assert "竞品对比" in result
        assert "@comp1" in result

    def test_format_comparison_empty(self, monitor):
        result = monitor.format_comparison([])
        assert "无竞品数据" in result


class TestMonitorSummary:
    def test_get_summary(self, monitor):
        monitor.add_keyword_monitor("python", "chat1")
        monitor.add_competitor_monitor("rival", "chat2")
        summary = monitor.get_monitor_summary()
        assert "监控列表" in summary
        assert "python" in summary
        assert "rival" in summary

    def test_get_summary_empty(self, monitor):
        summary = monitor.get_monitor_summary()
        assert "暂无" in summary
