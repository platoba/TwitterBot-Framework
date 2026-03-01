"""
test_engagement.py - 自动互动策略测试
"""

import pytest
from unittest.mock import patch

from bot.strategies.engagement import EngagementStrategy, EngagementRule


class TestEngagementRule:
    def test_matches(self):
        rule = EngagementRule("test", r"python|ai", action="like")
        assert rule.matches("I love python!") is True
        assert rule.matches("Hello world") is False

    def test_matches_case_insensitive(self):
        rule = EngagementRule("test", r"Python")
        assert rule.matches("PYTHON is great") is True

    def test_follower_filter_pass(self):
        rule = EngagementRule("test", ".", min_followers=100, max_followers=10000)
        assert rule.check_follower_filter(5000) is True

    def test_follower_filter_too_low(self):
        rule = EngagementRule("test", ".", min_followers=100)
        assert rule.check_follower_filter(50) is False

    def test_follower_filter_too_high(self):
        rule = EngagementRule("test", ".", max_followers=1000)
        assert rule.check_follower_filter(5000) is False

    def test_follower_filter_no_limit(self):
        rule = EngagementRule("test", ".")
        assert rule.check_follower_filter(999999) is True

    def test_to_dict(self):
        rule = EngagementRule("test_rule", r"python", reply_template="Nice!",
                              action="reply", min_followers=100)
        d = rule.to_dict()
        assert d["name"] == "test_rule"
        assert d["action"] == "reply"
        assert d["min_followers"] == 100

    def test_disabled_rule(self):
        rule = EngagementRule("test", "python", enabled=False)
        assert rule.enabled is False


class TestEngagementStrategy:
    def test_add_rule(self, engagement):
        rule = EngagementRule("r1", "python", action="like")
        engagement.add_rule(rule)
        assert len(engagement.get_rules()) == 1

    def test_remove_rule(self, engagement):
        engagement.add_rule(EngagementRule("r1", "python"))
        engagement.add_rule(EngagementRule("r2", "javascript"))
        assert engagement.remove_rule("r1") is True
        assert len(engagement.get_rules()) == 1

    def test_remove_nonexistent(self, engagement):
        assert engagement.remove_rule("nope") is False

    def test_get_rules(self, engagement):
        engagement.add_rule(EngagementRule("r1", "python"))
        rules = engagement.get_rules()
        assert isinstance(rules, list)
        assert rules[0]["name"] == "r1"

    def test_process_tweet_no_rules(self, engagement, sample_tweet):
        results = engagement.process_tweet(sample_tweet)
        assert results == []

    def test_process_tweet_match(self, engagement, sample_tweet):
        rule = EngagementRule("py_rule", r"Python", reply_template="Nice!")
        engagement.add_rule(rule)
        results = engagement.process_tweet(sample_tweet)
        assert len(results) == 1
        assert results[0]["status"] == "dry_run"

    def test_process_tweet_no_match(self, engagement, sample_tweet):
        rule = EngagementRule("rust_rule", r"Rust")
        engagement.add_rule(rule)
        results = engagement.process_tweet(sample_tweet)
        assert results == []

    def test_process_tweet_disabled_rule(self, engagement, sample_tweet):
        rule = EngagementRule("disabled", r"Python", enabled=False)
        engagement.add_rule(rule)
        results = engagement.process_tweet(sample_tweet)
        assert results == []

    def test_process_tweet_follower_filter(self, engagement, sample_tweet):
        rule = EngagementRule("big_only", r"Python", min_followers=100000)
        engagement.add_rule(rule)
        author = {"public_metrics": {"followers_count": 500}}
        results = engagement.process_tweet(sample_tweet, author)
        assert results == []

    def test_format_reply(self, engagement):
        rule = EngagementRule("r1", ".", reply_template="Hey @{username}!")
        author = {"username": "testuser", "name": "Test"}
        tweet = {"text": "hello"}
        reply = engagement.format_reply(rule, tweet, author)
        assert "@testuser" in reply

    def test_custom_reply_formatter(self, engagement):
        def custom_fmt(tweet, author):
            return f"Custom reply to {author.get('username', '')}"

        rule = EngagementRule("custom", ".", reply_template="default")
        engagement.add_rule(rule)
        engagement.register_reply_formatter("custom", custom_fmt)
        reply = engagement.format_reply(rule, {}, {"username": "bob"})
        assert "Custom reply to bob" == reply

    def test_process_search_results_dry_run(self, engagement):
        engagement.add_rule(EngagementRule("py", "python", action="like"))
        with patch.object(engagement.api, "search_recent", return_value={
            "data": [
                {"id": "1", "text": "I love python", "author_id": "u1"}
            ],
            "includes": {"users": [{"id": "u1", "username": "testuser"}]}
        }):
            results = engagement.process_search_results("python")
            assert len(results) >= 1

    def test_get_engagement_stats(self, engagement, tmp_db):
        tmp_db.log_engagement("like", "tw1", "user1")
        tmp_db.log_engagement("reply", "tw2", "user2", "nice")
        stats = engagement.get_engagement_stats(7)
        assert "like" in stats
