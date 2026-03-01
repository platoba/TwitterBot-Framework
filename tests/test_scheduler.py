"""
test_scheduler.py - 调度器策略测试
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

from bot.strategies.scheduler import SchedulerStrategy


class TestScheduleTweet:
    def test_schedule_tweet(self, scheduler):
        sid = scheduler.schedule_tweet("Hello!", "2026-03-01T10:00:00")
        assert sid > 0

    def test_schedule_at(self, scheduler):
        dt = datetime(2026, 3, 1, 10, 0, tzinfo=timezone.utc)
        sid = scheduler.schedule_at("Scheduled!", dt)
        assert sid > 0

    def test_schedule_in(self, scheduler):
        sid = scheduler.schedule_in("Delayed tweet", minutes=30)
        assert sid > 0

    def test_schedule_generated(self, scheduler):
        sid = scheduler.schedule_generated(
            "announcement",
            {"title": "Big News", "body": "Details here", "hashtags": "#news"},
            "2026-03-01T12:00:00"
        )
        assert sid > 0

    def test_schedule_generated_invalid_category(self, scheduler):
        with pytest.raises(ValueError):
            scheduler.schedule_generated("nonexistent", {}, "2026-03-01T12:00:00")


class TestABTest:
    def test_schedule_ab_test(self, scheduler):
        result = scheduler.schedule_ab_test(
            "Test Campaign",
            "announcement",
            {"title": "Product", "body": "Buy now", "hashtags": "#sale",
             "call_to_action": "Click"},
            "2026-03-01T10:00:00",
            delay_minutes=60
        )
        assert "test_id" in result
        assert "variant_a" in result
        assert "variant_b" in result
        assert result["schedule_a_id"] > 0
        assert result["schedule_b_id"] > 0


class TestQueue:
    def test_get_queue(self, scheduler):
        scheduler.schedule_tweet("t1", "2026-03-01T10:00:00")
        scheduler.schedule_tweet("t2", "2026-03-02T10:00:00")
        queue = scheduler.get_queue()
        assert len(queue) == 2

    def test_get_pending(self, scheduler):
        scheduler.schedule_tweet("past", "2020-01-01T00:00:00")
        scheduler.schedule_tweet("future", "2099-01-01T00:00:00")
        pending = scheduler.get_pending()
        assert len(pending) == 1
        assert pending[0]["content"] == "past"

    def test_cancel_tweet(self, scheduler):
        sid = scheduler.schedule_tweet("cancel me", "2026-03-01T10:00:00")
        assert scheduler.cancel_tweet(sid) is True
        queue = scheduler.get_queue("pending")
        assert len(queue) == 0


class TestSendTweet:
    def test_send_success(self, scheduler):
        sid = scheduler.schedule_tweet("test", "2020-01-01T00:00:00")
        pending = scheduler.get_pending()

        with patch.object(scheduler.api, "post_tweet", return_value={
            "data": {"id": "tw999"}
        }):
            result = scheduler.send_tweet(pending[0])
            assert result["status"] == "sent"
            assert result["tweet_id"] == "tw999"

    def test_send_failure(self, scheduler):
        sid = scheduler.schedule_tweet("test", "2020-01-01T00:00:00")
        pending = scheduler.get_pending()

        with patch.object(scheduler.api, "post_tweet", return_value=None):
            result = scheduler.send_tweet(pending[0])
            assert result["status"] == "failed"

    def test_process_pending(self, scheduler):
        scheduler.schedule_tweet("p1", "2020-01-01T00:00:00")
        scheduler.schedule_tweet("p2", "2020-01-02T00:00:00")

        with patch.object(scheduler.api, "post_tweet", return_value={
            "data": {"id": "tw100"}
        }):
            with patch("time.sleep"):  # skip sleep
                results = scheduler.process_pending()
                assert len(results) == 2
                assert all(r["status"] == "sent" for r in results)


class TestHooks:
    def test_pre_send_hook(self, scheduler):
        scheduler.set_pre_send_hook(lambda text: text.upper())
        sid = scheduler.schedule_tweet("hello", "2020-01-01T00:00:00")
        pending = scheduler.get_pending()

        with patch.object(scheduler.api, "post_tweet", return_value={
            "data": {"id": "tw1"}
        }) as mock_post:
            scheduler.send_tweet(pending[0])
            call_args = mock_post.call_args
            assert call_args[0][0] == "HELLO"

    def test_post_send_hook(self, scheduler):
        hook_called = []
        scheduler.set_post_send_hook(lambda item, result: hook_called.append(True))
        sid = scheduler.schedule_tweet("test", "2020-01-01T00:00:00")
        pending = scheduler.get_pending()

        with patch.object(scheduler.api, "post_tweet", return_value={
            "data": {"id": "tw1"}
        }):
            scheduler.send_tweet(pending[0])
            assert len(hook_called) == 1


class TestSchedulerRunning:
    def test_is_running_default(self, scheduler):
        assert scheduler.is_running is False

    def test_start_stop(self, scheduler):
        with patch.object(scheduler, "_run_loop"):
            scheduler.start()
            assert scheduler.is_running is True
            scheduler.stop()
            assert scheduler.is_running is False

    def test_start_idempotent(self, scheduler):
        with patch.object(scheduler, "_run_loop"):
            scheduler.start()
            scheduler.start()  # should not create second thread
            assert scheduler.is_running is True
            scheduler.stop()


class TestFormatQueue:
    def test_format_queue_empty(self, scheduler):
        result = scheduler.format_queue()
        assert "为空" in result

    def test_format_queue_with_items(self, scheduler):
        scheduler.schedule_tweet("tweet 1", "2026-03-01T10:00:00")
        scheduler.schedule_tweet("tweet 2", "2026-03-02T10:00:00")
        result = scheduler.format_queue()
        assert "调度队列" in result
        assert "tweet 1" in result
