"""
test_webhook.py - Telegram Webhook通知测试
"""

import pytest
from unittest.mock import patch, MagicMock

from bot.webhook import TelegramWebhook


class TestWebhookInit:
    def test_is_configured(self, webhook):
        assert webhook.is_configured is True

    def test_not_configured(self):
        wh = TelegramWebhook(bot_token="")
        assert wh.is_configured is False

    def test_api_url(self, webhook):
        assert "fake_bot_token" in webhook.api_url


class TestWebhookSendMessage:
    @patch("bot.webhook.requests.Session")
    def test_send_message_success(self, mock_cls):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"ok": True, "result": {"message_id": 1}}
        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp
        mock_session.post.return_value = mock_resp

        wh = TelegramWebhook(bot_token="fake", default_chat_id="123")
        wh._session = mock_session
        result = wh.send_message("123", "hello")
        assert result is not None

    @patch("bot.webhook.requests.Session")
    def test_send_message_with_reply(self, mock_cls):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"ok": True}
        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp

        wh = TelegramWebhook(bot_token="fake")
        wh._session = mock_session
        wh.send_message("123", "reply", reply_to=42)
        mock_session.get.assert_called()


class TestWebhookNotifications:
    @patch.object(TelegramWebhook, "send_message", return_value={"ok": True})
    def test_notify(self, mock_send, webhook):
        assert webhook.notify("test message") is True
        mock_send.assert_called_once()

    @patch.object(TelegramWebhook, "send_message", return_value={"ok": True})
    def test_notify_no_chat_id(self, mock_send):
        wh = TelegramWebhook(bot_token="fake", default_chat_id="")
        assert wh.notify("test") is False

    @patch.object(TelegramWebhook, "send_message", return_value={"ok": True})
    def test_notify_new_tweets(self, mock_send, webhook):
        tweets = [
            {"author_username": "user1", "text": "hello", "public_metrics": {"like_count": 5, "retweet_count": 2}},
            {"author_username": "user2", "text": "world", "public_metrics": {"like_count": 10, "retweet_count": 3}},
        ]
        assert webhook.notify_new_tweets("python", tweets) is True

    @patch.object(TelegramWebhook, "send_message", return_value={"ok": True})
    def test_notify_new_tweets_empty(self, mock_send, webhook):
        assert webhook.notify_new_tweets("python", []) is False

    @patch.object(TelegramWebhook, "send_message", return_value={"ok": True})
    def test_notify_engagement(self, mock_send, webhook):
        assert webhook.notify_engagement("like", "@user", "rule1") is True

    @patch.object(TelegramWebhook, "send_message", return_value={"ok": True})
    def test_notify_scheduled_tweet(self, mock_send, webhook):
        assert webhook.notify_scheduled_tweet("content", "sent", "tw123") is True

    @patch.object(TelegramWebhook, "send_message", return_value={"ok": True})
    def test_notify_analytics(self, mock_send, webhook):
        assert webhook.notify_analytics("testuser", "report text") is True

    @patch.object(TelegramWebhook, "send_message", return_value={"ok": True})
    def test_notify_alert_levels(self, mock_send, webhook):
        for level in ("info", "warning", "error", "success"):
            assert webhook.notify_alert("Title", "msg", level) is True

    @patch.object(TelegramWebhook, "send_message", return_value={"ok": True})
    def test_notify_ab_test_result(self, mock_send, webhook):
        assert webhook.notify_ab_test_result(
            "Test1", "A",
            {"likes": 100, "retweets": 20},
            {"likes": 80, "retweets": 15}
        ) is True
