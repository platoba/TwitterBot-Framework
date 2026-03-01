"""
test_twitter_api.py - Twitter API客户端测试
"""

import time
import pytest
from unittest.mock import patch, MagicMock

from bot.twitter_api import TwitterAPI, RateLimiter


class TestRateLimiter:
    def test_initial_state(self, rate_limiter):
        can, wait = rate_limiter.check("test")
        assert can is True
        assert wait == 0

    def test_update_and_check(self, rate_limiter):
        rate_limiter.update("/test", {
            "x-rate-limit-remaining": "5",
            "x-rate-limit-reset": str(int(time.time()) + 60),
            "x-rate-limit-limit": "15"
        })
        can, wait = rate_limiter.check("/test")
        assert can is True

    def test_rate_limited(self, rate_limiter):
        rate_limiter.update("/test", {
            "x-rate-limit-remaining": "0",
            "x-rate-limit-reset": str(int(time.time()) + 60),
            "x-rate-limit-limit": "15"
        })
        can, wait = rate_limiter.check("/test")
        assert can is False
        assert wait > 0

    def test_rate_limit_expired(self, rate_limiter):
        rate_limiter.update("/test", {
            "x-rate-limit-remaining": "0",
            "x-rate-limit-reset": str(int(time.time()) - 10),
            "x-rate-limit-limit": "15"
        })
        can, wait = rate_limiter.check("/test")
        assert can is True

    def test_get_status(self, rate_limiter):
        rate_limiter.update("/endpoint1", {
            "x-rate-limit-remaining": "10",
            "x-rate-limit-reset": str(int(time.time()) + 60),
            "x-rate-limit-limit": "15"
        })
        status = rate_limiter.get_status()
        assert "/endpoint1" in status
        assert status["/endpoint1"]["remaining"] == 10

    def test_wait_if_needed_no_wait(self, rate_limiter):
        waited = rate_limiter.wait_if_needed("/nonexistent")
        assert waited == 0


class TestTwitterAPIProperties:
    def test_is_configured(self, mock_api):
        assert mock_api.is_configured is True

    def test_not_configured(self):
        api = TwitterAPI(bearer_token="")
        assert api.is_configured is False

    def test_can_write_false(self, mock_api):
        assert mock_api.can_write is False

    def test_can_write_true(self):
        api = TwitterAPI(
            bearer_token="b",
            api_key="k", api_secret="s",
            access_token="t", access_secret="as"
        )
        assert api.can_write is True

    def test_bearer_headers(self, mock_api):
        headers = mock_api._bearer_headers()
        assert "Authorization" in headers
        assert headers["Authorization"].startswith("Bearer ")


class TestTwitterAPIRequests:
    @patch("bot.twitter_api.requests.Session")
    def test_get_success(self, mock_session_cls):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": []}
        mock_resp.headers = {}

        mock_session = MagicMock()
        mock_session.request.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        api = TwitterAPI(bearer_token="test")
        api._session = mock_session
        result = api.get("/test/endpoint", {"q": "hello"})
        assert result == {"data": []}

    @patch("bot.twitter_api.requests.Session")
    def test_get_error(self, mock_session_cls):
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 403
        mock_resp.text = "Forbidden"
        mock_resp.headers = {}

        mock_session = MagicMock()
        mock_session.request.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        api = TwitterAPI(bearer_token="test")
        api._session = mock_session
        result = api.get("/test/endpoint")
        assert result is not None
        assert result.get("error") == 403

    @patch("bot.twitter_api.requests.Session")
    def test_search_recent(self, mock_session_cls):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "data": [{"id": "1", "text": "test"}],
            "meta": {"result_count": 1}
        }
        mock_resp.headers = {}

        mock_session = MagicMock()
        mock_session.request.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        api = TwitterAPI(bearer_token="test")
        api._session = mock_session
        result = api.search_recent("python", max_results=10)
        assert result is not None
        assert "data" in result

    @patch("bot.twitter_api.requests.Session")
    def test_get_user(self, mock_session_cls):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "data": {"id": "123", "username": "testuser"}
        }
        mock_resp.headers = {}

        mock_session = MagicMock()
        mock_session.request.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        api = TwitterAPI(bearer_token="test")
        api._session = mock_session
        result = api.get_user("testuser")
        assert result["data"]["username"] == "testuser"

    def test_resolve_username_none(self, mock_api):
        with patch.object(mock_api, "get_user", return_value=None):
            assert mock_api.resolve_username("nobody") is None

    def test_resolve_username_success(self, mock_api):
        with patch.object(mock_api, "get_user",
                          return_value={"data": {"id": "123"}}):
            assert mock_api.resolve_username("user") == "123"

    def test_get_rate_limit_status(self, mock_api):
        status = mock_api.get_rate_limit_status()
        assert isinstance(status, dict)
