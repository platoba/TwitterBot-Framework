"""
tests/test_rate_limit_guardian.py - API限流守卫测试
"""

import os
import time
import pytest
from bot.rate_limit_guardian import (
    RateLimitGuardian, SlidingWindow, TokenBucket, CircuitBreaker,
    BreakerState, Priority, TWITTER_V2_LIMITS, TWITTER_DAILY_LIMITS,
)

TEST_DB = "/tmp/test_rate_limit_guardian.db"


@pytest.fixture(autouse=True)
def cleanup():
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


@pytest.fixture
def guardian():
    return RateLimitGuardian(db_path=TEST_DB, use_twitter_defaults=False)


@pytest.fixture
def twitter_guardian():
    return RateLimitGuardian(db_path=TEST_DB, use_twitter_defaults=True)


# ── SlidingWindow Tests ─────────────────────────────

class TestSlidingWindow:
    def test_create(self):
        w = SlidingWindow(window_seconds=60, max_requests=10)
        assert w.remaining() == 10

    def test_record_within_limit(self):
        w = SlidingWindow(window_seconds=60, max_requests=5)
        for _ in range(5):
            assert w.record()
        assert w.remaining() == 0

    def test_record_over_limit(self):
        w = SlidingWindow(window_seconds=60, max_requests=3)
        for _ in range(3):
            w.record()
        assert not w.record()

    def test_remaining(self):
        w = SlidingWindow(window_seconds=60, max_requests=10)
        w.record()
        w.record()
        assert w.remaining() == 8

    def test_usage_percent(self):
        w = SlidingWindow(window_seconds=60, max_requests=10)
        for _ in range(5):
            w.record()
        assert w.usage_percent() == 50.0

    def test_usage_percent_zero_max(self):
        w = SlidingWindow(window_seconds=60, max_requests=0)
        assert w.usage_percent() == 0.0

    def test_reset_after(self):
        w = SlidingWindow(window_seconds=60, max_requests=10)
        assert w.reset_after() == 0.0
        w.record()
        assert w.reset_after() > 0

    def test_window_expiry(self):
        w = SlidingWindow(window_seconds=1, max_requests=2)
        w.record()
        w.record()
        assert w.remaining() == 0
        time.sleep(1.1)
        assert w.remaining() == 2


# ── TokenBucket Tests ────────────────────────────────

class TestTokenBucket:
    def test_create(self):
        b = TokenBucket(capacity=10, refill_rate=1.0)
        assert b.available() == 10

    def test_consume(self):
        b = TokenBucket(capacity=10, refill_rate=1.0, tokens=10.0)
        assert b.consume(5)
        assert b.available() >= 5

    def test_consume_over_limit(self):
        b = TokenBucket(capacity=5, refill_rate=0.1, tokens=3.0)
        assert not b.consume(5)

    def test_refill(self):
        b = TokenBucket(capacity=10, refill_rate=100.0, tokens=0.0)
        time.sleep(0.05)
        assert b.available() > 0

    def test_wait_time_available(self):
        b = TokenBucket(capacity=10, refill_rate=1.0, tokens=10.0)
        assert b.wait_time(1) == 0.0

    def test_wait_time_needed(self):
        b = TokenBucket(capacity=10, refill_rate=1.0, tokens=0.0)
        assert b.wait_time(5) > 0

    def test_wait_time_zero_rate(self):
        b = TokenBucket(capacity=10, refill_rate=0.0, tokens=0.0)
        assert b.wait_time(1) == float("inf")

    def test_capacity_limit(self):
        b = TokenBucket(capacity=5, refill_rate=100.0, tokens=5.0)
        time.sleep(0.1)
        assert b.available() <= 5


# ── CircuitBreaker Tests ─────────────────────────────

class TestCircuitBreaker:
    def test_initial_state(self):
        cb = CircuitBreaker()
        assert cb.state == BreakerState.CLOSED
        assert cb.can_proceed()

    def test_trip_open(self):
        cb = CircuitBreaker(failure_threshold=3)
        for _ in range(3):
            cb.record_failure()
        assert cb.state == BreakerState.OPEN
        assert not cb.can_proceed()

    def test_half_open_after_timeout(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == BreakerState.OPEN
        time.sleep(0.15)
        assert cb.can_proceed()
        assert cb.state == BreakerState.HALF_OPEN

    def test_close_after_success(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1,
                            half_open_max_calls=2)
        cb.record_failure()
        cb.record_failure()
        time.sleep(0.15)
        cb.can_proceed()  # Transition to half_open
        cb.record_success()
        cb.record_success()
        assert cb.state == BreakerState.CLOSED

    def test_reopen_on_failure(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1)
        cb.record_failure()
        cb.record_failure()
        time.sleep(0.15)
        cb.can_proceed()  # half_open
        cb.record_failure()
        assert cb.state == BreakerState.OPEN

    def test_reset(self):
        cb = CircuitBreaker(failure_threshold=2)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == BreakerState.OPEN
        cb.reset()
        assert cb.state == BreakerState.CLOSED
        assert cb.failure_count == 0

    def test_success_reduces_count(self):
        cb = CircuitBreaker(failure_threshold=5)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        assert cb.failure_count == 1

    def test_half_open_limit(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1,
                            half_open_max_calls=2)
        cb.record_failure()
        cb.record_failure()
        time.sleep(0.15)
        assert cb.can_proceed()  # half_open, call 0
        assert cb.can_proceed()  # call 1 (< 2)
        # After 2 calls in half_open
        cb.half_open_calls = 3
        assert not cb.can_proceed()


# ── RateLimitGuardian Core Tests ─────────────────────

class TestGuardianCore:
    def test_configure_endpoint(self, guardian):
        result = guardian.configure_endpoint("GET /test", max_requests=10,
                                              window_seconds=60)
        assert result

    def test_check_allowed(self, guardian):
        guardian.configure_endpoint("GET /check", max_requests=10)
        result = guardian.check("GET /check")
        assert result["allowed"]

    def test_check_unknown_endpoint(self, guardian):
        result = guardian.check("GET /unknown")
        assert result["allowed"]  # No config = no limit

    def test_acquire_success(self, guardian):
        guardian.configure_endpoint("GET /acquire", max_requests=5)
        result = guardian.acquire("GET /acquire")
        assert result["allowed"]

    def test_acquire_until_limit(self, guardian):
        guardian.configure_endpoint("GET /limited", max_requests=3, window_seconds=60)
        for _ in range(3):
            r = guardian.acquire("GET /limited")
            assert r["allowed"]
        r = guardian.acquire("GET /limited")
        assert not r["allowed"]

    def test_release_success(self, guardian):
        guardian.configure_endpoint("GET /release", max_requests=10)
        guardian.acquire("GET /release")
        guardian.release("GET /release", success=True)
        # Should not error

    def test_release_failure(self, guardian):
        guardian.configure_endpoint("GET /fail", max_requests=10,
                                    breaker_threshold=2)
        guardian.acquire("GET /fail")
        guardian.release("GET /fail", success=False)
        guardian.release("GET /fail", success=False)
        # Breaker should be open
        result = guardian.check("GET /fail")
        assert not result["allowed"]

    def test_release_unknown(self, guardian):
        # Should not error
        guardian.release("GET /unknown", success=True)

    def test_handle_429(self, guardian):
        guardian.configure_endpoint("GET /429", max_requests=10)
        result = guardian.handle_429("GET /429", retry_after=30)
        assert result["wait_seconds"] == 30

    def test_handle_429_default_retry(self, guardian):
        guardian.configure_endpoint("GET /429d", max_requests=10)
        result = guardian.handle_429("GET /429d")
        assert result["wait_seconds"] == 60.0


# ── Twitter Defaults Tests ───────────────────────────

class TestTwitterDefaults:
    def test_twitter_limits_loaded(self, twitter_guardian):
        config = twitter_guardian.get_endpoint_config("GET /2/tweets")
        assert config is not None
        assert config["max_requests"] == 300

    def test_tweet_limit(self, twitter_guardian):
        config = twitter_guardian.get_endpoint_config("POST /2/tweets")
        assert config is not None
        assert config["max_requests"] == 200

    def test_search_limit(self, twitter_guardian):
        config = twitter_guardian.get_endpoint_config("GET /2/tweets/search/recent")
        assert config is not None
        assert config["max_requests"] == 180

    def test_follower_limit(self, twitter_guardian):
        config = twitter_guardian.get_endpoint_config("GET /2/users/:id/followers")
        assert config is not None
        assert config["max_requests"] == 15

    def test_dm_limit(self, twitter_guardian):
        ep = "POST /2/dm_conversations/with/:participant_id/messages"
        config = twitter_guardian.get_endpoint_config(ep)
        assert config is not None
        assert config["max_requests"] == 200


# ── Endpoint Config Tests ────────────────────────────

class TestEndpointConfig:
    def test_get_config(self, guardian):
        guardian.configure_endpoint("GET /config", max_requests=50,
                                    window_seconds=300)
        config = guardian.get_endpoint_config("GET /config")
        assert config is not None
        assert config["max_requests"] == 50
        assert config["window_seconds"] == 300

    def test_get_nonexistent_config(self, guardian):
        assert guardian.get_endpoint_config("GET /nonexistent") is None

    def test_usage_tracking(self, guardian):
        guardian.configure_endpoint("GET /usage", max_requests=10)
        for _ in range(5):
            guardian.acquire("GET /usage")
        config = guardian.get_endpoint_config("GET /usage")
        assert config["usage_percent"] == 50.0
        assert config["remaining"] == 5


# ── Daily Usage Tests ────────────────────────────────

class TestDailyUsage:
    def test_get_daily_usage(self, guardian):
        usage = guardian.get_daily_usage()
        assert "date" in usage
        assert "actions" in usage

    def test_daily_usage_date(self, guardian):
        usage = guardian.get_daily_usage("2026-01-01")
        assert usage["date"] == "2026-01-01"


# ── Stats & Reports Tests ───────────────────────────

class TestGuardianStats:
    def test_stats_empty(self, guardian):
        stats = guardian.stats()
        assert stats["total_allowed"] == 0
        assert stats["total_denied"] == 0

    def test_stats_with_usage(self, guardian):
        guardian.configure_endpoint("GET /stats", max_requests=3)
        for _ in range(3):
            guardian.acquire("GET /stats")
        guardian.acquire("GET /stats")  # denied
        stats = guardian.stats()
        assert stats["total_allowed"] == 3
        assert stats["total_denied"] == 1

    def test_health_check_healthy(self, guardian):
        guardian.configure_endpoint("GET /health", max_requests=100)
        health = guardian.health_check()
        assert health["healthy"]
        assert len(health["issues"]) == 0

    def test_health_check_breaker_open(self, guardian):
        guardian.configure_endpoint("GET /broken", max_requests=100,
                                    breaker_threshold=2)
        guardian.release("GET /broken", success=False)
        guardian.release("GET /broken", success=False)
        health = guardian.health_check()
        assert not health["healthy"]
        assert len(health["issues"]) > 0

    def test_health_check_high_usage(self, guardian):
        guardian.configure_endpoint("GET /busy", max_requests=10)
        for _ in range(10):
            guardian.acquire("GET /busy")
        health = guardian.health_check()
        # Should have a warning about high usage
        assert len(health["warnings"]) > 0

    def test_report_text(self, guardian):
        guardian.configure_endpoint("GET /report", max_requests=5)
        guardian.acquire("GET /report")
        report = guardian.report(format="text")
        assert "Rate Limit Guardian Report" in report

    def test_report_json(self, guardian):
        guardian.configure_endpoint("GET /json", max_requests=5)
        report = guardian.report(format="json")
        import json
        data = json.loads(report)
        assert "stats" in data
        assert "health" in data


# ── Reset Tests ──────────────────────────────────────

class TestGuardianReset:
    def test_reset_endpoint(self, guardian):
        guardian.configure_endpoint("GET /reset", max_requests=3)
        for _ in range(3):
            guardian.acquire("GET /reset")
        assert not guardian.acquire("GET /reset")["allowed"]
        guardian.reset_endpoint("GET /reset")
        assert guardian.acquire("GET /reset")["allowed"]

    def test_reset_nonexistent(self, guardian):
        assert not guardian.reset_endpoint("GET /nonexistent")

    def test_reset_all(self, guardian):
        guardian.configure_endpoint("GET /a", max_requests=2)
        guardian.configure_endpoint("GET /b", max_requests=2)
        guardian.acquire("GET /a")
        guardian.acquire("GET /a")
        guardian.acquire("GET /b")
        guardian.acquire("GET /b")
        guardian.reset_all()
        assert guardian.acquire("GET /a")["allowed"]
        assert guardian.acquire("GET /b")["allowed"]


# ── Logging Tests ────────────────────────────────────

class TestGuardianLogging:
    def test_log_on_acquire(self, guardian):
        guardian.configure_endpoint("GET /logged", max_requests=5)
        guardian.acquire("GET /logged")
        logs = guardian.get_log(endpoint="GET /logged")
        assert len(logs) >= 1

    def test_log_denied(self, guardian):
        guardian.configure_endpoint("GET /deny_log", max_requests=1)
        guardian.acquire("GET /deny_log")
        guardian.acquire("GET /deny_log")  # denied
        logs = guardian.get_log(endpoint="GET /deny_log", result="denied")
        assert len(logs) >= 1

    def test_log_limit(self, guardian):
        guardian.configure_endpoint("GET /limit_log", max_requests=10)
        for _ in range(5):
            guardian.acquire("GET /limit_log")
        logs = guardian.get_log(limit=3)
        assert len(logs) <= 3


# ── Priority Tests ───────────────────────────────────

class TestPriority:
    def test_priority_values(self):
        assert Priority.CRITICAL < Priority.HIGH
        assert Priority.HIGH < Priority.NORMAL
        assert Priority.NORMAL < Priority.LOW
        assert Priority.LOW < Priority.BACKGROUND

    def test_acquire_with_priority(self, guardian):
        guardian.configure_endpoint("GET /priority", max_requests=5)
        result = guardian.acquire("GET /priority", priority=Priority.HIGH)
        assert result["allowed"]
