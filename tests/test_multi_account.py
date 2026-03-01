"""Tests for Multi-Account Manager."""
import time
import pytest
from bot.multi_account import (
    TwitterAccount, AccountCredentials, AccountStatus, AccountRole,
    RateLimitState, AccountHealth, MultiAccountManager,
)


# ──── AccountCredentials ────

class TestAccountCredentials:
    def test_masked_hides_middle(self):
        creds = AccountCredentials(api_key="abcdefghijklmnop")
        masked = creds.masked()
        assert masked["api_key"].startswith("abcd")
        assert masked["api_key"].endswith("mnop")
        assert "..." in masked["api_key"]

    def test_masked_short_key(self):
        creds = AccountCredentials(api_key="short")
        assert creds.masked()["api_key"] == "***"

    def test_fingerprint_unique(self):
        c1 = AccountCredentials(api_key="key1", access_token="tok1")
        c2 = AccountCredentials(api_key="key2", access_token="tok2")
        assert c1.fingerprint() != c2.fingerprint()

    def test_fingerprint_deterministic(self):
        c = AccountCredentials(api_key="k", access_token="t")
        assert c.fingerprint() == c.fingerprint()


# ──── RateLimitState ────

class TestRateLimitState:
    def test_tweet_not_limited_initially(self):
        rl = RateLimitState()
        assert not rl.is_tweet_limited()

    def test_consume_tweet_decrements(self):
        rl = RateLimitState(tweets_remaining=5, tweets_reset_at=time.time() + 900)
        assert rl.consume_tweet()
        assert rl.tweets_remaining == 4

    def test_consume_tweet_when_exhausted(self):
        rl = RateLimitState(tweets_remaining=0, tweets_reset_at=time.time() + 900)
        assert not rl.consume_tweet()

    def test_tweet_reset_after_window(self):
        rl = RateLimitState(tweets_remaining=0, tweets_reset_at=time.time() - 1)
        assert not rl.is_tweet_limited()  # resets
        assert rl.tweets_remaining == 300

    def test_consume_dm(self):
        rl = RateLimitState(dm_remaining=10, dm_reset_at=time.time() + 86400)
        assert rl.consume_dm()
        assert rl.dm_remaining == 9

    def test_dm_limited_when_zero(self):
        rl = RateLimitState(dm_remaining=0, dm_reset_at=time.time() + 86400)
        assert not rl.consume_dm()

    def test_consume_search(self):
        rl = RateLimitState(search_remaining=10, search_reset_at=time.time() + 900)
        assert rl.consume_search()
        assert rl.search_remaining == 9

    def test_search_limited_when_zero(self):
        rl = RateLimitState(search_remaining=0, search_reset_at=time.time() + 900)
        assert not rl.consume_search()

    def test_consume_follow(self):
        rl = RateLimitState(follows_remaining=10, follows_reset_at=time.time() + 86400)
        assert rl.consume_follow()
        assert rl.follows_remaining == 9

    def test_follow_resets_after_window(self):
        rl = RateLimitState(follows_remaining=0, follows_reset_at=time.time() - 1)
        assert rl.consume_follow()
        assert rl.follows_remaining == 399


# ──── AccountHealth ────

class TestAccountHealth:
    def test_default_score(self):
        h = AccountHealth()
        score = h.score()
        assert 40 <= score <= 60

    def test_high_engagement_boosts(self):
        h = AccountHealth(engagement_rate=0.05, follower_growth_7d=50)
        assert h.score() > 60

    def test_violations_reduce_score(self):
        h = AccountHealth(violation_count=3)
        assert h.score() < 30

    def test_consecutive_errors_reduce(self):
        h = AccountHealth(consecutive_errors=5)
        assert h.score() <= 40

    def test_grade_A_plus(self):
        h = AccountHealth(engagement_rate=0.1, follower_growth_7d=100, uptime_pct=100)
        assert h.grade() in ("A+", "A")

    def test_grade_F(self):
        h = AccountHealth(violation_count=5, consecutive_errors=5)
        assert h.grade() in ("D", "F")

    def test_score_clamped_0_100(self):
        h = AccountHealth(violation_count=20, consecutive_errors=20)
        assert h.score() == 0
        h2 = AccountHealth(engagement_rate=1.0, follower_growth_7d=1000, uptime_pct=200)
        assert h2.score() <= 100


# ──── TwitterAccount ────

class TestTwitterAccount:
    def _make(self, **kw) -> TwitterAccount:
        defaults = {"account_id": "acc1", "username": "testuser"}
        defaults.update(kw)
        return TwitterAccount(**defaults)

    def test_is_available_when_active(self):
        a = self._make()
        assert a.is_available()

    def test_not_available_when_suspended(self):
        a = self._make(status=AccountStatus.SUSPENDED)
        assert not a.is_available()

    def test_not_available_during_cooldown(self):
        a = self._make()
        a.set_cooldown(60)
        assert not a.is_available()

    def test_not_available_when_daily_limit_reached(self):
        a = self._make(daily_tweet_count=50, daily_tweet_limit=50)
        assert not a.is_available()

    def test_can_tweet(self):
        a = self._make()
        assert a.can_tweet()

    def test_cannot_tweet_when_limited(self):
        a = self._make()
        a.rate_limits = RateLimitState(tweets_remaining=0, tweets_reset_at=time.time() + 900)
        assert not a.can_tweet()

    def test_record_tweet(self):
        a = self._make()
        a.record_tweet()
        assert a.daily_tweet_count == 1

    def test_record_error_triggers_cooldown(self):
        a = self._make()
        for _ in range(5):
            a.record_error()
        assert a.status == AccountStatus.COOLDOWN

    def test_reset_errors(self):
        a = self._make()
        a.health.consecutive_errors = 3
        a.reset_errors()
        assert a.health.consecutive_errors == 0

    def test_summary_contains_fields(self):
        a = self._make()
        s = a.summary()
        assert "username" in s
        assert s["username"] == "@testuser"
        assert "health_score" in s
        assert "available" in s


# ──── MultiAccountManager ────

class TestMultiAccountManager:
    def _mgr(self) -> MultiAccountManager:
        return MultiAccountManager(db_path=":memory:")

    def _acc(self, aid="a1", username="user1", role=AccountRole.MAIN) -> TwitterAccount:
        return TwitterAccount(account_id=aid, username=username, role=role)

    def test_register(self):
        mgr = self._mgr()
        assert mgr.register(self._acc())

    def test_register_duplicate_fails(self):
        mgr = self._mgr()
        mgr.register(self._acc())
        assert not mgr.register(self._acc())

    def test_first_registered_becomes_active(self):
        mgr = self._mgr()
        mgr.register(self._acc("a1"))
        assert mgr.active_account_id == "a1"

    def test_unregister(self):
        mgr = self._mgr()
        mgr.register(self._acc("a1"))
        assert mgr.unregister("a1")
        assert mgr.get("a1") is None

    def test_unregister_nonexistent(self):
        mgr = self._mgr()
        assert not mgr.unregister("nope")

    def test_unregister_active_switches(self):
        mgr = self._mgr()
        mgr.register(self._acc("a1"))
        mgr.register(self._acc("a2", "user2"))
        mgr.unregister("a1")
        assert mgr.active_account_id == "a2"

    def test_get(self):
        mgr = self._mgr()
        mgr.register(self._acc("a1"))
        assert mgr.get("a1").username == "user1"

    def test_get_active(self):
        mgr = self._mgr()
        mgr.register(self._acc("a1"))
        assert mgr.get_active().account_id == "a1"

    def test_switch_to(self):
        mgr = self._mgr()
        mgr.register(self._acc("a1"))
        mgr.register(self._acc("a2", "user2"))
        assert mgr.switch_to("a2")
        assert mgr.active_account_id == "a2"

    def test_switch_to_nonexistent(self):
        mgr = self._mgr()
        assert not mgr.switch_to("nope")

    def test_get_best_for_tweet(self):
        mgr = self._mgr()
        a1 = self._acc("a1")
        a1.health.engagement_rate = 0.05
        a2 = self._acc("a2", "user2", AccountRole.BACKUP)
        mgr.register(a1)
        mgr.register(a2)
        best = mgr.get_best_for_tweet()
        assert best is not None
        assert best.account_id == "a1"  # MAIN role wins

    def test_get_best_for_tweet_none_available(self):
        mgr = self._mgr()
        a = self._acc("a1")
        a.status = AccountStatus.SUSPENDED
        mgr.register(a)
        assert mgr.get_best_for_tweet() is None

    def test_get_best_for_dm(self):
        mgr = self._mgr()
        mgr.register(self._acc("a1"))
        assert mgr.get_best_for_dm() is not None

    def test_get_best_for_search(self):
        mgr = self._mgr()
        mgr.register(self._acc("a1"))
        assert mgr.get_best_for_search() is not None

    def test_rotate(self):
        mgr = self._mgr()
        mgr.register(self._acc("a1"))
        mgr.register(self._acc("a2", "user2"))
        first = mgr.rotate()
        second = mgr.rotate()
        assert first.account_id != second.account_id

    def test_rotate_empty(self):
        mgr = self._mgr()
        assert mgr.rotate() is None

    def test_failover(self):
        mgr = self._mgr()
        mgr.register(self._acc("a1"))
        mgr.register(self._acc("a2", "user2"))
        result = mgr.failover("a1", "rate limited")
        assert result is not None
        assert result.account_id == "a2"

    def test_failover_records_error(self):
        mgr = self._mgr()
        a = self._acc("a1")
        mgr.register(a)
        mgr.register(self._acc("a2", "user2"))
        mgr.failover("a1")
        assert a.health.consecutive_errors == 1

    def test_failover_no_alternatives(self):
        mgr = self._mgr()
        a = self._acc("a1")
        mgr.register(a)
        assert mgr.failover("a1") is None

    def test_list_by_role(self):
        mgr = self._mgr()
        mgr.register(self._acc("a1", role=AccountRole.MAIN))
        mgr.register(self._acc("a2", "u2", role=AccountRole.BACKUP))
        assert len(mgr.list_by_role(AccountRole.MAIN)) == 1

    def test_list_by_status(self):
        mgr = self._mgr()
        a = self._acc("a1")
        mgr.register(a)
        assert len(mgr.list_by_status(AccountStatus.ACTIVE)) == 1

    def test_list_by_tag(self):
        mgr = self._mgr()
        a = self._acc("a1")
        a.tags = ["crypto", "tech"]
        mgr.register(a)
        assert len(mgr.list_by_tag("crypto")) == 1
        assert len(mgr.list_by_tag("sports")) == 0

    def test_pool_summary(self):
        mgr = self._mgr()
        mgr.register(self._acc("a1"))
        mgr.register(self._acc("a2", "u2"))
        summary = mgr.get_pool_summary()
        assert summary["total_accounts"] == 2
        assert summary["available"] == 2

    def test_aggregated_analytics(self):
        mgr = self._mgr()
        a1 = self._acc("a1")
        a1.health.engagement_rate = 0.03
        a2 = self._acc("a2", "u2")
        a2.health.engagement_rate = 0.05
        mgr.register(a1)
        mgr.register(a2)
        analytics = mgr.get_aggregated_analytics()
        assert analytics["accounts"] == 2
        assert analytics["avg_engagement_rate"] == 0.04

    def test_aggregated_analytics_empty(self):
        mgr = self._mgr()
        assert mgr.get_aggregated_analytics() == {"accounts": 0}

    def test_reset_daily_counts(self):
        mgr = self._mgr()
        a = self._acc("a1")
        a.daily_tweet_count = 50
        mgr.register(a)
        mgr.reset_daily_counts()
        assert a.daily_tweet_count == 0

    def test_reset_daily_reactivates_cooldown(self):
        mgr = self._mgr()
        a = self._acc("a1")
        a.status = AccountStatus.COOLDOWN
        a.cooldown_until = time.time() - 1  # expired
        mgr.register(a)
        mgr.reset_daily_counts()
        assert a.status == AccountStatus.ACTIVE

    def test_bulk_set_status(self):
        mgr = self._mgr()
        mgr.register(self._acc("a1"))
        mgr.register(self._acc("a2", "u2"))
        mgr.bulk_set_status(["a1", "a2"], AccountStatus.DISABLED)
        assert mgr.get("a1").status == AccountStatus.DISABLED
        assert mgr.get("a2").status == AccountStatus.DISABLED

    def test_export_accounts(self):
        mgr = self._mgr()
        mgr.register(self._acc("a1"))
        export = mgr.export_accounts()
        assert len(export) == 1
        assert "credentials_fingerprint" in export[0]

    def test_event_log(self):
        mgr = self._mgr()
        mgr.register(self._acc("a1"))
        events = mgr.get_event_log()
        assert len(events) >= 1
        assert events[0]["event_type"] == "registered"

    def test_event_log_filtered(self):
        mgr = self._mgr()
        mgr.register(self._acc("a1"))
        mgr.register(self._acc("a2", "u2"))
        events = mgr.get_event_log(account_id="a1")
        assert len(events) >= 1
        assert all(e["account_id"] == "a1" for e in events)
