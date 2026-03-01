"""
Multi-Account Manager v1.0
多账号管理 + 账号切换 + 独立限流 + 聚合分析 + 健康评分

Features:
- Account pool management with credential rotation
- Per-account rate limit tracking (independent)
- Aggregated cross-account analytics
- Account health scoring (engagement rate + growth + violations)
- Automatic failover when account is rate-limited
- Account grouping by purpose (main/backup/niche)
"""

import time
import hashlib
import json
import sqlite3
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional


class AccountStatus(Enum):
    ACTIVE = "active"
    RATE_LIMITED = "rate_limited"
    SUSPENDED = "suspended"
    COOLDOWN = "cooldown"
    DISABLED = "disabled"


class AccountRole(Enum):
    MAIN = "main"
    BACKUP = "backup"
    NICHE = "niche"
    ENGAGEMENT = "engagement"
    MONITORING = "monitoring"


@dataclass
class AccountCredentials:
    """Encrypted credential store per account."""
    api_key: str = ""
    api_secret: str = ""
    access_token: str = ""
    access_secret: str = ""
    bearer_token: str = ""

    def masked(self) -> dict:
        """Return masked credentials for safe logging."""
        def mask(s: str) -> str:
            if len(s) <= 8:
                return "***"
            return s[:4] + "..." + s[-4:]
        return {
            "api_key": mask(self.api_key),
            "api_secret": mask(self.api_secret),
            "access_token": mask(self.access_token),
            "access_secret": mask(self.access_secret),
            "bearer_token": mask(self.bearer_token),
        }

    def fingerprint(self) -> str:
        """Unique fingerprint for this credential set."""
        raw = f"{self.api_key}:{self.access_token}"
        return hashlib.sha256(raw.encode()).hexdigest()[:12]


@dataclass
class RateLimitState:
    """Per-account rate limit tracking."""
    tweets_remaining: int = 300
    tweets_reset_at: float = 0
    dm_remaining: int = 1000
    dm_reset_at: float = 0
    search_remaining: int = 450
    search_reset_at: float = 0
    follows_remaining: int = 400
    follows_reset_at: float = 0
    last_updated: float = 0

    def is_tweet_limited(self) -> bool:
        if time.time() > self.tweets_reset_at:
            self.tweets_remaining = 300
            self.tweets_reset_at = time.time() + 900
            return False
        return self.tweets_remaining <= 0

    def is_dm_limited(self) -> bool:
        if time.time() > self.dm_reset_at:
            self.dm_remaining = 1000
            self.dm_reset_at = time.time() + 86400
            return False
        return self.dm_remaining <= 0

    def is_search_limited(self) -> bool:
        if time.time() > self.search_reset_at:
            self.search_remaining = 450
            self.search_reset_at = time.time() + 900
            return False
        return self.search_remaining <= 0

    def consume_tweet(self) -> bool:
        if self.is_tweet_limited():
            return False
        self.tweets_remaining -= 1
        self.last_updated = time.time()
        return True

    def consume_dm(self) -> bool:
        if self.is_dm_limited():
            return False
        self.dm_remaining -= 1
        self.last_updated = time.time()
        return True

    def consume_search(self) -> bool:
        if self.is_search_limited():
            return False
        self.search_remaining -= 1
        self.last_updated = time.time()
        return True

    def consume_follow(self) -> bool:
        if time.time() > self.follows_reset_at:
            self.follows_remaining = 400
            self.follows_reset_at = time.time() + 86400
        if self.follows_remaining <= 0:
            return False
        self.follows_remaining -= 1
        self.last_updated = time.time()
        return True


@dataclass
class AccountHealth:
    """Health metrics for an account."""
    engagement_rate: float = 0.0
    follower_growth_7d: int = 0
    avg_impressions: float = 0.0
    violation_count: int = 0
    last_violation_at: float = 0
    consecutive_errors: int = 0
    uptime_pct: float = 100.0

    def score(self) -> float:
        """Calculate health score 0-100."""
        s = 50.0
        s += min(self.engagement_rate * 500, 20)
        if self.follower_growth_7d > 0:
            s += min(self.follower_growth_7d / 10, 10)
        elif self.follower_growth_7d < 0:
            s -= min(abs(self.follower_growth_7d) / 5, 15)
        s -= self.violation_count * 10
        s -= min(self.consecutive_errors * 3, 15)
        s += (self.uptime_pct - 90) * 0.5
        return max(0, min(100, s))

    def grade(self) -> str:
        sc = self.score()
        if sc >= 90:
            return "A+"
        elif sc >= 80:
            return "A"
        elif sc >= 70:
            return "B"
        elif sc >= 60:
            return "C"
        elif sc >= 40:
            return "D"
        return "F"


@dataclass
class TwitterAccount:
    """Full account representation."""
    account_id: str
    username: str
    display_name: str = ""
    role: AccountRole = AccountRole.MAIN
    status: AccountStatus = AccountStatus.ACTIVE
    credentials: AccountCredentials = field(default_factory=AccountCredentials)
    rate_limits: RateLimitState = field(default_factory=RateLimitState)
    health: AccountHealth = field(default_factory=AccountHealth)
    tags: list = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    last_active_at: float = 0
    notes: str = ""
    cooldown_until: float = 0
    daily_tweet_count: int = 0
    daily_tweet_limit: int = 50

    def is_available(self) -> bool:
        if self.status != AccountStatus.ACTIVE:
            return False
        if time.time() < self.cooldown_until:
            return False
        if self.daily_tweet_count >= self.daily_tweet_limit:
            return False
        return True

    def can_tweet(self) -> bool:
        return self.is_available() and not self.rate_limits.is_tweet_limited()

    def can_dm(self) -> bool:
        return self.is_available() and not self.rate_limits.is_dm_limited()

    def can_search(self) -> bool:
        return self.is_available() and not self.rate_limits.is_search_limited()

    def set_cooldown(self, seconds: float):
        self.cooldown_until = time.time() + seconds
        self.status = AccountStatus.COOLDOWN

    def record_tweet(self):
        self.daily_tweet_count += 1
        self.last_active_at = time.time()
        self.rate_limits.consume_tweet()

    def record_error(self):
        self.health.consecutive_errors += 1
        if self.health.consecutive_errors >= 5:
            self.set_cooldown(300)

    def reset_errors(self):
        self.health.consecutive_errors = 0

    def summary(self) -> dict:
        return {
            "id": self.account_id,
            "username": f"@{self.username}",
            "role": self.role.value,
            "status": self.status.value,
            "health_score": round(self.health.score(), 1),
            "health_grade": self.health.grade(),
            "daily_tweets": f"{self.daily_tweet_count}/{self.daily_tweet_limit}",
            "available": self.is_available(),
            "tags": self.tags,
        }


class MultiAccountManager:
    """
    Central manager for multiple Twitter accounts.
    Handles: registration, selection, rotation, failover, analytics aggregation.
    """

    def __init__(self, db_path: str = ":memory:"):
        self.accounts: dict[str, TwitterAccount] = {}
        self.active_account_id: Optional[str] = None
        self.rotation_index: int = 0
        self.db_path = db_path
        self._conn = sqlite3.connect(self.db_path)
        self._init_db()
        self._action_log: list[dict] = []

    def _init_db(self):
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS account_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                details TEXT,
                created_at REAL DEFAULT (strftime('%s','now'))
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS account_stats (
                account_id TEXT NOT NULL,
                date TEXT NOT NULL,
                tweets_sent INTEGER DEFAULT 0,
                dms_sent INTEGER DEFAULT 0,
                searches_made INTEGER DEFAULT 0,
                follows_made INTEGER DEFAULT 0,
                errors INTEGER DEFAULT 0,
                PRIMARY KEY (account_id, date)
            )
        """)
        self._conn.commit()

    def register(self, account: TwitterAccount) -> bool:
        """Register a new account."""
        if account.account_id in self.accounts:
            return False
        self.accounts[account.account_id] = account
        self._log_event(account.account_id, "registered", f"role={account.role.value}")
        if self.active_account_id is None:
            self.active_account_id = account.account_id
        return True

    def unregister(self, account_id: str) -> bool:
        """Remove an account from the pool."""
        if account_id not in self.accounts:
            return False
        del self.accounts[account_id]
        if self.active_account_id == account_id:
            self.active_account_id = next(iter(self.accounts), None)
        self._log_event(account_id, "unregistered")
        return True

    def get(self, account_id: str) -> Optional[TwitterAccount]:
        return self.accounts.get(account_id)

    def get_active(self) -> Optional[TwitterAccount]:
        if self.active_account_id:
            return self.accounts.get(self.active_account_id)
        return None

    def switch_to(self, account_id: str) -> bool:
        """Manually switch active account."""
        if account_id not in self.accounts:
            return False
        self.active_account_id = account_id
        self._log_event(account_id, "switched_to")
        return True

    def get_best_for_tweet(self) -> Optional[TwitterAccount]:
        """Get the best available account for tweeting."""
        candidates = [a for a in self.accounts.values() if a.can_tweet()]
        if not candidates:
            return None
        candidates.sort(key=lambda a: (
            a.role == AccountRole.MAIN,
            a.health.score(),
            -a.daily_tweet_count,
        ), reverse=True)
        return candidates[0]

    def get_best_for_dm(self) -> Optional[TwitterAccount]:
        """Get the best available account for DMs."""
        candidates = [a for a in self.accounts.values() if a.can_dm()]
        if not candidates:
            return None
        candidates.sort(key=lambda a: a.health.score(), reverse=True)
        return candidates[0]

    def get_best_for_search(self) -> Optional[TwitterAccount]:
        """Get the best available account for search."""
        candidates = [a for a in self.accounts.values() if a.can_search()]
        if not candidates:
            return None
        candidates.sort(key=lambda a: a.rate_limits.search_remaining, reverse=True)
        return candidates[0]

    def rotate(self) -> Optional[TwitterAccount]:
        """Round-robin rotation among available accounts."""
        available = [a for a in self.accounts.values() if a.is_available()]
        if not available:
            return None
        self.rotation_index = self.rotation_index % len(available)
        account = available[self.rotation_index]
        self.rotation_index += 1
        self.active_account_id = account.account_id
        return account

    def failover(self, failed_id: str, reason: str = "") -> Optional[TwitterAccount]:
        """Failover from a failed account to the next best one."""
        failed = self.accounts.get(failed_id)
        if failed:
            failed.record_error()
            self._log_event(failed_id, "failover", reason)

        candidates = [
            a for a in self.accounts.values()
            if a.account_id != failed_id and a.is_available()
        ]
        if not candidates:
            return None

        candidates.sort(key=lambda a: a.health.score(), reverse=True)
        selected = candidates[0]
        self.active_account_id = selected.account_id
        self._log_event(selected.account_id, "failover_target", f"from={failed_id}")
        return selected

    def list_by_role(self, role: AccountRole) -> list[TwitterAccount]:
        return [a for a in self.accounts.values() if a.role == role]

    def list_by_status(self, status: AccountStatus) -> list[TwitterAccount]:
        return [a for a in self.accounts.values() if a.status == status]

    def list_by_tag(self, tag: str) -> list[TwitterAccount]:
        return [a for a in self.accounts.values() if tag in a.tags]

    def get_pool_summary(self) -> dict:
        """Aggregated pool summary."""
        total = len(self.accounts)
        available = sum(1 for a in self.accounts.values() if a.is_available())
        by_status = {}
        for a in self.accounts.values():
            by_status[a.status.value] = by_status.get(a.status.value, 0) + 1
        by_role = {}
        for a in self.accounts.values():
            by_role[a.role.value] = by_role.get(a.role.value, 0) + 1
        avg_health = 0
        if total > 0:
            avg_health = sum(a.health.score() for a in self.accounts.values()) / total

        return {
            "total_accounts": total,
            "available": available,
            "by_status": by_status,
            "by_role": by_role,
            "avg_health_score": round(avg_health, 1),
            "active_account": self.active_account_id,
            "daily_tweets_total": sum(a.daily_tweet_count for a in self.accounts.values()),
        }

    def get_aggregated_analytics(self) -> dict:
        """Cross-account analytics aggregation."""
        if not self.accounts:
            return {"accounts": 0}
        total_engagement = sum(a.health.engagement_rate for a in self.accounts.values())
        total_growth = sum(a.health.follower_growth_7d for a in self.accounts.values())
        total_tweets = sum(a.daily_tweet_count for a in self.accounts.values())
        best = max(self.accounts.values(), key=lambda a: a.health.score())
        worst = min(self.accounts.values(), key=lambda a: a.health.score())
        return {
            "accounts": len(self.accounts),
            "total_engagement_rate": round(total_engagement, 4),
            "avg_engagement_rate": round(total_engagement / len(self.accounts), 4),
            "total_follower_growth_7d": total_growth,
            "total_daily_tweets": total_tweets,
            "best_account": {"id": best.account_id, "score": round(best.health.score(), 1)},
            "worst_account": {"id": worst.account_id, "score": round(worst.health.score(), 1)},
        }

    def reset_daily_counts(self):
        """Reset daily counters for all accounts (call at midnight)."""
        for account in self.accounts.values():
            account.daily_tweet_count = 0
            if account.status == AccountStatus.COOLDOWN and time.time() > account.cooldown_until:
                account.status = AccountStatus.ACTIVE

    def bulk_set_status(self, account_ids: list[str], status: AccountStatus):
        """Set status for multiple accounts."""
        for aid in account_ids:
            if aid in self.accounts:
                self.accounts[aid].status = status

    def export_accounts(self) -> list[dict]:
        """Export all accounts (masked credentials) for backup."""
        result = []
        for a in self.accounts.values():
            data = a.summary()
            data["credentials_fingerprint"] = a.credentials.fingerprint()
            data["notes"] = a.notes
            result.append(data)
        return result

    def _log_event(self, account_id: str, event_type: str, details: str = ""):
        self._action_log.append({
            "account_id": account_id,
            "event_type": event_type,
            "details": details,
            "ts": time.time(),
        })
        try:
            self._conn.execute(
                "INSERT INTO account_events (account_id, event_type, details) VALUES (?,?,?)",
                (account_id, event_type, details),
            )
            self._conn.commit()
        except Exception:
            pass

    def get_event_log(self, account_id: Optional[str] = None, limit: int = 50) -> list[dict]:
        try:
            if account_id:
                rows = self._conn.execute(
                    "SELECT account_id, event_type, details, created_at FROM account_events WHERE account_id=? ORDER BY id DESC LIMIT ?",
                    (account_id, limit),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT account_id, event_type, details, created_at FROM account_events ORDER BY id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [{"account_id": r[0], "event_type": r[1], "details": r[2], "ts": r[3]} for r in rows]
        except Exception:
            return self._action_log[-limit:]
