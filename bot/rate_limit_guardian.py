"""
API Rate Limit Guardian for Twitter/X Bot
API限流守卫: 智能限流 + 滑动窗口 + 端点级配额 + 熔断器 + 排队

Features:
- 滑动窗口限流 (per-endpoint)
- Twitter API v2 官方限制预设
- 令牌桶 + 漏桶双算法
- 熔断器模式 (closed → open → half_open → closed)
- 请求排队 + 优先级
- 限流统计 + 报告
- 429响应自动处理
"""

import json
import sqlite3
import time
import threading
import math
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum


class BreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class Priority(int, Enum):
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


# Twitter API v2 Official Rate Limits (per 15-min window)
TWITTER_V2_LIMITS = {
    # Tweets
    "POST /2/tweets": {"limit": 200, "window": 900},  # per user
    "DELETE /2/tweets/:id": {"limit": 50, "window": 900},
    "GET /2/tweets": {"limit": 300, "window": 900},  # per app
    "GET /2/tweets/:id": {"limit": 300, "window": 900},
    "GET /2/tweets/search/recent": {"limit": 180, "window": 900},
    "GET /2/tweets/counts/recent": {"limit": 300, "window": 900},
    # Users
    "GET /2/users/me": {"limit": 75, "window": 900},
    "GET /2/users/:id": {"limit": 300, "window": 900},
    "GET /2/users/by/username/:username": {"limit": 300, "window": 900},
    "GET /2/users/:id/followers": {"limit": 15, "window": 900},
    "GET /2/users/:id/following": {"limit": 15, "window": 900},
    # Follows
    "POST /2/users/:id/following": {"limit": 50, "window": 900},
    "DELETE /2/users/:source_user_id/following/:target_user_id": {"limit": 50, "window": 900},
    # Likes
    "POST /2/users/:id/likes": {"limit": 50, "window": 900},
    "DELETE /2/users/:id/likes/:tweet_id": {"limit": 50, "window": 900},
    "GET /2/users/:id/liked_tweets": {"limit": 75, "window": 900},
    # Retweets
    "POST /2/users/:id/retweets": {"limit": 50, "window": 900},
    "DELETE /2/users/:id/retweets/:source_tweet_id": {"limit": 50, "window": 900},
    # DMs
    "POST /2/dm_conversations/with/:participant_id/messages": {"limit": 200, "window": 900},
    "GET /2/dm_events": {"limit": 100, "window": 900},
    # Lists
    "POST /2/lists": {"limit": 300, "window": 900},
    "GET /2/lists/:id": {"limit": 75, "window": 900},
}

# Daily limits
TWITTER_DAILY_LIMITS = {
    "tweets_per_day": 2400,
    "follows_per_day": 400,
    "dms_per_day": 500,
    "likes_per_day": 1000,
}


@dataclass
class SlidingWindow:
    """滑动窗口"""
    window_seconds: int = 900  # 15 minutes
    max_requests: int = 100
    timestamps: List[float] = field(default_factory=list)

    def record(self) -> bool:
        """记录请求，返回是否在限制内"""
        now = time.time()
        cutoff = now - self.window_seconds
        self.timestamps = [t for t in self.timestamps if t > cutoff]

        if len(self.timestamps) >= self.max_requests:
            return False

        self.timestamps.append(now)
        return True

    def remaining(self) -> int:
        """剩余请求数"""
        now = time.time()
        cutoff = now - self.window_seconds
        self.timestamps = [t for t in self.timestamps if t > cutoff]
        return max(0, self.max_requests - len(self.timestamps))

    def reset_after(self) -> float:
        """窗口重置剩余秒数"""
        if not self.timestamps:
            return 0.0
        oldest = min(self.timestamps)
        reset_at = oldest + self.window_seconds
        return max(0.0, reset_at - time.time())

    def usage_percent(self) -> float:
        """使用百分比"""
        now = time.time()
        cutoff = now - self.window_seconds
        active = len([t for t in self.timestamps if t > cutoff])
        return (active / self.max_requests * 100) if self.max_requests > 0 else 0.0


@dataclass
class TokenBucket:
    """令牌桶"""
    capacity: int = 100
    refill_rate: float = 1.0  # tokens per second
    tokens: float = 100.0
    last_refill: float = field(default_factory=time.time)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def consume(self, count: int = 1) -> bool:
        """消耗令牌"""
        with self._lock:
            self._refill()
            if self.tokens >= count:
                self.tokens -= count
                return True
            return False

    def _refill(self):
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

    def available(self) -> int:
        with self._lock:
            self._refill()
            return int(self.tokens)

    def wait_time(self, count: int = 1) -> float:
        """等待获取N个令牌需要的时间"""
        with self._lock:
            self._refill()
            if self.tokens >= count:
                return 0.0
            deficit = count - self.tokens
            return deficit / self.refill_rate if self.refill_rate > 0 else float("inf")


@dataclass
class CircuitBreaker:
    """熔断器"""
    failure_threshold: int = 5
    recovery_timeout: float = 60.0  # seconds
    half_open_max_calls: int = 3
    state: str = "closed"
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0.0
    half_open_calls: int = 0

    def can_proceed(self) -> bool:
        """是否允许请求通过"""
        if self.state == BreakerState.CLOSED:
            return True
        elif self.state == BreakerState.OPEN:
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = BreakerState.HALF_OPEN
                self.half_open_calls = 0
                return True
            return False
        elif self.state == BreakerState.HALF_OPEN:
            return self.half_open_calls < self.half_open_max_calls
        return False

    def record_success(self):
        """记录成功"""
        if self.state == BreakerState.HALF_OPEN:
            self.success_count += 1
            self.half_open_calls += 1
            if self.success_count >= self.half_open_max_calls:
                self.state = BreakerState.CLOSED
                self.failure_count = 0
                self.success_count = 0
        elif self.state == BreakerState.CLOSED:
            self.failure_count = max(0, self.failure_count - 1)

    def record_failure(self):
        """记录失败"""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.state == BreakerState.HALF_OPEN:
            self.state = BreakerState.OPEN
            self.success_count = 0
        elif self.state == BreakerState.CLOSED:
            if self.failure_count >= self.failure_threshold:
                self.state = BreakerState.OPEN

    def reset(self):
        """重置熔断器"""
        self.state = BreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.half_open_calls = 0


class RateLimitGuardian:
    """API限流守卫"""

    def __init__(self, db_path: str = "twitterbot.db",
                 use_twitter_defaults: bool = True):
        self.db_path = db_path
        self._windows: Dict[str, SlidingWindow] = {}
        self._buckets: Dict[str, TokenBucket] = {}
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._daily_counts: Dict[str, Dict[str, int]] = {}  # {date: {action: count}}
        self._lock = threading.Lock()
        self._stats = defaultdict(lambda: {"allowed": 0, "denied": 0, "errors": 0})
        self._init_tables()

        if use_twitter_defaults:
            self._load_twitter_defaults()

    def _init_tables(self):
        conn = sqlite3.connect(self.db_path)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS rate_limit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                endpoint TEXT NOT NULL,
                action TEXT NOT NULL,
                result TEXT NOT NULL,
                details TEXT DEFAULT '',
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS rate_limit_config (
                endpoint TEXT PRIMARY KEY,
                window_seconds INTEGER DEFAULT 900,
                max_requests INTEGER DEFAULT 100,
                burst_size INTEGER DEFAULT 0,
                breaker_threshold INTEGER DEFAULT 5,
                breaker_timeout REAL DEFAULT 60.0,
                priority INTEGER DEFAULT 2,
                enabled INTEGER DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS daily_usage (
                date TEXT NOT NULL,
                action TEXT NOT NULL,
                count INTEGER DEFAULT 0,
                PRIMARY KEY (date, action)
            );
            CREATE INDEX IF NOT EXISTS idx_ratelimit_log_time ON rate_limit_log(created_at);
            CREATE INDEX IF NOT EXISTS idx_ratelimit_log_endpoint ON rate_limit_log(endpoint);
        """)
        conn.commit()
        conn.close()

    def _load_twitter_defaults(self):
        """加载Twitter API v2默认限制"""
        for endpoint, config in TWITTER_V2_LIMITS.items():
            self._windows[endpoint] = SlidingWindow(
                window_seconds=config["window"],
                max_requests=config["limit"],
            )
            # Token bucket: refill rate = limit / window
            refill_rate = config["limit"] / config["window"]
            self._buckets[endpoint] = TokenBucket(
                capacity=config["limit"],
                refill_rate=refill_rate,
                tokens=float(config["limit"]),
            )
            self._breakers[endpoint] = CircuitBreaker()

    # ── Core API ─────────────────────────────────────────

    def check(self, endpoint: str, count: int = 1) -> dict:
        """检查是否允许请求 (不消耗配额)"""
        with self._lock:
            result = {
                "endpoint": endpoint,
                "allowed": True,
                "remaining": 0,
                "reset_after": 0.0,
                "breaker_state": "closed",
                "daily_remaining": None,
            }

            # Circuit breaker check
            breaker = self._breakers.get(endpoint)
            if breaker:
                result["breaker_state"] = breaker.state
                if not breaker.can_proceed():
                    result["allowed"] = False
                    result["reason"] = "circuit_breaker_open"
                    return result

            # Sliding window check
            window = self._windows.get(endpoint)
            if window:
                result["remaining"] = window.remaining()
                result["reset_after"] = window.reset_after()
                if window.remaining() < count:
                    result["allowed"] = False
                    result["reason"] = "rate_limit_exceeded"

            # Daily limit check
            daily = self._check_daily_limit(endpoint)
            if daily is not None:
                result["daily_remaining"] = daily
                if daily < count:
                    result["allowed"] = False
                    result["reason"] = "daily_limit_exceeded"

            return result

    def acquire(self, endpoint: str, count: int = 1, priority: int = Priority.NORMAL) -> dict:
        """获取请求配额 (消耗)"""
        with self._lock:
            result = self.check(endpoint, count)

            if not result["allowed"]:
                self._stats[endpoint]["denied"] += count
                self._log(endpoint, "acquire", "denied",
                          result.get("reason", "limit_exceeded"))
                return result

            # Consume from sliding window
            window = self._windows.get(endpoint)
            if window:
                for _ in range(count):
                    if not window.record():
                        result["allowed"] = False
                        result["reason"] = "window_exhausted"
                        self._stats[endpoint]["denied"] += 1
                        return result

            # Consume from token bucket
            bucket = self._buckets.get(endpoint)
            if bucket:
                bucket.consume(count)

            # Update daily count
            self._increment_daily(endpoint, count)

            self._stats[endpoint]["allowed"] += count
            result["remaining"] = window.remaining() if window else 0
            self._log(endpoint, "acquire", "allowed")
            return result

    def release(self, endpoint: str, success: bool = True):
        """释放 (回调熔断器)"""
        breaker = self._breakers.get(endpoint)
        if not breaker:
            return

        if success:
            breaker.record_success()
        else:
            breaker.record_failure()
            self._stats[endpoint]["errors"] += 1
            self._log(endpoint, "release", "failure",
                       f"Breaker: {breaker.state}")

    def handle_429(self, endpoint: str, retry_after: float = None):
        """处理429 Too Many Requests响应"""
        with self._lock:
            # Record failure in circuit breaker
            breaker = self._breakers.get(endpoint)
            if breaker:
                breaker.record_failure()

            # Clear the sliding window (server says we're over limit)
            window = self._windows.get(endpoint)
            if window:
                window.timestamps = []  # Reset

            self._stats[endpoint]["errors"] += 1
            wait = retry_after or 60.0
            self._log(endpoint, "429_received", "throttled",
                       f"Retry after: {wait}s, Breaker: {breaker.state if breaker else 'n/a'}")

            return {"endpoint": endpoint, "wait_seconds": wait,
                    "breaker_state": breaker.state if breaker else "n/a"}

    # ── Configuration ────────────────────────────────────

    def configure_endpoint(self, endpoint: str, max_requests: int = 100,
                            window_seconds: int = 900,
                            burst_size: int = 0,
                            breaker_threshold: int = 5,
                            breaker_timeout: float = 60.0) -> bool:
        """配置端点限制"""
        self._windows[endpoint] = SlidingWindow(
            window_seconds=window_seconds,
            max_requests=max_requests,
        )
        refill_rate = max_requests / window_seconds if window_seconds > 0 else 1.0
        self._buckets[endpoint] = TokenBucket(
            capacity=max_requests + burst_size,
            refill_rate=refill_rate,
            tokens=float(max_requests),
        )
        self._breakers[endpoint] = CircuitBreaker(
            failure_threshold=breaker_threshold,
            recovery_timeout=breaker_timeout,
        )

        # Persist config
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT OR REPLACE INTO rate_limit_config "
            "(endpoint, window_seconds, max_requests, burst_size, "
            "breaker_threshold, breaker_timeout) VALUES (?, ?, ?, ?, ?, ?)",
            (endpoint, window_seconds, max_requests, burst_size,
             breaker_threshold, breaker_timeout),
        )
        conn.commit()
        conn.close()
        return True

    def get_endpoint_config(self, endpoint: str) -> Optional[dict]:
        """获取端点配置"""
        window = self._windows.get(endpoint)
        bucket = self._buckets.get(endpoint)
        breaker = self._breakers.get(endpoint)

        if not window:
            return None

        return {
            "endpoint": endpoint,
            "window_seconds": window.window_seconds,
            "max_requests": window.max_requests,
            "remaining": window.remaining(),
            "usage_percent": round(window.usage_percent(), 1),
            "reset_after": round(window.reset_after(), 1),
            "bucket_tokens": bucket.available() if bucket else None,
            "breaker_state": breaker.state if breaker else None,
            "breaker_failures": breaker.failure_count if breaker else 0,
        }

    # ── Daily Limits ─────────────────────────────────────

    def _check_daily_limit(self, endpoint: str) -> Optional[int]:
        """检查日限制"""
        action_map = {
            "POST /2/tweets": "tweets_per_day",
            "POST /2/users/:id/following": "follows_per_day",
            "POST /2/dm_conversations": "dms_per_day",
            "POST /2/users/:id/likes": "likes_per_day",
        }

        # Find matching daily limit
        action_key = None
        for pattern, key in action_map.items():
            if endpoint.startswith(pattern.split("/")[0]) and pattern.split("/")[-1] in endpoint:
                action_key = key
                break

        if not action_key:
            return None

        daily_limit = TWITTER_DAILY_LIMITS.get(action_key)
        if not daily_limit:
            return None

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        daily = self._daily_counts.get(today, {})
        used = daily.get(action_key, 0)
        return daily_limit - used

    def _increment_daily(self, endpoint: str, count: int = 1):
        """增加日计数"""
        action_map = {
            "POST /2/tweets": "tweets_per_day",
            "POST /2/users/:id/following": "follows_per_day",
        }

        for pattern, key in action_map.items():
            if pattern in endpoint or endpoint in pattern:
                today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                if today not in self._daily_counts:
                    self._daily_counts[today] = {}
                self._daily_counts[today][key] = self._daily_counts[today].get(key, 0) + count

                # Persist
                try:
                    conn = sqlite3.connect(self.db_path)
                    conn.execute(
                        "INSERT INTO daily_usage (date, action, count) VALUES (?, ?, ?) "
                        "ON CONFLICT(date, action) DO UPDATE SET count=count+?",
                        (today, key, count, count),
                    )
                    conn.commit()
                    conn.close()
                except Exception:
                    pass
                break

    def get_daily_usage(self, date: str = None) -> dict:
        """获取日使用量"""
        date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM daily_usage WHERE date=?", (date,)
        ).fetchall()
        conn.close()

        usage = {}
        for row in rows:
            d = dict(row)
            action = d["action"]
            limit = TWITTER_DAILY_LIMITS.get(action, 0)
            usage[action] = {
                "used": d["count"],
                "limit": limit,
                "remaining": max(0, limit - d["count"]),
                "percent": round(d["count"] / limit * 100, 1) if limit > 0 else 0,
            }

        return {"date": date, "actions": usage}

    # ── Logging ──────────────────────────────────────────

    def _log(self, endpoint: str, action: str, result: str, details: str = ""):
        """记录日志"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute(
                "INSERT INTO rate_limit_log (endpoint, action, result, details, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (endpoint, action, result, details,
                 datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass  # Don't fail on logging errors

    def get_log(self, endpoint: str = None, limit: int = 100,
                result: str = None) -> List[dict]:
        """获取日志"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        query = "SELECT * FROM rate_limit_log"
        params = []
        conditions = []

        if endpoint:
            conditions.append("endpoint=?")
            params.append(endpoint)
        if result:
            conditions.append("result=?")
            params.append(result)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += f" ORDER BY created_at DESC LIMIT {limit}"

        rows = conn.execute(query, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Stats & Reports ──────────────────────────────────

    def stats(self) -> dict:
        """全局统计"""
        total_allowed = sum(s["allowed"] for s in self._stats.values())
        total_denied = sum(s["denied"] for s in self._stats.values())
        total_errors = sum(s["errors"] for s in self._stats.values())

        endpoints_status = {}
        for ep in self._windows:
            window = self._windows[ep]
            breaker = self._breakers.get(ep)
            endpoints_status[ep] = {
                "remaining": window.remaining(),
                "usage": round(window.usage_percent(), 1),
                "breaker": breaker.state if breaker else "n/a",
            }

        # Top throttled endpoints
        throttled = sorted(
            [(ep, s["denied"]) for ep, s in self._stats.items() if s["denied"] > 0],
            key=lambda x: x[1], reverse=True,
        )[:10]

        return {
            "total_allowed": total_allowed,
            "total_denied": total_denied,
            "total_errors": total_errors,
            "deny_rate": round(total_denied / max(1, total_allowed + total_denied) * 100, 2),
            "configured_endpoints": len(self._windows),
            "open_breakers": sum(
                1 for b in self._breakers.values() if b.state == BreakerState.OPEN
            ),
            "top_throttled": throttled,
            "endpoints": endpoints_status,
        }

    def health_check(self) -> dict:
        """健康检查"""
        issues = []
        warnings = []

        for ep, breaker in self._breakers.items():
            if breaker.state == BreakerState.OPEN:
                issues.append(f"Circuit breaker OPEN for: {ep}")
            elif breaker.state == BreakerState.HALF_OPEN:
                warnings.append(f"Circuit breaker half-open for: {ep}")

        for ep, window in self._windows.items():
            usage = window.usage_percent()
            if usage > 90:
                warnings.append(f"High usage ({usage:.0f}%) for: {ep}")

        return {
            "healthy": len(issues) == 0,
            "issues": issues,
            "warnings": warnings,
            "endpoints_count": len(self._windows),
            "active_breakers": sum(
                1 for b in self._breakers.values() if b.state != BreakerState.CLOSED
            ),
        }

    def report(self, format: str = "text") -> str:
        """生成报告"""
        stats = self.stats()
        health = self.health_check()

        if format == "json":
            return json.dumps({"stats": stats, "health": health}, indent=2)

        lines = [
            "═══ Rate Limit Guardian Report ═══",
            f"Status: {'✅ Healthy' if health['healthy'] else '❌ Issues detected'}",
            f"Endpoints: {stats['configured_endpoints']}",
            f"Open breakers: {stats['open_breakers']}",
            "",
            f"Requests: {stats['total_allowed']} allowed / {stats['total_denied']} denied",
            f"Errors: {stats['total_errors']}",
            f"Deny rate: {stats['deny_rate']}%",
            "",
        ]

        if stats["top_throttled"]:
            lines.append("Top throttled:")
            for ep, count in stats["top_throttled"][:5]:
                lines.append(f"  {ep}: {count} denied")
            lines.append("")

        if health["issues"]:
            lines.append("Issues:")
            for issue in health["issues"]:
                lines.append(f"  ❌ {issue}")
            lines.append("")

        if health["warnings"]:
            lines.append("Warnings:")
            for warning in health["warnings"]:
                lines.append(f"  ⚠️ {warning}")

        return "\n".join(lines)

    def reset_endpoint(self, endpoint: str) -> bool:
        """重置端点状态"""
        if endpoint in self._windows:
            self._windows[endpoint].timestamps = []
        if endpoint in self._breakers:
            self._breakers[endpoint].reset()
        if endpoint in self._buckets:
            bucket = self._buckets[endpoint]
            bucket.tokens = float(bucket.capacity)
        if endpoint in self._stats:
            self._stats[endpoint] = {"allowed": 0, "denied": 0, "errors": 0}
        return endpoint in self._windows

    def reset_all(self):
        """重置所有状态"""
        for ep in list(self._windows.keys()):
            self.reset_endpoint(ep)
