"""
Account Security Monitor
-------------------------
Monitors account security: API key rotation tracking, login anomaly detection,
suspicious activity patterns, rate limit forensics, and security scoring.
"""

import sqlite3
import hashlib
import json
import re
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from collections import defaultdict


class ThreatLevel(Enum):
    NONE = "none"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertType(Enum):
    LOGIN_ANOMALY = "login_anomaly"
    API_KEY_EXPIRED = "api_key_expired"
    RATE_LIMIT_ABUSE = "rate_limit_abuse"
    SUSPICIOUS_ACTIVITY = "suspicious_activity"
    IP_CHANGE = "ip_change"
    PERMISSION_ESCALATION = "permission_escalation"
    UNUSUAL_HOURS = "unusual_hours"
    GEO_IMPOSSIBLE = "geo_impossible"
    BRUTE_FORCE = "brute_force"
    TOKEN_LEAKED = "token_leaked"


class KeyStatus(Enum):
    ACTIVE = "active"
    EXPIRING_SOON = "expiring_soon"
    EXPIRED = "expired"
    REVOKED = "revoked"
    COMPROMISED = "compromised"


@dataclass
class APIKeyInfo:
    """Tracks an API key's lifecycle."""
    key_id: str
    key_name: str
    key_hash: str  # SHA256 hash, never store plaintext
    status: KeyStatus = KeyStatus.ACTIVE
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    expires_at: Optional[str] = None
    last_used: Optional[str] = None
    last_rotated: Optional[str] = None
    rotation_count: int = 0
    permissions: list[str] = field(default_factory=list)

    def is_expired(self) -> bool:
        if not self.expires_at:
            return False
        try:
            exp = datetime.fromisoformat(self.expires_at)
            return datetime.now(timezone.utc) > exp
        except ValueError:
            return False

    def days_until_expiry(self) -> Optional[int]:
        if not self.expires_at:
            return None
        try:
            exp = datetime.fromisoformat(self.expires_at)
            delta = exp - datetime.now(timezone.utc)
            return max(delta.days, 0)
        except ValueError:
            return None

    def needs_rotation(self, max_age_days: int = 90) -> bool:
        ref = self.last_rotated or self.created_at
        try:
            ref_dt = datetime.fromisoformat(ref)
            age = (datetime.now(timezone.utc) - ref_dt).days
            return age >= max_age_days
        except ValueError:
            return True


@dataclass
class LoginEvent:
    """Records a login/access event."""
    event_id: str
    ip_address: str
    user_agent: str = ""
    country: str = ""
    city: str = ""
    success: bool = True
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    method: str = "api"  # api, oauth, password, 2fa
    risk_score: float = 0.0


@dataclass
class SecurityAlert:
    """A security alert triggered by anomaly detection."""
    alert_id: str
    alert_type: AlertType
    threat_level: ThreatLevel
    title: str
    description: str
    evidence: dict = field(default_factory=dict)
    acknowledged: bool = False
    resolved: bool = False
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    resolved_at: Optional[str] = None


class AnomalyDetector:
    """Detects anomalous login and access patterns."""

    def __init__(self):
        self._login_history: list[LoginEvent] = []
        self._known_ips: set[str] = set()
        self._known_countries: set[str] = set()
        self._hourly_pattern: dict[int, int] = defaultdict(int)

    def add_known_ip(self, ip: str) -> None:
        self._known_ips.add(ip)

    def add_known_country(self, country: str) -> None:
        self._known_countries.add(country)

    def record_login(self, event: LoginEvent) -> list[SecurityAlert]:
        """Record a login event and return any triggered alerts."""
        alerts = []
        self._login_history.append(event)

        # Build patterns
        try:
            hour = datetime.fromisoformat(event.timestamp).hour
            self._hourly_pattern[hour] += 1
        except ValueError:
            hour = 0

        # Check 1: Unknown IP
        if event.ip_address not in self._known_ips and self._known_ips:
            alerts.append(SecurityAlert(
                alert_id=hashlib.md5(f"ip:{event.event_id}".encode()).hexdigest()[:12],
                alert_type=AlertType.IP_CHANGE,
                threat_level=ThreatLevel.MEDIUM,
                title="New IP Address Detected",
                description=f"Login from unknown IP: {event.ip_address}",
                evidence={"ip": event.ip_address, "known_ips": list(self._known_ips)[:5]},
            ))
        self._known_ips.add(event.ip_address)

        # Check 2: Unusual hours (between 2am-5am local)
        if 2 <= hour <= 5:
            alerts.append(SecurityAlert(
                alert_id=hashlib.md5(f"hour:{event.event_id}".encode()).hexdigest()[:12],
                alert_type=AlertType.UNUSUAL_HOURS,
                threat_level=ThreatLevel.LOW,
                title="Login During Unusual Hours",
                description=f"Access at {hour}:00 UTC",
                evidence={"hour": hour, "ip": event.ip_address},
            ))

        # Check 3: Geographic impossibility
        if event.country and self._known_countries:
            if event.country not in self._known_countries:
                last_login = self._get_last_login_before(event)
                if last_login and last_login.country:
                    alerts.append(SecurityAlert(
                        alert_id=hashlib.md5(f"geo:{event.event_id}".encode()).hexdigest()[:12],
                        alert_type=AlertType.GEO_IMPOSSIBLE,
                        threat_level=ThreatLevel.HIGH,
                        title="Impossible Geographic Travel",
                        description=f"Login from {event.country} after {last_login.country}",
                        evidence={
                            "current_country": event.country,
                            "previous_country": last_login.country,
                            "time_gap_minutes": self._time_gap_minutes(last_login, event),
                        },
                    ))
        if event.country:
            self._known_countries.add(event.country)

        # Check 4: Brute force (5+ failed logins in 10 minutes)
        if not event.success:
            recent_failures = self._count_recent_failures(minutes=10)
            if recent_failures >= 5:
                alerts.append(SecurityAlert(
                    alert_id=hashlib.md5(f"brute:{event.event_id}".encode()).hexdigest()[:12],
                    alert_type=AlertType.BRUTE_FORCE,
                    threat_level=ThreatLevel.CRITICAL,
                    title="Possible Brute Force Attack",
                    description=f"{recent_failures} failed login attempts in 10 minutes",
                    evidence={
                        "failed_attempts": recent_failures,
                        "window_minutes": 10,
                        "ip": event.ip_address,
                    },
                ))

        # Check 5: Failed login
        if not event.success:
            alerts.append(SecurityAlert(
                alert_id=hashlib.md5(f"fail:{event.event_id}".encode()).hexdigest()[:12],
                alert_type=AlertType.LOGIN_ANOMALY,
                threat_level=ThreatLevel.LOW,
                title="Failed Login Attempt",
                description=f"Failed login from {event.ip_address}",
                evidence={"ip": event.ip_address, "method": event.method},
            ))

        return alerts

    def _get_last_login_before(self, current: LoginEvent) -> Optional[LoginEvent]:
        for event in reversed(self._login_history[:-1]):
            if event.success:
                return event
        return None

    def _time_gap_minutes(self, a: LoginEvent, b: LoginEvent) -> int:
        try:
            ta = datetime.fromisoformat(a.timestamp)
            tb = datetime.fromisoformat(b.timestamp)
            return abs(int((tb - ta).total_seconds() / 60))
        except ValueError:
            return 0

    def _count_recent_failures(self, minutes: int = 10) -> int:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
        count = 0
        for event in reversed(self._login_history):
            try:
                t = datetime.fromisoformat(event.timestamp)
                if t < cutoff:
                    break
                if not event.success:
                    count += 1
            except ValueError:
                continue
        return count

    def get_risk_score(self) -> float:
        """Calculate overall account risk score 0-100."""
        if not self._login_history:
            return 0.0

        score = 0.0
        total = len(self._login_history)

        # Factor 1: Failed login ratio (30%)
        failures = sum(1 for e in self._login_history if not e.success)
        fail_ratio = failures / total
        score += min(fail_ratio * 100, 30)

        # Factor 2: IP diversity (20%)
        unique_ips = len(set(e.ip_address for e in self._login_history))
        ip_ratio = unique_ips / total
        score += min(ip_ratio * 40, 20)

        # Factor 3: Country diversity (25%)
        countries = set(e.country for e in self._login_history if e.country)
        if len(countries) > 3:
            score += 25
        elif len(countries) > 1:
            score += 15

        # Factor 4: Unusual hour ratio (25%)
        unusual = sum(1 for e in self._login_history
                      if 2 <= datetime.fromisoformat(e.timestamp).hour <= 5)
        unusual_ratio = unusual / total
        score += min(unusual_ratio * 100, 25)

        return round(min(score, 100), 1)


class RateLimitForensics:
    """Analyze rate limit patterns to detect abuse and optimize usage."""

    def __init__(self):
        self._events: list[dict] = []

    def record_rate_limit(self, endpoint: str, limit: int, remaining: int,
                          reset_at: str, status_code: int = 200) -> None:
        """Record a rate limit header observation."""
        self._events.append({
            "endpoint": endpoint,
            "limit": limit,
            "remaining": remaining,
            "reset_at": reset_at,
            "status_code": status_code,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "usage_pct": round((1 - remaining / limit) * 100, 1) if limit > 0 else 100,
        })
        # Keep last 1000 events
        if len(self._events) > 1000:
            self._events = self._events[-1000:]

    def get_endpoint_stats(self) -> dict[str, dict]:
        """Get rate limit statistics per endpoint."""
        stats: dict[str, dict] = {}
        for event in self._events:
            ep = event["endpoint"]
            if ep not in stats:
                stats[ep] = {
                    "total_calls": 0,
                    "rate_limited": 0,
                    "avg_usage_pct": 0,
                    "max_usage_pct": 0,
                    "usage_values": [],
                }
            stats[ep]["total_calls"] += 1
            if event["status_code"] == 429:
                stats[ep]["rate_limited"] += 1
            stats[ep]["usage_values"].append(event["usage_pct"])
            stats[ep]["max_usage_pct"] = max(stats[ep]["max_usage_pct"], event["usage_pct"])

        for ep, s in stats.items():
            values = s.pop("usage_values")
            s["avg_usage_pct"] = round(sum(values) / len(values), 1) if values else 0
            s["rate_limit_pct"] = round(
                s["rate_limited"] / s["total_calls"] * 100, 1
            ) if s["total_calls"] > 0 else 0

        return stats

    def detect_abuse_patterns(self) -> list[dict]:
        """Detect potential rate limit abuse patterns."""
        patterns = []
        stats = self.get_endpoint_stats()

        for ep, s in stats.items():
            # Pattern 1: Consistently hitting limits
            if s["avg_usage_pct"] > 90:
                patterns.append({
                    "pattern": "high_usage",
                    "endpoint": ep,
                    "severity": "high",
                    "detail": f"Average usage at {s['avg_usage_pct']}%",
                })

            # Pattern 2: Frequent 429s
            if s["rate_limit_pct"] > 10:
                patterns.append({
                    "pattern": "frequent_429",
                    "endpoint": ep,
                    "severity": "critical",
                    "detail": f"{s['rate_limit_pct']}% of requests rate-limited",
                })

        # Pattern 3: Burst detection (10+ calls in 1 minute to same endpoint)
        by_minute: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for event in self._events:
            minute_key = event["timestamp"][:16]
            by_minute[minute_key][event["endpoint"]] += 1

        for minute, endpoints in by_minute.items():
            for ep, count in endpoints.items():
                if count >= 10:
                    patterns.append({
                        "pattern": "burst",
                        "endpoint": ep,
                        "severity": "medium",
                        "detail": f"{count} calls in 1 minute at {minute}",
                    })

        return patterns

    def suggest_optimizations(self) -> list[str]:
        """Suggest rate limit optimization strategies."""
        suggestions = []
        stats = self.get_endpoint_stats()
        patterns = self.detect_abuse_patterns()

        high_usage_eps = [p["endpoint"] for p in patterns if p["pattern"] == "high_usage"]
        if high_usage_eps:
            suggestions.append(
                f"⚠️ Implement caching for high-usage endpoints: {', '.join(high_usage_eps)}"
            )

        burst_eps = [p["endpoint"] for p in patterns if p["pattern"] == "burst"]
        if burst_eps:
            suggestions.append(
                f"🔄 Add request queuing/throttling for burst endpoints: {', '.join(burst_eps)}"
            )

        limited_eps = [p["endpoint"] for p in patterns if p["pattern"] == "frequent_429"]
        if limited_eps:
            suggestions.append(
                f"🛑 Reduce call frequency or add exponential backoff for: {', '.join(limited_eps)}"
            )

        if not patterns:
            suggestions.append("✅ Rate limit usage is healthy across all endpoints")

        return suggestions


class TokenLeakScanner:
    """Scans for potential API token leaks in text content."""

    PATTERNS = {
        "twitter_bearer": re.compile(r'AAAA[A-Za-z0-9%]{40,}'),
        "twitter_api_key": re.compile(r'[A-Za-z0-9]{25}'),
        "generic_token": re.compile(r'(token|api_key|secret|bearer)\s*[=:]\s*["\']?([A-Za-z0-9_\-]{20,})["\']?', re.IGNORECASE),
        "env_var": re.compile(r'(TWITTER|API|SECRET|TOKEN|KEY|BEARER)_[A-Z_]*\s*=\s*["\']?([A-Za-z0-9_\-]{10,})["\']?'),
        "jwt": re.compile(r'eyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}'),
        "base64_secret": re.compile(r'[A-Za-z0-9+/]{40,}={0,2}'),
    }

    def scan_text(self, text: str) -> list[dict]:
        """Scan text for potential token leaks."""
        findings = []
        for pattern_name, regex in self.PATTERNS.items():
            matches = regex.findall(text)
            for match in matches:
                # Handle tuple matches from groups
                value = match[-1] if isinstance(match, tuple) else match
                if len(value) < 10:
                    continue

                # Skip obvious false positives
                if value in ("Authorization", "application/json", "Content-Type"):
                    continue

                findings.append({
                    "pattern": pattern_name,
                    "value_preview": value[:8] + "..." + value[-4:] if len(value) > 16 else "***",
                    "length": len(value),
                    "severity": "critical" if pattern_name in ("twitter_bearer", "jwt") else "high",
                })

        return findings

    def scan_file(self, file_path: str) -> list[dict]:
        """Scan a file for token leaks."""
        try:
            with open(file_path, "r", errors="ignore") as f:
                content = f.read()
            findings = self.scan_text(content)
            for f_item in findings:
                f_item["file"] = file_path
            return findings
        except (OSError, IOError):
            return []


class SecurityStore:
    """SQLite persistence for security data."""

    def __init__(self, db_path: str = "security.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS api_keys (
                    key_id TEXT PRIMARY KEY,
                    key_name TEXT NOT NULL,
                    key_hash TEXT NOT NULL,
                    status TEXT DEFAULT 'active',
                    created_at TEXT NOT NULL,
                    expires_at TEXT,
                    last_used TEXT,
                    last_rotated TEXT,
                    rotation_count INTEGER DEFAULT 0,
                    permissions TEXT DEFAULT '[]'
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS login_events (
                    event_id TEXT PRIMARY KEY,
                    ip_address TEXT NOT NULL,
                    user_agent TEXT DEFAULT '',
                    country TEXT DEFAULT '',
                    city TEXT DEFAULT '',
                    success INTEGER DEFAULT 1,
                    timestamp TEXT NOT NULL,
                    method TEXT DEFAULT 'api',
                    risk_score REAL DEFAULT 0.0
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS security_alerts (
                    alert_id TEXT PRIMARY KEY,
                    alert_type TEXT NOT NULL,
                    threat_level TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    evidence TEXT DEFAULT '{}',
                    acknowledged INTEGER DEFAULT 0,
                    resolved INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_login_ts ON login_events(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_type ON security_alerts(alert_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_level ON security_alerts(threat_level)")
            conn.commit()

    def save_api_key(self, key: APIKeyInfo) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO api_keys
                (key_id, key_name, key_hash, status, created_at, expires_at,
                 last_used, last_rotated, rotation_count, permissions)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (key.key_id, key.key_name, key.key_hash, key.status.value,
                  key.created_at, key.expires_at, key.last_used, key.last_rotated,
                  key.rotation_count, json.dumps(key.permissions)))
            conn.commit()

    def get_api_key(self, key_id: str) -> Optional[APIKeyInfo]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM api_keys WHERE key_id = ?", (key_id,)).fetchone()
            if not row:
                return None
            return APIKeyInfo(
                key_id=row["key_id"], key_name=row["key_name"], key_hash=row["key_hash"],
                status=KeyStatus(row["status"]), created_at=row["created_at"],
                expires_at=row["expires_at"], last_used=row["last_used"],
                last_rotated=row["last_rotated"], rotation_count=row["rotation_count"],
                permissions=json.loads(row["permissions"]),
            )

    def list_api_keys(self) -> list[APIKeyInfo]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM api_keys ORDER BY created_at DESC").fetchall()
            return [APIKeyInfo(
                key_id=r["key_id"], key_name=r["key_name"], key_hash=r["key_hash"],
                status=KeyStatus(r["status"]), created_at=r["created_at"],
                expires_at=r["expires_at"], last_used=r["last_used"],
                last_rotated=r["last_rotated"], rotation_count=r["rotation_count"],
                permissions=json.loads(r["permissions"]),
            ) for r in rows]

    def save_login_event(self, event: LoginEvent) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO login_events
                (event_id, ip_address, user_agent, country, city, success,
                 timestamp, method, risk_score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (event.event_id, event.ip_address, event.user_agent, event.country,
                  event.city, 1 if event.success else 0, event.timestamp,
                  event.method, event.risk_score))
            conn.commit()

    def save_alert(self, alert: SecurityAlert) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO security_alerts
                (alert_id, alert_type, threat_level, title, description,
                 evidence, acknowledged, resolved, created_at, resolved_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (alert.alert_id, alert.alert_type.value, alert.threat_level.value,
                  alert.title, alert.description, json.dumps(alert.evidence),
                  1 if alert.acknowledged else 0, 1 if alert.resolved else 0,
                  alert.created_at, alert.resolved_at))
            conn.commit()

    def get_unresolved_alerts(self) -> list[SecurityAlert]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM security_alerts WHERE resolved = 0 ORDER BY created_at DESC"
            ).fetchall()
            return [SecurityAlert(
                alert_id=r["alert_id"], alert_type=AlertType(r["alert_type"]),
                threat_level=ThreatLevel(r["threat_level"]),
                title=r["title"], description=r["description"],
                evidence=json.loads(r["evidence"]),
                acknowledged=bool(r["acknowledged"]), resolved=bool(r["resolved"]),
                created_at=r["created_at"], resolved_at=r["resolved_at"],
            ) for r in rows]

    def acknowledge_alert(self, alert_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "UPDATE security_alerts SET acknowledged = 1 WHERE alert_id = ?",
                (alert_id,)
            )
            conn.commit()
            return cursor.rowcount > 0

    def resolve_alert(self, alert_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "UPDATE security_alerts SET resolved = 1, resolved_at = ? WHERE alert_id = ?",
                (datetime.now(timezone.utc).isoformat(), alert_id)
            )
            conn.commit()
            return cursor.rowcount > 0

    def get_login_stats(self, days: int = 30) -> dict:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("""
                SELECT
                    COUNT(*) as total_logins,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                    SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as failed,
                    COUNT(DISTINCT ip_address) as unique_ips,
                    COUNT(DISTINCT country) as unique_countries,
                    AVG(risk_score) as avg_risk
                FROM login_events
                WHERE timestamp >= ?
            """, (cutoff,)).fetchone()
            return {
                "period_days": days,
                "total_logins": row["total_logins"],
                "successful": row["successful"],
                "failed": row["failed"],
                "unique_ips": row["unique_ips"],
                "unique_countries": row["unique_countries"],
                "avg_risk_score": round(row["avg_risk"] or 0, 1),
                "failure_rate": round(
                    (row["failed"] / row["total_logins"] * 100)
                    if row["total_logins"] > 0 else 0, 1
                ),
            }


class AccountSecurityMonitor:
    """Main security monitor orchestrating all sub-systems."""

    def __init__(self, db_path: str = "security.db"):
        self.store = SecurityStore(db_path)
        self.anomaly_detector = AnomalyDetector()
        self.rate_forensics = RateLimitForensics()
        self.leak_scanner = TokenLeakScanner()

    def register_api_key(self, key_name: str, key_value: str,
                         expires_at: Optional[str] = None,
                         permissions: Optional[list[str]] = None) -> APIKeyInfo:
        """Register a new API key for tracking (stores hash only)."""
        key_hash = hashlib.sha256(key_value.encode()).hexdigest()
        key_id = hashlib.md5(f"{key_name}:{key_hash[:16]}".encode()).hexdigest()[:12]

        key_info = APIKeyInfo(
            key_id=key_id,
            key_name=key_name,
            key_hash=key_hash,
            expires_at=expires_at,
            permissions=permissions or [],
        )
        self.store.save_api_key(key_info)
        return key_info

    def rotate_api_key(self, key_id: str, new_key_value: str) -> Optional[APIKeyInfo]:
        """Record an API key rotation."""
        key_info = self.store.get_api_key(key_id)
        if not key_info:
            return None

        key_info.key_hash = hashlib.sha256(new_key_value.encode()).hexdigest()
        key_info.last_rotated = datetime.now(timezone.utc).isoformat()
        key_info.rotation_count += 1
        key_info.status = KeyStatus.ACTIVE
        self.store.save_api_key(key_info)
        return key_info

    def check_key_health(self) -> list[dict]:
        """Check all API keys for health issues."""
        issues = []
        keys = self.store.list_api_keys()

        for key in keys:
            if key.status == KeyStatus.COMPROMISED:
                issues.append({
                    "key_id": key.key_id,
                    "key_name": key.key_name,
                    "issue": "compromised",
                    "severity": "critical",
                    "action": "Revoke and rotate immediately",
                })
            elif key.is_expired():
                issues.append({
                    "key_id": key.key_id,
                    "key_name": key.key_name,
                    "issue": "expired",
                    "severity": "high",
                    "action": "Rotate key immediately",
                })
            elif key.needs_rotation():
                days = key.days_until_expiry()
                issues.append({
                    "key_id": key.key_id,
                    "key_name": key.key_name,
                    "issue": "needs_rotation",
                    "severity": "medium",
                    "action": f"Rotate key (age > 90 days, expires in {days} days)" if days else "Rotate key (age > 90 days)",
                })
            else:
                days = key.days_until_expiry()
                if days is not None and days <= 7:
                    issues.append({
                        "key_id": key.key_id,
                        "key_name": key.key_name,
                        "issue": "expiring_soon",
                        "severity": "medium",
                        "action": f"Key expires in {days} days",
                    })

        return issues

    def record_login(self, ip: str, success: bool = True, country: str = "",
                     city: str = "", user_agent: str = "",
                     method: str = "api") -> tuple[LoginEvent, list[SecurityAlert]]:
        """Record a login event and check for anomalies."""
        event_id = hashlib.md5(
            f"{ip}:{datetime.now(timezone.utc).isoformat()}".encode()
        ).hexdigest()[:12]

        event = LoginEvent(
            event_id=event_id,
            ip_address=ip,
            user_agent=user_agent,
            country=country,
            city=city,
            success=success,
            method=method,
        )

        # Run anomaly detection
        alerts = self.anomaly_detector.record_login(event)

        # Calculate risk score
        event.risk_score = self.anomaly_detector.get_risk_score()

        # Persist
        self.store.save_login_event(event)
        for alert in alerts:
            self.store.save_alert(alert)

        return event, alerts

    def record_rate_limit(self, endpoint: str, limit: int, remaining: int,
                          reset_at: str, status_code: int = 200) -> None:
        """Record rate limit observation."""
        self.rate_forensics.record_rate_limit(endpoint, limit, remaining, reset_at, status_code)

    def scan_for_leaks(self, text: str) -> list[dict]:
        """Scan text for potential token leaks."""
        return self.leak_scanner.scan_text(text)

    def get_security_score(self) -> dict:
        """Calculate overall account security score."""
        score = 100.0
        issues = []

        # Key health (-20 per critical, -10 per high, -5 per medium)
        key_issues = self.check_key_health()
        for ki in key_issues:
            if ki["severity"] == "critical":
                score -= 20
                issues.append(f"🔴 {ki['key_name']}: {ki['issue']}")
            elif ki["severity"] == "high":
                score -= 10
                issues.append(f"🟠 {ki['key_name']}: {ki['issue']}")
            elif ki["severity"] == "medium":
                score -= 5
                issues.append(f"🟡 {ki['key_name']}: {ki['issue']}")

        # Login risk
        risk = self.anomaly_detector.get_risk_score()
        if risk > 50:
            score -= 20
            issues.append(f"🔴 High login risk score: {risk}")
        elif risk > 25:
            score -= 10
            issues.append(f"🟠 Elevated login risk: {risk}")

        # Unresolved alerts
        unresolved = self.store.get_unresolved_alerts()
        critical_alerts = sum(1 for a in unresolved if a.threat_level == ThreatLevel.CRITICAL)
        high_alerts = sum(1 for a in unresolved if a.threat_level == ThreatLevel.HIGH)
        if critical_alerts:
            score -= critical_alerts * 15
            issues.append(f"🔴 {critical_alerts} critical unresolved alerts")
        if high_alerts:
            score -= high_alerts * 5
            issues.append(f"🟠 {high_alerts} high-severity unresolved alerts")

        # Rate limit abuse
        abuse = self.rate_forensics.detect_abuse_patterns()
        if abuse:
            score -= len(abuse) * 3
            issues.append(f"🟡 {len(abuse)} rate limit abuse patterns detected")

        score = max(score, 0)

        # Grade
        if score >= 90:
            grade = "A+"
        elif score >= 80:
            grade = "A"
        elif score >= 70:
            grade = "B"
        elif score >= 60:
            grade = "C"
        elif score >= 50:
            grade = "D"
        else:
            grade = "F"

        return {
            "score": round(score, 1),
            "grade": grade,
            "issues": issues,
            "key_count": len(self.store.list_api_keys()),
            "unresolved_alerts": len(unresolved),
            "login_risk": round(risk, 1),
            "rate_limit_patterns": len(abuse),
        }

    def generate_report(self) -> str:
        """Generate security status report."""
        security = self.get_security_score()
        login_stats = self.store.get_login_stats(30)
        key_issues = self.check_key_health()
        unresolved = self.store.get_unresolved_alerts()
        rl_suggestions = self.rate_forensics.suggest_optimizations()

        lines = [
            "🛡️ Account Security Report",
            f"{'='*45}",
            "",
            f"📊 Security Score: {security['score']}/100 ({security['grade']})",
            "",
            f"🔑 API Keys: {security['key_count']} registered",
        ]

        if key_issues:
            for ki in key_issues:
                emoji = {"critical": "🔴", "high": "🟠", "medium": "🟡"}.get(ki["severity"], "⚪")
                lines.append(f"  {emoji} {ki['key_name']}: {ki['action']}")
        else:
            lines.append("  ✅ All keys healthy")

        lines += [
            "",
            f"🔐 Login Activity ({login_stats['period_days']} days):",
            f"  Total: {login_stats['total_logins']} ({login_stats['successful']} ok, {login_stats['failed']} failed)",
            f"  Unique IPs: {login_stats['unique_ips']}",
            f"  Countries: {login_stats['unique_countries']}",
            f"  Failure Rate: {login_stats['failure_rate']}%",
            f"  Avg Risk: {login_stats['avg_risk_score']}",
        ]

        if unresolved:
            lines += ["", f"⚠️ Unresolved Alerts ({len(unresolved)}):"]
            for alert in unresolved[:5]:
                emoji = {
                    ThreatLevel.CRITICAL: "🔴",
                    ThreatLevel.HIGH: "🟠",
                    ThreatLevel.MEDIUM: "🟡",
                    ThreatLevel.LOW: "⚪",
                }.get(alert.threat_level, "⚪")
                lines.append(f"  {emoji} [{alert.alert_type.value}] {alert.title}")
        else:
            lines.append("\n✅ No unresolved security alerts")

        if rl_suggestions:
            lines += ["", "📡 Rate Limit Status:"]
            for s in rl_suggestions:
                lines.append(f"  {s}")

        if security["issues"]:
            lines += ["", "📋 Action Items:"]
            for issue in security["issues"]:
                lines.append(f"  {issue}")

        return "\n".join(lines)
