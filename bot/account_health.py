"""
Account Health & Safety Monitor for Twitter/X

Comprehensive account health monitoring and risk management:
- Shadow ban detection (ghost ban, search ban, reply deboosting)
- Rate limit tracking and API quota management
- Account standing assessment (based on content signals)
- Suspension risk scoring (0-100)
- Content policy violation scanner
- API endpoint health tracking
- Account age and trust score
- Engagement anomaly detection (bot-like behavior flags)
- Recovery recommendations
- Health history with trend analysis
"""

import json
import re
import sqlite3
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional


class BanType(str, Enum):
    """Types of Twitter shadow bans."""
    NONE = "none"
    GHOST_BAN = "ghost_ban"         # Tweets invisible to others
    SEARCH_BAN = "search_ban"       # Not appearing in search results
    REPLY_DEBOOSTING = "reply_deboosting"  # Replies hidden behind "Show more"
    THREAD_BAN = "thread_ban"       # Threads not showing in conversations
    SENSITIVE_FLAG = "sensitive_flag"  # Account flagged as sensitive


class RiskLevel(str, Enum):
    """Account suspension risk levels."""
    SAFE = "safe"           # 0-20
    LOW = "low"             # 21-40
    MODERATE = "moderate"   # 41-60
    HIGH = "high"           # 61-80
    CRITICAL = "critical"   # 81-100


class HealthCategory(str, Enum):
    """Health assessment categories."""
    EXCELLENT = "excellent"   # 90-100
    GOOD = "good"             # 70-89
    FAIR = "fair"             # 50-69
    POOR = "poor"             # 30-49
    CRITICAL = "critical"     # 0-29


class ViolationType(str, Enum):
    """Content policy violation types."""
    SPAM = "spam"
    HARASSMENT = "harassment"
    HATE_SPEECH = "hate_speech"
    MANIPULATION = "manipulation"
    MISLEADING = "misleading"
    SENSITIVE_MEDIA = "sensitive_media"
    COPYRIGHT = "copyright"
    AUTOMATED_BEHAVIOR = "automated_behavior"
    PLATFORM_MANIPULATION = "platform_manipulation"


class EndpointStatus(str, Enum):
    """API endpoint health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    RATE_LIMITED = "rate_limited"
    DOWN = "down"
    UNKNOWN = "unknown"


# Content patterns that may trigger policy violations
VIOLATION_PATTERNS = {
    ViolationType.SPAM: [
        r'(?i)(buy now|click here|limited offer|act now|free money)',
        r'(?i)(dm me for|send \$|cashapp|paypal me)',
        r'(?i)(follow for follow|f4f|like4like|l4l)',
        r'(.)\1{5,}',  # Repeated characters
    ],
    ViolationType.HARASSMENT: [
        r'(?i)(kill yourself|kys|go die|neck yourself)',
        r'(?i)(you\'re? (stupid|dumb|idiot|trash|garbage))',
    ],
    ViolationType.MANIPULATION: [
        r'(?i)(retweet to win|rt to enter|follow .* giveaway)',
        r'(?i)(engagement pod|boost group|comment chain)',
    ],
    ViolationType.AUTOMATED_BEHAVIOR: [
        # These are pattern flags, actual detection uses behavior analysis
    ],
}

# Rate limits per endpoint (Twitter API v2)
API_RATE_LIMITS = {
    "tweets/create": {"limit": 200, "window_minutes": 15},
    "tweets/search": {"limit": 450, "window_minutes": 15},
    "users/lookup": {"limit": 300, "window_minutes": 15},
    "follows/create": {"limit": 15, "window_minutes": 15},
    "likes/create": {"limit": 50, "window_minutes": 15},
    "dm/create": {"limit": 200, "window_minutes": 1440},
    "lists/create": {"limit": 300, "window_minutes": 15},
    "retweets/create": {"limit": 300, "window_minutes": 180},
}


@dataclass
class RateLimitState:
    """Current rate limit state for an endpoint."""
    endpoint: str
    limit: int
    remaining: int
    reset_at: str
    window_minutes: int
    utilization_pct: float = 0.0
    status: EndpointStatus = EndpointStatus.HEALTHY

    def __post_init__(self):
        used = self.limit - self.remaining
        self.utilization_pct = round(used / max(self.limit, 1) * 100, 1)
        if self.remaining <= 0:
            self.status = EndpointStatus.RATE_LIMITED
        elif self.utilization_pct > 80:
            self.status = EndpointStatus.DEGRADED


@dataclass
class ShadowBanResult:
    """Shadow ban check result."""
    is_banned: bool = False
    ban_types: List[BanType] = field(default_factory=list)
    confidence: float = 0.0
    evidence: List[str] = field(default_factory=list)
    checked_at: str = ""
    recommendations: List[str] = field(default_factory=list)

    def __post_init__(self):
        if not self.checked_at:
            self.checked_at = datetime.now(timezone.utc).isoformat()


@dataclass
class ViolationReport:
    """Content violation scan result."""
    text: str
    violations: List[Dict[str, Any]] = field(default_factory=list)
    risk_score: float = 0.0
    safe: bool = True
    scanned_at: str = ""

    def __post_init__(self):
        if not self.scanned_at:
            self.scanned_at = datetime.now(timezone.utc).isoformat()
        self.safe = len(self.violations) == 0


@dataclass
class HealthReport:
    """Comprehensive account health report."""
    account_id: str = ""
    health_score: float = 100.0
    category: HealthCategory = HealthCategory.EXCELLENT
    risk_level: RiskLevel = RiskLevel.SAFE
    suspension_risk: float = 0.0
    shadow_ban: Optional[ShadowBanResult] = None
    rate_limits: List[RateLimitState] = field(default_factory=list)
    violations_found: int = 0
    warnings: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    engagement_health: Dict[str, Any] = field(default_factory=dict)
    generated_at: str = ""

    def __post_init__(self):
        if not self.generated_at:
            self.generated_at = datetime.now(timezone.utc).isoformat()
        self._update_category()

    def _update_category(self):
        if self.health_score >= 90:
            self.category = HealthCategory.EXCELLENT
        elif self.health_score >= 70:
            self.category = HealthCategory.GOOD
        elif self.health_score >= 50:
            self.category = HealthCategory.FAIR
        elif self.health_score >= 30:
            self.category = HealthCategory.POOR
        else:
            self.category = HealthCategory.CRITICAL


class ContentPolicyScanner:
    """Scan content for potential policy violations."""

    @classmethod
    def scan_text(cls, text: str) -> ViolationReport:
        """Scan text for policy violations."""
        report = ViolationReport(text=text)

        for vtype, patterns in VIOLATION_PATTERNS.items():
            for pattern in patterns:
                matches = re.findall(pattern, text)
                if matches:
                    report.violations.append({
                        "type": vtype.value,
                        "pattern": pattern,
                        "matches": [str(m) for m in matches[:3]],
                        "severity": cls._violation_severity(vtype),
                    })

        # Calculate risk score
        if report.violations:
            severities = [v["severity"] for v in report.violations]
            report.risk_score = min(100.0,
                                     sum(severities) / len(severities) * len(severities) * 15)
            report.safe = False

        return report

    @classmethod
    def scan_batch(cls, texts: List[str]) -> Dict[str, Any]:
        """Scan multiple texts and return aggregate report."""
        reports = [cls.scan_text(t) for t in texts]
        unsafe = [r for r in reports if not r.safe]

        return {
            "total_scanned": len(texts),
            "safe_count": len(texts) - len(unsafe),
            "unsafe_count": len(unsafe),
            "violation_breakdown": cls._aggregate_violations(reports),
            "avg_risk_score": round(
                sum(r.risk_score for r in reports) / max(len(reports), 1), 1
            ),
            "highest_risk_text": max(reports, key=lambda r: r.risk_score).text if reports else None,
        }

    @staticmethod
    def _violation_severity(vtype: ViolationType) -> float:
        """Severity score for violation type (0-10)."""
        severity_map = {
            ViolationType.SPAM: 4.0,
            ViolationType.HARASSMENT: 8.0,
            ViolationType.HATE_SPEECH: 9.0,
            ViolationType.MANIPULATION: 5.0,
            ViolationType.MISLEADING: 6.0,
            ViolationType.SENSITIVE_MEDIA: 5.0,
            ViolationType.COPYRIGHT: 7.0,
            ViolationType.AUTOMATED_BEHAVIOR: 6.0,
            ViolationType.PLATFORM_MANIPULATION: 7.0,
        }
        return severity_map.get(vtype, 5.0)

    @staticmethod
    def _aggregate_violations(reports: List[ViolationReport]) -> Dict[str, int]:
        """Count violations by type across reports."""
        counts: Dict[str, int] = {}
        for report in reports:
            for v in report.violations:
                vtype = v["type"]
                counts[vtype] = counts.get(vtype, 0) + 1
        return counts


class BehaviorAnalyzer:
    """Analyze account behavior for bot-like patterns."""

    @classmethod
    def analyze_posting_pattern(cls, timestamps: List[str]) -> Dict[str, Any]:
        """Analyze posting timestamps for suspicious patterns."""
        if len(timestamps) < 3:
            return {"suspicious": False, "reason": "insufficient_data", "score": 0.0}

        # Parse timestamps
        times = []
        for ts in timestamps:
            try:
                if isinstance(ts, str):
                    dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                else:
                    dt = ts
                times.append(dt)
            except (ValueError, TypeError):
                continue

        if len(times) < 3:
            return {"suspicious": False, "reason": "parse_error", "score": 0.0}

        times.sort()
        intervals = [(times[i + 1] - times[i]).total_seconds()
                      for i in range(len(times) - 1)]

        flags = []
        score = 0.0

        # Check 1: Perfectly regular intervals (bot signature)
        if len(intervals) >= 5:
            avg = sum(intervals) / len(intervals)
            if avg > 0:
                variance = sum((i - avg) ** 2 for i in intervals) / len(intervals)
                cv = (variance ** 0.5) / avg  # Coefficient of variation
                if cv < 0.05:
                    flags.append("perfectly_regular_intervals")
                    score += 40.0

        # Check 2: Posting during unusual hours consistently
        hours = [t.hour for t in times]
        night_posts = sum(1 for h in hours if 2 <= h <= 5)
        night_ratio = night_posts / max(len(hours), 1)
        if night_ratio > 0.5 and len(hours) > 10:
            flags.append("excessive_night_posting")
            score += 20.0

        # Check 3: Burst posting (many tweets in short window)
        for i in range(len(intervals)):
            if intervals[i] < 10:  # Less than 10 seconds apart
                flags.append("burst_posting")
                score += 15.0
                break

        # Check 4: No engagement variation (same action patterns)
        if len(set(intervals)) == 1 and len(intervals) > 3:
            flags.append("identical_intervals")
            score += 25.0

        return {
            "suspicious": score > 30,
            "flags": flags,
            "score": min(100.0, score),
            "total_posts": len(times),
            "avg_interval_seconds": round(sum(intervals) / max(len(intervals), 1), 1),
            "posting_hours_distribution": cls._hour_distribution(times),
        }

    @classmethod
    def analyze_engagement_ratio(cls, followers: int, following: int,
                                  avg_likes: float, avg_retweets: float) -> Dict[str, Any]:
        """Analyze engagement ratios for health signals."""
        follow_ratio = following / max(followers, 1)
        engagement_rate = (avg_likes + avg_retweets) / max(followers, 1) * 100

        flags = []
        health_score = 100.0

        # High following/follower ratio
        if follow_ratio > 3.0 and following > 500:
            flags.append("high_follow_ratio")
            health_score -= 20

        # Very low engagement rate
        if engagement_rate < 0.1 and followers > 1000:
            flags.append("very_low_engagement")
            health_score -= 25

        # Suspicious engagement rate (too high = bought engagement)
        if engagement_rate > 20 and followers > 1000:
            flags.append("suspiciously_high_engagement")
            health_score -= 15

        return {
            "follow_ratio": round(follow_ratio, 2),
            "engagement_rate": round(engagement_rate, 3),
            "flags": flags,
            "health_score": max(0.0, health_score),
            "assessment": "healthy" if not flags else "needs_attention",
        }

    @staticmethod
    def _hour_distribution(times: list) -> Dict[str, int]:
        """Count posts by hour bucket."""
        buckets = {"morning_6_12": 0, "afternoon_12_18": 0,
                   "evening_18_24": 0, "night_0_6": 0}
        for t in times:
            h = t.hour
            if 6 <= h < 12:
                buckets["morning_6_12"] += 1
            elif 12 <= h < 18:
                buckets["afternoon_12_18"] += 1
            elif 18 <= h < 24:
                buckets["evening_18_24"] += 1
            else:
                buckets["night_0_6"] += 1
        return buckets


class RateLimitTracker:
    """Track and manage API rate limits."""

    def __init__(self):
        self._limits: Dict[str, RateLimitState] = {}
        self._history: List[Dict[str, Any]] = []

    def update(self, endpoint: str, remaining: int,
               reset_at: Optional[str] = None) -> RateLimitState:
        """Update rate limit state for an endpoint."""
        defaults = API_RATE_LIMITS.get(endpoint, {"limit": 100, "window_minutes": 15})
        if not reset_at:
            reset_at = (datetime.now(timezone.utc) +
                        timedelta(minutes=defaults["window_minutes"])).isoformat()

        state = RateLimitState(
            endpoint=endpoint,
            limit=defaults["limit"],
            remaining=remaining,
            reset_at=reset_at,
            window_minutes=defaults["window_minutes"],
        )
        self._limits[endpoint] = state

        self._history.append({
            "endpoint": endpoint,
            "remaining": remaining,
            "utilization_pct": state.utilization_pct,
            "status": state.status.value,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        return state

    def get_state(self, endpoint: str) -> Optional[RateLimitState]:
        """Get current rate limit state for endpoint."""
        return self._limits.get(endpoint)

    def get_all_states(self) -> Dict[str, RateLimitState]:
        """Get all tracked rate limit states."""
        return dict(self._limits)

    def can_request(self, endpoint: str) -> bool:
        """Check if a request can be made to endpoint."""
        state = self._limits.get(endpoint)
        if not state:
            return True
        if state.remaining <= 0:
            # Check if window has reset
            try:
                reset = datetime.fromisoformat(state.reset_at.replace('Z', '+00:00'))
                if datetime.now(timezone.utc) > reset:
                    return True
            except (ValueError, TypeError):
                pass
            return False
        return True

    def quota_summary(self) -> Dict[str, Any]:
        """Get summary of API quota usage."""
        total_endpoints = len(self._limits)
        rate_limited = sum(1 for s in self._limits.values()
                           if s.status == EndpointStatus.RATE_LIMITED)
        degraded = sum(1 for s in self._limits.values()
                       if s.status == EndpointStatus.DEGRADED)

        return {
            "total_endpoints_tracked": total_endpoints,
            "healthy": total_endpoints - rate_limited - degraded,
            "degraded": degraded,
            "rate_limited": rate_limited,
            "overall_status": (
                "critical" if rate_limited > total_endpoints * 0.5 else
                "degraded" if rate_limited > 0 or degraded > 2 else
                "healthy"
            ),
            "endpoints": {
                name: {
                    "remaining": state.remaining,
                    "limit": state.limit,
                    "utilization": state.utilization_pct,
                    "status": state.status.value,
                }
                for name, state in self._limits.items()
            },
        }

    def get_history(self, endpoint: Optional[str] = None,
                    limit: int = 50) -> List[Dict[str, Any]]:
        """Get rate limit history."""
        history = self._history
        if endpoint:
            history = [h for h in history if h["endpoint"] == endpoint]
        return history[-limit:]


class ShadowBanDetector:
    """Detect various forms of Twitter shadow banning."""

    @classmethod
    def check(cls, account_data: Dict[str, Any],
              tweet_data: Optional[List[Dict]] = None) -> ShadowBanResult:
        """
        Perform shadow ban check based on available signals.

        account_data should include:
        - search_visible: bool (can tweets be found in search)
        - replies_visible: bool (are replies shown normally)
        - profile_visible: bool (is profile accessible)
        - engagement_drop_pct: float (recent engagement change)
        - impressions_drop_pct: float (recent impression change)
        """
        result = ShadowBanResult()
        ban_types = []
        evidence = []
        confidence = 0.0

        # Check search visibility
        if not account_data.get("search_visible", True):
            ban_types.append(BanType.SEARCH_BAN)
            evidence.append("Tweets not appearing in search results")
            confidence += 0.3

        # Check reply visibility
        if not account_data.get("replies_visible", True):
            ban_types.append(BanType.REPLY_DEBOOSTING)
            evidence.append("Replies hidden behind 'Show more replies'")
            confidence += 0.25

        # Check profile visibility
        if not account_data.get("profile_visible", True):
            ban_types.append(BanType.GHOST_BAN)
            evidence.append("Profile not accessible to logged-out users")
            confidence += 0.35

        # Engagement drop analysis
        eng_drop = account_data.get("engagement_drop_pct", 0)
        if eng_drop > 50:
            evidence.append(f"Engagement dropped {eng_drop}% suddenly")
            confidence += 0.15
            if eng_drop > 80:
                ban_types.append(BanType.GHOST_BAN)
                confidence += 0.2

        # Impression drop analysis
        imp_drop = account_data.get("impressions_drop_pct", 0)
        if imp_drop > 60:
            evidence.append(f"Impressions dropped {imp_drop}%")
            confidence += 0.15

        # Sensitive flag
        if account_data.get("marked_sensitive", False):
            ban_types.append(BanType.SENSITIVE_FLAG)
            evidence.append("Account marked as containing sensitive content")
            confidence += 0.1

        result.is_banned = len(ban_types) > 0
        result.ban_types = ban_types
        result.confidence = min(1.0, confidence)
        result.evidence = evidence

        # Generate recommendations
        if result.is_banned:
            result.recommendations = cls._generate_recommendations(ban_types)

        return result

    @staticmethod
    def _generate_recommendations(ban_types: List[BanType]) -> List[str]:
        """Generate recovery recommendations based on ban types."""
        recs = []

        if BanType.SEARCH_BAN in ban_types:
            recs.extend([
                "Reduce posting frequency for 24-48 hours",
                "Avoid using flagged hashtags",
                "Remove any tweets with reported content",
            ])

        if BanType.REPLY_DEBOOSTING in ban_types:
            recs.extend([
                "Stop mass-replying to popular tweets",
                "Engage more naturally with fewer, quality replies",
                "Avoid repetitive or similar reply content",
            ])

        if BanType.GHOST_BAN in ban_types:
            recs.extend([
                "Contact Twitter support for account review",
                "Review recent content for policy violations",
                "Consider a 48-72 hour posting break",
                "Verify email and phone number on account",
            ])

        if BanType.SENSITIVE_FLAG in ban_types:
            recs.extend([
                "Review media content for NSFW flags",
                "Update profile to remove sensitive content markers",
                "Go to Settings > Privacy > uncheck 'Mark media as sensitive'",
            ])

        return recs


class AccountHealthMonitor:
    """Main account health monitoring engine."""

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.policy_scanner = ContentPolicyScanner()
        self.behavior_analyzer = BehaviorAnalyzer()
        self.rate_tracker = RateLimitTracker()
        self.shadow_detector = ShadowBanDetector()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self):
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS health_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                health_score REAL,
                category TEXT,
                risk_level TEXT,
                suspension_risk REAL,
                is_shadow_banned INTEGER DEFAULT 0,
                violations_found INTEGER DEFAULT 0,
                warnings TEXT DEFAULT '[]',
                recommendations TEXT DEFAULT '[]',
                report_json TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS violation_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                text TEXT,
                violation_type TEXT,
                risk_score REAL,
                details TEXT DEFAULT '{}',
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS rate_limit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                endpoint TEXT NOT NULL,
                remaining INTEGER,
                limit_val INTEGER,
                utilization_pct REAL,
                status TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_health_account
                ON health_snapshots(account_id);
            CREATE INDEX IF NOT EXISTS idx_health_created
                ON health_snapshots(created_at);
            CREATE INDEX IF NOT EXISTS idx_violations_account
                ON violation_log(account_id);
        """)
        conn.commit()
        conn.close()

    def full_health_check(self, account_id: str,
                          account_data: Dict[str, Any],
                          recent_tweets: Optional[List[str]] = None,
                          posting_timestamps: Optional[List[str]] = None) -> HealthReport:
        """Run comprehensive health check."""
        report = HealthReport(account_id=account_id)
        health_score = 100.0
        warnings = []
        recommendations = []

        # 1. Shadow ban check
        shadow_result = self.shadow_detector.check(account_data)
        report.shadow_ban = shadow_result
        if shadow_result.is_banned:
            health_score -= 30
            warnings.append(f"Shadow ban detected: {', '.join(b.value for b in shadow_result.ban_types)}")
            recommendations.extend(shadow_result.recommendations)

        # 2. Content policy scan
        if recent_tweets:
            batch_result = self.policy_scanner.scan_batch(recent_tweets)
            report.violations_found = batch_result["unsafe_count"]
            if batch_result["unsafe_count"] > 0:
                penalty = min(30, batch_result["unsafe_count"] * 5)
                health_score -= penalty
                warnings.append(f"{batch_result['unsafe_count']} tweets flagged for policy violations")
                recommendations.append("Review and edit flagged content to reduce risk")

        # 3. Behavior analysis
        if posting_timestamps:
            behavior = self.behavior_analyzer.analyze_posting_pattern(posting_timestamps)
            if behavior.get("suspicious"):
                health_score -= 20
                warnings.append(f"Suspicious posting pattern detected: {', '.join(behavior.get('flags', []))}")
                recommendations.append("Vary posting times and intervals to appear more natural")
            report.engagement_health["posting_pattern"] = behavior

        # 4. Engagement ratio analysis
        if all(k in account_data for k in ["followers", "following"]):
            eng_ratio = self.behavior_analyzer.analyze_engagement_ratio(
                account_data.get("followers", 0),
                account_data.get("following", 0),
                account_data.get("avg_likes", 0),
                account_data.get("avg_retweets", 0),
            )
            if eng_ratio["flags"]:
                health_score -= 10
                for flag in eng_ratio["flags"]:
                    warnings.append(f"Engagement flag: {flag}")
            report.engagement_health["engagement_ratio"] = eng_ratio

        # 5. Rate limit status
        quota = self.rate_tracker.quota_summary()
        report.rate_limits = list(self.rate_tracker.get_all_states().values())
        if quota["rate_limited"] > 0:
            health_score -= 10
            warnings.append(f"{quota['rate_limited']} API endpoints rate limited")

        # 6. Account trust signals
        account_age_days = account_data.get("account_age_days", 365)
        if account_age_days < 30:
            health_score -= 15
            warnings.append("New account (<30 days). Higher suspension risk during probation.")
        elif account_age_days < 90:
            health_score -= 5
            warnings.append("Account still building trust (<90 days)")

        verified = account_data.get("verified", False)
        if verified:
            health_score = min(health_score + 10, 100)

        # Finalize report
        report.health_score = max(0.0, min(100.0, health_score))
        report.warnings = warnings
        report.recommendations = recommendations

        # Calculate suspension risk
        report.suspension_risk = cls_suspension_risk(report)
        report.risk_level = cls_risk_level(report.suspension_risk)

        # Save to DB
        self._save_snapshot(report)

        return report

    def scan_content(self, text: str, account_id: str = "") -> ViolationReport:
        """Scan content for policy violations."""
        report = self.policy_scanner.scan_text(text)

        if not report.safe and account_id:
            conn = self._get_conn()
            for v in report.violations:
                conn.execute("""
                    INSERT INTO violation_log
                    (account_id, text, violation_type, risk_score, details)
                    VALUES (?, ?, ?, ?, ?)
                """, (account_id, text, v["type"], report.risk_score, json.dumps(v)))
            conn.commit()
            conn.close()

        return report

    def update_rate_limit(self, endpoint: str, remaining: int,
                          reset_at: Optional[str] = None) -> RateLimitState:
        """Update rate limit for an endpoint."""
        state = self.rate_tracker.update(endpoint, remaining, reset_at)

        conn = self._get_conn()
        conn.execute("""
            INSERT INTO rate_limit_log
            (endpoint, remaining, limit_val, utilization_pct, status)
            VALUES (?, ?, ?, ?, ?)
        """, (endpoint, state.remaining, state.limit,
              state.utilization_pct, state.status.value))
        conn.commit()
        conn.close()

        return state

    def get_health_history(self, account_id: str,
                           limit: int = 30) -> List[Dict[str, Any]]:
        """Get health check history for account."""
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT * FROM health_snapshots
            WHERE account_id = ?
            ORDER BY created_at DESC LIMIT ?
        """, (account_id, limit)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_health_trend(self, account_id: str,
                         days: int = 7) -> Dict[str, Any]:
        """Get health score trend over time."""
        conn = self._get_conn()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = conn.execute("""
            SELECT health_score, category, suspension_risk, created_at
            FROM health_snapshots
            WHERE account_id = ? AND created_at > ?
            ORDER BY created_at ASC
        """, (account_id, cutoff)).fetchall()
        conn.close()

        if not rows:
            return {"account_id": account_id, "data_points": 0, "trend": "unknown"}

        scores = [r["health_score"] for r in rows]
        avg = sum(scores) / len(scores)

        # Determine trend direction
        if len(scores) >= 2:
            first_half = scores[:len(scores) // 2]
            second_half = scores[len(scores) // 2:]
            first_avg = sum(first_half) / len(first_half)
            second_avg = sum(second_half) / len(second_half)

            if second_avg > first_avg + 5:
                trend = "improving"
            elif second_avg < first_avg - 5:
                trend = "declining"
            else:
                trend = "stable"
        else:
            trend = "insufficient_data"

        return {
            "account_id": account_id,
            "period_days": days,
            "data_points": len(scores),
            "current_score": scores[-1],
            "average_score": round(avg, 1),
            "min_score": min(scores),
            "max_score": max(scores),
            "trend": trend,
        }

    def get_violation_history(self, account_id: str,
                              limit: int = 50) -> List[Dict[str, Any]]:
        """Get content violation history."""
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT * FROM violation_log
            WHERE account_id = ?
            ORDER BY created_at DESC LIMIT ?
        """, (account_id, limit)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def _save_snapshot(self, report: HealthReport):
        """Save health snapshot to database."""
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO health_snapshots
            (account_id, health_score, category, risk_level, suspension_risk,
             is_shadow_banned, violations_found, warnings, recommendations, report_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            report.account_id, report.health_score, report.category.value,
            report.risk_level.value, report.suspension_risk,
            int(report.shadow_ban.is_banned if report.shadow_ban else False),
            report.violations_found,
            json.dumps(report.warnings),
            json.dumps(report.recommendations),
            json.dumps(asdict(report), default=str),
        ))
        conn.commit()
        conn.close()


def cls_suspension_risk(report: HealthReport) -> float:
    """Calculate suspension risk score (0-100)."""
    risk = 0.0

    # Shadow ban is a strong signal
    if report.shadow_ban and report.shadow_ban.is_banned:
        risk += 30 * report.shadow_ban.confidence

    # Violations add risk
    risk += min(30, report.violations_found * 8)

    # Low health score indicates elevated risk
    if report.health_score < 50:
        risk += (50 - report.health_score) * 0.5

    # Warnings count
    risk += min(15, len(report.warnings) * 3)

    return min(100.0, round(risk, 1))


def cls_risk_level(suspension_risk: float) -> RiskLevel:
    """Determine risk level from suspension risk score."""
    if suspension_risk <= 20:
        return RiskLevel.SAFE
    elif suspension_risk <= 40:
        return RiskLevel.LOW
    elif suspension_risk <= 60:
        return RiskLevel.MODERATE
    elif suspension_risk <= 80:
        return RiskLevel.HIGH
    else:
        return RiskLevel.CRITICAL
