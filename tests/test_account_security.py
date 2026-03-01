"""Tests for Account Security Monitor module."""

import pytest
from datetime import datetime, timezone, timedelta

from bot.account_security import (
    ThreatLevel, AlertType, KeyStatus, APIKeyInfo, LoginEvent, SecurityAlert,
    AnomalyDetector, RateLimitForensics, TokenLeakScanner, SecurityStore,
    AccountSecurityMonitor,
)


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "test_security.db")


@pytest.fixture
def monitor(tmp_db):
    return AccountSecurityMonitor(db_path=tmp_db)


@pytest.fixture
def store(tmp_db):
    return SecurityStore(db_path=tmp_db)


class TestAPIKeyInfo:
    def test_create_key(self):
        key = APIKeyInfo(key_id="k1", key_name="test_key", key_hash="abc123")
        assert key.key_id == "k1"
        assert key.status == KeyStatus.ACTIVE

    def test_is_expired_no_expiry(self):
        key = APIKeyInfo(key_id="k1", key_name="test", key_hash="abc")
        assert key.is_expired() is False

    def test_is_expired_future(self):
        future = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
        key = APIKeyInfo(key_id="k1", key_name="test", key_hash="abc", expires_at=future)
        assert key.is_expired() is False

    def test_is_expired_past(self):
        past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        key = APIKeyInfo(key_id="k1", key_name="test", key_hash="abc", expires_at=past)
        assert key.is_expired() is True

    def test_days_until_expiry(self):
        future = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
        key = APIKeyInfo(key_id="k1", key_name="test", key_hash="abc", expires_at=future)
        days = key.days_until_expiry()
        assert 6 <= days <= 7  # Allow for timing variance

    def test_needs_rotation_new_key(self):
        key = APIKeyInfo(key_id="k1", key_name="test", key_hash="abc")
        assert key.needs_rotation(max_age_days=90) is False

    def test_needs_rotation_old_key(self):
        old = (datetime.now(timezone.utc) - timedelta(days=100)).isoformat()
        key = APIKeyInfo(key_id="k1", key_name="test", key_hash="abc", created_at=old)
        assert key.needs_rotation(max_age_days=90) is True


class TestAnomalyDetector:
    def test_known_ip_tracking(self):
        d = AnomalyDetector()
        d.add_known_ip("1.2.3.4")
        assert "1.2.3.4" in d._known_ips

    def test_record_login_success(self):
        d = AnomalyDetector()
        d.add_known_ip("1.2.3.4")
        event = LoginEvent(event_id="e1", ip_address="1.2.3.4", success=True)
        alerts = d.record_login(event)
        assert len(alerts) == 0  # Known IP, no alerts

    def test_record_login_new_ip(self):
        d = AnomalyDetector()
        d.add_known_ip("1.2.3.4")
        event = LoginEvent(event_id="e1", ip_address="5.6.7.8", success=True)
        alerts = d.record_login(event)
        assert len(alerts) >= 1
        assert any(a.alert_type == AlertType.IP_CHANGE for a in alerts)

    def test_record_login_failed(self):
        d = AnomalyDetector()
        event = LoginEvent(event_id="e1", ip_address="1.2.3.4", success=False)
        alerts = d.record_login(event)
        assert any(a.alert_type == AlertType.LOGIN_ANOMALY for a in alerts)

    def test_unusual_hours(self):
        d = AnomalyDetector()
        # Mock time to 3am UTC
        ts = datetime.now(timezone.utc).replace(hour=3, minute=0, second=0).isoformat()
        event = LoginEvent(event_id="e1", ip_address="1.2.3.4", timestamp=ts)
        alerts = d.record_login(event)
        assert any(a.alert_type == AlertType.UNUSUAL_HOURS for a in alerts)

    def test_brute_force_detection(self):
        d = AnomalyDetector()
        # Record 6 failed logins
        for i in range(6):
            event = LoginEvent(
                event_id=f"e{i}", ip_address="1.2.3.4",
                success=False,
                timestamp=(datetime.now(timezone.utc) - timedelta(minutes=i)).isoformat()
            )
            alerts = d.record_login(event)
        # Last one should trigger brute force
        assert any(a.alert_type == AlertType.BRUTE_FORCE for a in alerts)

    def test_geo_impossibility(self):
        d = AnomalyDetector()
        d.add_known_country("US")
        event1 = LoginEvent(event_id="e1", ip_address="1.2.3.4", country="US", success=True)
        d.record_login(event1)
        event2 = LoginEvent(event_id="e2", ip_address="5.6.7.8", country="CN", success=True)
        alerts = d.record_login(event2)
        assert any(a.alert_type == AlertType.GEO_IMPOSSIBLE for a in alerts)

    def test_risk_score_clean(self):
        d = AnomalyDetector()
        for i in range(10):
            event = LoginEvent(event_id=f"e{i}", ip_address="1.2.3.4", success=True, country="US")
            d.record_login(event)
        score = d.get_risk_score()
        assert score < 20  # Low risk

    def test_risk_score_high(self):
        d = AnomalyDetector()
        for i in range(10):
            event = LoginEvent(
                event_id=f"e{i}", ip_address=f"1.2.3.{i}",
                success=i % 2 == 0, country=f"C{i}"
            )
            d.record_login(event)
        score = d.get_risk_score()
        assert score > 30  # Higher risk


class TestRateLimitForensics:
    def test_record_rate_limit(self):
        f = RateLimitForensics()
        f.record_rate_limit("/tweets", 300, 250, "2026-01-01T01:00:00Z")
        assert len(f._events) == 1
        assert f._events[0]["endpoint"] == "/tweets"
        assert f._events[0]["usage_pct"] == pytest.approx(16.7, abs=0.1)

    def test_get_endpoint_stats(self):
        f = RateLimitForensics()
        for i in range(10):
            f.record_rate_limit("/tweets", 300, 250 - i * 10, "2026-01-01T01:00:00Z")
        stats = f.get_endpoint_stats()
        assert "/tweets" in stats
        assert stats["/tweets"]["total_calls"] == 10

    def test_detect_abuse_high_usage(self):
        f = RateLimitForensics()
        for i in range(10):
            f.record_rate_limit("/search", 300, 10, "2026-01-01T01:00:00Z")
        patterns = f.detect_abuse_patterns()
        assert any(p["pattern"] == "high_usage" for p in patterns)

    def test_detect_abuse_429(self):
        f = RateLimitForensics()
        for i in range(20):
            f.record_rate_limit("/post", 100, 0, "2026-01-01T01:00:00Z", status_code=429 if i < 5 else 200)
        patterns = f.detect_abuse_patterns()
        assert any(p["pattern"] == "frequent_429" for p in patterns)

    def test_detect_burst(self):
        f = RateLimitForensics()
        ts = datetime.now(timezone.utc).isoformat()
        for i in range(15):
            f._events.append({
                "endpoint": "/tweets", "limit": 300, "remaining": 250,
                "reset_at": "2026-01-01T01:00:00Z", "status_code": 200,
                "timestamp": ts, "usage_pct": 16.7,
            })
        patterns = f.detect_abuse_patterns()
        assert any(p["pattern"] == "burst" for p in patterns)

    def test_suggest_optimizations(self):
        f = RateLimitForensics()
        for i in range(10):
            f.record_rate_limit("/tweets", 300, 20, "2026-01-01T01:00:00Z")
        suggestions = f.suggest_optimizations()
        assert len(suggestions) > 0
        assert any("caching" in s.lower() or "healthy" in s.lower() for s in suggestions)


class TestTokenLeakScanner:
    def test_scan_text_clean(self):
        s = TokenLeakScanner()
        findings = s.scan_text("This is a normal tweet about cats.")
        assert len(findings) == 0

    def test_scan_twitter_bearer(self):
        s = TokenLeakScanner()
        text = "Bearer token: AAAAAAAAAAAAAAAAAAAAABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
        findings = s.scan_text(text)
        assert len(findings) >= 1
        assert any(f["pattern"] == "twitter_bearer" for f in findings)

    def test_scan_jwt(self):
        s = TokenLeakScanner()
        text = "JWT: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature"
        findings = s.scan_text(text)
        assert any(f["pattern"] == "jwt" for f in findings)

    def test_scan_env_var(self):
        s = TokenLeakScanner()
        text = 'TWITTER_API_KEY="abc123def456ghi789jkl012mno345"'
        findings = s.scan_text(text)
        assert any(f["pattern"] == "env_var" for f in findings)

    def test_scan_generic_token(self):
        s = TokenLeakScanner()
        text = 'api_key = "test_live_abcdefghijklmnopqrstuvwxyz123456"'
        findings = s.scan_text(text)
        assert len(findings) >= 1

    def test_false_positive_filter(self):
        s = TokenLeakScanner()
        text = "Authorization header application/json Content-Type"
        findings = s.scan_text(text)
        assert len(findings) == 0


class TestSecurityStore:
    def test_save_and_get_api_key(self, store):
        key = APIKeyInfo(
            key_id="k1", key_name="prod_key", key_hash="hash123",
            permissions=["read", "write"],
        )
        store.save_api_key(key)
        loaded = store.get_api_key("k1")
        assert loaded is not None
        assert loaded.key_name == "prod_key"
        assert loaded.permissions == ["read", "write"]

    def test_list_api_keys(self, store):
        for i in range(3):
            store.save_api_key(APIKeyInfo(
                key_id=f"k{i}", key_name=f"key{i}", key_hash=f"hash{i}",
            ))
        keys = store.list_api_keys()
        assert len(keys) == 3

    def test_save_login_event(self, store):
        event = LoginEvent(
            event_id="e1", ip_address="1.2.3.4", country="US", success=True,
        )
        store.save_login_event(event)
        stats = store.get_login_stats(30)
        assert stats["total_logins"] == 1

    def test_save_and_get_alert(self, store):
        alert = SecurityAlert(
            alert_id="a1", alert_type=AlertType.IP_CHANGE,
            threat_level=ThreatLevel.MEDIUM, title="New IP", description="Test",
        )
        store.save_alert(alert)
        unresolved = store.get_unresolved_alerts()
        assert len(unresolved) == 1

    def test_acknowledge_alert(self, store):
        alert = SecurityAlert(
            alert_id="a1", alert_type=AlertType.LOGIN_ANOMALY,
            threat_level=ThreatLevel.LOW, title="Test", description="Test",
        )
        store.save_alert(alert)
        assert store.acknowledge_alert("a1") is True
        # Re-fetch won't filter acknowledged (still unresolved)

    def test_resolve_alert(self, store):
        alert = SecurityAlert(
            alert_id="a1", alert_type=AlertType.IP_CHANGE,
            threat_level=ThreatLevel.MEDIUM, title="Test", description="Test",
        )
        store.save_alert(alert)
        assert store.resolve_alert("a1") is True
        unresolved = store.get_unresolved_alerts()
        assert len(unresolved) == 0

    def test_login_stats(self, store):
        for i in range(10):
            store.save_login_event(LoginEvent(
                event_id=f"e{i}", ip_address=f"1.2.3.{i}",
                country="US", success=i % 2 == 0,
            ))
        stats = store.get_login_stats(30)
        assert stats["total_logins"] == 10
        assert stats["successful"] == 5
        assert stats["failed"] == 5
        assert stats["failure_rate"] == 50.0


class TestAccountSecurityMonitor:
    def test_register_api_key(self, monitor):
        key = monitor.register_api_key("test_key", "secret_abc123",
                                        permissions=["read", "write"])
        assert key.key_id
        assert key.key_name == "test_key"

    def test_rotate_api_key(self, monitor):
        key = monitor.register_api_key("test_key", "old_secret")
        rotated = monitor.rotate_api_key(key.key_id, "new_secret")
        assert rotated is not None
        assert rotated.rotation_count == 1
        assert rotated.last_rotated is not None

    def test_check_key_health_expired(self, monitor):
        past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        key = monitor.register_api_key("expired_key", "secret", expires_at=past)
        issues = monitor.check_key_health()
        assert len(issues) >= 1
        assert any(i["issue"] == "expired" for i in issues)

    def test_check_key_health_needs_rotation(self, monitor):
        old = (datetime.now(timezone.utc) - timedelta(days=100)).isoformat()
        key_info = APIKeyInfo(
            key_id="old_key", key_name="old", key_hash="hash",
            created_at=old,
        )
        monitor.store.save_api_key(key_info)
        issues = monitor.check_key_health()
        assert any(i["issue"] == "needs_rotation" for i in issues)

    def test_record_login(self, monitor):
        event, alerts = monitor.record_login("1.2.3.4", success=True, country="US")
        assert event.event_id
        assert event.risk_score >= 0

    def test_record_login_failed(self, monitor):
        event, alerts = monitor.record_login("1.2.3.4", success=False)
        assert len(alerts) >= 1
        assert any(a.alert_type == AlertType.LOGIN_ANOMALY for a in alerts)

    def test_record_rate_limit(self, monitor):
        monitor.record_rate_limit("/tweets", 300, 250, "2026-01-01T01:00:00Z")
        stats = monitor.rate_forensics.get_endpoint_stats()
        assert "/tweets" in stats

    def test_scan_for_leaks(self, monitor):
        text = "My token is AAAAAAAAAAAAAAAAAAAAABCDEFGHIJKLMNOPQRSTUVWXYZ123456"
        findings = monitor.scan_for_leaks(text)
        assert len(findings) >= 1

    def test_get_security_score_clean(self, monitor):
        score_data = monitor.get_security_score()
        assert score_data["score"] >= 80  # New account, clean
        assert score_data["grade"] in ["A+", "A", "B", "C", "D", "F"]

    def test_get_security_score_with_issues(self, monitor):
        # Add expired key
        past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        monitor.register_api_key("expired", "secret", expires_at=past)
        # Add failed logins
        for i in range(5):
            monitor.record_login(f"1.2.3.{i}", success=False)
        score_data = monitor.get_security_score()
        assert score_data["score"] < 100
        assert len(score_data["issues"]) > 0

    def test_generate_report(self, monitor):
        monitor.register_api_key("test_key", "secret")
        monitor.record_login("1.2.3.4", success=True, country="US")
        report = monitor.generate_report()
        assert "Security Report" in report
        assert "Security Score" in report
        assert "API Keys" in report

    def test_full_flow(self, monitor):
        """Integration test: full security monitoring flow."""
        # 1. Register keys
        key1 = monitor.register_api_key("prod_key", "prod_secret",
                                         permissions=["read", "write", "admin"])
        
        # 2. Record logins
        for i in range(5):
            monitor.record_login(f"1.2.3.{i}", success=True, country="US")
        
        # 3. Record a failed login
        event, alerts = monitor.record_login("9.9.9.9", success=False, country="CN")
        assert len(alerts) >= 1
        
        # 4. Record rate limits
        for i in range(10):
            monitor.record_rate_limit("/search", 300, 250 - i * 10, "2026-01-01T01:00:00Z")
        
        # 5. Check health
        issues = monitor.check_key_health()
        
        # 6. Get security score
        score = monitor.get_security_score()
        assert 0 <= score["score"] <= 100
        
        # 7. Generate report
        report = monitor.generate_report()
        assert "Security Report" in report
