"""Tests for link_intelligence module"""

import json
import os
import tempfile
import pytest

from bot.link_intelligence import (
    ClickEvent, LinkAnalytics, LinkIntelligence, LinkStatus,
    LinkStore, ShortCodeGenerator, TrackedLink, UTMBuilder,
    UTMMedium, UTMParams, UTMSource,
)


# ── UTMParams ──

class TestUTMParams:
    def test_to_dict_full(self):
        utm = UTMParams(
            source="twitter", medium="social",
            campaign="launch", term="ai", content="cta1"
        )
        d = utm.to_dict()
        assert d["utm_source"] == "twitter"
        assert d["utm_medium"] == "social"
        assert len(d) == 5

    def test_to_dict_partial(self):
        utm = UTMParams(source="twitter")
        d = utm.to_dict()
        assert len(d) == 1
        assert "utm_source" in d

    def test_to_dict_empty(self):
        utm = UTMParams()
        assert utm.to_dict() == {}

    def test_from_url(self):
        url = "https://example.com?utm_source=twitter&utm_medium=social&utm_campaign=test"
        utm = UTMParams.from_url(url)
        assert utm.source == "twitter"
        assert utm.medium == "social"
        assert utm.campaign == "test"

    def test_from_url_no_utm(self):
        utm = UTMParams.from_url("https://example.com")
        assert utm.source == ""

    def test_twitter_default(self):
        utm = UTMParams.twitter_default(campaign="launch")
        assert utm.source == "twitter"
        assert utm.medium == "social"
        assert utm.campaign == "launch"

    def test_twitter_default_with_content(self):
        utm = UTMParams.twitter_default(content="link_1")
        assert utm.content == "link_1"


# ── ShortCodeGenerator ──

class TestShortCodeGenerator:
    def test_generate_length(self):
        code = ShortCodeGenerator.generate(length=8)
        assert len(code) == 8

    def test_generate_default_length(self):
        code = ShortCodeGenerator.generate()
        assert len(code) == 6

    def test_generate_with_seed(self):
        code1 = ShortCodeGenerator.generate(seed="test123")
        code2 = ShortCodeGenerator.generate(seed="test123")
        assert code1 == code2

    def test_generate_different_seeds(self):
        code1 = ShortCodeGenerator.generate(seed="a")
        code2 = ShortCodeGenerator.generate(seed="b")
        assert code1 != code2

    def test_from_url_deterministic(self):
        url = "https://example.com/page"
        code1 = ShortCodeGenerator.from_url(url)
        code2 = ShortCodeGenerator.from_url(url)
        assert code1 == code2

    def test_from_url_different(self):
        code1 = ShortCodeGenerator.from_url("https://a.com")
        code2 = ShortCodeGenerator.from_url("https://b.com")
        assert code1 != code2

    def test_is_valid(self):
        assert ShortCodeGenerator.is_valid("abc123") is True
        assert ShortCodeGenerator.is_valid("AbCdEf") is True

    def test_is_valid_empty(self):
        assert ShortCodeGenerator.is_valid("") is False

    def test_is_valid_special_chars(self):
        assert ShortCodeGenerator.is_valid("ab-cd") is False
        assert ShortCodeGenerator.is_valid("ab cd") is False


# ── UTMBuilder ──

class TestUTMBuilder:
    def test_build_url(self):
        utm = UTMParams(source="twitter", medium="social")
        url = UTMBuilder.build_url("https://example.com", utm)
        assert "utm_source=twitter" in url
        assert "utm_medium=social" in url

    def test_build_url_preserves_existing_params(self):
        utm = UTMParams(source="twitter")
        url = UTMBuilder.build_url("https://example.com?page=1", utm)
        assert "page=1" in url
        assert "utm_source=twitter" in url

    def test_strip_utm(self):
        url = "https://example.com?utm_source=twitter&page=1&utm_medium=social"
        result = UTMBuilder.strip_utm(url)
        assert "utm_source" not in result
        assert "utm_medium" not in result
        assert "page=1" in result

    def test_strip_utm_no_utm(self):
        url = "https://example.com?page=1"
        result = UTMBuilder.strip_utm(url)
        assert "page=1" in result

    def test_strip_utm_only_utm(self):
        url = "https://example.com?utm_source=twitter"
        result = UTMBuilder.strip_utm(url)
        assert "utm_source" not in result

    def test_validate_utm_valid(self):
        utm = UTMParams(source="twitter", medium="social", campaign="launch")
        issues = UTMBuilder.validate_utm(utm)
        assert len(issues) == 0

    def test_validate_utm_missing_source(self):
        utm = UTMParams(medium="social")
        issues = UTMBuilder.validate_utm(utm)
        assert any("source" in i.lower() for i in issues)

    def test_validate_utm_spaces(self):
        utm = UTMParams(source="my source", campaign="my campaign")
        issues = UTMBuilder.validate_utm(utm)
        assert any("space" in i.lower() for i in issues)


# ── LinkStore ──

class TestLinkStore:
    @pytest.fixture
    def store(self, tmp_path):
        db = str(tmp_path / "test_links.db")
        s = LinkStore(db_path=db)
        yield s
        s.close()

    def _make_link(self, link_id="link1", code="abc123"):
        return TrackedLink(
            link_id=link_id,
            original_url="https://example.com",
            short_code=code,
            short_url=f"https://link.test/{code}",
            utm=UTMParams(source="twitter"),
            created_at="2025-01-01T00:00:00Z",
        )

    def test_save_and_get(self, store):
        link = self._make_link()
        store.save_link(link)
        result = store.get_link("link1")
        assert result is not None
        assert result["short_code"] == "abc123"

    def test_get_by_code(self, store):
        link = self._make_link()
        store.save_link(link)
        result = store.get_by_code("abc123")
        assert result is not None
        assert result["link_id"] == "link1"

    def test_get_nonexistent(self, store):
        assert store.get_link("nope") is None
        assert store.get_by_code("nope") is None

    def test_record_click(self, store):
        link = self._make_link()
        store.save_link(link)
        event = ClickEvent(
            link_id="link1",
            clicked_at="2025-01-01T12:00:00Z",
            referrer="https://twitter.com",
            device="mobile",
        )
        store.record_click(event)
        updated = store.get_link("link1")
        assert updated["clicks"] == 1

    def test_get_clicks(self, store):
        link = self._make_link()
        store.save_link(link)
        for i in range(3):
            store.record_click(ClickEvent(
                link_id="link1",
                clicked_at=f"2025-01-0{i+1}T00:00:00Z",
            ))
        clicks = store.get_clicks("link1")
        assert len(clicks) == 3

    def test_get_all_links(self, store):
        for i in range(5):
            store.save_link(self._make_link(f"link{i}", f"code{i}"))
        links = store.get_all_links()
        assert len(links) == 5

    def test_get_all_links_by_status(self, store):
        store.save_link(self._make_link("l1", "c1"))
        store.update_status("l1", LinkStatus.EXPIRED)
        store.save_link(self._make_link("l2", "c2"))
        active = store.get_all_links(status="active")
        assert len(active) == 1

    def test_update_status(self, store):
        store.save_link(self._make_link())
        store.update_status("link1", LinkStatus.BROKEN)
        result = store.get_link("link1")
        assert result["status"] == "broken"

    def test_analytics(self, store):
        store.save_link(self._make_link())
        for i in range(5):
            store.record_click(ClickEvent(
                link_id="link1",
                clicked_at="2025-01-01T12:00:00Z",
                ip_hash=f"hash{i}",
                device="mobile" if i % 2 == 0 else "desktop",
                country="US" if i < 3 else "UK",
            ))
        analytics = store.get_analytics("link1")
        assert analytics.total_clicks == 5
        assert analytics.unique_clicks == 5

    def test_analytics_empty(self, store):
        analytics = store.get_analytics("nonexistent")
        assert analytics.total_clicks == 0


# ── LinkIntelligence ──

class TestLinkIntelligence:
    @pytest.fixture
    def engine(self, tmp_path):
        db = str(tmp_path / "test_li.db")
        store = LinkStore(db_path=db)
        return LinkIntelligence(base_domain="link.test", store=store)

    def test_create_tracked_link(self, engine):
        link = engine.create_tracked_link(
            "https://example.com/page",
            campaign="test_campaign",
        )
        assert link.link_id
        assert link.short_code
        assert link.short_url.startswith("https://link.test/")
        assert link.utm.source == "twitter"
        assert link.utm.campaign == "test_campaign"

    def test_create_with_custom_code(self, engine):
        link = engine.create_tracked_link(
            "https://example.com",
            custom_code="mycode",
        )
        assert link.short_code == "mycode"

    def test_create_with_custom_utm(self, engine):
        utm = UTMParams(source="facebook", medium="cpc", campaign="fb_ad")
        link = engine.create_tracked_link("https://example.com", utm=utm)
        assert link.utm.source == "facebook"

    def test_create_with_tags(self, engine):
        link = engine.create_tracked_link(
            "https://example.com",
            tags=["promo", "q1"],
        )
        assert "promo" in link.tags

    def test_create_campaign_links(self, engine):
        urls = [f"https://example.com/page{i}" for i in range(3)]
        links = engine.create_campaign_links(urls, campaign="batch_test")
        assert len(links) == 3
        for link in links:
            assert link.utm.campaign == "batch_test"

    def test_record_click(self, engine):
        link = engine.create_tracked_link("https://example.com")
        success = engine.record_click(
            link.link_id, referrer="https://twitter.com",
            user_agent="Mozilla/5.0 (iPhone)", ip="1.2.3.4",
        )
        assert success is True

    def test_record_click_nonexistent(self, engine):
        assert engine.record_click("fake_id") is False

    def test_resolve_short_link(self, engine):
        link = engine.create_tracked_link(
            "https://example.com", custom_code="test99",
        )
        resolved = engine.resolve_short_link("test99")
        assert resolved is not None
        assert "example.com" in resolved

    def test_resolve_nonexistent(self, engine):
        assert engine.resolve_short_link("nope") is None

    def test_get_analytics(self, engine):
        link = engine.create_tracked_link("https://example.com")
        for i in range(3):
            engine.record_click(link.link_id, ip=f"1.2.3.{i}")
        analytics = engine.get_analytics(link.link_id)
        assert analytics.total_clicks == 3

    def test_check_expired_links(self, engine):
        link = engine.create_tracked_link(
            "https://example.com",
            expires_at="2020-01-01T00:00:00Z",
        )
        expired = engine.check_expired_links()
        assert link.link_id in expired

    def test_check_no_expired(self, engine):
        engine.create_tracked_link(
            "https://example.com",
            expires_at="2099-01-01T00:00:00Z",
        )
        expired = engine.check_expired_links()
        assert len(expired) == 0

    def test_campaign_report(self, engine):
        for i in range(3):
            engine.create_tracked_link(
                f"https://example.com/p{i}",
                campaign="my_campaign",
            )
        report = engine.get_campaign_report("my_campaign")
        assert report["campaign"] == "my_campaign"
        assert report["links"] == 3

    def test_campaign_report_empty(self, engine):
        report = engine.get_campaign_report("nonexistent")
        assert report["links"] == 0

    def test_export_json(self, engine):
        engine.create_tracked_link("https://example.com")
        output = engine.export_links(format="json")
        data = json.loads(output)
        assert isinstance(data, list)
        assert len(data) == 1

    def test_export_csv(self, engine):
        engine.create_tracked_link("https://example.com")
        output = engine.export_links(format="csv")
        lines = output.strip().split("\n")
        assert lines[0].startswith("link_id")
        assert len(lines) == 2

    def test_find_links_in_text(self, engine):
        text = "Check https://a.com and https://b.com for details"
        urls = engine.find_links_in_text(text)
        assert len(urls) == 2

    def test_find_no_links(self, engine):
        assert engine.find_links_in_text("No links here") == []

    def test_replace_links_in_text(self, engine):
        text = "Visit https://example.com/page for info"
        new_text, created = engine.replace_links_in_text(text, campaign="auto")
        assert len(created) == 1
        assert "https://link.test/" in new_text
        assert "https://example.com/page" not in new_text

    def test_detect_device_mobile(self):
        assert LinkIntelligence._detect_device("Mozilla/5.0 (iPhone)") == "mobile"

    def test_detect_device_desktop(self):
        assert LinkIntelligence._detect_device("Mozilla/5.0 (Windows NT)") == "desktop"

    def test_detect_device_tablet(self):
        assert LinkIntelligence._detect_device("Mozilla/5.0 (iPad)") == "tablet"

    def test_detect_platform_twitter(self):
        assert LinkIntelligence._detect_platform("https://twitter.com/ref") == "twitter"

    def test_detect_platform_direct(self):
        assert LinkIntelligence._detect_platform("") == "direct"

    def test_detect_platform_other(self):
        assert LinkIntelligence._detect_platform("https://random.com") == "other"

    def test_detect_platform_x(self):
        assert LinkIntelligence._detect_platform("https://x.com/user") == "twitter"
