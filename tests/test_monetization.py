"""Tests for Monetization Tracker Engine"""

import json
import pytest
from bot.monetization import (
    AffiliateLink, RevenueEvent, ClickEvent, RevenueSource, Currency,
    LinkDetector, MonetizationStore, ROICalculator, MonetizationReport,
    MonetizationEngine, AFFILIATE_PATTERNS, DEFAULT_COMMISSION_RATES,
)


# ─── AffiliateLink ───

class TestAffiliateLink:
    def test_create_default(self):
        link = AffiliateLink()
        assert link.id
        assert link.original_url == ""
        assert link.platform == ""
        assert link.commission_rate == 0.0

    def test_create_with_values(self):
        link = AffiliateLink(
            original_url="https://amazon.com/dp/B08?tag=myid-20",
            platform="amazon",
            tag="myid-20",
            commission_rate=0.03,
        )
        assert link.platform == "amazon"
        assert link.commission_rate == 0.03

    def test_to_dict(self):
        link = AffiliateLink(original_url="https://test.com", platform="test")
        d = link.to_dict()
        assert d["original_url"] == "https://test.com"
        assert d["platform"] == "test"
        assert "id" in d
        assert "created_at" in d

    def test_from_dict(self):
        d = {"id": "abc", "original_url": "https://x.com", "platform": "amazon", "tag": "t1", "commission_rate": 0.05}
        link = AffiliateLink.from_dict(d)
        assert link.id == "abc"
        assert link.platform == "amazon"

    def test_roundtrip(self):
        link = AffiliateLink(original_url="https://test.com", platform="ebay", tag="camp1")
        d = link.to_dict()
        link2 = AffiliateLink.from_dict(d)
        assert link2.original_url == link.original_url
        assert link2.platform == link.platform


# ─── RevenueEvent ───

class TestRevenueEvent:
    def test_create_default(self):
        e = RevenueEvent()
        assert e.source == RevenueSource.AFFILIATE
        assert e.amount == 0.0
        assert e.currency == Currency.USD

    def test_create_sponsored(self):
        e = RevenueEvent(source=RevenueSource.SPONSORED, amount=500.0, tweet_id="t1")
        assert e.source == RevenueSource.SPONSORED
        assert e.amount == 500.0

    def test_to_dict_serializes_enums(self):
        e = RevenueEvent(source=RevenueSource.TIP_JAR, currency=Currency.EUR)
        d = e.to_dict()
        assert d["source"] == "tip_jar"
        assert d["currency"] == "EUR"

    def test_from_dict_parses_enums(self):
        d = {"source": "sponsored", "amount": 100, "currency": "GBP"}
        e = RevenueEvent.from_dict(d)
        assert e.source == RevenueSource.SPONSORED
        assert e.currency == Currency.GBP

    def test_from_dict_invalid_source(self):
        d = {"source": "invalid_source", "amount": 10}
        e = RevenueEvent.from_dict(d)
        assert e.source == RevenueSource.AFFILIATE  # fallback

    def test_from_dict_metadata_string(self):
        d = {"metadata": '{"key": "val"}'}
        e = RevenueEvent.from_dict(d)
        assert e.metadata == {"key": "val"}


# ─── ClickEvent ───

class TestClickEvent:
    def test_create(self):
        c = ClickEvent(link_id="L1", tweet_id="T1", country="US")
        assert c.link_id == "L1"
        assert c.country == "US"

    def test_to_dict(self):
        c = ClickEvent(link_id="L1")
        d = c.to_dict()
        assert d["link_id"] == "L1"
        assert "clicked_at" in d


# ─── LinkDetector ───

class TestLinkDetector:
    def test_detect_amazon_tag(self):
        platform, tag = LinkDetector.detect_platform("https://amazon.com/dp/B08XYZ?tag=myid-20")
        assert platform == "amazon"
        assert tag == "myid-20"

    def test_detect_amazon_short(self):
        platform, tag = LinkDetector.detect_platform("https://amzn.to/3abc")
        assert platform == "amazon"

    def test_detect_clickbank(self):
        platform, tag = LinkDetector.detect_platform("https://myid.hop.clickbank.net/product")
        assert platform == "clickbank"
        assert tag == "myid"

    def test_detect_shareasale(self):
        platform, _ = LinkDetector.detect_platform("https://shareasale.com/r.cfm?id=123")
        assert platform == "shareasale"

    def test_detect_impact(self):
        platform, _ = LinkDetector.detect_platform("https://example.sjv.io/click123")
        assert platform == "impact"

    def test_detect_unknown(self):
        platform, tag = LinkDetector.detect_platform("https://example.com/page")
        assert platform == ""
        assert tag == ""

    def test_extract_links(self):
        text = "Check https://amazon.com/dp/B08 and https://test.com/page"
        urls = LinkDetector.extract_links(text)
        assert len(urls) == 2
        assert "https://amazon.com/dp/B08" in urls[0]

    def test_extract_no_links(self):
        assert LinkDetector.extract_links("no links here") == []

    def test_scan_tweet_amazon(self):
        text = "Great product! https://amazon.com/dp/B08?tag=myref-20"
        links = LinkDetector.scan_tweet(text)
        assert len(links) == 1
        assert links[0].platform == "amazon"
        assert links[0].commission_rate == 0.03

    def test_scan_tweet_multiple(self):
        text = "A: https://amazon.com/dp/B?tag=x-20 B: https://myid.hop.clickbank.net/p"
        links = LinkDetector.scan_tweet(text)
        assert len(links) == 2

    def test_scan_tweet_no_affiliate(self):
        text = "Just a regular https://example.com tweet"
        links = LinkDetector.scan_tweet(text)
        assert len(links) == 0

    def test_scan_empty(self):
        assert LinkDetector.scan_tweet("") == []


# ─── MonetizationStore ───

class TestMonetizationStore:
    def setup_method(self):
        self.store = MonetizationStore()

    def test_add_get_link(self):
        link = AffiliateLink(platform="amazon")
        lid = self.store.add_link(link)
        assert self.store.get_link(lid) == link

    def test_get_missing_link(self):
        assert self.store.get_link("nonexistent") is None

    def test_list_links(self):
        self.store.add_link(AffiliateLink(platform="amazon"))
        self.store.add_link(AffiliateLink(platform="ebay"))
        assert len(self.store.list_links()) == 2

    def test_list_links_filter(self):
        self.store.add_link(AffiliateLink(platform="amazon"))
        self.store.add_link(AffiliateLink(platform="ebay"))
        assert len(self.store.list_links(platform="amazon")) == 1

    def test_remove_link(self):
        link = AffiliateLink(platform="test")
        lid = self.store.add_link(link)
        assert self.store.remove_link(lid) is True
        assert self.store.get_link(lid) is None

    def test_remove_missing_link(self):
        assert self.store.remove_link("nope") is False

    def test_associate_tweet(self):
        self.store.associate_tweet("tw1", ["l1", "l2"])
        assert self.store.get_tweet_links("tw1") == ["l1", "l2"]

    def test_get_tweet_links_missing(self):
        assert self.store.get_tweet_links("nope") == []

    def test_record_revenue(self):
        e = RevenueEvent(amount=50.0, tweet_id="t1")
        eid = self.store.record_revenue(e)
        assert eid == e.id
        assert len(self.store.get_revenue()) == 1

    def test_record_click(self):
        c = ClickEvent(link_id="l1")
        cid = self.store.record_click(c)
        assert cid == c.id
        assert len(self.store.get_clicks()) == 1

    def test_filter_revenue_by_source(self):
        self.store.record_revenue(RevenueEvent(source=RevenueSource.AFFILIATE, amount=10))
        self.store.record_revenue(RevenueEvent(source=RevenueSource.SPONSORED, amount=20))
        assert len(self.store.get_revenue(source=RevenueSource.AFFILIATE)) == 1

    def test_filter_revenue_by_tweet(self):
        self.store.record_revenue(RevenueEvent(tweet_id="t1", amount=10))
        self.store.record_revenue(RevenueEvent(tweet_id="t2", amount=20))
        r = self.store.get_revenue(tweet_id="t1")
        assert len(r) == 1
        assert r[0].amount == 10

    def test_filter_revenue_by_campaign(self):
        self.store.record_revenue(RevenueEvent(campaign_id="c1"))
        self.store.record_revenue(RevenueEvent(campaign_id="c2"))
        assert len(self.store.get_revenue(campaign_id="c1")) == 1

    def test_filter_clicks_by_link(self):
        self.store.record_click(ClickEvent(link_id="l1"))
        self.store.record_click(ClickEvent(link_id="l2"))
        assert len(self.store.get_clicks(link_id="l1")) == 1

    def test_total_revenue(self):
        self.store.record_revenue(RevenueEvent(amount=10, currency=Currency.USD))
        self.store.record_revenue(RevenueEvent(amount=20, currency=Currency.USD))
        self.store.record_revenue(RevenueEvent(amount=5, currency=Currency.EUR))
        assert self.store.total_revenue(Currency.USD) == 30
        assert self.store.total_revenue(Currency.EUR) == 5

    def test_clear(self):
        self.store.add_link(AffiliateLink())
        self.store.record_revenue(RevenueEvent(amount=10))
        self.store.record_click(ClickEvent(link_id="l1"))
        self.store.clear()
        assert len(self.store.list_links()) == 0
        assert len(self.store.get_revenue()) == 0
        assert len(self.store.get_clicks()) == 0


# ─── ROICalculator ───

class TestROICalculator:
    def setup_method(self):
        self.store = MonetizationStore()
        self.calc = ROICalculator(self.store)

    def test_tweet_roi_basic(self):
        self.store.record_revenue(RevenueEvent(amount=100, tweet_id="t1"))
        self.store.record_click(ClickEvent(link_id="l1", tweet_id="t1"))
        self.store.record_click(ClickEvent(link_id="l1", tweet_id="t1"))
        roi = self.calc.tweet_roi("t1", cost=20)
        assert roi["revenue"] == 100
        assert roi["cost"] == 20
        assert roi["profit"] == 80
        assert roi["roi_percent"] == 400.0
        assert roi["clicks"] == 2
        assert roi["conversions"] == 1

    def test_tweet_roi_zero_cost(self):
        self.store.record_revenue(RevenueEvent(amount=50, tweet_id="t1"))
        roi = self.calc.tweet_roi("t1", cost=0)
        assert roi["roi_percent"] == 0.0

    def test_tweet_roi_no_data(self):
        roi = self.calc.tweet_roi("t999")
        assert roi["revenue"] == 0
        assert roi["clicks"] == 0

    def test_campaign_roi(self):
        self.store.record_revenue(RevenueEvent(amount=200, campaign_id="c1", source=RevenueSource.AFFILIATE))
        self.store.record_revenue(RevenueEvent(amount=300, campaign_id="c1", source=RevenueSource.SPONSORED))
        roi = self.calc.campaign_roi("c1", budget=100)
        assert roi["revenue"] == 500
        assert roi["profit"] == 400
        assert roi["roi_percent"] == 400.0
        assert "affiliate" in roi["by_source"]
        assert "sponsored" in roi["by_source"]

    def test_platform_breakdown(self):
        link = AffiliateLink(platform="amazon")
        lid = self.store.add_link(link)
        self.store.record_click(ClickEvent(link_id=lid))
        self.store.record_revenue(RevenueEvent(amount=10, link_id=lid))
        bd = self.calc.platform_breakdown()
        assert "amazon" in bd
        assert bd["amazon"]["links"] == 1
        assert bd["amazon"]["clicks"] == 1
        assert bd["amazon"]["revenue"] == 10.0

    def test_daily_summary(self):
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.store.record_revenue(RevenueEvent(amount=42))
        self.store.record_click(ClickEvent(link_id="l1"))
        summary = self.calc.daily_summary(today)
        assert summary["date"] == today
        assert summary["total_revenue"] == 42
        assert summary["clicks"] == 1


# ─── MonetizationReport ───

class TestMonetizationReport:
    def setup_method(self):
        self.store = MonetizationStore()
        self.report = MonetizationReport(self.store)

    def test_full_report_empty(self):
        r = self.report.full_report()
        assert r["summary"]["total_revenue"] == 0
        assert r["summary"]["total_transactions"] == 0

    def test_full_report_with_data(self):
        link = AffiliateLink(platform="amazon")
        lid = self.store.add_link(link)
        self.store.record_revenue(RevenueEvent(amount=100, tweet_id="t1", link_id=lid))
        self.store.record_revenue(RevenueEvent(amount=50, tweet_id="t2", link_id=lid))
        self.store.record_click(ClickEvent(link_id=lid))
        r = self.report.full_report()
        assert r["summary"]["total_revenue"] == 150
        assert r["summary"]["total_transactions"] == 2
        assert r["summary"]["total_links"] == 1
        assert len(r["top_tweets"]) == 2

    def test_export_csv(self):
        self.store.record_revenue(RevenueEvent(amount=10, source=RevenueSource.AFFILIATE))
        csv = self.report.export_csv()
        assert "affiliate" in csv
        assert "10" in csv

    def test_export_json(self):
        self.store.record_revenue(RevenueEvent(amount=25))
        j = self.report.export_json()
        data = json.loads(j)
        assert len(data) == 1
        assert data[0]["amount"] == 25

    def test_text_summary(self):
        self.store.record_revenue(RevenueEvent(amount=100))
        text = self.report.text_summary()
        assert "💰" in text
        assert "100" in text


# ─── MonetizationEngine ───

class TestMonetizationEngine:
    def setup_method(self):
        self.engine = MonetizationEngine()

    def test_process_tweet_with_affiliate(self):
        links = self.engine.process_tweet("t1", "Buy this https://amazon.com/dp/B?tag=x-20")
        assert len(links) == 1
        assert links[0].platform == "amazon"
        # Check association
        assert len(self.engine.store.get_tweet_links("t1")) == 1

    def test_process_tweet_no_affiliate(self):
        links = self.engine.process_tweet("t2", "Just a regular tweet")
        assert len(links) == 0

    def test_record_sale(self):
        eid = self.engine.record_sale(50.0, tweet_id="t1")
        assert eid
        assert self.engine.store.total_revenue() == 50.0

    def test_record_click(self):
        cid = self.engine.record_click("l1", tweet_id="t1", country="US")
        assert cid
        assert len(self.engine.store.get_clicks(link_id="l1")) == 1

    def test_get_report(self):
        self.engine.record_sale(100)
        r = self.engine.get_report()
        assert r["summary"]["total_revenue"] == 100

    def test_get_text_report(self):
        self.engine.record_sale(200)
        text = self.engine.get_text_report()
        assert "200" in text

    def test_tweet_roi(self):
        self.engine.record_sale(100, tweet_id="t1")
        self.engine.record_click("l1", tweet_id="t1")
        roi = self.engine.get_tweet_roi("t1", cost=10)
        assert roi["revenue"] == 100
        assert roi["profit"] == 90

    def test_campaign_roi(self):
        self.engine.record_sale(500, campaign_id="c1")
        roi = self.engine.get_campaign_roi("c1", budget=100)
        assert roi["revenue"] == 500
        assert roi["roi_percent"] == 400.0

    def test_end_to_end_flow(self):
        """端到端: 处理推文→检测链接→记录点击→记录销售→计算ROI"""
        links = self.engine.process_tweet("tweet1", "Check https://amazon.com/dp/B08?tag=test-20")
        assert len(links) == 1

        self.engine.record_click(links[0].id, tweet_id="tweet1")
        self.engine.record_click(links[0].id, tweet_id="tweet1")
        self.engine.record_sale(45.0, tweet_id="tweet1", link_id=links[0].id)

        roi = self.engine.get_tweet_roi("tweet1")
        assert roi["clicks"] == 2
        assert roi["revenue"] == 45.0
        assert roi["conversions"] == 1


# ─── Constants ───

class TestConstants:
    def test_affiliate_patterns_exist(self):
        assert "amazon" in AFFILIATE_PATTERNS
        assert "clickbank" in AFFILIATE_PATTERNS
        assert len(AFFILIATE_PATTERNS) >= 8

    def test_default_rates(self):
        assert DEFAULT_COMMISSION_RATES["amazon"] == 0.03
        assert DEFAULT_COMMISSION_RATES["clickbank"] == 0.50
        assert all(0 < r < 1 for r in DEFAULT_COMMISSION_RATES.values())
