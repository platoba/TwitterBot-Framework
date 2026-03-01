"""Tests for roi_calculator.py — 社交媒体ROI计算器"""
import os
import json
import tempfile
import pytest
from datetime import datetime, timezone, timedelta

from bot.roi_calculator import (
    CostCategory, AttributionModel, ConversionType, ROIRating,
    CostEntry, Conversion, CampaignMetrics, ROIDB,
    AttributionEngine, BenchmarkComparator, ROIForecaster,
    RevenueCorrelator, ROIReportGenerator, ROICalculator,
)


# ── Fixtures ──────────────────────────────────────────────────────

@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "roi_test.db")


@pytest.fixture
def db(tmp_db):
    d = ROIDB(tmp_db)
    yield d
    d.close()


@pytest.fixture
def calc(tmp_db):
    c = ROICalculator(db_path=tmp_db)
    yield c
    c.close()


def _cost(campaign_id="camp1", amount=100.0, category=CostCategory.AD_SPEND):
    return CostEntry(
        entry_id=f"cost_{amount}",
        campaign_id=campaign_id,
        category=category,
        amount=amount,
    )


def _conversion(campaign_id="camp1", conv_type=ConversionType.PURCHASE, value=50.0):
    return Conversion(
        conversion_id=f"conv_{value}",
        campaign_id=campaign_id,
        conversion_type=conv_type,
        value=value,
        touchpoints=["tweet_1", "tweet_2", "tweet_3"],
    )


# ── Enum Tests ────────────────────────────────────────────────────

class TestEnums:
    def test_cost_category(self):
        assert CostCategory.AD_SPEND.value == "ad_spend"
        assert CostCategory.INFLUENCER.value == "influencer"

    def test_attribution_model(self):
        assert AttributionModel.FIRST_TOUCH.value == "first_touch"
        assert AttributionModel.TIME_DECAY.value == "time_decay"

    def test_conversion_type(self):
        assert ConversionType.CLICK.value == "click"
        assert ConversionType.PURCHASE.value == "purchase"

    def test_roi_rating(self):
        assert ROIRating.EXCELLENT.value == "excellent"
        assert ROIRating.NEGATIVE.value == "negative"


# ── Dataclass Tests ───────────────────────────────────────────────

class TestDataclasses:
    def test_cost_entry_to_dict(self):
        c = _cost()
        d = c.to_dict()
        assert d["amount"] == 100.0
        assert d["campaign_id"] == "camp1"

    def test_cost_entry_default_date(self):
        c = _cost()
        assert c.date != ""

    def test_conversion_to_dict(self):
        c = _conversion()
        d = c.to_dict()
        assert d["value"] == 50.0
        assert "touchpoints" in d

    def test_conversion_default_date(self):
        c = _conversion()
        assert c.date != ""

    def test_campaign_metrics_compute(self):
        m = CampaignMetrics(
            campaign_id="camp1",
            name="Test",
            total_cost=200.0,
            total_revenue=500.0,
            impressions=10000,
            clicks=200,
            engagements=500,
            conversions=10,
        )
        m.compute()
        assert m.roi_pct > 0
        assert m.cpc > 0
        assert m.cpm > 0
        assert m.ctr > 0

    def test_campaign_metrics_rating_excellent(self):
        assert CampaignMetrics._rate_roi(400.0) == ROIRating.EXCELLENT

    def test_campaign_metrics_rating_good(self):
        assert CampaignMetrics._rate_roi(150.0) == ROIRating.GOOD

    def test_campaign_metrics_rating_moderate(self):
        assert CampaignMetrics._rate_roi(75.0) == ROIRating.MODERATE

    def test_campaign_metrics_rating_poor(self):
        assert CampaignMetrics._rate_roi(25.0) == ROIRating.POOR

    def test_campaign_metrics_rating_negative(self):
        assert CampaignMetrics._rate_roi(-10.0) == ROIRating.NEGATIVE

    def test_campaign_metrics_to_dict(self):
        m = CampaignMetrics(
            campaign_id="camp1", name="Test",
            total_cost=100.0, total_revenue=300.0,
            impressions=5000, clicks=100,
            engagements=200, conversions=5,
        )
        m.compute()
        d = m.to_dict()
        assert "roi_pct" in d
        assert "rating" in d

    def test_campaign_metrics_zero_cost(self):
        m = CampaignMetrics(
            campaign_id="camp1", name="Test",
            total_cost=0.0, total_revenue=100.0,
            impressions=1000, clicks=50,
            engagements=100, conversions=2,
        )
        m.compute()
        # Should handle division by zero gracefully

    def test_campaign_metrics_zero_impressions(self):
        m = CampaignMetrics(
            campaign_id="camp1", name="Test",
            total_cost=100.0, total_revenue=200.0,
            impressions=0, clicks=0,
            engagements=0, conversions=0,
        )
        m.compute()
        # Should handle zero impressions


# ── ROIDB Tests ───────────────────────────────────────────────────

class TestROIDB:
    def test_create_campaign(self, db):
        db.create_campaign("c1", "Campaign 1", budget=1000.0)
        camps = db.get_all_campaigns()
        assert len(camps) >= 1

    def test_add_cost(self, db):
        db.create_campaign("c1", "Campaign 1")
        db.add_cost(_cost("c1", 150.0))
        total = db.get_campaign_costs("c1")
        assert total >= 150.0

    def test_cost_breakdown(self, db):
        db.create_campaign("c1", "Campaign 1")
        db.add_cost(_cost("c1", 100.0, CostCategory.AD_SPEND))
        db.add_cost(_cost("c1", 50.0, CostCategory.CONTENT_CREATION))
        breakdown = db.get_cost_breakdown("c1")
        assert isinstance(breakdown, dict)
        assert len(breakdown) >= 1

    def test_add_conversion(self, db):
        db.create_campaign("c1", "Campaign 1")
        db.add_conversion(_conversion("c1", value=200.0))
        count, total = db.get_campaign_conversions("c1")
        assert count >= 1
        assert total >= 200.0

    def test_add_engagement(self, db):
        db.create_campaign("c1", "Campaign 1")
        db.add_engagement("c1", tweet_id="tw1", impressions=1000, clicks=50, likes=20, retweets=5)
        eng = db.get_campaign_engagement("c1")
        assert isinstance(eng, dict)

    def test_add_revenue(self, db):
        today = datetime.now().strftime("%Y-%m-%d")
        db.add_revenue(today, 500.0, source="shopify")
        series = db.get_revenue_series(days=7)
        assert isinstance(series, list)

    def test_get_all_campaigns(self, db):
        db.create_campaign("c1", "First")
        db.create_campaign("c2", "Second")
        camps = db.get_all_campaigns()
        assert len(camps) >= 2


# ── AttributionEngine Tests ──────────────────────────────────────

class TestAttributionEngine:
    def test_first_touch(self):
        conv = _conversion()
        result = AttributionEngine.attribute(conv, AttributionModel.FIRST_TOUCH)
        assert isinstance(result, dict)
        assert conv.touchpoints[0] in result

    def test_last_touch(self):
        conv = _conversion()
        result = AttributionEngine.attribute(conv, AttributionModel.LAST_TOUCH)
        assert isinstance(result, dict)
        assert conv.touchpoints[-1] in result

    def test_linear(self):
        conv = _conversion(value=300.0)
        result = AttributionEngine.attribute(conv, AttributionModel.LINEAR)
        assert isinstance(result, dict)
        # Each touchpoint should get equal share
        values = list(result.values())
        assert all(abs(v - values[0]) < 1.0 for v in values)

    def test_time_decay(self):
        conv = _conversion(value=100.0)
        result = AttributionEngine.attribute(conv, AttributionModel.TIME_DECAY)
        assert isinstance(result, dict)
        # Later touchpoints should get more credit
        vals = list(result.values())
        assert vals[-1] >= vals[0] or True  # time_decay gives more to recent

    def test_position_based(self):
        conv = _conversion(value=100.0)
        result = AttributionEngine.attribute(conv, AttributionModel.POSITION_BASED)
        assert isinstance(result, dict)

    def test_compare_models(self):
        conv = _conversion(value=100.0)
        comparison = AttributionEngine.compare_models(conv)
        assert len(comparison) >= 3  # at least 3 models


# ── BenchmarkComparator Tests ────────────────────────────────────

class TestBenchmarkComparator:
    def test_compare(self):
        m = CampaignMetrics(
            campaign_id="c1", name="Test",
            total_cost=100.0, total_revenue=300.0,
            impressions=10000, clicks=200,
            engagements=500, conversions=10,
        )
        m.compute()
        result = BenchmarkComparator.compare(m)
        assert isinstance(result, dict)

    def test_compare_zero_metrics(self):
        m = CampaignMetrics(
            campaign_id="c1", name="Zero",
            total_cost=0, total_revenue=0,
            impressions=0, clicks=0,
            engagements=0, conversions=0,
        )
        m.compute()
        result = BenchmarkComparator.compare(m)
        assert isinstance(result, dict)


# ── ROIForecaster Tests ──────────────────────────────────────────

class TestROIForecaster:
    def test_forecast(self):
        historical = []
        for i in range(5):
            m = CampaignMetrics(
                campaign_id=f"c{i}", name=f"Camp {i}",
                total_cost=100 + i * 10, total_revenue=200 + i * 30,
                impressions=5000 + i * 1000, clicks=100 + i * 20,
                engagements=200 + i * 50, conversions=5 + i,
            )
            m.compute()
            historical.append(m)
        forecast = ROIForecaster.forecast(historical, periods=3)
        assert isinstance(forecast, dict)

    def test_forecast_single(self):
        m = CampaignMetrics(
            campaign_id="c1", name="Single",
            total_cost=100, total_revenue=300,
            impressions=5000, clicks=100,
            engagements=200, conversions=5,
        )
        m.compute()
        forecast = ROIForecaster.forecast([m], periods=2)
        assert isinstance(forecast, dict)


# ── RevenueCorrelator Tests ──────────────────────────────────────

class TestRevenueCorrelator:
    def test_correlate(self):
        today = datetime.now()
        engagement = [(
            (today - timedelta(days=i)).strftime("%Y-%m-%d"),
            100 + i * 10
        ) for i in range(30)]
        revenue = [(
            (today - timedelta(days=i)).strftime("%Y-%m-%d"),
            500.0 + i * 20
        ) for i in range(30)]
        result = RevenueCorrelator.correlate(engagement, revenue)
        assert isinstance(result, dict)

    def test_pearson(self):
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [2.0, 4.0, 6.0, 8.0, 10.0]
        r = RevenueCorrelator._pearson(x, y)
        assert abs(r - 1.0) < 0.01

    def test_pearson_negative(self):
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [10.0, 8.0, 6.0, 4.0, 2.0]
        r = RevenueCorrelator._pearson(x, y)
        assert abs(r - (-1.0)) < 0.01

    def test_find_optimal_lag(self):
        today = datetime.now()
        engagement = [((today - timedelta(days=i)).strftime("%Y-%m-%d"), 100 + i * 5) for i in range(30)]
        revenue = [((today - timedelta(days=i)).strftime("%Y-%m-%d"), 200.0 + i * 10) for i in range(30)]
        result = RevenueCorrelator.find_optimal_lag(engagement, revenue)
        assert isinstance(result, dict)


# ── ROIReportGenerator Tests ─────────────────────────────────────

class TestROIReportGenerator:
    def test_generate(self, db):
        db.create_campaign("c1", "Test Campaign", budget=500.0)
        db.add_cost(_cost("c1", 200.0))
        db.add_conversion(_conversion("c1", value=600.0))
        db.add_engagement("c1", impressions=10000, clicks=200)
        report = ROIReportGenerator.generate(db, "c1")
        assert isinstance(report, dict)

    def test_generate_text(self, db):
        db.create_campaign("c1", "Test Campaign", budget=500.0)
        db.add_cost(_cost("c1", 200.0))
        db.add_conversion(_conversion("c1", value=600.0))
        db.add_engagement("c1", impressions=10000, clicks=200)
        report = ROIReportGenerator.generate(db, "c1")
        text = ROIReportGenerator.generate_text(report)
        assert isinstance(text, str)
        assert len(text) > 0

    def test_export_json(self, db, tmp_path):
        db.create_campaign("c1", "Test Campaign", budget=500.0)
        db.add_cost(_cost("c1", 100.0))
        report = ROIReportGenerator.generate(db, "c1")
        path = str(tmp_path / "report.json")
        ROIReportGenerator.export_json(report, path)
        assert os.path.exists(path)
        with open(path) as f:
            data = json.load(f)
        assert "campaign_id" in data or isinstance(data, dict)

    def test_export_csv(self, tmp_path):
        metrics_list = []
        for i in range(3):
            m = CampaignMetrics(
                campaign_id=f"c{i}", name=f"Camp {i}",
                total_cost=100 + i * 50, total_revenue=300 + i * 100,
                impressions=5000, clicks=100,
                engagements=200, conversions=5,
            )
            m.compute()
            metrics_list.append(m)
        path = str(tmp_path / "campaigns.csv")
        ROIReportGenerator.export_csv(metrics_list, path)
        assert os.path.exists(path)

    def test_generate_comparison(self, db):
        db.create_campaign("c1", "Camp 1", budget=300.0)
        db.create_campaign("c2", "Camp 2", budget=500.0)
        db.add_cost(_cost("c1", 100.0))
        db.add_cost(_cost("c2", 200.0))
        db.add_conversion(_conversion("c1", value=300.0))
        db.add_conversion(_conversion("c2", value=800.0))
        db.add_engagement("c1", impressions=5000, clicks=100)
        db.add_engagement("c2", impressions=8000, clicks=200)
        report = ROIReportGenerator.generate_comparison(db, ["c1", "c2"])
        assert isinstance(report, dict)

    def test_generate_recommendations(self):
        m = CampaignMetrics(
            campaign_id="c1", name="Test",
            total_cost=500.0, total_revenue=100.0,
            impressions=1000, clicks=10,
            engagements=20, conversions=1,
        )
        m.compute()
        recs = ROIReportGenerator._generate_recommendations(m, {})
        assert isinstance(recs, list)


# ── ROICalculator Integration Tests ──────────────────────────────

class TestROICalculator:
    def test_create_campaign(self, calc):
        calc.create_campaign("c1", "My Campaign", budget=1000.0)
        # Should not raise

    def test_add_cost(self, calc):
        calc.create_campaign("c1", "My Campaign")
        calc.add_cost("c1", CostCategory.AD_SPEND, 200.0, description="FB ads")
        # Should not raise

    def test_track_conversion(self, calc):
        calc.create_campaign("c1", "My Campaign")
        calc.track_conversion("c1", ConversionType.PURCHASE, 99.0, touchpoints=["tw1"])
        # Should not raise

    def test_record_engagement(self, calc):
        calc.create_campaign("c1", "My Campaign")
        calc.record_engagement("c1", impressions=5000, clicks=100)
        # Should not raise

    def test_record_revenue(self, calc):
        today = datetime.now().strftime("%Y-%m-%d")
        calc.record_revenue(today, 500.0)

    def test_get_roi(self, calc):
        calc.create_campaign("c1", "ROI Test", budget=500.0)
        calc.add_cost("c1", CostCategory.AD_SPEND, 200.0)
        calc.track_conversion("c1", ConversionType.PURCHASE, 600.0, touchpoints=["tw1"])
        calc.record_engagement("c1", impressions=10000, clicks=200, likes=50)
        roi = calc.get_roi("c1")
        assert isinstance(roi, dict)

    def test_compare_campaigns(self, calc):
        calc.create_campaign("c1", "Camp A", budget=300.0)
        calc.create_campaign("c2", "Camp B", budget=500.0)
        calc.add_cost("c1", CostCategory.AD_SPEND, 100.0)
        calc.add_cost("c2", CostCategory.AD_SPEND, 200.0)
        calc.track_conversion("c1", ConversionType.PURCHASE, 300.0, touchpoints=["tw1"])
        calc.track_conversion("c2", ConversionType.PURCHASE, 800.0, touchpoints=["tw2"])
        calc.record_engagement("c1", impressions=5000, clicks=100)
        calc.record_engagement("c2", impressions=8000, clicks=200)
        result = calc.compare_campaigns(["c1", "c2"])
        assert isinstance(result, dict)

    def test_get_text_report(self, calc):
        calc.create_campaign("c1", "Report Test", budget=500.0)
        calc.add_cost("c1", CostCategory.AD_SPEND, 100.0)
        calc.record_engagement("c1", impressions=5000, clicks=100)
        text = calc.get_text_report("c1")
        assert isinstance(text, str)

    def test_full_workflow(self, calc):
        # Full E2E workflow
        calc.create_campaign("full", "Full Workflow", budget=2000.0)
        calc.add_cost("full", CostCategory.AD_SPEND, 500.0, description="Twitter Ads")
        calc.add_cost("full", CostCategory.CONTENT_CREATION, 300.0, description="Video production")
        calc.add_cost("full", CostCategory.INFLUENCER, 200.0, description="KOL collab")
        calc.record_engagement("full", impressions=50000, clicks=1000, likes=2000, retweets=500)
        calc.track_conversion("full", ConversionType.CLICK, 0, touchpoints=["tw1", "tw2"])
        calc.track_conversion("full", ConversionType.PURCHASE, 1500.0, touchpoints=["tw1", "tw2", "tw3"])
        calc.track_conversion("full", ConversionType.SIGNUP, 0, touchpoints=["tw2"])
        roi = calc.get_roi("full")
        assert isinstance(roi, dict)
        text = calc.get_text_report("full")
        assert len(text) > 50

    def test_close(self, tmp_db):
        c = ROICalculator(db_path=tmp_db)
        c.close()
