"""Tests for Twitter/X Ads Manager"""
import pytest
import json
from bot.ad_manager import (
    AdManager, Campaign, AdGroup, Creative, Targeting,
    AdGroupPerformance, BudgetOptimizer, BidManager, CreativeRotator,
    CampaignStatus, CampaignObjective, BidStrategy, CreativeType,
    AdGroupStatus
)


class TestTargeting:
    def test_default_targeting(self):
        t = Targeting()
        assert t.age_min == 18
        assert t.age_max == 65
        assert "all" in t.genders
    
    def test_estimated_reach(self):
        t = Targeting(locations=["US"], interests=["tech"])
        reach = t.estimated_reach()
        assert reach > 0
    
    def test_estimated_reach_with_followers(self):
        t = Targeting(followers_of=["@elonmusk", "@BillGates"])
        reach = t.estimated_reach()
        assert reach > 0
    
    def test_to_dict(self):
        t = Targeting(locations=["US", "UK"], languages=["en"])
        d = t.to_dict()
        assert d["locations"] == ["US", "UK"]


class TestCreative:
    def test_text_creative_valid(self):
        c = Creative("c1", "Test", CreativeType.TEXT, body="Hello world")
        errors = c.validate()
        assert len(errors) == 0
    
    def test_text_creative_too_long(self):
        c = Creative("c1", "Test", CreativeType.TEXT, body="x" * 281)
        errors = c.validate()
        assert any("280" in e for e in errors)
    
    def test_image_creative_no_url(self):
        c = Creative("c1", "Test", CreativeType.IMAGE, headline="Buy now")
        errors = c.validate()
        assert any("Image URL" in e for e in errors)
    
    def test_image_creative_valid(self):
        c = Creative("c1", "Test", CreativeType.IMAGE, 
                     headline="Buy now", media_url="https://img.com/ad.jpg")
        errors = c.validate()
        assert len(errors) == 0
    
    def test_carousel_needs_items(self):
        c = Creative("c1", "Test", CreativeType.CAROUSEL, headline="Shop")
        errors = c.validate()
        assert any("Carousel" in e for e in errors)
    
    def test_to_dict(self):
        c = Creative("c1", "Test", CreativeType.TEXT, body="Hello")
        d = c.to_dict()
        assert d["creative_type"] == "text"


class TestAdGroupPerformance:
    def test_ctr(self):
        p = AdGroupPerformance(impressions=1000, clicks=50)
        assert p.ctr == 5.0
    
    def test_ctr_zero_impressions(self):
        p = AdGroupPerformance(impressions=0, clicks=0)
        assert p.ctr == 0.0
    
    def test_cpc(self):
        p = AdGroupPerformance(clicks=100, spend=50.0)
        assert p.cpc == 0.5
    
    def test_cpm(self):
        p = AdGroupPerformance(impressions=10000, spend=20.0)
        assert p.cpm == 2.0
    
    def test_cpa(self):
        p = AdGroupPerformance(conversions=10, spend=100.0)
        assert p.cpa == 10.0
    
    def test_roas(self):
        p = AdGroupPerformance(spend=100.0, revenue=300.0)
        assert p.roas == 3.0
    
    def test_roas_zero_spend(self):
        p = AdGroupPerformance(spend=0.0, revenue=0.0)
        assert p.roas == 0.0
    
    def test_conversion_rate(self):
        p = AdGroupPerformance(clicks=200, conversions=10)
        assert p.conversion_rate == 5.0
    
    def test_to_dict_includes_derived(self):
        p = AdGroupPerformance(impressions=1000, clicks=50, conversions=5,
                               spend=25.0, revenue=100.0)
        d = p.to_dict()
        assert "ctr" in d
        assert "roas" in d


class TestCampaign:
    def test_create(self):
        c = Campaign("camp1", "Test Campaign", CampaignObjective.TRAFFIC)
        assert c.status == CampaignStatus.DRAFT
        assert c.created_at != ""
    
    def test_to_dict(self):
        c = Campaign("camp1", "Test", CampaignObjective.AWARENESS)
        d = c.to_dict()
        assert d["objective"] == "awareness"
        assert d["status"] == "draft"


class TestAdGroup:
    def test_create(self):
        ag = AdGroup("ag1", "camp1", "Test Group", Targeting())
        assert ag.status == AdGroupStatus.ACTIVE
    
    def test_is_running(self):
        ag = AdGroup("ag1", "camp1", "Test", Targeting())
        assert ag.is_running
    
    def test_not_running_when_paused(self):
        ag = AdGroup("ag1", "camp1", "Test", Targeting(), status=AdGroupStatus.PAUSED)
        assert not ag.is_running
    
    def test_to_dict(self):
        ag = AdGroup("ag1", "camp1", "Test", Targeting())
        d = ag.to_dict()
        assert d["bid_strategy"] == "lowest_cost"
        assert "performance" in d


class TestBudgetOptimizer:
    def test_record_and_get_spend(self):
        bo = BudgetOptimizer()
        bo.record_spend("c1", 10.0)
        bo.record_spend("c1", 5.0)
        assert bo.get_total_spend("c1") == 15.0
    
    def test_budget_utilization(self):
        bo = BudgetOptimizer()
        bo.record_spend("c1", 50.0)
        util = bo.get_budget_utilization("c1", 100.0)
        assert util["today_spend"] == 50.0
        assert util["daily_budget"] == 100.0
        assert "pacing_status" in util
    
    def test_suggest_reallocation(self):
        bo = BudgetOptimizer()
        
        ag1 = AdGroup("ag1", "c1", "High Performer", Targeting(), daily_budget=50.0)
        ag1.performance = AdGroupPerformance(
            impressions=10000, clicks=500, conversions=50,
            spend=50.0, revenue=200.0
        )
        
        ag2 = AdGroup("ag2", "c1", "Low Performer", Targeting(), daily_budget=50.0)
        ag2.performance = AdGroupPerformance(
            impressions=10000, clicks=50, conversions=2,
            spend=50.0, revenue=10.0
        )
        
        suggestions = bo.suggest_reallocation([ag1, ag2])
        assert len(suggestions) > 0
    
    def test_suggest_reallocation_empty(self):
        bo = BudgetOptimizer()
        assert bo.suggest_reallocation([]) == []


class TestBidManager:
    def test_manual_bid(self):
        bm = BidManager()
        result = bm.calculate_optimal_bid(
            BidStrategy.MANUAL, AdGroupPerformance(), current_bid=1.0
        )
        assert result["recommended_bid"] == 1.0
    
    def test_target_cpa(self):
        bm = BidManager()
        perf = AdGroupPerformance(conversions=10, spend=100.0, clicks=100)
        result = bm.calculate_optimal_bid(
            BidStrategy.TARGET_CPA, perf, target_value=5.0, current_bid=1.0
        )
        assert result["recommended_bid"] != 1.0 or result["reason"] != ""
    
    def test_max_clicks_no_data(self):
        bm = BidManager()
        result = bm.calculate_optimal_bid(
            BidStrategy.MAX_CLICKS, AdGroupPerformance(), current_bid=1.0
        )
        assert result["recommended_bid"] > 1.0  # Should increase
    
    def test_lowest_cost(self):
        bm = BidManager()
        perf = AdGroupPerformance(impressions=1000, clicks=50, spend=25.0)
        result = bm.calculate_optimal_bid(
            BidStrategy.LOWEST_COST, perf, current_bid=1.0
        )
        assert result["recommended_bid"] < 1.0
    
    def test_record_bid_change(self):
        bm = BidManager()
        bm.record_bid_change("ag1", 1.0, 1.5, "Test")
        history = bm.get_bid_history("ag1")
        assert len(history) == 1
        assert history[0]["old_bid"] == 1.0
    
    def test_bid_history_empty(self):
        bm = BidManager()
        assert bm.get_bid_history("nonexistent") == []


class TestCreativeRotator:
    def test_record_and_get_performance(self):
        cr = CreativeRotator()
        cr.record_impression("c1")
        cr.record_impression("c1")
        cr.record_click("c1")
        
        perf = cr.get_creative_performance("c1")
        assert perf["impressions"] == 2
        assert perf["clicks"] == 1
        assert perf["ctr"] == 50.0
    
    def test_select_round_robin(self):
        cr = CreativeRotator()
        selected = cr.select_creative(["c1", "c2", "c3"], strategy="round_robin")
        assert selected in ["c1", "c2", "c3"]
    
    def test_select_best_performer(self):
        cr = CreativeRotator()
        for _ in range(100):
            cr.record_impression("c1")
            cr.record_impression("c2")
        for _ in range(20):
            cr.record_click("c1")
        for _ in range(5):
            cr.record_click("c2")
        
        selected = cr.select_creative(["c1", "c2"], strategy="best_performer")
        assert selected == "c1"
    
    def test_select_weighted(self):
        cr = CreativeRotator()
        cr.record_impression("c1")
        cr.record_click("c1")
        
        selected = cr.select_creative(["c1", "c2"], strategy="weighted")
        assert selected in ["c1", "c2"]
    
    def test_select_empty(self):
        cr = CreativeRotator()
        assert cr.select_creative([]) is None
    
    def test_ab_comparison(self):
        cr = CreativeRotator()
        for _ in range(100):
            cr.record_impression("c1")
            cr.record_impression("c2")
        for _ in range(10):
            cr.record_click("c1")
        for _ in range(5):
            cr.record_click("c2")
        
        result = cr.get_ab_comparison(["c1", "c2"])
        assert result["winner"] == "c1"
        assert len(result["creatives"]) == 2
    
    def test_ab_insufficient_data(self):
        cr = CreativeRotator()
        cr.record_impression("c1")
        
        result = cr.get_ab_comparison(["c1"])
        assert "Need more data" in result["recommendation"]


class TestAdManager:
    def test_create_campaign(self):
        mgr = AdManager()
        camp = Campaign("c1", "Test", CampaignObjective.TRAFFIC)
        result = mgr.create_campaign(camp)
        assert result.campaign_id == "c1"
    
    def test_get_campaign(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "Test", CampaignObjective.TRAFFIC))
        assert mgr.get_campaign("c1") is not None
        assert mgr.get_campaign("nonexistent") is None
    
    def test_update_campaign_status(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "Test", CampaignObjective.TRAFFIC))
        
        assert mgr.update_campaign_status("c1", CampaignStatus.ACTIVE)
        assert mgr.get_campaign("c1").status == CampaignStatus.ACTIVE
    
    def test_invalid_status_transition(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "Test", CampaignObjective.TRAFFIC))
        
        # Can't go from DRAFT to COMPLETED
        assert not mgr.update_campaign_status("c1", CampaignStatus.COMPLETED)
    
    def test_list_campaigns(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "T1", CampaignObjective.TRAFFIC))
        mgr.create_campaign(Campaign("c2", "T2", CampaignObjective.AWARENESS))
        mgr.update_campaign_status("c2", CampaignStatus.ACTIVE)
        
        all_camps = mgr.list_campaigns()
        assert len(all_camps) == 2
        
        active = mgr.list_campaigns(status=CampaignStatus.ACTIVE)
        assert len(active) == 1
    
    def test_delete_campaign(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "Test", CampaignObjective.TRAFFIC))
        assert mgr.delete_campaign("c1")
        assert mgr.get_campaign("c1") is None
    
    def test_delete_active_campaign_fails(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "Test", CampaignObjective.TRAFFIC))
        mgr.update_campaign_status("c1", CampaignStatus.ACTIVE)
        assert not mgr.delete_campaign("c1")
    
    def test_create_adgroup(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "Test", CampaignObjective.TRAFFIC))
        
        ag = AdGroup("ag1", "c1", "Group1", Targeting())
        result = mgr.create_adgroup(ag)
        assert result is not None
    
    def test_create_adgroup_invalid_campaign(self):
        mgr = AdManager()
        ag = AdGroup("ag1", "nonexistent", "Group1", Targeting())
        assert mgr.create_adgroup(ag) is None
    
    def test_list_adgroups(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "Test", CampaignObjective.TRAFFIC))
        mgr.create_adgroup(AdGroup("ag1", "c1", "G1", Targeting()))
        mgr.create_adgroup(AdGroup("ag2", "c1", "G2", Targeting()))
        
        groups = mgr.list_adgroups("c1")
        assert len(groups) == 2
    
    def test_add_creative(self):
        mgr = AdManager()
        c = Creative("cr1", "Ad1", CreativeType.TEXT, body="Buy now!")
        result = mgr.add_creative(c)
        assert result.creative_id == "cr1"
    
    def test_add_invalid_creative(self):
        mgr = AdManager()
        c = Creative("cr1", "Ad1", CreativeType.TEXT, body="x" * 281)
        with pytest.raises(ValueError):
            mgr.add_creative(c)
    
    def test_assign_creative(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "Test", CampaignObjective.TRAFFIC))
        mgr.create_adgroup(AdGroup("ag1", "c1", "G1", Targeting()))
        mgr.add_creative(Creative("cr1", "Ad1", CreativeType.TEXT, body="Hi"))
        
        assert mgr.assign_creative("ag1", "cr1")
        assert "cr1" in mgr.get_adgroup("ag1").creatives
    
    def test_assign_creative_duplicate(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "Test", CampaignObjective.TRAFFIC))
        mgr.create_adgroup(AdGroup("ag1", "c1", "G1", Targeting()))
        mgr.add_creative(Creative("cr1", "Ad1", CreativeType.TEXT, body="Hi"))
        
        mgr.assign_creative("ag1", "cr1")
        mgr.assign_creative("ag1", "cr1")
        assert mgr.get_adgroup("ag1").creatives.count("cr1") == 1
    
    def test_record_performance(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "Test", CampaignObjective.TRAFFIC))
        mgr.create_adgroup(AdGroup("ag1", "c1", "G1", Targeting()))
        mgr.add_creative(Creative("cr1", "Ad1", CreativeType.TEXT, body="Hi"))
        
        mgr.record_performance("c1", "ag1", "cr1",
                               impressions=1000, clicks=50, conversions=5,
                               spend=25.0, revenue=100.0)
        
        ag = mgr.get_adgroup("ag1")
        assert ag.performance.impressions == 1000
        assert ag.performance.clicks == 50
    
    def test_campaign_performance(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "Test", CampaignObjective.TRAFFIC, daily_budget=100))
        mgr.create_adgroup(AdGroup("ag1", "c1", "G1", Targeting()))
        mgr.add_creative(Creative("cr1", "Ad1", CreativeType.TEXT, body="Hi"))
        
        mgr.record_performance("c1", "ag1", "cr1",
                               impressions=5000, clicks=100, spend=50.0, revenue=200.0)
        
        perf = mgr.get_campaign_performance("c1")
        assert perf["performance"]["impressions"] == 5000
        assert "budget_utilization" in perf
    
    def test_optimize_bids(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "Test", CampaignObjective.TRAFFIC))
        ag = AdGroup("ag1", "c1", "G1", Targeting(),
                     bid_strategy=BidStrategy.LOWEST_COST, bid_amount=1.0)
        mgr.create_adgroup(ag)
        ag.performance = AdGroupPerformance(impressions=1000, clicks=50, spend=25.0)
        
        recs = mgr.optimize_bids("c1")
        assert len(recs) > 0
    
    def test_optimize_budgets(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "Test", CampaignObjective.TRAFFIC))
        
        ag1 = AdGroup("ag1", "c1", "G1", Targeting(), daily_budget=50.0)
        ag1.performance = AdGroupPerformance(
            impressions=10000, clicks=500, conversions=50, spend=50, revenue=200
        )
        mgr.create_adgroup(ag1)
        
        ag2 = AdGroup("ag2", "c1", "G2", Targeting(), daily_budget=50.0)
        ag2.performance = AdGroupPerformance(
            impressions=10000, clicks=50, conversions=2, spend=50, revenue=10
        )
        mgr.create_adgroup(ag2)
        
        suggestions = mgr.optimize_budgets("c1")
        assert len(suggestions) > 0
    
    def test_report_text(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "Test Campaign", CampaignObjective.TRAFFIC))
        mgr.create_adgroup(AdGroup("ag1", "c1", "G1", Targeting()))
        
        report = mgr.generate_report(format="text")
        assert "Test Campaign" in report
        assert "Ads Performance Report" in report
    
    def test_report_json(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "Test", CampaignObjective.TRAFFIC))
        
        report = mgr.generate_report(format="json")
        data = json.loads(report)
        assert "campaigns" in data
        assert "grand_total" in data
    
    def test_stats(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "Test", CampaignObjective.TRAFFIC))
        mgr.update_campaign_status("c1", CampaignStatus.ACTIVE)
        mgr.add_creative(Creative("cr1", "Ad1", CreativeType.TEXT, body="Hi"))
        
        stats = mgr.get_stats()
        assert stats["total_campaigns"] == 1
        assert stats["active_campaigns"] == 1
        assert stats["total_creatives"] == 1
    
    def test_creative_insights(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "Test", CampaignObjective.TRAFFIC))
        mgr.create_adgroup(AdGroup("ag1", "c1", "G1", Targeting()))
        mgr.add_creative(Creative("cr1", "Ad1", CreativeType.TEXT, body="V1"))
        mgr.add_creative(Creative("cr2", "Ad2", CreativeType.TEXT, body="V2"))
        mgr.assign_creative("ag1", "cr1")
        mgr.assign_creative("ag1", "cr2")
        
        insights = mgr.get_creative_insights("ag1")
        assert "creatives" in insights
    
    def test_creative_insights_invalid(self):
        mgr = AdManager()
        result = mgr.get_creative_insights("nonexistent")
        assert "error" in result
    
    def test_update_adgroup_status(self):
        mgr = AdManager()
        mgr.create_campaign(Campaign("c1", "Test", CampaignObjective.TRAFFIC))
        mgr.create_adgroup(AdGroup("ag1", "c1", "G1", Targeting()))
        
        assert mgr.update_adgroup_status("ag1", AdGroupStatus.PAUSED)
        assert not mgr.update_adgroup_status("nonexistent", AdGroupStatus.PAUSED)


class TestBidManagerTargetROAS:
    def test_target_roas_with_data(self):
        bm = BidManager()
        perf = AdGroupPerformance(
            impressions=10000, clicks=500, conversions=20,
            spend=100.0, revenue=500.0
        )
        result = bm.calculate_optimal_bid(
            BidStrategy.TARGET_ROAS, perf, target_value=3.0, current_bid=1.0
        )
        assert result["reason"] != ""
    
    def test_target_roas_no_data(self):
        bm = BidManager()
        result = bm.calculate_optimal_bid(
            BidStrategy.TARGET_ROAS, AdGroupPerformance(), target_value=3.0
        )
        assert "Insufficient" in result["reason"]

    def test_max_impressions_above_target(self):
        bm = BidManager()
        perf = AdGroupPerformance(impressions=1000, spend=10.0)
        result = bm.calculate_optimal_bid(
            BidStrategy.MAX_IMPRESSIONS, perf, target_value=5.0, current_bid=1.0
        )
        assert result["recommended_bid"] != 1.0
