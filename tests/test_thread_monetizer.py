"""Tests for Thread Monetizer module."""

import pytest

from bot.thread_monetizer import (
    CTAType, PlacementPosition, ViralStage, CTAConfig, ConversionEvent,
    ThreadMetrics, ViralDetector, CTAOptimizer, MonetizationStore,
    ThreadMonetizer,
)


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "test_monetize.db")


@pytest.fixture
def monetizer(tmp_db):
    return ThreadMonetizer(db_path=tmp_db)


@pytest.fixture
def store(tmp_db):
    return MonetizationStore(db_path=tmp_db)


class TestCTAConfig:
    def test_create_cta(self):
        cta = CTAConfig(
            cta_id="test1", cta_type=CTAType.NEWSLETTER,
            text="Subscribe now", url="https://example.com",
        )
        assert cta.cta_id == "test1"
        assert cta.cta_type == CTAType.NEWSLETTER
        assert cta.active is True

    def test_render(self):
        cta = CTAConfig(
            cta_id="test1", cta_type=CTAType.PRODUCT,
            text="Buy now", url="https://shop.com", emoji="🛒",
        )
        rendered = cta.render()
        assert "🛒" in rendered
        assert "Buy now" in rendered
        assert "https://shop.com" in rendered

    def test_to_dict(self):
        cta = CTAConfig(
            cta_id="test1", cta_type=CTAType.AFFILIATE,
            text="Check it", url="https://aff.com",
        )
        d = cta.to_dict()
        assert d["cta_type"] == "affiliate"
        assert d["placement"] == "thread_end"
        assert d["cta_id"] == "test1"

    def test_all_cta_types(self):
        for ct in CTAType:
            cta = CTAConfig(
                cta_id=f"t_{ct.value}", cta_type=ct,
                text="text", url="https://x.com",
            )
            assert cta.cta_type == ct


class TestViralDetector:
    def test_initial_velocity_zero(self):
        d = ViralDetector()
        assert d.calculate_velocity("t1") == 0.0

    def test_record_and_velocity(self):
        d = ViralDetector()
        d._history["t1"] = [
            {"timestamp": "2026-01-01T00:00:00+00:00", "likes": 0, "retweets": 0, "replies": 0, "impressions": 0},
            {"timestamp": "2026-01-01T01:00:00+00:00", "likes": 100, "retweets": 50, "replies": 20, "impressions": 5000},
        ]
        v = d.calculate_velocity("t1")
        assert v == 170.0  # (170 - 0) / 1 hour

    def test_classify_dormant(self):
        d = ViralDetector()
        stage = d.classify_stage("t1", {"engagement_rate": 0.5})
        assert stage == ViralStage.DORMANT

    def test_classify_warming(self):
        d = ViralDetector()
        d._history["t1"] = [
            {"timestamp": "2026-01-01T00:00:00+00:00", "likes": 0, "retweets": 0, "replies": 0, "impressions": 0},
            {"timestamp": "2026-01-01T01:00:00+00:00", "likes": 5, "retweets": 2, "replies": 1, "impressions": 200},
        ]
        stage = d.classify_stage("t1", {"engagement_rate": 2.0})
        assert stage == ViralStage.WARMING

    def test_classify_viral(self):
        d = ViralDetector()
        d._history["t1"] = [
            {"timestamp": "2026-01-01T00:00:00+00:00", "likes": 0, "retweets": 0, "replies": 0, "impressions": 0},
            {"timestamp": "2026-01-01T01:00:00+00:00", "likes": 40, "retweets": 15, "replies": 5, "impressions": 10000},
        ]
        stage = d.classify_stage("t1", {"engagement_rate": 6.0})
        assert stage == ViralStage.VIRAL

    def test_classify_peak(self):
        d = ViralDetector()
        d._history["t1"] = [
            {"timestamp": "2026-01-01T00:00:00+00:00", "likes": 0, "retweets": 0, "replies": 0, "impressions": 0},
            {"timestamp": "2026-01-01T01:00:00+00:00", "likes": 80, "retweets": 30, "replies": 10, "impressions": 50000},
        ]
        stage = d.classify_stage("t1", {"engagement_rate": 9.0})
        assert stage == ViralStage.PEAK

    def test_viral_score(self):
        d = ViralDetector()
        score = d.get_viral_score("t1", {"engagement_rate": 5.0, "impressions": 50000})
        assert 0 <= score <= 100

    def test_viral_score_high(self):
        d = ViralDetector()
        d._history["t1"] = [
            {"timestamp": "2026-01-01T00:00:00+00:00", "likes": 0, "retweets": 0, "replies": 0, "impressions": 0},
            {"timestamp": "2026-01-01T01:00:00+00:00", "likes": 100, "retweets": 50, "replies": 20, "impressions": 100000},
        ]
        score = d.get_viral_score("t1", {"engagement_rate": 10.0, "impressions": 100000})
        assert score >= 50

    def test_snapshot_limit(self):
        d = ViralDetector()
        for i in range(60):
            d.record_snapshot("t1", {"likes": i, "retweets": 0, "replies": 0, "impressions": i * 100})
        assert len(d._history["t1"]) == 48


class TestCTAOptimizer:
    def test_record_performance(self):
        o = CTAOptimizer()
        o.record_performance("cta1", 1000, 50, 5, 10.0)
        assert len(o._performance["cta1"]) == 1
        assert o._performance["cta1"][0]["ctr"] == 5.0

    def test_get_best_placement(self):
        o = CTAOptimizer()
        o.record_performance("thread_end_1", 1000, 50, 5)
        o.record_performance("mid_thread_1", 1000, 80, 8)
        best = o.get_best_placement()
        assert isinstance(best, PlacementPosition)

    def test_ab_winner_insufficient_data(self):
        o = CTAOptimizer()
        o.record_performance("a", 100, 10, 1)
        o.record_performance("b", 100, 10, 2)
        assert o.get_ab_winner("a", "b") is None  # < 30 clicks

    def test_ab_winner_with_data(self):
        o = CTAOptimizer()
        for _ in range(5):
            o.record_performance("a", 1000, 10, 1)
            o.record_performance("b", 1000, 10, 5)
        winner = o.get_ab_winner("a", "b")
        assert winner == "b"

    def test_suggest_cta_text(self):
        o = CTAOptimizer()
        for ct in CTAType:
            suggestions = o.suggest_cta_text("SEO Tools", ct)
            assert len(suggestions) >= 1
            for s in suggestions:
                assert isinstance(s, str)


class TestMonetizationStore:
    def test_save_and_get_cta(self, store):
        cta = CTAConfig(
            cta_id="cta1", cta_type=CTAType.NEWSLETTER,
            text="Subscribe", url="https://news.com",
        )
        store.save_cta(cta)
        loaded = store.get_cta("cta1")
        assert loaded is not None
        assert loaded.cta_type == CTAType.NEWSLETTER
        assert loaded.url == "https://news.com"

    def test_list_active_ctas(self, store):
        for i in range(3):
            cta = CTAConfig(
                cta_id=f"cta{i}", cta_type=CTAType.PRODUCT,
                text=f"Product {i}", url=f"https://p{i}.com",
                active=i != 1,  # cta1 is inactive
            )
            store.save_cta(cta)
        active = store.list_active_ctas()
        assert len(active) == 2

    def test_save_thread_metrics(self, store):
        tm = ThreadMetrics(
            thread_id="thread1", total_likes=100,
            total_impressions=5000, engagement_rate=3.5,
            viral_stage=ViralStage.TRENDING,
        )
        store.save_thread_metrics(tm)
        loaded = store.get_thread_metrics("thread1")
        assert loaded is not None
        assert loaded.total_likes == 100
        assert loaded.viral_stage == ViralStage.TRENDING

    def test_get_viral_threads(self, store):
        for stage in [ViralStage.DORMANT, ViralStage.TRENDING, ViralStage.VIRAL]:
            store.save_thread_metrics(ThreadMetrics(
                thread_id=f"t_{stage.value}",
                viral_stage=stage, velocity=10.0,
            ))
        viral = store.get_viral_threads(ViralStage.TRENDING)
        assert len(viral) == 2
        stages = {v.viral_stage for v in viral}
        assert ViralStage.TRENDING in stages
        assert ViralStage.VIRAL in stages

    def test_record_conversion(self, store):
        event = ConversionEvent(
            event_id="e1", thread_id="t1", cta_id="c1",
            click_count=10, conversion_count=2, revenue=5.0,
        )
        store.record_conversion(event)
        summary = store.get_revenue_summary(30)
        assert summary["total_clicks"] == 10
        assert summary["total_revenue"] == 5.0

    def test_revenue_summary_empty(self, store):
        summary = store.get_revenue_summary(30)
        assert summary["total_revenue"] == 0
        assert summary["total_clicks"] == 0

    def test_top_threads_by_revenue(self, store):
        for i in range(3):
            store.record_conversion(ConversionEvent(
                event_id=f"e{i}", thread_id=f"t{i}", cta_id="c1",
                click_count=10, revenue=float(i * 10),
            ))
        top = store.get_top_threads_by_revenue(2)
        assert len(top) == 2
        assert top[0]["total_revenue"] >= top[1]["total_revenue"]


class TestThreadMonetizer:
    def test_create_cta(self, monetizer):
        cta = monetizer.create_cta(
            CTAType.NEWSLETTER, "Subscribe", "https://news.com",
        )
        assert cta.cta_id
        assert cta.cta_type == CTAType.NEWSLETTER

    def test_update_thread_metrics(self, monetizer):
        tm = monetizer.update_thread_metrics("t1", {
            "likes": 50, "retweets": 20, "replies": 10,
            "impressions": 5000, "bookmarks": 15,
        })
        assert tm.thread_id == "t1"
        assert tm.total_likes == 50
        assert tm.engagement_rate > 0

    def test_should_monetize_no_metrics(self, monetizer):
        should, reason = monetizer.should_monetize("nonexistent")
        assert should is False
        assert "No metrics" in reason

    def test_should_monetize_no_cta(self, monetizer):
        monetizer.update_thread_metrics("t1", {
            "likes": 100, "retweets": 50, "replies": 20,
            "impressions": 10000,
        })
        should, reason = monetizer.should_monetize("t1")
        assert should is False
        assert "No active CTAs" in reason

    def test_should_monetize_qualifies(self, monetizer):
        monetizer.create_cta(CTAType.PRODUCT, "Buy", "https://shop.com",
                             min_engagement_rate=1.0, min_impressions=100)
        monetizer.update_thread_metrics("t1", {
            "likes": 100, "retweets": 50, "replies": 20,
            "impressions": 5000,
        })
        should, reason = monetizer.should_monetize("t1")
        assert should is True
        assert "qualifies" in reason

    def test_should_monetize_already_monetized(self, monetizer):
        monetizer.create_cta(CTAType.PRODUCT, "Buy", "https://shop.com",
                             min_engagement_rate=0.5)
        monetizer.update_thread_metrics("t1", {
            "likes": 100, "retweets": 50, "replies": 20,
            "impressions": 5000,
        })
        monetizer.mark_monetized("t1")
        should, reason = monetizer.should_monetize("t1")
        assert should is False
        assert "already monetized" in reason

    def test_get_best_cta(self, monetizer):
        monetizer.create_cta(CTAType.NEWSLETTER, "Subscribe", "https://n.com",
                             min_engagement_rate=0.5)
        monetizer.create_cta(CTAType.PRODUCT, "Buy", "https://p.com",
                             min_engagement_rate=0.5)
        monetizer.update_thread_metrics("t1", {
            "likes": 100, "retweets": 50, "replies": 20,
            "impressions": 5000,
        })
        best = monetizer.get_best_cta_for_thread("t1")
        assert best is not None
        assert best.cta_type == CTAType.PRODUCT  # Higher priority

    def test_record_conversion(self, monetizer):
        monetizer.create_cta(CTAType.PRODUCT, "Buy", "https://p.com")
        event = monetizer.record_conversion("t1", "cta1", clicks=100,
                                             conversions=10, revenue=50.0)
        assert event.click_count == 100
        assert event.revenue == 50.0

    def test_viral_score(self, monetizer):
        monetizer.update_thread_metrics("t1", {
            "likes": 10, "retweets": 5, "replies": 2,
            "impressions": 1000,
        })
        score = monetizer.get_viral_score("t1")
        assert 0 <= score <= 100

    def test_generate_report(self, monetizer):
        monetizer.create_cta(CTAType.PRODUCT, "Buy", "https://p.com")
        monetizer.update_thread_metrics("t1", {
            "likes": 100, "retweets": 50, "impressions": 5000,
        })
        monetizer.record_conversion("t1", "cta1", clicks=50, revenue=25.0)
        report = monetizer.generate_report(30)
        assert "Monetization Report" in report
        assert "$" in report

    def test_generate_json_report(self, monetizer):
        report = monetizer.generate_json_report(30)
        assert "revenue" in report
        assert "viral_threads" in report
        assert "active_ctas" in report
        assert "generated_at" in report

    def test_mark_monetized(self, monetizer):
        monetizer.update_thread_metrics("t1", {
            "likes": 50, "impressions": 5000,
        })
        monetizer.mark_monetized("t1")
        tm = monetizer.store.get_thread_metrics("t1")
        assert tm.monetized is True
        assert tm.cta_inserted_at is not None

    def test_full_flow(self, monetizer):
        """Integration test: full monetization flow."""
        # 1. Create CTA
        cta = monetizer.create_cta(
            CTAType.AFFILIATE, "Check our tools", "https://tools.com",
            min_engagement_rate=1.0, min_impressions=500,
        )

        # 2. Track thread metrics
        monetizer.update_thread_metrics("thread_001", {
            "likes": 200, "retweets": 80, "replies": 40,
            "impressions": 10000, "bookmarks": 50,
        })

        # 3. Check if monetizable
        should, _ = monetizer.should_monetize("thread_001")
        assert should is True

        # 4. Get best CTA
        best = monetizer.get_best_cta_for_thread("thread_001")
        assert best is not None

        # 5. Mark monetized
        monetizer.mark_monetized("thread_001")

        # 6. Record conversion
        monetizer.record_conversion("thread_001", cta.cta_id,
                                     clicks=500, conversions=25, revenue=125.0)

        # 7. Verify report
        report = monetizer.generate_report()
        assert "$125.00" in report
