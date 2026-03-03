"""Tests for Social Listening Engine"""
from bot.social_listening import (
    SocialListeningEngine, ListeningQuery, ListeningMatch,
    AlertRule, Alert, VolumeTracker, CompetitorTracker,
    QueryType, AlertType, AlertSeverity, SentimentLabel
)


class TestListeningQuery:
    def test_keyword_match(self):
        q = ListeningQuery("q1", "test", QueryType.KEYWORD, ["python", "coding"])
        assert q.matches("I love Python programming")
        assert q.matches("coding is fun")
        assert not q.matches("nothing here")
    
    def test_exclude_keywords(self):
        q = ListeningQuery("q1", "test", QueryType.KEYWORD, 
                           ["python"], exclude_keywords=["spam"])
        assert q.matches("python is great")
        assert not q.matches("python spam")
    
    def test_hashtag_match(self):
        q = ListeningQuery("q1", "tags", QueryType.HASHTAG, ["#python", "#ai"])
        assert q.matches("Learning #python today")
        assert q.matches("The future of #AI")
        assert not q.matches("Just a normal tweet")
    
    def test_phrase_match(self):
        q = ListeningQuery("q1", "phrase", QueryType.PHRASE, ["machine learning"])
        assert q.matches("I study machine learning daily")
        assert not q.matches("I study machines")
    
    def test_boolean_match(self):
        q = ListeningQuery("q1", "bool", QueryType.BOOLEAN, ["python", "ai or ml"])
        assert q.matches("python and ai together")
        assert q.matches("python with ml tools")
        assert not q.matches("only python here")
    
    def test_case_insensitive(self):
        q = ListeningQuery("q1", "test", QueryType.KEYWORD, ["Python"])
        assert q.matches("PYTHON IS GREAT")
        assert q.matches("python is great")
    
    def test_to_dict(self):
        q = ListeningQuery("q1", "test", QueryType.KEYWORD, ["python"])
        d = q.to_dict()
        assert d["query_id"] == "q1"
        assert d["query_type"] == "keyword"
    
    def test_default_created_at(self):
        q = ListeningQuery("q1", "test", QueryType.KEYWORD, ["python"])
        assert q.created_at != ""


class TestListeningMatch:
    def test_basic_match(self):
        m = ListeningMatch(
            "m1", "q1", "t1", "user1", 5000,
            "great product", SentimentLabel.POSITIVE, 0.8, ["product"]
        )
        assert m.match_id == "m1"
        assert not m.is_influencer
    
    def test_influencer_detection(self):
        m = ListeningMatch(
            "m1", "q1", "t1", "biguser", 50000,
            "text", SentimentLabel.NEUTRAL, 0.0, []
        )
        assert m.is_influencer
    
    def test_to_dict(self):
        m = ListeningMatch(
            "m1", "q1", "t1", "user", 100,
            "text", SentimentLabel.NEGATIVE, -0.5, ["bad"]
        )
        d = m.to_dict()
        assert d["sentiment"] == "negative"
        assert d["is_influencer"] is False


class TestVolumeTracker:
    def test_record_and_get(self):
        vt = VolumeTracker()
        vt.record("q1")
        vt.record("q1")
        vt.record("q1")
        assert vt.get_current_volume("q1", 1) == 3
    
    def test_moving_average(self):
        vt = VolumeTracker()
        vt.record("q1")
        vt.record("q1")
        avg = vt.get_moving_average("q1", 24)
        assert avg > 0
    
    def test_spike_detection_no_data(self):
        vt = VolumeTracker()
        is_spike, ratio = vt.detect_spike("q1")
        assert not is_spike
        assert ratio == 0.0
    
    def test_spike_detection_with_data(self):
        vt = VolumeTracker()
        # Record a bunch of data
        for _ in range(10):
            vt.record("q1")
        is_spike, ratio = vt.detect_spike("q1", threshold=0.5)
        assert is_spike or ratio > 0
    
    def test_hourly_breakdown(self):
        vt = VolumeTracker()
        vt.record("q1")
        breakdown = vt.get_hourly_breakdown("q1", 24)
        assert len(breakdown) == 24
        total = sum(b["count"] for b in breakdown)
        assert total == 1
    
    def test_clear_specific(self):
        vt = VolumeTracker()
        vt.record("q1")
        vt.record("q2")
        vt.clear("q1")
        assert vt.get_current_volume("q1", 1) == 0
        assert vt.get_current_volume("q2", 1) == 1
    
    def test_clear_all(self):
        vt = VolumeTracker()
        vt.record("q1")
        vt.record("q2")
        vt.clear()
        assert vt.get_current_volume("q1", 1) == 0
        assert vt.get_current_volume("q2", 1) == 0


class TestCompetitorTracker:
    def test_add_competitor(self):
        ct = CompetitorTracker()
        ct.add_competitor("Nike", ["nike", "just do it"])
        comps = ct.list_competitors()
        assert "Nike" in comps
        assert "nike" in comps["Nike"]
    
    def test_remove_competitor(self):
        ct = CompetitorTracker()
        ct.add_competitor("Nike", ["nike"])
        assert ct.remove_competitor("Nike")
        assert not ct.remove_competitor("NonExistent")
    
    def test_check_mention(self):
        ct = CompetitorTracker()
        ct.add_competitor("Nike", ["nike"])
        ct.add_competitor("Adidas", ["adidas"])
        
        match = ListeningMatch(
            "m1", "q1", "t1", "user", 100,
            "Nike shoes are great", SentimentLabel.POSITIVE, 0.5, []
        )
        mentioned = ct.check_mention("Nike shoes are great", match)
        assert "Nike" in mentioned
        assert "Adidas" not in mentioned
    
    def test_sentiment_breakdown(self):
        ct = CompetitorTracker()
        ct.add_competitor("Nike", ["nike"])
        
        for i, sentiment in enumerate([SentimentLabel.POSITIVE, SentimentLabel.NEGATIVE, SentimentLabel.POSITIVE]):
            match = ListeningMatch(
                f"m{i}", "q1", f"t{i}", "user", 100,
                "nike text", sentiment, 0.5 if sentiment == SentimentLabel.POSITIVE else -0.5, []
            )
            ct.check_mention("nike text", match)
        
        breakdown = ct.sentiment_breakdown("Nike") if hasattr(ct, 'sentiment_breakdown') else ct.get_sentiment_breakdown("Nike")
        assert breakdown["positive"] == 2
        assert breakdown["negative"] == 1
    
    def test_share_of_voice(self):
        ct = CompetitorTracker()
        ct.add_competitor("Brand1", ["brand1"])
        ct.add_competitor("Brand2", ["brand2"])
        
        m1 = ListeningMatch("m1", "q1", "t1", "u", 100, "brand1", SentimentLabel.POSITIVE, 0.5, [])
        m2 = ListeningMatch("m2", "q1", "t2", "u", 100, "brand2", SentimentLabel.POSITIVE, 0.5, [])
        m3 = ListeningMatch("m3", "q1", "t3", "u", 100, "brand1", SentimentLabel.POSITIVE, 0.5, [])
        
        ct.check_mention("brand1 rocks", m1)
        ct.check_mention("brand2 cool", m2)
        ct.check_mention("brand1 again", m3)
        
        sov = ct.get_share_of_voice()
        assert sov["Brand1"]["mentions"] == 2
        assert sov["Brand2"]["mentions"] == 1
        assert sov["Brand1"]["share"] > sov["Brand2"]["share"]
    
    def test_compare_competitors(self):
        ct = CompetitorTracker()
        ct.add_competitor("A", ["a_brand"])
        m = ListeningMatch("m1", "q1", "t1", "u", 100, "a_brand", SentimentLabel.POSITIVE, 0.5, [])
        ct.check_mention("a_brand is great", m)
        
        comparison = ct.compare_competitors()
        assert comparison["leader"] == "A"
        assert comparison["total_mentions"] == 1


class TestSocialListeningEngine:
    def test_add_and_list_queries(self):
        engine = SocialListeningEngine()
        q = ListeningQuery("q1", "test", QueryType.KEYWORD, ["python"])
        engine.add_query(q)
        assert len(engine.list_queries()) == 1
    
    def test_remove_query(self):
        engine = SocialListeningEngine()
        q = ListeningQuery("q1", "test", QueryType.KEYWORD, ["python"])
        engine.add_query(q)
        assert engine.remove_query("q1")
        assert not engine.remove_query("nonexistent")
        assert len(engine.list_queries()) == 0
    
    def test_get_query(self):
        engine = SocialListeningEngine()
        q = ListeningQuery("q1", "test", QueryType.KEYWORD, ["python"])
        engine.add_query(q)
        assert engine.get_query("q1") is not None
        assert engine.get_query("nonexistent") is None
    
    def test_process_tweet_match(self):
        engine = SocialListeningEngine()
        engine.add_query(ListeningQuery("q1", "test", QueryType.KEYWORD, ["python"]))
        
        matches = engine.process_tweet("t1", "user1", "I love python programming")
        assert len(matches) == 1
        assert matches[0].query_id == "q1"
    
    def test_process_tweet_no_match(self):
        engine = SocialListeningEngine()
        engine.add_query(ListeningQuery("q1", "test", QueryType.KEYWORD, ["python"]))
        
        matches = engine.process_tweet("t1", "user1", "I love javascript")
        assert len(matches) == 0
    
    def test_process_tweet_multiple_queries(self):
        engine = SocialListeningEngine()
        engine.add_query(ListeningQuery("q1", "py", QueryType.KEYWORD, ["python"]))
        engine.add_query(ListeningQuery("q2", "ai", QueryType.KEYWORD, ["AI"]))
        
        matches = engine.process_tweet("t1", "user1", "python AI project")
        assert len(matches) == 2
    
    def test_sentiment_analysis_positive(self):
        engine = SocialListeningEngine()
        engine.add_query(ListeningQuery("q1", "test", QueryType.KEYWORD, ["product"]))
        
        matches = engine.process_tweet("t1", "user1", "This product is amazing and great")
        assert len(matches) == 1
        assert matches[0].sentiment == SentimentLabel.POSITIVE
    
    def test_sentiment_analysis_negative(self):
        engine = SocialListeningEngine()
        engine.add_query(ListeningQuery("q1", "test", QueryType.KEYWORD, ["product"]))
        
        matches = engine.process_tweet("t1", "user1", "This product is terrible and awful")
        assert len(matches) == 1
        assert matches[0].sentiment == SentimentLabel.NEGATIVE
    
    def test_sentiment_analysis_neutral(self):
        engine = SocialListeningEngine()
        engine.add_query(ListeningQuery("q1", "test", QueryType.KEYWORD, ["product"]))
        
        matches = engine.process_tweet("t1", "user1", "The product arrived today")
        assert len(matches) == 1
        assert matches[0].sentiment == SentimentLabel.NEUTRAL
    
    def test_alert_rule_management(self):
        engine = SocialListeningEngine()
        rule = AlertRule("r1", "spike", AlertType.VOLUME_SPIKE, "q1", 2.0)
        engine.add_alert_rule(rule)
        assert len(engine.alert_rules) == 1
        assert engine.remove_alert_rule("r1")
        assert not engine.remove_alert_rule("nonexistent")
    
    def test_get_matches_with_filter(self):
        engine = SocialListeningEngine()
        engine.add_query(ListeningQuery("q1", "test", QueryType.KEYWORD, ["python"]))
        engine.add_query(ListeningQuery("q2", "test2", QueryType.KEYWORD, ["java"]))
        
        engine.process_tweet("t1", "user1", "python is great")
        engine.process_tweet("t2", "user1", "java is good")
        
        all_matches = engine.get_matches()
        assert len(all_matches) == 2
        
        q1_matches = engine.get_matches(query_id="q1")
        assert len(q1_matches) == 1
    
    def test_get_matches_sentiment_filter(self):
        engine = SocialListeningEngine()
        engine.add_query(ListeningQuery("q1", "test", QueryType.KEYWORD, ["product"]))
        
        engine.process_tweet("t1", "u1", "product is amazing")
        engine.process_tweet("t2", "u2", "product is terrible")
        
        pos = engine.get_matches(sentiment=SentimentLabel.POSITIVE)
        neg = engine.get_matches(sentiment=SentimentLabel.NEGATIVE)
        assert len(pos) >= 1
        assert len(neg) >= 1
    
    def test_top_authors(self):
        engine = SocialListeningEngine()
        engine.add_query(ListeningQuery("q1", "test", QueryType.KEYWORD, ["test"]))
        
        engine.process_tweet("t1", "alice", "test tweet 1", author_followers=1000)
        engine.process_tweet("t2", "alice", "test tweet 2", author_followers=1000)
        engine.process_tweet("t3", "bob", "test tweet 3", author_followers=500)
        
        top = engine.get_top_authors()
        assert top[0]["author"] == "alice"
        assert top[0]["mentions"] == 2
    
    def test_keyword_frequency(self):
        engine = SocialListeningEngine()
        engine.add_query(ListeningQuery("q1", "test", QueryType.KEYWORD, ["python", "ai"]))
        
        engine.process_tweet("t1", "u1", "python and ai together")
        engine.process_tweet("t2", "u2", "python is cool")
        
        freq = engine.get_keyword_frequency("q1")
        assert freq["python"] >= 2
    
    def test_competitor_integration(self):
        engine = SocialListeningEngine()
        engine.add_query(ListeningQuery("q1", "test", QueryType.KEYWORD, ["shoes"]))
        engine.competitor_tracker.add_competitor("Nike", ["nike"])
        
        engine.process_tweet("t1", "u1", "nike shoes are great")
        
        sov = engine.competitor_tracker.get_share_of_voice()
        assert "Nike" in sov
    
    def test_register_callback(self):
        engine = SocialListeningEngine()
        alerts_received = []
        
        engine.register_callback(AlertType.VOLUME_SPIKE, lambda a: alerts_received.append(a))
        # Just verify it doesn't error
        assert len(engine._callbacks[AlertType.VOLUME_SPIKE]) == 1
    
    def test_acknowledge_alert(self):
        engine = SocialListeningEngine()
        alert = Alert("a1", "r1", AlertType.VOLUME_SPIKE, AlertSeverity.WARNING, "test")
        engine._alerts.append(alert)
        
        assert engine.acknowledge_alert("a1")
        assert not engine.acknowledge_alert("nonexistent")
        assert engine._alerts[0].acknowledged
    
    def test_get_alerts_filter(self):
        engine = SocialListeningEngine()
        engine._alerts.append(
            Alert("a1", "r1", AlertType.VOLUME_SPIKE, AlertSeverity.WARNING, "warn")
        )
        engine._alerts.append(
            Alert("a2", "r2", AlertType.CRISIS_SIGNAL, AlertSeverity.CRITICAL, "crisis")
        )
        
        all_alerts = engine.get_alerts()
        assert len(all_alerts) == 2
        
        critical = engine.get_alerts(severity=AlertSeverity.CRITICAL)
        assert len(critical) == 1
        
        unack = engine.get_alerts(unacknowledged_only=True)
        assert len(unack) == 2
    
    def test_report_text(self):
        engine = SocialListeningEngine()
        engine.add_query(ListeningQuery("q1", "test", QueryType.KEYWORD, ["python"]))
        engine.process_tweet("t1", "user1", "python is amazing", author_followers=5000, reach=10000)
        
        report = engine.generate_report(format="text")
        assert "Social Listening Report" in report
        assert "Total Matches: 1" in report
    
    def test_report_json(self):
        engine = SocialListeningEngine()
        engine.add_query(ListeningQuery("q1", "test", QueryType.KEYWORD, ["python"]))
        engine.process_tweet("t1", "user1", "python is great")
        
        report = engine.generate_report(format="json")
        data = json.loads(report)
        assert data["total_matches"] == 1
    
    def test_report_csv(self):
        engine = SocialListeningEngine()
        engine.add_query(ListeningQuery("q1", "test", QueryType.KEYWORD, ["python"]))
        engine.process_tweet("t1", "user1", "python is great")
        
        report = engine.generate_report(format="csv")
        assert "Total Matches" in report
    
    def test_stats(self):
        engine = SocialListeningEngine()
        engine.add_query(ListeningQuery("q1", "test", QueryType.KEYWORD, ["python"]))
        engine.add_alert_rule(AlertRule("r1", "test", AlertType.VOLUME_SPIKE, "q1", 2.0))
        
        stats = engine.get_stats()
        assert stats["active_queries"] == 1
        assert stats["alert_rules"] == 1
        assert stats["total_matches"] == 0
    
    def test_inactive_query_not_matched(self):
        engine = SocialListeningEngine()
        q = ListeningQuery("q1", "test", QueryType.KEYWORD, ["python"], active=False)
        engine.add_query(q)
        
        matches = engine.process_tweet("t1", "user1", "python is great")
        assert len(matches) == 0
    
    def test_sentiment_over_time(self):
        engine = SocialListeningEngine()
        engine.add_query(ListeningQuery("q1", "test", QueryType.KEYWORD, ["test"]))
        engine.process_tweet("t1", "u1", "test is amazing")
        
        trend = engine.get_sentiment_over_time("q1", hours=24)
        assert len(trend) == 24


import json


class TestAlertRule:
    def test_to_dict(self):
        rule = AlertRule("r1", "test", AlertType.VOLUME_SPIKE, "q1", 2.0,
                         severity=AlertSeverity.CRITICAL)
        d = rule.to_dict()
        assert d["alert_type"] == "volume_spike"
        assert d["severity"] == "critical"


class TestAlert:
    def test_default_timestamp(self):
        alert = Alert("a1", "r1", AlertType.VOLUME_SPIKE, AlertSeverity.WARNING, "msg")
        assert alert.triggered_at != ""
    
    def test_to_dict(self):
        alert = Alert("a1", "r1", AlertType.VOLUME_SPIKE, AlertSeverity.WARNING, "msg",
                       data={"ratio": 3.5})
        d = alert.to_dict()
        assert d["alert_type"] == "volume_spike"
        assert d["data"]["ratio"] == 3.5
