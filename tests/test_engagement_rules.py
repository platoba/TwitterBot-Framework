"""Tests for Engagement Rules Engine."""
import time
import pytest
from bot.engagement_rules import (
    ActionType, ConditionType, RulePriority,
    RuleCondition, RuleAction, EngagementRule,
    SafetyGuardrails, EngagementRulesEngine, RuleTemplates,
)


# ──── RuleCondition ────

class TestRuleCondition:
    def _tweet(self, **kw) -> dict:
        defaults = {
            "text": "Check out this new AI tool for marketing",
            "author": {"id": "a1", "followers_count": 5000, "verified": False},
            "hashtags": ["ai", "marketing"],
            "like_count": 100,
            "retweet_count": 50,
            "lang": "en",
            "media": [],
        }
        defaults.update(kw)
        return defaults

    def test_keyword_match(self):
        c = RuleCondition(ConditionType.KEYWORD_MATCH, ["AI", "tool"])
        assert c.evaluate(self._tweet())

    def test_keyword_no_match(self):
        c = RuleCondition(ConditionType.KEYWORD_MATCH, ["blockchain"])
        assert not c.evaluate(self._tweet())

    def test_keyword_exclude(self):
        c = RuleCondition(ConditionType.KEYWORD_EXCLUDE, ["spam", "scam"])
        assert c.evaluate(self._tweet())

    def test_keyword_exclude_hit(self):
        c = RuleCondition(ConditionType.KEYWORD_EXCLUDE, ["AI"])
        assert not c.evaluate(self._tweet())

    def test_hashtag_match(self):
        c = RuleCondition(ConditionType.HASHTAG_MATCH, ["#ai"])
        assert c.evaluate(self._tweet())

    def test_hashtag_no_match(self):
        c = RuleCondition(ConditionType.HASHTAG_MATCH, ["#crypto"])
        assert not c.evaluate(self._tweet())

    def test_follower_min(self):
        c = RuleCondition(ConditionType.FOLLOWER_MIN, 1000)
        assert c.evaluate(self._tweet())

    def test_follower_min_not_met(self):
        c = RuleCondition(ConditionType.FOLLOWER_MIN, 10000)
        assert not c.evaluate(self._tweet())

    def test_follower_max(self):
        c = RuleCondition(ConditionType.FOLLOWER_MAX, 10000)
        assert c.evaluate(self._tweet())

    def test_engagement_min(self):
        c = RuleCondition(ConditionType.ENGAGEMENT_MIN, 100)
        assert c.evaluate(self._tweet())  # 100+50=150

    def test_engagement_min_not_met(self):
        c = RuleCondition(ConditionType.ENGAGEMENT_MIN, 200)
        assert not c.evaluate(self._tweet())

    def test_author_in_list(self):
        c = RuleCondition(ConditionType.AUTHOR_IN_LIST, ["a1", "a2"])
        assert c.evaluate(self._tweet())

    def test_author_not_in_list(self):
        c = RuleCondition(ConditionType.AUTHOR_NOT_IN_LIST, ["x1", "x2"])
        assert c.evaluate(self._tweet())

    def test_language(self):
        c = RuleCondition(ConditionType.LANGUAGE, "en")
        assert c.evaluate(self._tweet())

    def test_language_mismatch(self):
        c = RuleCondition(ConditionType.LANGUAGE, "ja")
        assert not c.evaluate(self._tweet())

    def test_is_reply(self):
        c = RuleCondition(ConditionType.IS_REPLY)
        assert not c.evaluate(self._tweet())
        assert c.evaluate(self._tweet(in_reply_to_id="t123"))

    def test_is_retweet(self):
        c = RuleCondition(ConditionType.IS_RETWEET)
        assert not c.evaluate(self._tweet())
        assert c.evaluate(self._tweet(text="RT @someone: hello"))

    def test_has_media(self):
        c = RuleCondition(ConditionType.HAS_MEDIA)
        assert not c.evaluate(self._tweet())
        assert c.evaluate(self._tweet(media=["img1.jpg"]))

    def test_has_link(self):
        c = RuleCondition(ConditionType.HAS_LINK)
        assert not c.evaluate(self._tweet())
        assert c.evaluate(self._tweet(text="Check https://example.com"))

    def test_tweet_length_min(self):
        c = RuleCondition(ConditionType.TWEET_LENGTH_MIN, 10)
        assert c.evaluate(self._tweet())

    def test_tweet_length_max(self):
        c = RuleCondition(ConditionType.TWEET_LENGTH_MAX, 280)
        assert c.evaluate(self._tweet())

    def test_verified_only(self):
        c = RuleCondition(ConditionType.VERIFIED_ONLY)
        assert not c.evaluate(self._tweet())
        t = self._tweet()
        t["author"]["verified"] = True
        assert c.evaluate(t)

    def test_time_window(self):
        c = RuleCondition(ConditionType.TIME_WINDOW, {"start_hour": 0, "end_hour": 24})
        assert c.evaluate(self._tweet())

    def test_negate(self):
        c = RuleCondition(ConditionType.KEYWORD_MATCH, ["AI"], negate=True)
        assert not c.evaluate(self._tweet())  # AI present, but negated


# ──── RuleAction ────

class TestRuleAction:
    def test_should_execute_always(self):
        a = RuleAction(ActionType.LIKE, probability=1.0)
        assert a.should_execute()

    def test_should_execute_never(self):
        a = RuleAction(ActionType.LIKE, probability=0.0)
        assert not a.should_execute()


# ──── EngagementRule ────

class TestEngagementRule:
    def _tweet(self) -> dict:
        return {
            "text": "AI is transforming marketing",
            "author": {"id": "a1", "followers_count": 5000},
            "hashtags": ["ai"],
            "like_count": 200,
            "retweet_count": 50,
        }

    def test_matches_all_conditions(self):
        rule = EngagementRule(
            rule_id="r1", name="Test",
            conditions=[
                RuleCondition(ConditionType.KEYWORD_MATCH, ["AI"]),
                RuleCondition(ConditionType.FOLLOWER_MIN, 1000),
            ],
            match_all=True,
        )
        assert rule.matches(self._tweet())

    def test_matches_any_condition(self):
        rule = EngagementRule(
            rule_id="r1", name="Test",
            conditions=[
                RuleCondition(ConditionType.KEYWORD_MATCH, ["blockchain"]),
                RuleCondition(ConditionType.FOLLOWER_MIN, 1000),
            ],
            match_all=False,
        )
        assert rule.matches(self._tweet())

    def test_no_match_all(self):
        rule = EngagementRule(
            rule_id="r1", name="Test",
            conditions=[
                RuleCondition(ConditionType.KEYWORD_MATCH, ["blockchain"]),
                RuleCondition(ConditionType.FOLLOWER_MIN, 1000),
            ],
            match_all=True,
        )
        assert not rule.matches(self._tweet())

    def test_disabled_rule_no_match(self):
        rule = EngagementRule(
            rule_id="r1", name="Test", enabled=False,
            conditions=[RuleCondition(ConditionType.KEYWORD_MATCH, ["AI"])],
        )
        assert not rule.matches(self._tweet())

    def test_daily_limit_reached(self):
        rule = EngagementRule(
            rule_id="r1", name="Test", max_daily_triggers=0,
            conditions=[RuleCondition(ConditionType.KEYWORD_MATCH, ["AI"])],
        )
        assert not rule.matches(self._tweet())

    def test_cooldown_respected(self):
        rule = EngagementRule(
            rule_id="r1", name="Test", cooldown_sec=60,
            conditions=[RuleCondition(ConditionType.KEYWORD_MATCH, ["AI"])],
        )
        rule.trigger()
        assert not rule.matches(self._tweet())

    def test_trigger_increments(self):
        rule = EngagementRule(rule_id="r1", name="Test")
        rule.trigger()
        assert rule._trigger_count == 1

    def test_reset_daily(self):
        rule = EngagementRule(rule_id="r1", name="Test")
        rule._trigger_count = 100
        rule.reset_daily()
        assert rule._trigger_count == 0

    def test_empty_conditions_matches_all(self):
        rule = EngagementRule(rule_id="r1", name="Test", conditions=[])
        assert rule.matches(self._tweet())

    def test_summary(self):
        rule = EngagementRule(
            rule_id="r1", name="Test",
            actions=[RuleAction(ActionType.LIKE)],
            tags=["niche"],
        )
        s = rule.summary()
        assert s["id"] == "r1"
        assert "like" in s["actions"]


# ──── SafetyGuardrails ────

class TestSafetyGuardrails:
    def test_can_act_normal(self):
        g = SafetyGuardrails()
        g._last_action_time = 0
        ok, _ = g.can_act(ActionType.LIKE)
        assert ok

    def test_blocked_author(self):
        g = SafetyGuardrails()
        g.add_to_blocklist("bad_user")
        ok, reason = g.can_act(ActionType.LIKE, "bad_user")
        assert not ok
        assert "blocklist" in reason.lower()

    def test_protected_author(self):
        g = SafetyGuardrails()
        g.protected_authors.add("vip")
        ok, reason = g.can_act(ActionType.LIKE, "vip")
        assert not ok

    def test_min_interval(self):
        g = SafetyGuardrails()
        g._last_action_time = time.time()
        ok, reason = g.can_act(ActionType.LIKE)
        assert not ok
        assert "interval" in reason.lower()

    def test_daily_limit(self):
        g = SafetyGuardrails()
        g._last_action_time = 0
        g._counts["like"] = 500
        ok, reason = g.can_act(ActionType.LIKE)
        assert not ok
        assert "limit" in reason.lower()

    def test_record_action(self):
        g = SafetyGuardrails()
        g.record_action(ActionType.LIKE)
        assert g._counts["like"] == 1

    def test_reset_daily(self):
        g = SafetyGuardrails()
        g._counts["like"] = 500
        g.reset_daily()
        assert g._counts["like"] == 0

    def test_remove_from_blocklist(self):
        g = SafetyGuardrails()
        g.add_to_blocklist("user1")
        g.remove_from_blocklist("user1")
        assert "user1" not in g.blocklist

    def test_stats(self):
        g = SafetyGuardrails()
        s = g.stats()
        assert "counts" in s
        assert "limits" in s


# ──── EngagementRulesEngine ────

class TestEngagementRulesEngine:
    def _engine(self) -> EngagementRulesEngine:
        return EngagementRulesEngine(db_path=":memory:")

    def _tweet(self, **kw) -> dict:
        defaults = {
            "id": "t1",
            "text": "AI tools for ecommerce growth",
            "author": {"id": "auth1", "followers_count": 5000},
            "hashtags": ["ai", "ecommerce"],
            "like_count": 200,
            "retweet_count": 50,
        }
        defaults.update(kw)
        return defaults

    def _rule(self, rid="r1", **kw) -> EngagementRule:
        defaults = {
            "rule_id": rid,
            "name": "Test Rule",
            "conditions": [RuleCondition(ConditionType.KEYWORD_MATCH, ["AI"])],
            "actions": [RuleAction(ActionType.LIKE, probability=1.0)],
            "cooldown_sec": 0,
        }
        defaults.update(kw)
        return EngagementRule(**defaults)

    def test_add_rule(self):
        e = self._engine()
        assert e.add_rule(self._rule())

    def test_add_duplicate_fails(self):
        e = self._engine()
        e.add_rule(self._rule())
        assert not e.add_rule(self._rule())

    def test_remove_rule(self):
        e = self._engine()
        e.add_rule(self._rule())
        assert e.remove_rule("r1")
        assert e.get_rule("r1") is None

    def test_remove_nonexistent(self):
        e = self._engine()
        assert not e.remove_rule("nope")

    def test_enable_disable(self):
        e = self._engine()
        e.add_rule(self._rule())
        e.disable_rule("r1")
        assert not e.get_rule("r1").enabled
        e.enable_rule("r1")
        assert e.get_rule("r1").enabled

    def test_evaluate_matches(self):
        e = self._engine()
        e.guardrails._last_action_time = 0
        e.add_rule(self._rule())
        matches = e.evaluate(self._tweet())
        assert len(matches) == 1

    def test_evaluate_no_match(self):
        e = self._engine()
        e.add_rule(self._rule())
        matches = e.evaluate(self._tweet(text="Nothing relevant here"))
        assert len(matches) == 0

    def test_evaluate_priority_order(self):
        e = self._engine()
        e.guardrails._last_action_time = 0
        e.add_rule(self._rule("r1", priority=RulePriority.LOW, conditions=[]))
        e.add_rule(self._rule("r2", priority=RulePriority.HIGH, conditions=[]))
        matches = e.evaluate(self._tweet())
        assert matches[0][0].rule_id == "r2"

    def test_process_tweet(self):
        e = self._engine()
        e.guardrails._last_action_time = 0
        e.add_rule(self._rule())
        results = e.process_tweet(self._tweet())
        assert len(results) >= 1
        assert results[0]["action"] == "like"

    def test_process_tweet_no_match(self):
        e = self._engine()
        results = e.process_tweet(self._tweet(text="No match"))
        assert len(results) == 0

    def test_execute_actions_logs(self):
        e = self._engine()
        e.guardrails._last_action_time = 0
        e.add_rule(self._rule())
        matches = e.evaluate(self._tweet())
        results = e.execute_actions(self._tweet(), matches)
        assert len(results) >= 1

    def test_list_rules(self):
        e = self._engine()
        e.add_rule(self._rule("r1"))
        e.add_rule(self._rule("r2", enabled=False))
        assert len(e.list_rules()) == 2
        assert len(e.list_rules(enabled_only=True)) == 1

    def test_get_stats(self):
        e = self._engine()
        e.add_rule(self._rule())
        s = e.get_stats()
        assert s["total_rules"] == 1
        assert s["enabled_rules"] == 1

    def test_reset_daily(self):
        e = self._engine()
        r = self._rule()
        e.add_rule(r)
        r._trigger_count = 50
        e.reset_daily()
        assert r._trigger_count == 0

    def test_action_history(self):
        e = self._engine()
        e.guardrails._last_action_time = 0
        e.add_rule(self._rule())
        e.process_tweet(self._tweet())
        history = e.get_action_history()
        assert len(history) >= 1

    def test_action_history_filtered(self):
        e = self._engine()
        e.guardrails._last_action_time = 0
        e.add_rule(self._rule("r1"))
        e.process_tweet(self._tweet())
        history = e.get_action_history(rule_id="r1")
        assert all(h["rule_id"] == "r1" for h in history)


# ──── RuleTemplates ────

class TestRuleTemplates:
    def test_niche_engagement(self):
        rule = RuleTemplates.niche_engagement("r1", ["AI", "ML"])
        assert rule.name.startswith("Niche")
        assert len(rule.conditions) == 3
        assert rule.actions[0].action_type == ActionType.LIKE

    def test_influencer_engage(self):
        rule = RuleTemplates.influencer_engage("r1", 10000)
        assert "Influencer" in rule.name
        assert len(rule.actions) == 2

    def test_follow_back(self):
        rule = RuleTemplates.follow_back("r1")
        assert rule.actions[0].action_type == ActionType.FOLLOW
        assert rule.actions[0].delay_sec > 0

    def test_viral_amplify(self):
        rule = RuleTemplates.viral_amplify("r1", 500)
        assert any(a.action_type == ActionType.RETWEET for a in rule.actions)

    def test_spam_filter(self):
        rule = RuleTemplates.spam_filter("r1")
        assert rule.priority == RulePriority.CRITICAL
        assert rule.actions[0].action_type == ActionType.MUTE

    def test_hashtag_engage(self):
        rule = RuleTemplates.hashtag_engage("r1", ["#ai", "#ml"])
        assert len(rule.conditions) == 2
