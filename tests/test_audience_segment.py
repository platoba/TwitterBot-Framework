"""Tests for Audience Segmentation Engine"""
import pytest
from bot.audience_segment import (
    AudienceSegmenter, AudienceProfile, SegmentRule, Segment,
    EngagementTier, InteractionType, RuleOperator,
)


@pytest.fixture
def segmenter():
    s = AudienceSegmenter(db_path=":memory:")
    yield s
    s.close()


class TestAudienceProfile:
    def test_default_tier(self):
        p = AudienceProfile(user_id="u1")
        assert p.engagement_tier == EngagementTier.COLD

    def test_default_interests(self):
        p = AudienceProfile(user_id="u1")
        assert p.interests == []


class TestSegmentRule:
    def test_equals(self):
        rule = SegmentRule(field="interaction_count", operator=RuleOperator.EQUALS, value=5)
        p = AudienceProfile(user_id="u1", interaction_count=5)
        assert rule.evaluate(p) is True

    def test_greater_than(self):
        rule = SegmentRule(field="interaction_count", operator=RuleOperator.GREATER_THAN, value=3)
        p = AudienceProfile(user_id="u1", interaction_count=5)
        assert rule.evaluate(p) is True

    def test_less_than(self):
        rule = SegmentRule(field="interaction_count", operator=RuleOperator.LESS_THAN, value=10)
        p = AudienceProfile(user_id="u1", interaction_count=5)
        assert rule.evaluate(p) is True

    def test_in_operator_list(self):
        rule = SegmentRule(field="interests", operator=RuleOperator.IN, value=["tech", "crypto"])
        p = AudienceProfile(user_id="u1", interests=["tech", "gaming"])
        assert rule.evaluate(p) is True

    def test_not_in_operator(self):
        rule = SegmentRule(field="interests", operator=RuleOperator.NOT_IN, value=["crypto"])
        p = AudienceProfile(user_id="u1", interests=["tech", "gaming"])
        assert rule.evaluate(p) is True

    def test_contains(self):
        rule = SegmentRule(field="interests", operator=RuleOperator.CONTAINS, value="tech")
        p = AudienceProfile(user_id="u1", interests=["tech", "gaming"])
        assert rule.evaluate(p) is True

    def test_equals_enum(self):
        rule = SegmentRule(field="engagement_tier", operator=RuleOperator.EQUALS, value="hot")
        p = AudienceProfile(user_id="u1", engagement_tier=EngagementTier.HOT)
        assert rule.evaluate(p) is True

    def test_in_enum(self):
        rule = SegmentRule(field="engagement_tier", operator=RuleOperator.IN, value=["hot", "superfan"])
        p = AudienceProfile(user_id="u1", engagement_tier=EngagementTier.SUPERFAN)
        assert rule.evaluate(p) is True

    def test_missing_field(self):
        rule = SegmentRule(field="nonexistent", operator=RuleOperator.EQUALS, value=5)
        p = AudienceProfile(user_id="u1")
        assert rule.evaluate(p) is False


class TestAudienceSegmenter:
    def test_add_interaction(self, segmenter):
        segmenter.add_interaction("u1", InteractionType.LIKE, "Great tweet about Python!")
        assert segmenter.get_total_interactions() == 1

    def test_profile_created(self, segmenter):
        segmenter.add_interaction("u1", InteractionType.LIKE)
        profile = segmenter.get_profile("u1")
        assert profile is not None
        assert profile.interaction_count == 1

    def test_engagement_tier_cold(self, segmenter):
        tier = segmenter.classify_engagement_tier("nonexistent")
        assert tier == EngagementTier.COLD

    def test_engagement_tier_warm(self, segmenter):
        for _ in range(3):
            segmenter.add_interaction("u1", InteractionType.LIKE)
        tier = segmenter.classify_engagement_tier("u1")
        assert tier == EngagementTier.WARM

    def test_engagement_tier_hot(self, segmenter):
        for _ in range(10):
            segmenter.add_interaction("u1", InteractionType.LIKE)
        tier = segmenter.classify_engagement_tier("u1")
        assert tier == EngagementTier.HOT

    def test_engagement_tier_superfan(self, segmenter):
        for _ in range(25):
            segmenter.add_interaction("u1", InteractionType.LIKE)
        tier = segmenter.classify_engagement_tier("u1")
        assert tier == EngagementTier.SUPERFAN

    def test_detect_interests(self, segmenter):
        segmenter.add_interaction("u1", InteractionType.REPLY, "I love Python and JavaScript coding")
        segmenter.add_interaction("u1", InteractionType.REPLY, "Great API and software dev tips")
        interests = segmenter.detect_interests("u1")
        assert "tech" in interests

    def test_timezone_distribution_empty(self, segmenter):
        dist = segmenter.get_timezone_distribution()
        assert dist == {}

    def test_timezone_distribution(self, segmenter):
        segmenter.add_interaction("u1", InteractionType.LIKE)
        segmenter.set_profile_info("u1", timezone_offset=8)
        segmenter.add_interaction("u2", InteractionType.LIKE)
        segmenter.set_profile_info("u2", timezone_offset=-5)
        dist = segmenter.get_timezone_distribution()
        assert "UTC+8" in dist
        assert "UTC-5" in dist

    def test_activity_distribution(self, segmenter):
        segmenter.add_interaction("u1", InteractionType.LIKE)
        dist = segmenter.get_activity_distribution()
        assert isinstance(dist, dict)

    def test_create_segment(self, segmenter):
        rules = [SegmentRule(field="interaction_count", operator=RuleOperator.GREATER_THAN, value=0)]
        segment = segmenter.create_segment("active", rules, "Active users")
        assert segment.name == "active"

    def test_get_segment_members(self, segmenter):
        for _ in range(5):
            segmenter.add_interaction("u1", InteractionType.LIKE)
        segmenter.add_interaction("u2", InteractionType.LIKE)
        rules = [SegmentRule(field="interaction_count", operator=RuleOperator.GREATER_THAN, value=3)]
        segmenter.create_segment("engaged", rules)
        members = segmenter.get_segment_members("engaged")
        assert len(members) == 1
        assert members[0].user_id == "u1"

    def test_get_segment_nonexistent(self, segmenter):
        assert segmenter.get_segment_members("none") == []

    def test_list_segments(self, segmenter):
        rules = [SegmentRule(field="interaction_count", operator=RuleOperator.GREATER_THAN, value=0)]
        segmenter.create_segment("test", rules)
        segments = segmenter.list_segments()
        assert len(segments) == 1

    def test_persona_summary_empty(self, segmenter):
        summary = segmenter.get_persona_summary()
        assert summary["total_audience"] == 0

    def test_persona_summary(self, segmenter):
        for i in range(5):
            segmenter.add_interaction(f"u{i}", InteractionType.LIKE, "Python coding")
        summary = segmenter.get_persona_summary()
        assert summary["total_audience"] == 5
        assert "tier_distribution" in summary

    def test_recommend_content(self, segmenter):
        for _ in range(5):
            segmenter.add_interaction("u1", InteractionType.REPLY, "Python AI machine learning")
        rules = [SegmentRule(field="interaction_count", operator=RuleOperator.GREATER_THAN, value=0)]
        segmenter.create_segment("tech_fans", rules)
        recs = segmenter.recommend_content_for_segment("tech_fans")
        assert "recommendations" in recs
        assert len(recs["recommendations"]) > 0

    def test_recommend_empty_segment(self, segmenter):
        rules = [SegmentRule(field="interaction_count", operator=RuleOperator.GREATER_THAN, value=9999)]
        segmenter.create_segment("empty", rules)
        recs = segmenter.recommend_content_for_segment("empty")
        assert recs["recommendations"] == []

    def test_set_profile_info(self, segmenter):
        segmenter.add_interaction("u1", InteractionType.LIKE)
        segmenter.set_profile_info("u1", username="testuser", bio="Developer")
        p = segmenter.get_profile("u1")
        assert p.username == "testuser"
        assert p.bio == "Developer"

    def test_total_profiles(self, segmenter):
        segmenter.add_interaction("u1", InteractionType.LIKE)
        segmenter.add_interaction("u2", InteractionType.LIKE)
        assert segmenter.get_total_profiles() == 2
