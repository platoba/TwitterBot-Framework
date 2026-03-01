"""
Tests for Campaign Manager
"""

import pytest
import json
from unittest.mock import MagicMock
from bot.campaign import (
    Campaign, CampaignManager, CampaignStatus, CampaignGoal,
    CampaignGoalType, CampaignTweet, CampaignComparison,
)
from bot.database import Database
from bot.twitter_api import TwitterAPI


@pytest.fixture
def db(tmp_path):
    return Database(str(tmp_path / "test.db"))


@pytest.fixture
def mock_api():
    api = MagicMock(spec=TwitterAPI)
    api.is_configured = True
    return api


@pytest.fixture
def manager(mock_api, db):
    return CampaignManager(mock_api, db)


class TestCampaignGoal:
    def test_progress(self):
        g = CampaignGoal(
            goal_type=CampaignGoalType.IMPRESSIONS,
            target_value=1000, current_value=500,
        )
        assert g.progress_pct == 50.0
        assert not g.is_achieved

    def test_achieved(self):
        g = CampaignGoal(
            goal_type=CampaignGoalType.ENGAGEMENTS,
            target_value=100, current_value=150,
        )
        assert g.is_achieved
        assert g.progress_pct == 100.0

    def test_zero_target(self):
        g = CampaignGoal(
            goal_type=CampaignGoalType.FOLLOWERS,
            target_value=0, current_value=0,
        )
        assert g.progress_pct == 0

    def test_to_dict(self):
        g = CampaignGoal(
            goal_type=CampaignGoalType.CLICKS,
            target_value=500, current_value=200,
        )
        d = g.to_dict()
        assert d["type"] == "clicks"
        assert d["progress_pct"] == 40.0


class TestCampaignTweet:
    def test_defaults(self):
        t = CampaignTweet(content="Hello world")
        assert t.status == "pending"
        assert t.tweet_id is None

    def test_to_dict(self):
        t = CampaignTweet(content="Test", tweet_id="123",
                          status="sent", variant="A")
        d = t.to_dict()
        assert d["tweet_id"] == "123"
        assert d["variant"] == "A"


class TestCampaign:
    def test_defaults(self):
        c = Campaign(name="Test Campaign")
        assert c.id != ""
        assert c.status == CampaignStatus.DRAFT
        assert c.created_at != ""

    def test_tweet_stats(self):
        c = Campaign(name="Test")
        c.tweets = [
            CampaignTweet(content="A", status="sent",
                          metrics={"like_count": 10, "retweet_count": 5,
                                   "reply_count": 2, "impression_count": 100}),
            CampaignTweet(content="B", status="pending"),
        ]
        assert c.total_tweets == 2
        assert c.sent_tweets == 1
        assert c.total_engagement == 17
        assert c.total_impressions == 100

    def test_engagement_rate(self):
        c = Campaign(name="Test")
        c.tweets = [
            CampaignTweet(content="A", status="sent",
                          metrics={"like_count": 10, "retweet_count": 5,
                                   "reply_count": 5, "impression_count": 1000}),
        ]
        assert c.avg_engagement_rate == 2.0

    def test_roi(self):
        c = Campaign(name="Test", spent_usd=10)
        c.tweets = [
            CampaignTweet(content="A", status="sent",
                          metrics={"like_count": 50, "retweet_count": 20,
                                   "reply_count": 10, "impression_count": 500}),
        ]
        assert c.roi == 8.0  # 80 engagements / $10

    def test_roi_no_budget(self):
        c = Campaign(name="Test")
        assert c.roi is None

    def test_overall_progress_with_goals(self):
        c = Campaign(name="Test")
        c.goals = [
            CampaignGoal(CampaignGoalType.IMPRESSIONS, 1000, 500),
            CampaignGoal(CampaignGoalType.ENGAGEMENTS, 100, 100),
        ]
        assert c.overall_progress == 75.0

    def test_overall_progress_no_goals(self):
        c = Campaign(name="Test")
        c.tweets = [
            CampaignTweet(content="A", status="sent"),
            CampaignTweet(content="B", status="pending"),
        ]
        assert c.overall_progress == 50.0

    def test_is_active(self):
        c = Campaign(name="Test", status=CampaignStatus.ACTIVE)
        assert c.is_active
        c.status = CampaignStatus.DRAFT
        assert not c.is_active


class TestCampaignManager:
    def test_create(self, manager):
        campaign = manager.create("Launch Campaign", description="Product launch")
        assert campaign.name == "Launch Campaign"
        assert campaign.status == CampaignStatus.DRAFT

    def test_create_with_goals(self, manager):
        campaign = manager.create(
            "Goal Campaign",
            goals=[
                {"type": "impressions", "target": 10000},
                {"type": "engagements", "target": 500},
            ]
        )
        assert len(campaign.goals) == 2
        assert campaign.goals[0].target_value == 10000

    def test_get(self, manager):
        campaign = manager.create("Test")
        retrieved = manager.get(campaign.id)
        assert retrieved is not None
        assert retrieved.name == "Test"

    def test_get_nonexistent(self, manager):
        assert manager.get("nonexistent") is None

    def test_list_campaigns(self, manager):
        manager.create("Campaign A")
        manager.create("Campaign B")
        all_campaigns = manager.list_campaigns()
        assert len(all_campaigns) == 2

    def test_list_by_status(self, manager):
        c = manager.create("Active")
        manager.update_status(c.id, CampaignStatus.ACTIVE)
        manager.create("Draft")

        active = manager.list_campaigns(CampaignStatus.ACTIVE)
        assert len(active) == 1
        assert active[0].name == "Active"

    def test_update_status(self, manager):
        c = manager.create("Test")
        updated = manager.update_status(c.id, CampaignStatus.ACTIVE)
        assert updated.status == CampaignStatus.ACTIVE

    def test_delete(self, manager):
        c = manager.create("ToDelete")
        assert manager.delete(c.id)
        assert manager.get(c.id) is None

    def test_delete_nonexistent(self, manager):
        assert not manager.delete("nope")

    def test_add_tweet(self, manager):
        c = manager.create("Test")
        updated = manager.add_tweet(c.id, "Hello world!", variant="A")
        assert updated is not None
        assert len(updated.tweets) == 1
        assert updated.tweets[0].content == "Hello world!"

    def test_add_tweet_nonexistent(self, manager):
        assert manager.add_tweet("nope", "text") is None

    def test_send_next(self, manager, mock_api):
        c = manager.create("Send Test")
        manager.update_status(c.id, CampaignStatus.ACTIVE)
        manager.add_tweet(c.id, "Post this!")

        mock_api.post_tweet.return_value = {
            "data": {"id": "tweet123"}
        }

        result = manager.send_next(c.id)
        assert result is not None
        assert result["status"] == "sent"
        assert result["tweet_id"] == "tweet123"

    def test_send_next_inactive(self, manager):
        c = manager.create("Inactive")
        manager.add_tweet(c.id, "Text")
        result = manager.send_next(c.id)
        assert result is None

    def test_send_next_no_pending(self, manager):
        c = manager.create("Empty")
        manager.update_status(c.id, CampaignStatus.ACTIVE)
        result = manager.send_next(c.id)
        assert result is None


class TestABTesting:
    def test_setup_ab_test(self, manager):
        c = manager.create("AB Test")
        result = manager.setup_ab_test(
            c.id, "Content A", "Content B",
            delay_minutes=60
        )
        assert result is not None
        assert result["variant_a"] == "Content A"
        campaign = manager.get(c.id)
        assert len(campaign.tweets) == 2

    def test_evaluate_ab_no_data(self, manager):
        c = manager.create("AB")
        result = manager.evaluate_ab_test(c.id)
        assert result is None

    def test_evaluate_ab_with_data(self, manager):
        c = manager.create("AB")
        c.tweets = [
            CampaignTweet(content="A", variant="A", status="sent",
                          metrics={"like_count": 50, "retweet_count": 20,
                                   "impression_count": 1000}),
            CampaignTweet(content="B", variant="B", status="sent",
                          metrics={"like_count": 30, "retweet_count": 10,
                                   "impression_count": 800}),
        ]
        manager._save_campaign(c)

        result = manager.evaluate_ab_test(c.id)
        assert result is not None
        assert result["winner"] == "A"
        assert result["variant_a"]["total_likes"] == 50


class TestFormatting:
    def test_format_summary(self, manager):
        c = Campaign(name="Test", status=CampaignStatus.ACTIVE)
        c.goals = [CampaignGoal(CampaignGoalType.IMPRESSIONS, 10000, 5000)]
        c.tweets = [
            CampaignTweet(content="A", status="sent",
                          metrics={"like_count": 10, "retweet_count": 5,
                                   "reply_count": 2, "impression_count": 500}),
        ]
        text = manager.format_summary(c)
        assert "Test" in text
        assert "🚀" in text
        assert "Goals" in text

    def test_format_list(self, manager):
        manager.create("Campaign A")
        manager.create("Campaign B")
        text = manager.format_list()
        assert "推广活动列表" in text
        assert "Campaign A" in text

    def test_format_empty_list(self, manager):
        text = manager.format_list()
        assert "暂无" in text


class TestPersistence:
    def test_save_and_reload(self, mock_api, db):
        mgr1 = CampaignManager(mock_api, db)
        c = mgr1.create("Persistent", description="Test")
        mgr1.add_tweet(c.id, "Tweet 1")

        # Create new manager (simulates restart)
        mgr2 = CampaignManager(mock_api, db)
        loaded = mgr2.get(c.id)
        assert loaded is not None
        assert loaded.name == "Persistent"
        assert len(loaded.tweets) == 1

    def test_campaign_table_check(self, manager):
        assert manager._campaigns_table_exists()
