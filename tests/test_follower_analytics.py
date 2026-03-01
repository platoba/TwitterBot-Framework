"""Tests for bot/follower_analytics.py"""

import time
import pytest
from bot.follower_analytics import FollowerAnalytics, FollowerRecord, GrowthReport
from bot.database import Database
import tempfile, os


@pytest.fixture
def db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    d = Database(path)
    yield d
    os.unlink(path)


@pytest.fixture
def analytics(db):
    return FollowerAnalytics(db)


def make_follower(uid="u1", username="alice", followers=1000, following=500, tweets=100, bio="Developer", location="NYC"):
    return {
        "user_id": uid,
        "username": username,
        "display_name": username.title(),
        "followers_count": followers,
        "following_count": following,
        "tweet_count": tweets,
        "bio": bio,
        "location": location,
        "is_following_back": False,
    }


class TestFollowerAnalytics:
    def test_quality_normal(self, analytics):
        f = make_follower(followers=1000, following=500, tweets=200, bio="Software engineer at FAANG")
        score = analytics.calculate_quality(f)
        assert 5.0 <= score <= 9.0

    def test_quality_bot(self, analytics):
        f = make_follower(followers=10, following=10000, tweets=0, bio="Follow back for follow")
        score = analytics.calculate_quality(f)
        assert score < 3.0

    def test_quality_influencer(self, analytics):
        f = make_follower(followers=50000, following=200, tweets=5000, bio="Tech influencer sharing insights")
        score = analytics.calculate_quality(f)
        assert score > 6.0

    def test_bot_detection(self, analytics):
        bot = make_follower(followers=5, following=8000, tweets=0, bio="DM for promo deals")
        assert analytics._is_likely_bot(bot) is True

    def test_not_bot(self, analytics):
        real = make_follower(followers=500, following=300, tweets=150, bio="Python developer")
        assert analytics._is_likely_bot(real) is False

    def test_record_snapshot(self, analytics):
        followers = [
            make_follower("u1", "alice"),
            make_follower("u2", "bob"),
            make_follower("u3", "carol"),
        ]
        result = analytics.record_snapshot(followers)
        assert result["total"] == 3
        assert result["new"] == 3
        assert result["lost"] == 0

    def test_detect_unfollow(self, analytics):
        # First snapshot
        analytics.record_snapshot([
            make_follower("u1", "alice"),
            make_follower("u2", "bob"),
        ])
        # Second snapshot: u2 unfollowed
        analytics.record_snapshot([
            make_follower("u1", "alice"),
            make_follower("u3", "carol"),
        ])
        unfollowers = analytics.detect_unfollowers()
        assert len(unfollowers) >= 1
        assert any(u["user_id"] == "u2" for u in unfollowers)

    def test_growth_report(self, analytics):
        analytics.record_snapshot([
            make_follower("u1", "alice"),
            make_follower("u2", "bob"),
        ])
        report = analytics.get_growth_report("7d")
        assert isinstance(report, GrowthReport)
        assert report.followers_end == 2

    def test_growth_report_format(self, analytics):
        analytics.record_snapshot([make_follower("u1", "alice")])
        report = analytics.get_growth_report("7d")
        text = report.format_report()
        assert "Growth Report" in text

    def test_demographics(self, analytics):
        followers = [
            make_follower("u1", "alice", bio="Python developer who loves AI and ML", location="San Francisco"),
            make_follower("u2", "bob", bio="Machine learning engineer at Google", location="New York"),
            make_follower("u3", "carol", bio="AI researcher and Python enthusiast", location="San Francisco"),
        ]
        analytics.record_snapshot(followers)
        demo = analytics.get_demographics()
        assert demo["total"] == 3
        assert len(demo["keywords"]) > 0
        assert len(demo["locations"]) > 0

    def test_empty_demographics(self, analytics):
        demo = analytics.get_demographics()
        assert demo["total"] == 0

    def test_follower_record(self):
        f = FollowerRecord(user_id="u1", username="alice")
        d = f.to_dict()
        assert d["user_id"] == "u1"

    def test_growth_report_to_dict(self, analytics):
        analytics.record_snapshot([make_follower("u1", "alice")])
        report = analytics.get_growth_report("7d")
        d = report.to_dict()
        assert "net_change" in d
        assert "follow_back_rate" in d

    def test_period_weeks(self, analytics):
        analytics.record_snapshot([make_follower("u1", "alice")])
        report = analytics.get_growth_report("2w")
        assert report.period == "2w"
