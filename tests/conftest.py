"""
conftest.py - 测试公共fixture
"""

import os
import sys
import sqlite3
import tempfile
import pytest

# 确保项目根目录在path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.database import Database
from bot.twitter_api import TwitterAPI, RateLimiter
from bot.content_generator import ContentGenerator
from bot.webhook import TelegramWebhook
from bot.strategies.analytics import AnalyticsStrategy
from bot.strategies.engagement import EngagementStrategy, EngagementRule
from bot.strategies.monitor import MonitorStrategy
from bot.strategies.scheduler import SchedulerStrategy


@pytest.fixture
def tmp_db(tmp_path):
    """临时数据库"""
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    yield db
    db.close()


@pytest.fixture
def rate_limiter():
    return RateLimiter()


@pytest.fixture
def mock_api():
    """无需真实token的TwitterAPI"""
    return TwitterAPI(bearer_token="test_bearer_token_fake")


@pytest.fixture
def content_gen():
    return ContentGenerator()


@pytest.fixture
def webhook():
    return TelegramWebhook(bot_token="fake_bot_token", default_chat_id="123456")


@pytest.fixture
def analytics(mock_api, tmp_db, webhook):
    return AnalyticsStrategy(mock_api, tmp_db, webhook)


@pytest.fixture
def engagement(mock_api, tmp_db, webhook):
    eng = EngagementStrategy(mock_api, tmp_db, webhook)
    eng.dry_run = True
    return eng


@pytest.fixture
def monitor(mock_api, tmp_db, webhook):
    return MonitorStrategy(mock_api, tmp_db, webhook)


@pytest.fixture
def scheduler(mock_api, tmp_db, webhook, content_gen):
    return SchedulerStrategy(mock_api, tmp_db, webhook, content_gen)


@pytest.fixture
def sample_tweet():
    return {
        "id": "12345",
        "text": "This is a test tweet about Python and AI #Python #AI",
        "author_id": "user123",
        "author_username": "testuser",
        "created_at": "2026-02-28T10:00:00Z",
        "public_metrics": {
            "like_count": 42,
            "retweet_count": 10,
            "reply_count": 5,
            "quote_count": 2,
            "impression_count": 1000
        }
    }


@pytest.fixture
def sample_tweets():
    """多条推文fixture"""
    return [
        {
            "id": str(i),
            "text": f"Test tweet #{i} about {'Python' if i % 2 == 0 else 'JavaScript'}",
            "author_id": f"user{i}",
            "author_username": f"testuser{i}",
            "created_at": f"2026-02-{20 + i:02d}T{8 + i:02d}:00:00Z",
            "public_metrics": {
                "like_count": i * 10,
                "retweet_count": i * 3,
                "reply_count": i * 2,
                "quote_count": i,
                "impression_count": i * 100,
            }
        }
        for i in range(1, 8)
    ]


@pytest.fixture
def sample_user():
    return {
        "data": {
            "id": "user123",
            "username": "testuser",
            "name": "Test User",
            "description": "A test user",
            "created_at": "2020-01-01T00:00:00Z",
            "public_metrics": {
                "followers_count": 5000,
                "following_count": 300,
                "tweet_count": 1200,
                "listed_count": 50
            }
        }
    }
