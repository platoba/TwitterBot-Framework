"""
Tests for Export Engine
"""

import json
import os
import pytest
from bot.export import ExportEngine
from bot.database import Database


@pytest.fixture
def db(tmp_path):
    database = Database(str(tmp_path / "test.db"))
    # Seed data
    for i in range(5):
        database.save_tweet({
            "id": f"tw{i}",
            "author_id": f"u{i % 2}",
            "author_username": f"user{i % 2}",
            "text": f"Test tweet #{i} with some content here",
            "public_metrics": {
                "like_count": i * 10,
                "retweet_count": i * 2,
                "reply_count": i,
                "impression_count": i * 100,
            },
            "created_at": f"2026-02-{20+i}T10:00:00Z",
        }, source_query="test")

    database.save_analytics_snapshot("user0", {
        "followers_count": 1000, "following_count": 200,
        "tweet_count": 500, "listed_count": 10,
    })
    database.save_analytics_snapshot("user0", {
        "followers_count": 1050, "following_count": 210,
        "tweet_count": 510, "listed_count": 11,
    })

    database.log_engagement("like", "tw1", "user1")
    database.log_engagement("reply", "tw2", "user0", "Nice!")

    database.add_scheduled_tweet("Scheduled post", "2026-03-01T10:00:00")

    database.create_ab_test("Test AB", "Variant A text", "Variant B text")

    return database


@pytest.fixture
def engine(db):
    return ExportEngine(db)


class TestCSVExport:
    def test_tweets_to_csv(self, engine):
        csv = engine.tweets_to_csv()
        assert "tweet_id" in csv
        assert "tw0" in csv
        lines = csv.strip().split("\n")
        assert len(lines) >= 2  # header + at least 1 row

    def test_tweets_to_csv_filtered(self, engine):
        csv = engine.tweets_to_csv(username="user0")
        assert "user0" in csv

    def test_analytics_to_csv(self, engine):
        csv = engine.analytics_to_csv("user0")
        assert "followers_count" in csv
        assert "1000" in csv or "1050" in csv

    def test_engagement_to_csv(self, engine):
        csv = engine.engagement_to_csv()
        assert "action_type" in csv
        assert "like" in csv

    def test_schedule_to_csv(self, engine):
        csv = engine.schedule_to_csv()
        assert "scheduled_at" in csv

    def test_ab_tests_to_csv(self, engine):
        csv = engine.ab_tests_to_csv()
        assert "test_name" in csv
        assert "Test AB" in csv

    def test_empty_csv(self, engine):
        csv = engine.tweets_to_csv(username="nonexistent")
        lines = csv.strip().split("\n")
        assert len(lines) == 1  # header only


class TestJSONExport:
    def test_tweets_to_json(self, engine):
        result = engine.tweets_to_json()
        data = json.loads(result)
        assert "tweets" in data
        assert data["count"] == 5
        assert "exported_at" in data

    def test_analytics_to_json(self, engine):
        result = engine.analytics_to_json("user0")
        data = json.loads(result)
        assert "analytics" in data
        assert data["username"] == "user0"

    def test_full_report_json(self, engine):
        result = engine.full_report_json("user0")
        data = json.loads(result)
        assert "tweets" in data
        assert "top_tweets" in data
        assert "schedule_queue" in data
        assert "ab_tests" in data
        assert "analytics" in data
        assert "engagement_stats" in data


class TestMarkdownExport:
    def test_tweets_to_markdown(self, engine):
        md = engine.tweets_to_markdown()
        assert "# Tweet History Report" in md
        assert "|" in md  # table

    def test_tweets_filtered(self, engine):
        md = engine.tweets_to_markdown(username="user0")
        assert "user0" in md

    def test_analytics_to_markdown(self, engine):
        md = engine.analytics_to_markdown("user0")
        assert "# Analytics Report" in md
        assert "user0" in md
        assert "Followers" in md


class TestHTMLExport:
    def test_tweets_to_html(self, engine):
        html = engine.tweets_to_html()
        assert "<!DOCTYPE html>" in html
        assert "TwitterBot Report" in html
        assert "<table>" in html
        assert "Total Tweets" in html

    def test_analytics_to_html(self, engine):
        html = engine.analytics_to_html("user0")
        assert "<!DOCTYPE html>" in html
        assert "Analytics Report" in html
        assert "user0" in html
        assert "<canvas" in html  # chart

    def test_html_escaping(self, engine):
        # Save a tweet with HTML-like content
        engine.db.save_tweet({
            "id": "xss1",
            "author_username": "hacker",
            "text": "<script>alert('xss')</script>",
            "public_metrics": {"like_count": 0, "retweet_count": 0,
                               "reply_count": 0, "impression_count": 0},
        })
        html = engine.tweets_to_html()
        assert "<script>" not in html
        assert "&lt;script&gt;" in html


class TestFileExport:
    def test_export_to_file(self, engine, tmp_path):
        filepath = str(tmp_path / "test_export.csv")
        result = engine.export_to_file("col1,col2\n1,2", filepath)
        assert result is True
        assert os.path.exists(filepath)
        with open(filepath) as f:
            assert "col1" in f.read()

    def test_batch_export(self, engine, tmp_path):
        output_dir = str(tmp_path / "exports")
        results = engine.batch_export("user0", output_dir)
        assert len(results) > 0
        # Should have multiple formats
        extensions = [os.path.splitext(f)[1] for f in results.keys()]
        assert ".csv" in extensions
        assert ".json" in extensions
        assert ".md" in extensions
        assert ".html" in extensions

    def test_export_to_invalid_path(self, engine):
        result = engine.export_to_file("test", "/nonexistent/deep/path/file.txt")
        assert result is False


class TestEdgeCases:
    def test_empty_database(self, tmp_path):
        db = Database(str(tmp_path / "empty.db"))
        engine = ExportEngine(db)

        assert "tweet_id" in engine.tweets_to_csv()
        data = json.loads(engine.tweets_to_json())
        assert data["count"] == 0

    def test_limit_parameter(self, engine):
        csv = engine.tweets_to_csv(limit=2)
        lines = csv.strip().split("\n")
        assert len(lines) <= 3  # header + 2 rows

    def test_now_format(self, engine):
        now = engine._now()
        assert "UTC" in now
        assert "-" in now
