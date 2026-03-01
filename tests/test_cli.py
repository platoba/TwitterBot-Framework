"""
Tests for CLI
"""

import pytest
from unittest.mock import MagicMock, patch
from bot.cli import build_parser, cmd_generate, cmd_stats, cmd_thread


class TestParser:
    def test_search_command(self):
        parser = build_parser()
        args = parser.parse_args(["search", "python"])
        assert args.command == "search"
        assert args.query == "python"

    def test_search_with_limit(self):
        parser = build_parser()
        args = parser.parse_args(["search", "AI", "-n", "20"])
        assert args.limit == 20

    def test_user_command(self):
        parser = build_parser()
        args = parser.parse_args(["user", "elonmusk"])
        assert args.command == "user"
        assert args.username == "elonmusk"

    def test_tweet_command(self):
        parser = build_parser()
        args = parser.parse_args(["tweet", "Hello world!"])
        assert args.command == "tweet"
        assert args.text == "Hello world!"

    def test_tweet_reply(self):
        parser = build_parser()
        args = parser.parse_args(["tweet", "Reply!", "--reply-to", "123"])
        assert args.reply_to == "123"

    def test_thread_command(self):
        parser = build_parser()
        args = parser.parse_args(["thread", "My Thread", "--body", "Content here"])
        assert args.command == "thread"
        assert args.title == "My Thread"
        assert args.body == "Content here"

    def test_analyze_command(self):
        parser = build_parser()
        args = parser.parse_args(["analyze", "testuser"])
        assert args.command == "analyze"
        assert args.username == "testuser"

    def test_sentiment_text(self):
        parser = build_parser()
        args = parser.parse_args(["sentiment", "--text", "Great product!"])
        assert args.command == "sentiment"
        assert args.text == "Great product!"

    def test_sentiment_user(self):
        parser = build_parser()
        args = parser.parse_args(["sentiment", "--username", "alice"])
        assert args.username == "alice"

    def test_competitor_add(self):
        parser = build_parser()
        args = parser.parse_args(["competitor", "add", "--username", "rival"])
        assert args.command == "competitor"
        assert args.action == "add"
        assert args.username == "rival"

    def test_competitor_list(self):
        parser = build_parser()
        args = parser.parse_args(["competitor", "list"])
        assert args.action == "list"

    def test_export_csv(self):
        parser = build_parser()
        args = parser.parse_args(["export", "--format", "csv", "-o", "out.csv"])
        assert args.command == "export"
        assert args.format == "csv"
        assert args.output == "out.csv"

    def test_export_all(self):
        parser = build_parser()
        args = parser.parse_args(["export", "--format", "all"])
        assert args.format == "all"

    def test_stats_command(self):
        parser = build_parser()
        args = parser.parse_args(["stats", "--days", "30"])
        assert args.command == "stats"
        assert args.days == 30

    def test_generate_command(self):
        parser = build_parser()
        args = parser.parse_args(["generate", "engagement", "--ab"])
        assert args.command == "generate"
        assert args.category == "engagement"
        assert args.ab is True

    def test_generate_with_vars(self):
        parser = build_parser()
        args = parser.parse_args([
            "generate", "announcement",
            "--vars", "title=Hello", "body=World"
        ])
        assert args.vars == ["title=Hello", "body=World"]

    def test_generate_list_categories(self):
        parser = build_parser()
        args = parser.parse_args(["generate", "--list-categories"])
        assert args.list_categories is True

    def test_no_command(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.command is None

    def test_db_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--db", "/tmp/test.db", "stats"])
        assert args.db == "/tmp/test.db"


class TestGenerateCommand:
    def test_generate(self, capsys):
        parser = build_parser()
        args = parser.parse_args(["generate", "engagement",
                                  "--vars", "question=What is AI?"])
        cmd_generate(args)
        output = capsys.readouterr().out
        assert len(output) > 0

    def test_generate_ab(self, capsys):
        parser = build_parser()
        args = parser.parse_args(["generate", "announcement", "--ab",
                                  "--vars", "title=Launch", "body=New product"])
        cmd_generate(args)
        output = capsys.readouterr().out
        assert "Variant A" in output
        assert "Variant B" in output

    def test_list_categories(self, capsys):
        parser = build_parser()
        args = parser.parse_args(["generate", "--list-categories"])
        cmd_generate(args)
        output = capsys.readouterr().out
        assert "engagement" in output
        assert "announcement" in output


class TestThreadCommand:
    def test_compose_thread(self, capsys):
        parser = build_parser()
        args = parser.parse_args([
            "thread", "Test Thread",
            "--body", "This is a thread about testing. " * 20,
            "--hashtags", "#test",
        ])
        cmd_thread(args)
        output = capsys.readouterr().out
        assert "Thread Preview" in output
        assert "Test Thread" in output


class TestStatsCommand:
    def test_stats(self, capsys, tmp_path):
        parser = build_parser()
        args = parser.parse_args(["--db", str(tmp_path / "test.db"), "stats"])
        cmd_stats(args)
        output = capsys.readouterr().out
        assert "Stats" in output
