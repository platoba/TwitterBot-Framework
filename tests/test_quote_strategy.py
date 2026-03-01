"""Tests for Quote Strategy Engine"""
import os
import tempfile
import pytest
from bot.quote_strategy import QuoteStyle, QuoteTweet, QuoteStrategyEngine


@pytest.fixture
def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    os.unlink(path)


@pytest.fixture
def engine(db_path):
    return QuoteStrategyEngine(db_path=db_path, cooldown_minutes=30, max_quotes_per_hour=5)


class TestQuoteStyle:
    def test_all_styles(self):
        assert len(QuoteStyle.ALL) == 8
        assert "agree" in QuoteStyle.ALL
        assert "hot_take" in QuoteStyle.ALL

    def test_constants(self):
        assert QuoteStyle.AGREE == "agree"
        assert QuoteStyle.DATA == "data"
        assert QuoteStyle.THREAD_HOOK == "thread_hook"


class TestQuoteTweet:
    def test_create(self):
        qt = QuoteTweet(
            original_tweet_id="123",
            original_author="elonmusk",
            quote_content="Great point!",
        )
        assert qt.original_tweet_id == "123"
        assert qt.original_author == "elonmusk"
        assert qt.style == "add_value"
        assert qt.status == "draft"

    def test_amplification_ratio(self):
        qt = QuoteTweet(
            original_tweet_id="1", original_author="x",
            original_likes=100, original_retweets=50,
            quote_likes=30, quote_retweets=10, quote_replies=5,
        )
        ratio = qt.amplification_ratio
        assert ratio == round(45 / 150, 4)

    def test_amplification_zero_original(self):
        qt = QuoteTweet(
            original_tweet_id="1", original_author="x",
            original_likes=0, original_retweets=0,
            quote_likes=10, quote_retweets=5, quote_replies=3,
        )
        assert qt.amplification_ratio == 18.0

    def test_amplification_zero_both(self):
        qt = QuoteTweet(original_tweet_id="1", original_author="x")
        assert qt.amplification_ratio == 0.0

    def test_engagement_rate(self):
        qt = QuoteTweet(
            original_tweet_id="1", original_author="x",
            quote_likes=50, quote_retweets=20, quote_replies=10,
            quote_impressions=1000,
        )
        assert qt.engagement_rate == 8.0

    def test_engagement_rate_zero_impressions(self):
        qt = QuoteTweet(original_tweet_id="1", original_author="x",
                        quote_likes=10)
        assert qt.engagement_rate == 0.0

    def test_to_dict(self):
        qt = QuoteTweet(original_tweet_id="1", original_author="user1",
                        quote_content="Nice!")
        d = qt.to_dict()
        assert d["original_tweet_id"] == "1"
        assert "amplification_ratio" in d
        assert "engagement_rate" in d


class TestQuoteEngineCRUD:
    def test_create_quote(self, engine):
        qt = engine.create_quote("t1", "author1", "My comment", style="agree")
        assert qt.original_tweet_id == "t1"
        assert qt.original_author == "author1"
        assert qt.style == "agree"

    def test_invalid_style_fallback(self, engine):
        qt = engine.create_quote("t1", "a", "X", style="invalid_style")
        assert qt.style == "add_value"

    def test_get_quote(self, engine):
        qt = engine.create_quote("t1", "a", "Comment")
        result = engine.get_quote(qt.quote_id)
        assert result is not None
        assert result.quote_content == "Comment"

    def test_get_nonexistent(self, engine):
        assert engine.get_quote("fake") is None

    def test_list_quotes(self, engine):
        engine.create_quote("t1", "a", "C1")
        engine.create_quote("t2", "b", "C2")
        quotes = engine.list_quotes()
        assert len(quotes) == 2

    def test_list_by_status(self, engine):
        qt1 = engine.create_quote("t1", "a", "C1")
        engine.create_quote("t2", "b", "C2")
        engine.mark_posted(qt1.quote_id)
        drafts = engine.list_quotes(status="draft")
        posted = engine.list_quotes(status="posted")
        assert len(drafts) == 1
        assert len(posted) == 1

    def test_list_by_style(self, engine):
        engine.create_quote("t1", "a", "C", style="agree")
        engine.create_quote("t2", "b", "C", style="humor")
        agree = engine.list_quotes(style="agree")
        assert len(agree) == 1

    def test_list_by_author(self, engine):
        engine.create_quote("t1", "alice", "C")
        engine.create_quote("t2", "bob", "C")
        alice = engine.list_quotes(author="alice")
        assert len(alice) == 1

    def test_mark_posted(self, engine):
        qt = engine.create_quote("t1", "a", "Posted!")
        assert engine.mark_posted(qt.quote_id, "tweet123")
        result = engine.get_quote(qt.quote_id)
        assert result.status == "posted"
        assert result.quote_tweet_id == "tweet123"
        assert result.posted_at is not None

    def test_mark_posted_already(self, engine):
        qt = engine.create_quote("t1", "a", "X")
        engine.mark_posted(qt.quote_id)
        assert not engine.mark_posted(qt.quote_id)

    def test_update_metrics(self, engine):
        qt = engine.create_quote("t1", "a", "X")
        assert engine.update_quote_metrics(qt.quote_id, likes=50, retweets=20,
                                           replies=10, impressions=1000)
        result = engine.get_quote(qt.quote_id)
        assert result.quote_likes == 50
        assert result.quote_impressions == 1000

    def test_delete_quote(self, engine):
        qt = engine.create_quote("t1", "a", "Delete me")
        assert engine.delete_quote(qt.quote_id)
        assert engine.get_quote(qt.quote_id) is None

    def test_delete_nonexistent(self, engine):
        assert not engine.delete_quote("fake")


class TestRateLimiting:
    def test_can_quote_initial(self, engine):
        can, msg = engine.can_quote()
        assert can is True
        assert msg == "OK"

    def test_can_quote_rate_limit(self, engine):
        for i in range(5):
            qt = engine.create_quote(f"t{i}", "a", f"C{i}")
            engine.mark_posted(qt.quote_id)
        can, msg = engine.can_quote()
        assert can is False
        assert "Rate limit" in msg

    def test_can_quote_author_cooldown(self, engine):
        qt = engine.create_quote("t1", "specific_author", "C")
        engine.mark_posted(qt.quote_id)
        can, msg = engine.can_quote(author="specific_author")
        assert can is False
        assert "Cooldown" in msg

    def test_can_quote_different_author(self, engine):
        qt = engine.create_quote("t1", "author_a", "C")
        engine.mark_posted(qt.quote_id)
        can, msg = engine.can_quote(author="author_b")
        assert can is True


class TestAuthorScores:
    def test_get_author_score(self, engine):
        qt = engine.create_quote("t1", "user1", "X")
        engine.mark_posted(qt.quote_id)
        score = engine.get_author_score("user1")
        assert score is not None
        assert score["quote_count"] == 1

    def test_get_nonexistent_author(self, engine):
        assert engine.get_author_score("nobody") is None

    def test_top_authors(self, engine):
        for author in ["a", "b", "c"]:
            qt = engine.create_quote(f"t_{author}", author, "X")
            engine.mark_posted(qt.quote_id)
        tops = engine.get_top_authors(limit=10)
        assert len(tops) == 3

    def test_set_relationship(self, engine):
        assert engine.set_author_relationship("user1", "ally", "Great partner")
        score = engine.get_author_score("user1")
        assert score["relationship"] == "ally"
        assert score["notes"] == "Great partner"

    def test_update_relationship(self, engine):
        engine.set_author_relationship("user1", "neutral")
        engine.set_author_relationship("user1", "competitor")
        score = engine.get_author_score("user1")
        assert score["relationship"] == "competitor"


class TestKeywords:
    def test_add_keyword(self, engine):
        assert engine.add_keyword("python", min_likes=20)

    def test_add_duplicate(self, engine):
        engine.add_keyword("python")
        assert not engine.add_keyword("python")

    def test_remove_keyword(self, engine):
        engine.add_keyword("python")
        assert engine.remove_keyword("python")
        assert not engine.remove_keyword("python")

    def test_get_keywords(self, engine):
        engine.add_keyword("python")
        engine.add_keyword("ai")
        kws = engine.get_keywords()
        assert len(kws) == 2

    def test_match_keywords(self, engine):
        engine.add_keyword("python")
        engine.add_keyword("ai")
        engine.add_keyword("rust")
        matches = engine.match_keywords("I love Python and AI programming")
        assert len(matches) == 2
        labels = [m["keyword"] for m in matches]
        assert "python" in labels
        assert "ai" in labels

    def test_match_no_keywords(self, engine):
        engine.add_keyword("python")
        matches = engine.match_keywords("I love JavaScript")
        assert len(matches) == 0


class TestTemplates:
    def test_get_template(self, engine):
        t = engine.get_template("agree")
        assert "{opinion}" in t

    def test_get_template_invalid(self, engine):
        t = engine.get_template("nonexistent_style")
        assert "{opinion}" in t  # falls back to add_value

    def test_fill_template(self, engine):
        result = engine.fill_template("agree", "this is spot on", "user1")
        assert "this is spot on" in result

    def test_template_index_wrap(self, engine):
        t1 = engine.get_template("agree", 0)
        t2 = engine.get_template("agree", 100)  # wraps around
        assert "{opinion}" in t1
        assert "{opinion}" in t2


class TestStylePerformance:
    def test_style_performance_empty(self, engine):
        perf = engine.get_style_performance()
        assert perf == {}

    def test_style_performance(self, engine):
        for style in ["agree", "agree", "humor"]:
            qt = engine.create_quote(f"t_{style}_{id(style)}", "a", "X", style=style)
            engine.mark_posted(qt.quote_id)
            engine.update_quote_metrics(qt.quote_id, likes=10, retweets=5,
                                        replies=2, impressions=500)
        perf = engine.get_style_performance()
        assert "agree" in perf
        assert "humor" in perf
        assert perf["agree"]["count"] == 2

    def test_best_style(self, engine):
        qt1 = engine.create_quote("t1", "a", "X", style="agree")
        engine.mark_posted(qt1.quote_id)
        engine.update_quote_metrics(qt1.quote_id, likes=100, retweets=50,
                                     replies=20, impressions=1000)

        qt2 = engine.create_quote("t2", "b", "Y", style="humor")
        engine.mark_posted(qt2.quote_id)
        engine.update_quote_metrics(qt2.quote_id, likes=5, retweets=2,
                                     replies=1, impressions=500)

        best = engine.get_best_style()
        assert best is not None

    def test_best_style_empty(self, engine):
        assert engine.get_best_style() is None


class TestReport:
    def test_generate_report(self, engine):
        engine.add_keyword("python")
        qt = engine.create_quote("t1", "a", "X")
        engine.mark_posted(qt.quote_id)

        report = engine.generate_report()
        assert report["total_quotes"] == 1
        assert report["posted_quotes"] == 1
        assert report["active_keywords"] == 1
        assert report["cooldown_minutes"] == 30
        assert report["max_per_hour"] == 5

    def test_report_empty(self, engine):
        report = engine.generate_report()
        assert report["total_quotes"] == 0
        assert report["posted_quotes"] == 0
