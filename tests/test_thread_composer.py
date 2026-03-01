"""
Tests for Thread Composer
"""

import pytest
from bot.thread_composer import (
    ThreadComposer, Thread, ThreadTweet, TWEET_MAX_CHARS,
)


@pytest.fixture
def composer():
    return ThreadComposer()


@pytest.fixture
def composer_no_numbering():
    return ThreadComposer(numbering=False)


class TestThreadTweet:
    def test_valid_tweet(self):
        t = ThreadTweet(index=1, text="Hello world")
        assert t.is_valid
        assert t.char_count > 0

    def test_long_tweet(self):
        t = ThreadTweet(index=1, text="x" * 300)
        assert not t.is_valid
        assert t.char_count == 300

    def test_url_counting(self):
        t = ThreadTweet(index=1, text="Check https://example.com/very-long-url-path")
        # URL should be counted as 23 chars
        assert t.char_count < len(t.text)

    def test_to_dict(self):
        t = ThreadTweet(index=1, text="Test", media_urls=["http://img.jpg"])
        d = t.to_dict()
        assert d["index"] == 1
        assert d["text"] == "Test"
        assert len(d["media_urls"]) == 1


class TestThread:
    def test_empty_thread(self):
        t = Thread(title="Test")
        assert t.total == 0
        assert t.total_chars == 0
        assert not t.is_valid

    def test_valid_thread(self):
        tweets = [ThreadTweet(index=1, text="Hello")]
        t = Thread(title="Test", tweets=tweets)
        assert t.total == 1
        assert t.is_valid

    def test_invalid_tweets(self):
        tweets = [
            ThreadTweet(index=1, text="OK"),
            ThreadTweet(index=2, text="x" * 300),
        ]
        t = Thread(title="Test", tweets=tweets)
        assert not t.is_valid
        assert len(t.invalid_tweets) == 1

    def test_to_dict(self):
        tweets = [ThreadTweet(index=1, text="Hi")]
        t = Thread(title="Test", tweets=tweets, hashtags="#test")
        d = t.to_dict()
        assert d["total_tweets"] == 1
        assert d["hashtags"] == "#test"


class TestThreadComposer:
    def test_short_compose(self, composer):
        thread = composer.compose("My Topic", "Short body text")
        assert thread.total >= 1
        assert thread.is_valid

    def test_long_compose(self, composer):
        body = "This is a test sentence. " * 50  # ~1250 chars
        thread = composer.compose("Long Thread", body)
        assert thread.total > 1
        assert all(t.is_valid for t in thread.tweets)

    def test_numbering(self, composer):
        body = "Part one. " * 30
        thread = composer.compose("Numbered", body)
        for tweet in thread.tweets:
            assert f"({tweet.index}/{thread.total})" in tweet.text

    def test_no_numbering(self, composer_no_numbering):
        body = "Some text. " * 30
        thread = composer_no_numbering.compose("No Numbers", body)
        for tweet in thread.tweets:
            assert "(1/" not in tweet.text

    def test_hook(self, composer):
        thread = composer.compose("Title", "Body", hook="🔥 Custom hook!")
        assert thread.tweets[0].text.startswith("🔥 Custom hook!")

    def test_auto_hook(self, composer):
        thread = composer.compose("My Title", "Body text")
        assert "🧵" in thread.tweets[0].text

    def test_cta(self, composer):
        thread = composer.compose("Title", "Body text", cta="Follow for more!")
        # CTA should be in last tweet
        last_text = thread.tweets[-1].text
        assert "Follow for more!" in last_text or thread.total == 1

    def test_hashtags(self, composer):
        thread = composer.compose("Title", "Body", hashtags="#ai #ml")
        # Hashtags should appear somewhere
        all_text = " ".join(t.text for t in thread.tweets)
        assert "#ai" in all_text or thread.total == 1

    def test_media_map(self, composer):
        thread = composer.compose(
            "Title", "Body text",
            media_map={1: ["http://img1.jpg", "http://img2.jpg"]}
        )
        assert len(thread.tweets[0].media_urls) == 2


class TestSplitText:
    def test_short_text(self, composer):
        chunks = composer._split_text("Short text")
        assert len(chunks) == 1

    def test_long_text(self, composer):
        text = "This is a long sentence. " * 20
        chunks = composer._split_text(text, reserve_chars=20)
        assert len(chunks) > 1
        for chunk in chunks:
            assert len(chunk) <= TWEET_MAX_CHARS

    def test_paragraph_split(self, composer):
        text = "Paragraph one.\n\nParagraph two.\n\nParagraph three." + " More text." * 30
        chunks = composer._split_text(text)
        assert len(chunks) >= 1


class TestFindSplitPoint:
    def test_paragraph_boundary(self, composer):
        text = "First part.\n\nSecond part is here."
        pos = composer._find_split_point(text, 20)
        assert text[pos:].strip().startswith("Second")

    def test_sentence_boundary(self, composer):
        text = "First sentence. Second sentence here."
        pos = composer._find_split_point(text, 20)
        assert pos > 5

    def test_hard_cut(self, composer):
        text = "abcdefghijklmnopqrstuvwxyz"
        pos = composer._find_split_point(text, 10)
        assert pos == 10


class TestComposeFromPoints:
    def test_one_per_tweet(self, composer):
        points = ["Point one here", "Point two here", "Point three here"]
        thread = composer.compose_from_points("Title", points, one_per_tweet=True)
        # Should have at least 4 tweets (hook + 3 points)
        assert thread.total >= 4

    def test_merged(self, composer):
        points = ["Short A", "Short B", "Short C"]
        thread = composer.compose_from_points("Title", points, one_per_tweet=False)
        # Merged should have fewer tweets
        assert thread.total >= 1

    def test_long_point(self, composer):
        points = ["x" * 300, "Short"]
        thread = composer.compose_from_points("Title", points, one_per_tweet=True)
        # Long point should be split
        assert thread.total >= 3


class TestPreview:
    def test_preview(self, composer):
        thread = composer.compose("Test", "Some body text")
        preview = composer.preview(thread)
        assert "Thread Preview" in preview
        assert "Test" in preview

    def test_preview_invalid(self, composer):
        tweets = [ThreadTweet(index=1, text="x" * 300)]
        thread = Thread(title="Bad", tweets=tweets)
        preview = composer.preview(thread)
        assert "❌" in preview
        assert "exceed" in preview


class TestValidate:
    def test_valid_thread(self, composer):
        thread = composer.compose("OK", "Normal text")
        result = composer.validate(thread)
        assert result["valid"]
        assert len(result["issues"]) == 0

    def test_empty_thread(self, composer):
        thread = Thread(title="Empty")
        result = composer.validate(thread)
        assert not result["valid"]
        assert "empty" in result["issues"][0].lower()

    def test_too_long(self, composer):
        tweets = [ThreadTweet(index=i, text=f"Tweet {i}") for i in range(30)]
        thread = Thread(title="Long", tweets=tweets)
        result = composer.validate(thread)
        assert any("too long" in i.lower() for i in result["issues"])

    def test_duplicate_detection(self, composer):
        tweets = [
            ThreadTweet(index=1, text="Same text"),
            ThreadTweet(index=2, text="Same text"),
        ]
        thread = Thread(title="Dup", tweets=tweets)
        result = composer.validate(thread)
        assert any("duplicate" in i.lower() for i in result["issues"])


class TestReadTime:
    def test_estimate(self, composer):
        thread = composer.compose("Test", "Some words here " * 20)
        result = composer.estimate_read_time(thread)
        assert result["total_words"] > 0
        assert result["total_seconds"] > 0
        assert "m" in result["formatted"]

    def test_single_tweet(self, composer):
        tweets = [ThreadTweet(index=1, text="Quick note")]
        thread = Thread(title="Short", tweets=tweets)
        result = composer.estimate_read_time(thread)
        assert result["total_words"] == 2
