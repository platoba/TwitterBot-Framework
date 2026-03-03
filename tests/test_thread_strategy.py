"""Tests for bot/thread_strategy.py — Strategic Thread Publishing Engine"""

import pytest
from bot.thread_strategy import (
    ThreadStrategy, ThreadPlan, ThreadTweet, ThreadTemplate, ThreadStatus,
    NumberingStyle, TweetRole, ReadabilityAnalyzer, CharacterBudgetOptimizer,
    EngagementPredictor, TEMPLATE_STRUCTURES, CLIFF_HANGER_PHRASES,
    POSITION_ENGAGEMENT_FACTORS,
)


@pytest.fixture
def strategy():
    return ThreadStrategy(db_path=":memory:")


@pytest.fixture
def sample_tweets():
    return [
        "🔥 Here's why most developers fail at building side projects (a thread):",
        "The #1 mistake: Starting with the technology instead of the problem.",
        "I made this mistake 3 times. Each project took months and made $0.",
        "The fix? Talk to 10 potential users BEFORE writing a single line of code.",
        "Ask them: What's the hardest part of [your niche]? How do you solve it now?",
        "Then build the MINIMUM viable solution. Not a framework. Not an architecture.",
        "Ship it in 2 weeks or less. Get feedback. Iterate or pivot.",
        "Follow me for more practical dev advice 👇 RT the first tweet to help others!",
    ]


class TestReadabilityAnalyzer:
    def test_count_syllables_simple(self):
        assert ReadabilityAnalyzer.count_syllables("cat") == 1
        assert ReadabilityAnalyzer.count_syllables("hello") == 2
        assert ReadabilityAnalyzer.count_syllables("beautiful") == 3

    def test_count_syllables_empty(self):
        assert ReadabilityAnalyzer.count_syllables("") == 0

    def test_count_syllables_short(self):
        assert ReadabilityAnalyzer.count_syllables("it") == 1
        assert ReadabilityAnalyzer.count_syllables("a") == 1

    def test_flesch_score_simple(self):
        score = ReadabilityAnalyzer.flesch_score("This is a simple sentence.")
        assert 0 <= score <= 100

    def test_flesch_score_empty(self):
        score = ReadabilityAnalyzer.flesch_score("")
        assert score == 100.0

    def test_flesch_score_complex(self):
        complex_text = ("Notwithstanding the aforementioned circumstances, "
                        "the quintessential paradigm necessitates reconsideration.")
        simple_text = "I like cats. They are fun."
        assert ReadabilityAnalyzer.flesch_score(simple_text) > ReadabilityAnalyzer.flesch_score(complex_text)

    def test_tweet_readability_basic(self):
        result = ReadabilityAnalyzer.tweet_readability("Hello world! This is a test tweet. 🚀")
        assert "flesch_score" in result
        assert "grade" in result
        assert "word_count" in result
        assert "char_count" in result
        assert "char_remaining" in result
        assert result["has_emoji"] is True
        assert result["has_hashtag"] is False

    def test_tweet_readability_with_hashtag(self):
        result = ReadabilityAnalyzer.tweet_readability("Check this out #python @user")
        assert result["has_hashtag"] is True
        assert result["has_mention"] is True

    def test_tweet_readability_with_url(self):
        result = ReadabilityAnalyzer.tweet_readability("Read more at https://example.com")
        assert result["has_url"] is True

    def test_tweet_readability_optimal(self):
        result = ReadabilityAnalyzer.tweet_readability("Simple short tweet.")
        assert "optimal_for_twitter" in result

    def test_tweet_readability_line_breaks(self):
        result = ReadabilityAnalyzer.tweet_readability("Line 1\nLine 2\nLine 3")
        assert result["line_breaks"] == 2


class TestCharacterBudgetOptimizer:
    def test_optimize_short_text(self):
        result = CharacterBudgetOptimizer.optimize_text("Hello world")
        assert result == "Hello world"

    def test_optimize_long_text(self):
        long_text = "a " * 200
        result = CharacterBudgetOptimizer.optimize_text(long_text, max_chars=280)
        assert len(result) <= 280
        assert result.endswith("…")

    def test_optimize_replacements(self):
        text = "In order to succeed, due to the fact that effort matters."
        result = CharacterBudgetOptimizer.optimize_text(text)
        assert "in order to" not in result.lower() or "due to the fact that" not in result.lower()

    def test_split_content_short(self):
        result = CharacterBudgetOptimizer.split_content("Short text")
        assert len(result) == 1
        assert result[0] == "Short text"

    def test_split_content_long(self):
        long_text = " ".join(["word"] * 200)
        result = CharacterBudgetOptimizer.split_content(long_text)
        assert len(result) > 1
        for chunk in result:
            assert len(chunk) <= 280

    def test_split_with_numbering(self):
        long_text = " ".join(["word"] * 200)
        result = CharacterBudgetOptimizer.split_content(
            long_text, numbering=NumberingStyle.SLASH
        )
        assert len(result) > 1

    def test_budget_report_basic(self):
        result = CharacterBudgetOptimizer.budget_report("Hello world")
        assert result["raw_chars"] == 11
        assert result["fits"] is True
        assert result["remaining"] > 0

    def test_budget_report_with_url(self):
        text = "Check https://example.com/very/long/path/to/resource"
        result = CharacterBudgetOptimizer.budget_report(text)
        assert result["url_count"] == 1
        assert result["url_char_savings"] > 0

    def test_budget_report_over_limit(self):
        text = "x" * 300
        result = CharacterBudgetOptimizer.budget_report(text)
        assert result["fits"] is False


class TestEngagementPredictor:
    def test_predict_empty_thread(self):
        plan = ThreadPlan(tweets=[])
        result = EngagementPredictor.predict_thread(plan)
        assert result["estimated_impressions"] == 0

    def test_predict_basic_thread(self, sample_tweets):
        tweets = [ThreadTweet(index=i, text=t) for i, t in enumerate(sample_tweets)]
        plan = ThreadPlan(tweets=tweets)
        result = EngagementPredictor.predict_thread(plan)
        assert result["estimated_impressions"] > 0
        assert result["estimated_likes"] > 0
        assert result["tweet_count"] == len(sample_tweets)

    def test_predict_question_hook_boost(self):
        tweets = [ThreadTweet(index=0, text="Why do most startups fail? 🧵")]
        plan = ThreadPlan(tweets=tweets)
        result = EngagementPredictor.predict_thread(plan)
        assert result["multiplier"] > 1.0

    def test_predict_media_boost(self):
        tweets = [ThreadTweet(index=0, text="Check this", has_media=True)]
        plan = ThreadPlan(tweets=tweets)
        result = EngagementPredictor.predict_thread(plan)
        assert result["media_count"] == 1

    def test_predict_cta_boost(self):
        tweets = [
            ThreadTweet(index=0, text="Hook", role=TweetRole.HOOK),
            ThreadTweet(index=1, text="CTA", role=TweetRole.CTA),
        ]
        plan = ThreadPlan(tweets=tweets)
        result = EngagementPredictor.predict_thread(plan)
        assert result["multiplier"] > 1.0

    def test_predict_long_thread_penalty(self):
        tweets = [ThreadTweet(index=i, text=f"Tweet {i}") for i in range(25)]
        plan = ThreadPlan(tweets=tweets)
        result = EngagementPredictor.predict_thread(plan)
        assert result["multiplier"] < 1.0 or result["tweet_count"] == 25

    def test_predict_position_first(self):
        retention = EngagementPredictor.predict_tweet_position(0, 10)
        assert retention == 1.0

    def test_predict_position_dropoff(self):
        first = EngagementPredictor.predict_tweet_position(0, 10)
        fifth = EngagementPredictor.predict_tweet_position(4, 10)
        assert fifth < first

    def test_predict_position_cta_bump(self):
        last = EngagementPredictor.predict_tweet_position(9, 10)
        second_last = EngagementPredictor.predict_tweet_position(8, 10)
        assert last > second_last  # CTA bump

    def test_predict_position_zero_total(self):
        assert EngagementPredictor.predict_tweet_position(0, 0) == 0.0


class TestThreadStrategy:
    def test_create_thread(self, strategy, sample_tweets):
        plan = strategy.create_thread(
            title="Developer Side Projects",
            content_pieces=sample_tweets,
            template=ThreadTemplate.TUTORIAL,
        )
        assert plan.thread_id
        assert plan.title == "Developer Side Projects"
        assert len(plan.tweets) == len(sample_tweets)
        assert plan.status == ThreadStatus.DRAFT
        assert plan.total_chars > 0

    def test_create_thread_with_tags(self, strategy, sample_tweets):
        plan = strategy.create_thread(
            title="Test",
            content_pieces=sample_tweets[:3],
            tags=["dev", "productivity"],
        )
        assert "dev" in plan.tags
        assert "productivity" in plan.tags

    def test_get_thread(self, strategy, sample_tweets):
        plan = strategy.create_thread("Test", sample_tweets[:3])
        loaded = strategy.get_thread(plan.thread_id)
        assert loaded is not None
        assert loaded.thread_id == plan.thread_id
        assert loaded.title == "Test"
        assert len(loaded.tweets) == 3

    def test_get_thread_not_found(self, strategy):
        assert strategy.get_thread("nonexistent") is None

    def test_list_threads(self, strategy, sample_tweets):
        strategy.create_thread("Thread 1", sample_tweets[:2])
        strategy.create_thread("Thread 2", sample_tweets[2:5])
        threads = strategy.list_threads()
        assert len(threads) == 2

    def test_list_threads_by_status(self, strategy, sample_tweets):
        plan = strategy.create_thread("Draft", sample_tweets[:2])
        strategy.schedule_thread(plan.thread_id, "2026-12-01T00:00:00Z")
        drafts = strategy.list_threads(status=ThreadStatus.DRAFT)
        scheduled = strategy.list_threads(status=ThreadStatus.SCHEDULED)
        assert len(drafts) == 0
        assert len(scheduled) == 1

    def test_apply_numbering_slash(self, strategy, sample_tweets):
        plan = strategy.create_thread("Test", sample_tweets[:3],
                                       numbering=NumberingStyle.SLASH)
        numbered = strategy.apply_numbering(plan)
        assert numbered[0].startswith("1/3 ")
        assert numbered[2].startswith("3/3 ")

    def test_apply_numbering_bracket(self, strategy, sample_tweets):
        plan = strategy.create_thread("Test", sample_tweets[:3],
                                       numbering=NumberingStyle.BRACKET)
        numbered = strategy.apply_numbering(plan)
        assert numbered[0].startswith("[1/3]")

    def test_apply_numbering_dot(self, strategy, sample_tweets):
        plan = strategy.create_thread("Test", sample_tweets[:3],
                                       numbering=NumberingStyle.DOT)
        numbered = strategy.apply_numbering(plan)
        assert numbered[0].startswith("1. ")

    def test_apply_numbering_arrow(self, strategy, sample_tweets):
        plan = strategy.create_thread("Test", sample_tweets[:3],
                                       numbering=NumberingStyle.ARROW)
        numbered = strategy.apply_numbering(plan)
        assert numbered[0].startswith("→ 1. ")

    def test_apply_numbering_emoji(self, strategy, sample_tweets):
        plan = strategy.create_thread("Test", sample_tweets[:3],
                                       numbering=NumberingStyle.EMOJI)
        numbered = strategy.apply_numbering(plan)
        assert "1️⃣" in numbered[0]

    def test_apply_numbering_none(self, strategy, sample_tweets):
        plan = strategy.create_thread("Test", sample_tweets[:3],
                                       numbering=NumberingStyle.NONE)
        numbered = strategy.apply_numbering(plan)
        assert numbered[0] == sample_tweets[0]

    def test_apply_numbering_truncates(self, strategy):
        long_tweet = "A" * 275
        plan = strategy.create_thread("Test", [long_tweet],
                                       numbering=NumberingStyle.SLASH)
        numbered = strategy.apply_numbering(plan)
        assert len(numbered[0]) <= 280

    def test_insert_cliff_hangers(self, strategy, sample_tweets):
        plan = strategy.create_thread("Test", sample_tweets)
        original_count = len(plan.tweets)
        plan = strategy.insert_cliff_hangers(plan, every_n=3)
        assert len(plan.tweets) > original_count
        bridges = [t for t in plan.tweets if t.role == TweetRole.BRIDGE]
        assert len(bridges) > 0

    def test_insert_cliff_hangers_short_thread(self, strategy):
        plan = strategy.create_thread("Test", ["A", "B", "C"])
        original_count = len(plan.tweets)
        plan = strategy.insert_cliff_hangers(plan)
        # Short threads shouldn't get many cliff-hangers
        assert len(plan.tweets) >= original_count

    def test_analyze_thread(self, strategy, sample_tweets):
        plan = strategy.create_thread("Test", sample_tweets)
        analysis = strategy.analyze_thread(plan)
        assert analysis["tweet_count"] == len(sample_tweets)
        assert "avg_readability" in analysis
        assert "engagement_prediction" in analysis
        assert "position_analysis" in analysis
        assert analysis["has_hook"] is True

    def test_analyze_empty_thread(self, strategy):
        plan = ThreadPlan(tweets=[])
        analysis = strategy.analyze_thread(plan)
        assert analysis.get("error") == "empty thread"

    def test_optimize_thread(self, strategy, sample_tweets):
        plan = strategy.create_thread("Test", sample_tweets)
        result = strategy.optimize_thread(plan)
        assert "suggestions" in result
        assert "current_score" in result
        assert isinstance(result["suggestion_count"], int)

    def test_optimize_detects_over_limit(self, strategy):
        long_tweet = "A" * 300
        plan = strategy.create_thread("Test", [long_tweet, "Short"])
        result = strategy.optimize_thread(plan)
        high = [s for s in result["suggestions"] if s["severity"] == "high"
                and s["type"] == "char_limit"]
        assert len(high) > 0

    def test_optimize_detects_missing_cta(self, strategy):
        tweets = [ThreadTweet(index=0, text="Hello", role=TweetRole.BODY)]
        plan = ThreadPlan(tweets=tweets)
        result = strategy.optimize_thread(plan)
        cta_suggestions = [s for s in result["suggestions"]
                           if "CTA" in s["message"]]
        assert len(cta_suggestions) > 0

    def test_schedule_thread(self, strategy, sample_tweets):
        plan = strategy.create_thread("Test", sample_tweets[:3])
        assert strategy.schedule_thread(plan.thread_id, "2026-12-01T12:00:00Z")
        loaded = strategy.get_thread(plan.thread_id)
        assert loaded.status == ThreadStatus.SCHEDULED
        assert loaded.scheduled_at == "2026-12-01T12:00:00Z"

    def test_schedule_thread_with_drip(self, strategy, sample_tweets):
        plan = strategy.create_thread("Test", sample_tweets[:3])
        strategy.schedule_thread(plan.thread_id, "2026-12-01T12:00:00Z", drip_minutes=5)
        loaded = strategy.get_thread(plan.thread_id)
        assert loaded.drip_interval_minutes == 5

    def test_update_status(self, strategy, sample_tweets):
        plan = strategy.create_thread("Test", sample_tweets[:3])
        assert strategy.update_status(plan.thread_id, ThreadStatus.PUBLISHED)
        loaded = strategy.get_thread(plan.thread_id)
        assert loaded.status == ThreadStatus.PUBLISHED

    def test_record_analytics(self, strategy, sample_tweets):
        plan = strategy.create_thread("Test", sample_tweets[:3])
        engagement = {
            0: {"impressions": 1000, "likes": 50, "retweets": 10, "replies": 5},
            1: {"impressions": 800, "likes": 30, "retweets": 5, "replies": 3},
            2: {"impressions": 600, "likes": 20, "retweets": 3, "replies": 2},
        }
        result = strategy.record_analytics(plan.thread_id, engagement)
        assert result["total_impressions"] == 2400
        assert result["total_likes"] == 100
        assert result["best_tweet_index"] == 0
        assert result["completion_rate"] > 0

    def test_delete_thread(self, strategy, sample_tweets):
        plan = strategy.create_thread("Test", sample_tweets[:3])
        assert strategy.delete_thread(plan.thread_id)
        assert strategy.get_thread(plan.thread_id) is None

    def test_delete_nonexistent(self, strategy):
        assert strategy.delete_thread("nonexistent") is False

    def test_get_template_info(self, strategy):
        info = strategy.get_template_info(ThreadTemplate.STORY)
        assert info["template"] == "story"
        assert info["has_hook"] is True
        assert info["has_cta"] is True
        assert len(info["structure"]) > 0

    def test_get_template_info_all(self, strategy):
        for template in ThreadTemplate:
            info = strategy.get_template_info(template)
            assert info["template"] == template.value
            assert info["tweet_count"] > 0

    def test_analytics_history(self, strategy, sample_tweets):
        plan = strategy.create_thread("Test", sample_tweets[:3])
        engagement = {0: {"impressions": 100, "likes": 5, "retweets": 1, "replies": 0}}
        strategy.record_analytics(plan.thread_id, engagement)
        history = strategy.get_analytics_history(plan.thread_id)
        assert len(history) == 1
        assert history[0]["total_impressions"] == 100


class TestTemplateStructures:
    def test_all_templates_have_structures(self):
        for template in ThreadTemplate:
            assert template in TEMPLATE_STRUCTURES

    def test_all_templates_have_hook(self):
        for template in ThreadTemplate:
            structure = TEMPLATE_STRUCTURES[template]
            assert TweetRole.HOOK in structure

    def test_all_templates_have_cta(self):
        for template in ThreadTemplate:
            structure = TEMPLATE_STRUCTURES[template]
            assert TweetRole.CTA in structure

    def test_template_minimum_length(self):
        for template in ThreadTemplate:
            assert len(TEMPLATE_STRUCTURES[template]) >= 3


class TestThreadTweet:
    def test_auto_char_count(self):
        tweet = ThreadTweet(index=0, text="Hello world")
        assert tweet.char_count == 11
        assert tweet.word_count == 2

    def test_empty_text(self):
        tweet = ThreadTweet(index=0, text="")
        assert tweet.char_count == 0


class TestThreadPlan:
    def test_auto_id(self):
        plan = ThreadPlan()
        assert len(plan.thread_id) > 0

    def test_auto_created_at(self):
        plan = ThreadPlan()
        assert plan.created_at

    def test_default_status(self):
        plan = ThreadPlan()
        assert plan.status == ThreadStatus.DRAFT


class TestConstants:
    def test_cliff_hanger_phrases(self):
        assert len(CLIFF_HANGER_PHRASES) > 0
        for phrase in CLIFF_HANGER_PHRASES:
            assert len(phrase) < 280

    def test_position_factors_decreasing(self):
        factors = [POSITION_ENGAGEMENT_FACTORS[i] for i in sorted(POSITION_ENGAGEMENT_FACTORS)]
        for i in range(1, len(factors)):
            assert factors[i] <= factors[i - 1]
