"""
test_content_generator.py - 内容生成器测试
"""

import pytest
from bot.content_generator import ContentGenerator, DEFAULT_TEMPLATES


class TestContentGeneratorInit:
    def test_default_templates(self, content_gen):
        cats = content_gen.get_categories()
        assert "announcement" in cats
        assert "engagement" in cats
        assert "thread_hook" in cats
        assert "promotion" in cats
        assert "insight" in cats
        assert "daily" in cats

    def test_custom_templates(self):
        gen = ContentGenerator({"custom": ["Hello {name}!"]})
        assert gen.get_categories() == ["custom"]


class TestGenerate:
    def test_basic_generate(self, content_gen):
        result = content_gen.generate("engagement", {
            "question": "What's your favorite language?",
            "hashtags": "#Python #JavaScript"
        })
        assert result is not None
        assert len(result) > 0

    def test_generate_with_index(self, content_gen):
        result = content_gen.generate("announcement", {
            "title": "Launch",
            "body": "We just launched!",
            "hashtags": "#launch"
        }, template_index=0)
        assert "Launch" in result

    def test_generate_missing_category(self, content_gen):
        result = content_gen.generate("nonexistent", {})
        assert result is None

    def test_generate_missing_variables(self, content_gen):
        result = content_gen.generate("announcement", {"title": "Test"})
        assert result is not None  # missing vars → empty string

    def test_generate_respects_max_length(self):
        gen = ContentGenerator({"long": ["{text}"]})
        result = gen.generate("long", {"text": "A" * 500})
        assert len(result) <= 280

    def test_set_variable(self, content_gen):
        content_gen.set_variable("hashtags", "#default")
        result = content_gen.generate("announcement", {
            "title": "Test",
            "body": "Body"
        })
        assert "#default" in result

    def test_set_variables_batch(self, content_gen):
        content_gen.set_variables({"hashtags": "#batch", "call_to_action": "Buy now"})
        result = content_gen.generate("announcement", {
            "title": "Sale",
            "body": "Big sale"
        }, template_index=1)
        assert "#batch" in result or "Sale" in result


class TestAddTemplate:
    def test_add_template_new_category(self, content_gen):
        content_gen.add_template("custom", "Custom: {msg}")
        result = content_gen.generate("custom", {"msg": "hello"})
        assert result == "Custom: hello"

    def test_add_template_existing_category(self, content_gen):
        original_count = len(content_gen.templates["announcement"])
        content_gen.add_template("announcement", "NEW: {title}")
        assert len(content_gen.templates["announcement"]) == original_count + 1


class TestVariants:
    def test_generate_variants(self, content_gen):
        variants = content_gen.generate_variants("announcement", {
            "title": "Test", "body": "Body",
            "hashtags": "#test", "call_to_action": "Click"
        }, count=2)
        assert len(variants) >= 1

    def test_generate_variants_empty_category(self, content_gen):
        variants = content_gen.generate_variants("nonexistent", {})
        assert variants == []

    def test_generate_ab_pair(self, content_gen):
        a, b = content_gen.generate_ab_pair("announcement", {
            "title": "Test", "body": "Body",
            "hashtags": "#test", "call_to_action": "Now"
        })
        assert len(a) > 0
        assert len(b) > 0


class TestHashtags:
    def test_generate_hashtags(self, content_gen):
        tags = content_gen.generate_hashtags(["Python", "AI", "ML"])
        assert tags == "#Python #AI #ML"

    def test_generate_hashtags_max(self, content_gen):
        topics = ["a", "b", "c", "d", "e", "f"]
        tags = content_gen.generate_hashtags(topics, max_tags=3)
        assert tags.count("#") == 3

    def test_generate_hashtags_clean(self, content_gen):
        tags = content_gen.generate_hashtags(["#already", " spaced "])
        assert tags == "#already #spaced"


class TestThread:
    def test_generate_thread(self, content_gen):
        thread = content_gen.generate_thread(
            "thread_hook",
            {"title": "Test", "hook": "Here we go", "topic": "AI"},
            ["Part 1 details", "Part 2 details"]
        )
        assert len(thread) == 3
        assert "(2/3)" in thread[1]
        assert "(3/3)" in thread[2]

    def test_generate_thread_empty_body(self, content_gen):
        thread = content_gen.generate_thread(
            "thread_hook",
            {"title": "Solo", "hook": "Just one", "topic": "AI"}
        )
        assert len(thread) == 1


class TestTruncate:
    def test_short_text(self):
        assert ContentGenerator.truncate("hello") == "hello"

    def test_long_text(self):
        text = "A" * 300
        result = ContentGenerator.truncate(text)
        assert len(result) <= 280
        assert result.endswith("...")

    def test_exact_280(self):
        text = "A" * 280
        assert ContentGenerator.truncate(text) == text


class TestEstimateEngagement:
    def test_basic_score(self, content_gen):
        result = content_gen.estimate_engagement("Hello world")
        assert "estimated_score" in result
        assert 0 <= result["estimated_score"] <= 100

    def test_question_bonus(self, content_gen):
        plain = content_gen.estimate_engagement("Hello world")
        question = content_gen.estimate_engagement("What do you think?")
        assert question["estimated_score"] > plain["estimated_score"]
        assert question["has_question"] is True

    def test_hashtag_detection(self, content_gen):
        result = content_gen.estimate_engagement("#Python #AI are great")
        assert result["has_hashtags"] is True

    def test_emoji_detection(self, content_gen):
        result = content_gen.estimate_engagement("🚀 Launch day!")
        assert result["has_emoji"] is True

    def test_cta_bonus(self, content_gen):
        plain = content_gen.estimate_engagement("Hello world")
        cta = content_gen.estimate_engagement("Reply with your thoughts 👇")
        assert cta["estimated_score"] > plain["estimated_score"]
        assert cta["has_cta"] is True
