"""Tests for Cross-Post Engine module."""

import pytest

from bot.crosspost_engine import (
    Platform, ContentType, PostStatus, PLATFORM_LIMITS,
    MediaAttachment, CrossPost, SourceContent, ContentAdapter,
    CrossPostStore, CrossPostEngine,
)


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "test_crosspost.db")


@pytest.fixture
def engine(tmp_db):
    return CrossPostEngine(db_path=tmp_db)


@pytest.fixture
def store(tmp_db):
    return CrossPostStore(db_path=tmp_db)


@pytest.fixture
def adapter():
    return ContentAdapter()


class TestPlatformLimits:
    def test_all_platforms_have_limits(self):
        for platform in Platform:
            assert platform in PLATFORM_LIMITS
            limits = PLATFORM_LIMITS[platform]
            assert "char_limit" in limits
            assert "thread_max" in limits
            assert limits["char_limit"] > 0

    def test_twitter_limits(self):
        limits = PLATFORM_LIMITS[Platform.TWITTER]
        assert limits["char_limit"] == 280
        assert limits["supports_threads"] is True
        assert limits["supports_polls"] is True

    def test_linkedin_limits(self):
        limits = PLATFORM_LIMITS[Platform.LINKEDIN]
        assert limits["char_limit"] == 3000
        assert limits["supports_threads"] is False

    def test_mastodon_limits(self):
        limits = PLATFORM_LIMITS[Platform.MASTODON]
        assert limits["char_limit"] == 500
        assert limits["supports_threads"] is True


class TestContentAdapter:
    def test_adapt_short_text(self, adapter):
        source = SourceContent(source_id="s1", content="Hello world")
        post = adapter.adapt(source, Platform.TWITTER)
        assert post.content == "Hello world"
        assert post.platform == Platform.TWITTER

    def test_adapt_long_text_twitter(self, adapter):
        long_text = "A" * 500
        source = SourceContent(source_id="s1", content=long_text)
        post = adapter.adapt(source, Platform.TWITTER)
        assert len(post.content) <= 280 or len(post.content_parts) > 0

    def test_adapt_long_text_linkedin(self, adapter):
        long_text = "A" * 500
        source = SourceContent(source_id="s1", content=long_text)
        post = adapter.adapt(source, Platform.LINKEDIN)
        assert len(post.content) <= 3000

    def test_adapt_thread_twitter(self, adapter):
        parts = ["Part 1", "Part 2", "Part 3"]
        source = SourceContent(
            source_id="s1", content="Thread", content_type=ContentType.THREAD,
            thread_parts=parts,
        )
        post = adapter.adapt(source, Platform.TWITTER)
        assert len(post.content_parts) <= 25  # Twitter thread max

    def test_adapt_thread_linkedin(self, adapter):
        parts = ["Part 1", "Part 2", "Part 3"]
        source = SourceContent(
            source_id="s1", content="Thread", content_type=ContentType.THREAD,
            thread_parts=parts,
        )
        post = adapter.adapt(source, Platform.LINKEDIN)
        # LinkedIn doesn't support threads, should merge
        assert len(post.content_parts) == 0
        assert "Part 1" in post.content or "Part 2" in post.content

    def test_adapt_hashtags(self, adapter):
        source = SourceContent(
            source_id="s1", content="Test post",
            hashtags=["marketing", "growth", "seo"],
        )
        post = adapter.adapt(source, Platform.TWITTER)
        assert len(post.hashtags) <= 30

    def test_adapt_hashtags_mastodon_camelcase(self, adapter):
        source = SourceContent(
            source_id="s1", content="Test",
            hashtags=["social_media", "content_marketing"],
        )
        post = adapter.adapt(source, Platform.MASTODON)
        # Mastodon prefers CamelCase
        assert any("Social" in h or "Content" in h for h in post.hashtags)

    def test_professionalize(self, adapter):
        text = "This is imo a great tool tbh"
        result = adapter._professionalize(text)
        assert "in my opinion" in result
        assert "to be honest" in result

    def test_strip_markdown(self, adapter):
        text = "**Bold** and *italic* and `code`"
        result = adapter._strip_markdown(text)
        assert "**" not in result
        assert "*" not in result
        assert "`" not in result

    def test_truncate_smart_sentence(self, adapter):
        text = "First sentence. Second sentence. Third sentence."
        result = adapter._truncate_smart(text, 20)
        assert len(result) <= 20
        assert result.endswith(".")

    def test_truncate_smart_word(self, adapter):
        text = "word1 word2 word3 word4 word5"
        result = adapter._truncate_smart(text, 15)
        assert len(result) <= 15
        assert not result.endswith(" ")

    def test_split_to_thread(self, adapter):
        long_text = ". ".join([f"Sentence {i}" for i in range(20)])
        parts = adapter._split_to_thread(long_text, 100)
        assert len(parts) > 1
        for part in parts:
            assert len(part) <= 100

    def test_merge_thread(self, adapter):
        parts = ["1/3 First part", "2/3 Second part", "3/3 Third part"]
        merged = adapter._merge_thread(parts, 500)
        assert "First part" in merged
        assert "Second part" in merged
        assert "1/3" not in merged  # Numbering removed


class TestCrossPostStore:
    def test_save_and_get_cross_post(self, store):
        post = CrossPost(
            post_id="p1", source_id="s1", platform=Platform.TWITTER,
            content="Test post", hashtags=["test"],
        )
        store.save_cross_post(post)
        loaded = store.get_cross_posts("s1")
        assert len(loaded) == 1
        assert loaded[0].content == "Test post"

    def test_get_posts_by_status(self, store):
        for i, status in enumerate([PostStatus.DRAFT, PostStatus.PUBLISHED, PostStatus.FAILED]):
            store.save_cross_post(CrossPost(
                post_id=f"p{i}", source_id=f"s{i}",
                platform=Platform.TWITTER, content=f"Post {i}",
                status=status,
            ))
        drafts = store.get_posts_by_status(PostStatus.DRAFT)
        assert len(drafts) == 1
        assert drafts[0].status == PostStatus.DRAFT

    def test_record_analytics(self, store):
        store.record_analytics(
            "p1", "twitter", likes=100, shares=20, comments=10,
            impressions=5000, clicks=200,
        )
        perf = store.get_platform_performance(30)
        assert "twitter" in perf
        assert perf["twitter"]["likes"] == 100

    def test_get_platform_performance(self, store):
        platforms = ["twitter", "linkedin", "mastodon"]
        for i, plat in enumerate(platforms):
            store.record_analytics(
                f"p{i}", plat, likes=i * 10, impressions=i * 1000,
            )
        perf = store.get_platform_performance(30)
        assert len(perf) == 3
        assert all(p in perf for p in platforms)

    def test_get_total_stats(self, store):
        source = SourceContent(source_id="s1", content="Test")
        store.save_source(source)
        for i, plat in enumerate([Platform.TWITTER, Platform.LINKEDIN]):
            store.save_cross_post(CrossPost(
                post_id=f"p{i}", source_id="s1",
                platform=plat, content="Test",
                status=PostStatus.PUBLISHED if i == 0 else PostStatus.FAILED,
            ))
        stats = store.get_total_stats()
        assert stats["total_sources"] == 1
        assert stats["total_crossposts"] == 2
        assert stats["published"] == 1
        assert stats["failed"] == 1


class TestCrossPostEngine:
    def test_enable_disable_platform(self, engine):
        engine.disable_platform(Platform.LINKEDIN)
        assert Platform.LINKEDIN not in engine.get_enabled_platforms()
        engine.enable_platform(Platform.LINKEDIN)
        assert Platform.LINKEDIN in engine.get_enabled_platforms()

    def test_create_crosspost_single_platform(self, engine):
        posts = engine.create_crosspost(
            "Hello world", platforms=[Platform.TWITTER],
        )
        assert len(posts) == 1
        assert Platform.TWITTER in posts

    def test_create_crosspost_all_platforms(self, engine):
        posts = engine.create_crosspost("Test post")
        assert len(posts) == len(engine.get_enabled_platforms())

    def test_create_crosspost_with_hashtags(self, engine):
        posts = engine.create_crosspost(
            "Marketing tips", hashtags=["marketing", "growth"],
            platforms=[Platform.TWITTER],
        )
        post = posts[Platform.TWITTER]
        assert len(post.hashtags) > 0

    def test_create_crosspost_with_thread(self, engine):
        parts = ["Part 1 of my thread", "Part 2 continues", "Part 3 final"]
        posts = engine.create_crosspost(
            "Thread", content_type=ContentType.THREAD,
            thread_parts=parts, platforms=[Platform.TWITTER, Platform.LINKEDIN],
        )
        # Twitter should have thread parts
        twitter_post = posts.get(Platform.TWITTER)
        if twitter_post:
            assert len(twitter_post.content_parts) > 0 or twitter_post.content
        # LinkedIn should merge
        linkedin_post = posts.get(Platform.LINKEDIN)
        if linkedin_post:
            assert len(linkedin_post.content_parts) == 0

    def test_preview_crosspost(self, engine):
        preview = engine.preview_crosspost(
            "This is a test post for preview",
            Platform.TWITTER, hashtags=["test"],
        )
        assert "TWITTER" in preview
        assert "test post" in preview
        assert "Characters:" in preview

    def test_preview_long_content(self, engine):
        long_text = "A" * 500
        preview = engine.preview_crosspost(long_text, Platform.TWITTER)
        assert "280" in preview  # Character limit shown

    def test_get_post_status(self, engine):
        posts = engine.create_crosspost("Test", platforms=[Platform.TWITTER])
        source_id = list(posts.values())[0].source_id
        status = engine.get_post_status(source_id)
        assert "twitter" in status
        assert status["twitter"]["status"] == "draft"

    def test_mark_published(self, engine):
        posts = engine.create_crosspost("Test", platforms=[Platform.TWITTER])
        post = posts[Platform.TWITTER]
        engine.mark_published(post.post_id, "tw_12345")
        status = engine.get_post_status(post.source_id)
        assert status["twitter"]["status"] == "published"
        assert status["twitter"]["platform_post_id"] == "tw_12345"

    def test_mark_failed(self, engine):
        posts = engine.create_crosspost("Test", platforms=[Platform.TWITTER])
        post = posts[Platform.TWITTER]
        engine.mark_failed(post.post_id, "API error")
        status = engine.get_post_status(post.source_id)
        assert status["twitter"]["status"] == "failed"
        assert "error" in status["twitter"]["error"].lower()

    def test_compare_platforms(self, engine):
        # Create posts and add analytics
        posts = engine.create_crosspost("Test", platforms=[Platform.TWITTER, Platform.LINKEDIN])
        for platform, post in posts.items():
            engine.mark_published(post.post_id, f"{platform.value}_123")
            engine.store.record_analytics(
                post.post_id, platform.value, likes=100, impressions=5000,
            )
        report = engine.compare_platforms(30)
        assert "Cross-Platform Performance" in report
        assert "TWITTER" in report or "LINKEDIN" in report

    def test_generate_report(self, engine):
        posts = engine.create_crosspost("Test")
        report = engine.generate_report()
        assert "Cross-Post Engine Report" in report
        assert "Overview" in report
        assert "Enabled Platforms" in report

    def test_full_flow(self, engine):
        """Integration test: full cross-posting flow."""
        # 1. Create cross-posts for multiple platforms
        posts = engine.create_crosspost(
            "Excited to share my new blog post on AI marketing strategies! "
            "Check it out and let me know what you think.",
            content_type=ContentType.TEXT,
            hashtags=["AI", "marketing", "contentmarketing", "growth"],
            link="https://blog.example.com/ai-marketing",
            platforms=[Platform.TWITTER, Platform.LINKEDIN, Platform.MASTODON],
        )
        
        assert len(posts) == 3
        
        # 2. Preview each platform
        for platform in [Platform.TWITTER, Platform.LINKEDIN]:
            preview = engine.preview_crosspost(
                "Sample content for preview", platform,
            )
            assert platform.value.upper() in preview
        
        # 3. Mark some as published
        twitter_post = posts[Platform.TWITTER]
        engine.mark_published(twitter_post.post_id, "tw_987654321")
        
        # 4. Mark one as failed
        mastodon_post = posts[Platform.MASTODON]
        engine.mark_failed(mastodon_post.post_id, "Network timeout")
        
        # 5. Check status
        status = engine.get_post_status(twitter_post.source_id)
        assert status["twitter"]["status"] == "published"
        assert status["mastodon"]["status"] == "failed"
        
        # 6. Record analytics for published posts
        engine.store.record_analytics(
            twitter_post.post_id, "twitter",
            likes=250, shares=45, comments=18, impressions=8500, clicks=320,
        )
        
        # 7. Compare platforms
        comparison = engine.compare_platforms(7)
        assert "Cross-Platform Performance" in comparison
        
        # 8. Generate summary report
        report = engine.generate_report()
        assert "Cross-Post Engine Report" in report

    def test_disabled_platform_not_created(self, engine):
        engine.disable_platform(Platform.MASTODON)
        posts = engine.create_crosspost("Test")
        assert Platform.MASTODON not in posts

    def test_media_attachment_limit(self, engine):
        media = [
            MediaAttachment(file_path=f"/path/to/image{i}.jpg", media_type="image")
            for i in range(10)
        ]
        posts = engine.create_crosspost(
            "Images", media=media, platforms=[Platform.TWITTER],
        )
        twitter_post = posts[Platform.TWITTER]
        # Twitter max 4 images
        assert len(twitter_post.media) <= 4
