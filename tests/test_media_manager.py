"""Tests for Media Manager."""
import time
import pytest
from bot.media_manager import (
    MediaItem, MediaType, MediaStatus, MediaValidator, AltTextGenerator,
    MediaLibrary, UploadQueue, WatermarkEngine, TWITTER_LIMITS,
)


# ──── MediaItem ────

class TestMediaItem:
    def test_is_image(self):
        m = MediaItem(media_id="m1", file_path="photo.jpg", media_type=MediaType.IMAGE)
        assert m.is_image()
        assert not m.is_gif()
        assert not m.is_video()

    def test_is_gif(self):
        m = MediaItem(media_id="m1", file_path="anim.gif", media_type=MediaType.GIF)
        assert m.is_gif()

    def test_is_video(self):
        m = MediaItem(media_id="m1", file_path="clip.mp4", media_type=MediaType.VIDEO)
        assert m.is_video()

    def test_aspect_ratio(self):
        m = MediaItem(media_id="m1", file_path="p.jpg", width=1200, height=675)
        assert abs(m.aspect_ratio() - 1.78) < 0.01

    def test_aspect_ratio_zero_height(self):
        m = MediaItem(media_id="m1", file_path="p.jpg", width=100, height=0)
        assert m.aspect_ratio() == 0

    def test_summary(self):
        m = MediaItem(media_id="m1", file_path="p.jpg", media_type=MediaType.IMAGE,
                      size_bytes=102400, width=800, height=600, alt_text="test", tags=["product"])
        s = m.summary()
        assert s["type"] == "image"
        assert "800x600" in s["dimensions"]
        assert "product" in s["tags"]

    def test_summary_truncates_alt_text(self):
        m = MediaItem(media_id="m1", file_path="p.jpg", alt_text="x" * 100)
        s = m.summary()
        assert s["alt_text"].endswith("...")


# ──── MediaValidator ────

class TestMediaValidator:
    def test_detect_type_jpg(self):
        assert MediaValidator.detect_type("photo.jpg") == MediaType.IMAGE

    def test_detect_type_png(self):
        assert MediaValidator.detect_type("img.PNG") == MediaType.IMAGE

    def test_detect_type_gif(self):
        assert MediaValidator.detect_type("anim.gif") == MediaType.GIF

    def test_detect_type_mp4(self):
        assert MediaValidator.detect_type("video.mp4") == MediaType.VIDEO

    def test_detect_type_unknown(self):
        assert MediaValidator.detect_type("data.csv") == MediaType.UNKNOWN

    def test_validate_good_image(self):
        m = MediaItem(media_id="m1", file_path="photo.jpg", media_type=MediaType.IMAGE,
                      size_bytes=1024 * 1024, width=1200, height=675)
        valid, issues = MediaValidator.validate(m)
        assert valid
        assert len(issues) == 0

    def test_validate_oversized_image(self):
        m = MediaItem(media_id="m1", file_path="big.jpg", media_type=MediaType.IMAGE,
                      size_bytes=10 * 1024 * 1024)
        valid, issues = MediaValidator.validate(m)
        assert not valid
        assert any("too large" in i.lower() for i in issues)

    def test_validate_wrong_format(self):
        m = MediaItem(media_id="m1", file_path="photo.bmp", media_type=MediaType.IMAGE,
                      size_bytes=1024)
        valid, issues = MediaValidator.validate(m)
        assert not valid

    def test_validate_oversized_dimensions(self):
        m = MediaItem(media_id="m1", file_path="huge.jpg", media_type=MediaType.IMAGE,
                      size_bytes=1024, width=5000, height=5000)
        valid, issues = MediaValidator.validate(m)
        assert not valid

    def test_validate_video_too_long(self):
        m = MediaItem(media_id="m1", file_path="long.mp4", media_type=MediaType.VIDEO,
                      size_bytes=1024, duration_sec=200)
        valid, issues = MediaValidator.validate(m)
        assert not valid
        assert any("duration" in i.lower() for i in issues)

    def test_validate_video_too_short(self):
        m = MediaItem(media_id="m1", file_path="short.mp4", media_type=MediaType.VIDEO,
                      size_bytes=1024, duration_sec=0.1)
        valid, issues = MediaValidator.validate(m)
        assert not valid

    def test_validate_unknown_type(self):
        m = MediaItem(media_id="m1", file_path="data.csv", media_type=MediaType.UNKNOWN)
        valid, issues = MediaValidator.validate(m)
        assert not valid

    def test_suggest_optimization_resize(self):
        m = MediaItem(media_id="m1", file_path="big.jpg", media_type=MediaType.IMAGE,
                      width=3000, height=2000, size_bytes=2 * 1024 * 1024)
        suggestions = MediaValidator.suggest_optimization(m)
        assert any("resize" in s.lower() for s in suggestions)

    def test_suggest_optimization_no_alt_text(self):
        m = MediaItem(media_id="m1", file_path="p.jpg", media_type=MediaType.IMAGE,
                      width=800, height=600, size_bytes=500000)
        suggestions = MediaValidator.suggest_optimization(m)
        assert any("alt text" in s.lower() for s in suggestions)

    def test_suggest_optimization_aspect_ratio(self):
        m = MediaItem(media_id="m1", file_path="p.jpg", media_type=MediaType.IMAGE,
                      width=800, height=800, size_bytes=500000)
        suggestions = MediaValidator.suggest_optimization(m)
        assert any("16:9" in s for s in suggestions)

    def test_suggest_compress(self):
        m = MediaItem(media_id="m1", file_path="p.jpg", media_type=MediaType.IMAGE,
                      width=1000, height=600, size_bytes=2 * 1024 * 1024)
        suggestions = MediaValidator.suggest_optimization(m)
        assert any("compress" in s.lower() for s in suggestions)


# ──── AltTextGenerator ────

class TestAltTextGenerator:
    def test_generate_product(self):
        text = AltTextGenerator.generate("product", product_name="Laptop", color="silver",
                                          material="aluminum", angle="front")
        assert "Laptop" in text
        assert "silver" in text

    def test_generate_default(self):
        text = AltTextGenerator.generate("nonexistent", topic="AI")
        assert "AI" in text

    def test_from_filename(self):
        text = AltTextGenerator.from_filename("product_photo_red.jpg")
        assert "product" in text.lower()

    def test_from_filename_single_word(self):
        text = AltTextGenerator.from_filename("photo.jpg")
        assert "photo" in text.lower()

    def test_list_templates(self):
        templates = AltTextGenerator.list_templates()
        assert "product" in templates
        assert "default" in templates
        assert len(templates) >= 5


# ──── MediaLibrary ────

class TestMediaLibrary:
    def _lib(self) -> MediaLibrary:
        return MediaLibrary(db_path=":memory:")

    def _item(self, mid="m1", **kw) -> MediaItem:
        defaults = {"media_id": mid, "file_path": f"{mid}.jpg", "media_type": MediaType.IMAGE}
        defaults.update(kw)
        return MediaItem(**defaults)

    def test_add(self):
        lib = self._lib()
        assert lib.add(self._item())

    def test_add_duplicate_fails(self):
        lib = self._lib()
        lib.add(self._item())
        assert not lib.add(self._item())

    def test_get(self):
        lib = self._lib()
        lib.add(self._item("m1"))
        assert lib.get("m1").media_id == "m1"

    def test_get_nonexistent(self):
        lib = self._lib()
        assert lib.get("nope") is None

    def test_remove(self):
        lib = self._lib()
        lib.add(self._item("m1"))
        assert lib.remove("m1")
        assert lib.get("m1") is None

    def test_remove_nonexistent(self):
        lib = self._lib()
        assert not lib.remove("nope")

    def test_search_by_tag(self):
        lib = self._lib()
        item = self._item("m1")
        item.tags = ["product", "sale"]
        lib.add(item)
        lib.add(self._item("m2"))
        assert len(lib.search_by_tag("product")) == 1

    def test_search_by_type(self):
        lib = self._lib()
        lib.add(self._item("m1", media_type=MediaType.IMAGE))
        lib.add(self._item("m2", media_type=MediaType.GIF, file_path="m2.gif"))
        assert len(lib.search_by_type(MediaType.IMAGE)) == 1

    def test_search_by_status(self):
        lib = self._lib()
        item = self._item("m1")
        item.status = MediaStatus.UPLOADED
        lib.add(item)
        lib.add(self._item("m2"))
        assert len(lib.search_by_status(MediaStatus.UPLOADED)) == 1

    def test_record_and_get_usage(self):
        lib = self._lib()
        lib.add(self._item("m1"))
        lib.record_usage("m1", "tweet1", "acc1")
        assert lib.get_usage_count("m1") == 1

    def test_get_unused(self):
        lib = self._lib()
        lib.add(self._item("m1"))
        lib.add(self._item("m2"))
        lib.record_usage("m1", "t1")
        unused = lib.get_unused()
        assert len(unused) == 1
        assert unused[0].media_id == "m2"

    def test_stats(self):
        lib = self._lib()
        lib.add(self._item("m1", size_bytes=1000, alt_text="yes"))
        lib.add(self._item("m2", size_bytes=2000))
        stats = lib.get_stats()
        assert stats["total_items"] == 2
        assert stats["with_alt_text"] == 1
        assert stats["without_alt_text"] == 1

    def test_find_duplicates(self):
        lib = self._lib()
        i1 = self._item("m1")
        i1.checksum = "abc123"
        i2 = self._item("m2")
        i2.checksum = "abc123"
        i3 = self._item("m3")
        i3.checksum = "def456"
        lib.add(i1)
        lib.add(i2)
        lib.add(i3)
        dupes = lib.find_duplicates()
        assert len(dupes) == 1
        assert set(dupes[0]) == {"m1", "m2"}


# ──── UploadQueue ────

class TestUploadQueue:
    def test_enqueue_dequeue(self):
        q = UploadQueue()
        item = MediaItem(media_id="m1", file_path="p.jpg")
        q.enqueue(item)
        assert q.pending_count() == 1
        result = q.dequeue()
        assert result.media_id == "m1"

    def test_dequeue_empty(self):
        q = UploadQueue()
        assert q.dequeue() is None

    def test_mark_uploaded(self):
        q = UploadQueue()
        item = MediaItem(media_id="m1", file_path="p.jpg")
        q.mark_uploaded(item, "twid_123")
        assert item.status == MediaStatus.UPLOADED
        assert item.twitter_media_id == "twid_123"

    def test_mark_failed_retries(self):
        q = UploadQueue(max_retries=3)
        item = MediaItem(media_id="m1", file_path="p.jpg")
        q.mark_failed(item)  # retry 1
        assert item.upload_retries == 1
        assert q.pending_count() == 1  # re-queued

    def test_mark_failed_exhausted(self):
        q = UploadQueue(max_retries=2)
        item = MediaItem(media_id="m1", file_path="p.jpg")
        q.mark_failed(item)  # retry 1
        q.dequeue()
        q.mark_failed(item)  # retry 2, exhausted
        assert item.status == MediaStatus.FAILED
        assert len(q.failed) == 1

    def test_stats(self):
        q = UploadQueue()
        q.enqueue(MediaItem(media_id="m1", file_path="p.jpg"))
        q.mark_uploaded(MediaItem(media_id="m2", file_path="p2.jpg"), "tw2")
        s = q.stats()
        assert s["pending"] == 1
        assert s["uploaded"] == 1


# ──── WatermarkEngine ────

class TestWatermarkEngine:
    def test_config(self):
        w = WatermarkEngine(text="@mybot", position="bottom-right")
        cfg = w.get_config()
        assert cfg["text"] == "@mybot"
        assert cfg["enabled"]

    def test_disabled_when_no_text(self):
        w = WatermarkEngine()
        assert not w.enabled

    def test_validate_position(self):
        w = WatermarkEngine(text="x", position="bottom-right")
        assert w.validate_position()
        w2 = WatermarkEngine(text="x", position="invalid")
        assert not w2.validate_position()

    def test_should_apply_image(self):
        w = WatermarkEngine(text="@bot")
        item = MediaItem(media_id="m1", file_path="p.jpg", media_type=MediaType.IMAGE)
        assert w.should_apply(item)

    def test_should_not_apply_video(self):
        w = WatermarkEngine(text="@bot")
        item = MediaItem(media_id="m1", file_path="v.mp4", media_type=MediaType.VIDEO)
        assert not w.should_apply(item)

    def test_should_not_apply_disabled(self):
        w = WatermarkEngine()
        item = MediaItem(media_id="m1", file_path="p.jpg", media_type=MediaType.IMAGE)
        assert not w.should_apply(item)
