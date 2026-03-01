"""Tests for Tweet Queue"""
import os
import tempfile
import time
import pytest
from bot.tweet_queue import (
    Priority, QueueItemStatus, QueueItem, DeduplicationEngine, TweetQueue,
)


@pytest.fixture
def queue():
    q = TweetQueue(db_path=":memory:")
    yield q
    q.close()


class TestPriority:
    def test_values(self):
        assert Priority.URGENT.value == 0
        assert Priority.HIGH.value == 1
        assert Priority.NORMAL.value == 2
        assert Priority.LOW.value == 3

    def test_ordering(self):
        assert Priority.URGENT.value < Priority.HIGH.value < Priority.NORMAL.value < Priority.LOW.value


class TestQueueItemStatus:
    def test_all_statuses(self):
        statuses = [s.value for s in QueueItemStatus]
        assert "pending" in statuses
        assert "completed" in statuses
        assert "failed" in statuses
        assert "blocked" in statuses


class TestQueueItem:
    def test_create(self):
        item = QueueItem(id="t1", content="Hello world")
        assert item.id == "t1"
        assert item.content == "Hello world"
        assert item.priority == Priority.NORMAL
        assert item.status == QueueItemStatus.PENDING
        assert item.retry_count == 0

    def test_priority_from_int(self):
        item = QueueItem(id="t1", content="X", priority=0)
        assert item.priority == Priority.URGENT

    def test_status_from_string(self):
        item = QueueItem(id="t1", content="X", status="completed")
        assert item.status == QueueItemStatus.COMPLETED

    def test_defaults(self):
        item = QueueItem(id="t1", content="X")
        assert item.depends_on == []
        assert item.tags == []
        assert item.max_retries == 3
        assert item.metadata == {}


class TestDeduplicationEngine:
    def test_jaccard_identical(self):
        d = DeduplicationEngine()
        assert d.jaccard_similarity("hello world", "hello world") == 1.0

    def test_jaccard_different(self):
        d = DeduplicationEngine()
        sim = d.jaccard_similarity("hello world", "foo bar baz")
        assert sim == 0.0

    def test_jaccard_partial(self):
        d = DeduplicationEngine()
        sim = d.jaccard_similarity("hello world foo", "hello world bar")
        assert 0 < sim < 1

    def test_jaccard_empty(self):
        d = DeduplicationEngine()
        assert d.jaccard_similarity("", "") == 1.0

    def test_edit_distance_same(self):
        d = DeduplicationEngine()
        assert d.edit_distance("hello", "hello") == 0

    def test_edit_distance_different(self):
        d = DeduplicationEngine()
        assert d.edit_distance("cat", "dog") == 3

    def test_edit_distance_empty(self):
        d = DeduplicationEngine()
        assert d.edit_distance("hello", "") == 5
        assert d.edit_distance("", "hello") == 5

    def test_edit_distance_insert(self):
        d = DeduplicationEngine()
        assert d.edit_distance("cat", "cats") == 1

    def test_normalized_edit_same(self):
        d = DeduplicationEngine()
        assert d.normalized_edit_similarity("hello", "hello") == 1.0

    def test_normalized_edit_empty(self):
        d = DeduplicationEngine()
        assert d.normalized_edit_similarity("", "") == 1.0

    def test_is_duplicate_true(self):
        d = DeduplicationEngine(similarity_threshold=0.8)
        existing = ["Hello world, this is a test tweet"]
        is_dup, score = d.is_duplicate("Hello world, this is a test tweet", existing)
        assert is_dup is True
        assert score > 0.8

    def test_is_duplicate_false(self):
        d = DeduplicationEngine(similarity_threshold=0.8)
        existing = ["The weather is nice today"]
        is_dup, score = d.is_duplicate("Python programming tutorial", existing)
        assert is_dup is False

    def test_fingerprint(self):
        d = DeduplicationEngine()
        d.add_fingerprint("t1", "test content")
        assert d.get_fingerprint("t1") is not None
        d.remove_fingerprint("t1")
        assert d.get_fingerprint("t1") is None

    def test_is_duplicate_edit_distance(self):
        d = DeduplicationEngine(similarity_threshold=0.8)
        existing = ["Hello world test"]
        is_dup, score = d.is_duplicate("Hello world tests", existing)
        assert is_dup is True


class TestTweetQueue:
    def test_enqueue(self, queue):
        item = queue.enqueue("Test tweet #1")
        assert item is not None
        assert item.content == "Test tweet #1"
        assert item.status == QueueItemStatus.PENDING

    def test_enqueue_with_priority(self, queue):
        item = queue.enqueue("Urgent!", priority=Priority.URGENT)
        assert item.priority == Priority.URGENT

    def test_enqueue_duplicate_rejected(self, queue):
        queue.enqueue("Same content here")
        dup = queue.enqueue("Same content here")
        assert dup is None

    def test_enqueue_skip_dedup(self, queue):
        queue.enqueue("Same content")
        item = queue.enqueue("Same content", check_duplicate=False)
        assert item is not None

    def test_dequeue_priority_order(self, queue):
        queue.enqueue("Low priority", priority=Priority.LOW, item_id="low")
        queue.enqueue("High priority", priority=Priority.HIGH, item_id="high")
        queue.enqueue("Urgent", priority=Priority.URGENT, item_id="urg")
        item = queue.dequeue()
        assert item.priority == Priority.URGENT

    def test_dequeue_empty(self, queue):
        assert queue.dequeue() is None

    def test_dequeue_marks_processing(self, queue):
        queue.enqueue("Test", item_id="t1")
        item = queue.dequeue()
        assert item.status == QueueItemStatus.PROCESSING
        db_item = queue.get_item("t1")
        assert db_item.status == QueueItemStatus.PROCESSING

    def test_complete(self, queue):
        queue.enqueue("Test", item_id="t1")
        queue.dequeue()
        queue.complete("t1")
        item = queue.get_item("t1")
        assert item.status == QueueItemStatus.COMPLETED

    def test_fail_with_retry(self, queue):
        queue.enqueue("Test", item_id="t1")
        queue.dequeue()
        queue.fail("t1", error="API error")
        item = queue.get_item("t1")
        assert item.status == QueueItemStatus.PENDING
        assert item.retry_count == 1
        assert item.error_message == "API error"

    def test_fail_max_retries(self, queue):
        item = queue.enqueue("Test", item_id="t1")
        for _ in range(4):
            queue.dequeue()
            queue.fail("t1", error="err")
        item = queue.get_item("t1")
        assert item.status == QueueItemStatus.FAILED

    def test_peek(self, queue):
        queue.enqueue("A", item_id="a")
        queue.enqueue("B", item_id="b")
        queue.enqueue("C", item_id="c")
        items = queue.peek(2)
        assert len(items) == 2

    def test_cancel(self, queue):
        queue.enqueue("Cancel me", item_id="c1")
        queue.cancel("c1")
        item = queue.get_item("c1")
        assert item.status == QueueItemStatus.CANCELLED

    def test_bulk_cancel(self, queue):
        queue.enqueue("T1", tags=["campaign1"], item_id="t1")
        queue.enqueue("T2", tags=["campaign1"], item_id="t2")
        queue.enqueue("T3", tags=["campaign2"], item_id="t3")
        count = queue.bulk_cancel("campaign1")
        assert count == 2

    def test_reschedule(self, queue):
        queue.enqueue("Test", item_id="t1")
        queue.reschedule("t1", "2026-12-31T00:00:00Z")
        item = queue.get_item("t1")
        assert "2026-12-31" in item.scheduled_at

    def test_retry_failed(self, queue):
        queue.enqueue("T1", item_id="t1")
        queue.dequeue()
        queue.fail("t1")
        queue.dequeue()
        queue.fail("t1")
        queue.dequeue()
        queue.fail("t1")
        queue.dequeue()
        queue.fail("t1")  # max retries exceeded
        count = queue.retry_failed()
        assert count == 0  # all maxed out

    def test_get_queue_stats(self, queue):
        queue.enqueue("A", item_id="a")
        queue.enqueue("B", item_id="b", priority=Priority.HIGH)
        queue.enqueue("C", item_id="c")
        queue.dequeue()
        stats = queue.get_queue_stats()
        assert stats["total"] == 3
        assert stats["active"] == 2

    def test_schedule_conflicts(self, queue):
        queue.enqueue("T1", item_id="t1", scheduled_at="2026-03-01T10:00:00+00:00")
        queue.enqueue("T2", item_id="t2", scheduled_at="2026-03-01T10:02:00+00:00")
        queue.enqueue("T3", item_id="t3", scheduled_at="2026-03-01T15:00:00+00:00")
        conflicts = queue.get_schedule_conflicts(window_minutes=5)
        assert len(conflicts) >= 1

    def test_posting_window(self, queue):
        queue.set_posting_window(9, 17)
        assert queue._posting_window == (9, 17, "UTC")
        queue.clear_posting_window()
        assert queue._posting_window is None

    def test_get_item(self, queue):
        queue.enqueue("Find me", item_id="find")
        item = queue.get_item("find")
        assert item.content == "Find me"

    def test_get_nonexistent(self, queue):
        assert queue.get_item("nope") is None

    def test_dependencies_blocked(self, queue):
        queue.enqueue("First", item_id="first")
        item = queue.enqueue("Second", depends_on=["first"], item_id="second")
        assert item.status == QueueItemStatus.BLOCKED

    def test_dependencies_resolve(self, queue):
        queue.enqueue("First", item_id="first")
        queue.enqueue("Second", depends_on=["first"], item_id="second")
        # Complete first
        d = queue.dequeue()
        assert d.id == "first"
        queue.complete("first")
        # Now second should be available
        d2 = queue.dequeue()
        assert d2 is not None
        assert d2.id == "second"

    def test_enqueue_custom_id(self, queue):
        item = queue.enqueue("Custom", item_id="myid123")
        assert item.id == "myid123"

    def test_metadata(self, queue):
        item = QueueItem(id="m1", content="meta", metadata={"campaign": "spring2026"})
        assert item.metadata["campaign"] == "spring2026"
