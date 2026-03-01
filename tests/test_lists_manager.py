"""Tests for bot/lists_manager.py"""

import time
import pytest
from bot.lists_manager import ListsManager, ListMember, ListConfig, ListStats
from bot.database import Database
import tempfile, os


@pytest.fixture
def db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    d = Database(path)
    yield d
    os.unlink(path)


@pytest.fixture
def manager(db):
    return ListsManager(db)


class TestListsManager:
    def test_create_list(self, manager):
        lid = manager.create_list("AI Researchers", description="Top AI people")
        assert lid.startswith("list_")
        lists = manager.get_lists()
        assert len(lists) == 1
        assert lists[0]["name"] == "AI Researchers"

    def test_delete_list(self, manager):
        lid = manager.create_list("Test")
        assert manager.delete_list(lid) is True
        assert manager.get_lists() == []

    def test_delete_nonexistent(self, manager):
        assert manager.delete_list("fake_id") is False

    def test_add_member(self, manager):
        lid = manager.create_list("Test")
        member = ListMember(user_id="u1", username="alice", followers_count=1000)
        assert manager.add_member(lid, member) is True
        members = manager.get_members(lid)
        assert len(members) == 1
        assert members[0]["username"] == "alice"

    def test_remove_member(self, manager):
        lid = manager.create_list("Test")
        manager.add_member(lid, ListMember(user_id="u1", username="alice"))
        assert manager.remove_member(lid, "u1") is True
        assert manager.get_members(lid) == []

    def test_remove_nonexistent_member(self, manager):
        lid = manager.create_list("Test")
        assert manager.remove_member(lid, "fake") is False

    def test_list_stats(self, manager):
        lid = manager.create_list("Test")
        for i in range(5):
            manager.add_member(lid, ListMember(
                user_id=f"u{i}", username=f"user{i}",
                followers_count=1000 * (i + 1),
                engagement_rate=0.05 * (i + 1),
            ))
        stats = manager.get_list_stats(lid)
        assert stats is not None
        assert stats.member_count == 5
        assert stats.avg_followers == 3000.0
        assert len(stats.top_members) == 5

    def test_list_stats_empty(self, manager):
        lid = manager.create_list("Empty")
        stats = manager.get_list_stats(lid)
        assert stats.member_count == 0

    def test_stats_nonexistent(self, manager):
        assert manager.get_list_stats("fake") is None

    def test_find_overlap(self, manager):
        lid1 = manager.create_list("List A")
        lid2 = manager.create_list("List B")
        manager.add_member(lid1, ListMember(user_id="u1", username="alice"))
        manager.add_member(lid1, ListMember(user_id="u2", username="bob"))
        manager.add_member(lid2, ListMember(user_id="u2", username="bob"))
        manager.add_member(lid2, ListMember(user_id="u3", username="carol"))
        overlap = manager.find_overlap(lid1, lid2)
        assert overlap == ["u2"]

    def test_export_import(self, manager):
        lid1 = manager.create_list("Source")
        manager.add_member(lid1, ListMember(user_id="u1", username="alice", followers_count=500))
        manager.add_member(lid1, ListMember(user_id="u2", username="bob", followers_count=1000))

        exported = manager.export_members(lid1)
        assert len(exported) == 2

        lid2 = manager.create_list("Target")
        imported = manager.import_members(lid2, exported)
        assert imported == 2
        assert len(manager.get_members(lid2)) == 2

    def test_sort_members(self, manager):
        lid = manager.create_list("Test")
        manager.add_member(lid, ListMember(user_id="u1", username="alice", followers_count=100))
        manager.add_member(lid, ListMember(user_id="u2", username="bob", followers_count=5000))
        members = manager.get_members(lid, sort_by="followers_count")
        assert members[0]["username"] == "bob"

    def test_snapshot(self, manager):
        lid = manager.create_list("Test")
        manager.add_member(lid, ListMember(user_id="u1", username="alice"))
        manager.snapshot(lid)  # Should not raise

    def test_member_follow_ratio(self):
        m = ListMember(user_id="u1", username="alice", followers_count=1000, following_count=200)
        assert m.follow_ratio == 5.0

    def test_list_config(self):
        cfg = ListConfig(name="AI", keywords=["AI", "ML"], min_followers=100)
        lid_id = "test"
        assert cfg.name == "AI"
        assert cfg.min_followers == 100
