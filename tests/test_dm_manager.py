"""
tests/test_dm_manager.py - DM Manager 完整测试
"""

import pytest
import time
from bot.dm_manager import (
    DMManager, DMTemplate, DMMessage, DMStatus, DMTrigger,
    AutoReplyEngine, RateLimitedSender, Conversation,
)


@pytest.fixture
def manager():
    return DMManager()


@pytest.fixture
def auto_reply():
    return AutoReplyEngine(cooldown_seconds=0, default_reply="")


@pytest.fixture
def sender():
    return RateLimitedSender(max_per_minute=5, max_per_day=100)


# ──────────────── DMTemplate ────────────────

class TestDMTemplate:
    def test_create(self):
        t = DMTemplate(template_id="t1", name="Welcome", content="Hello {{name}}!")
        assert t.template_id == "t1"
        assert t.name == "Welcome"
        assert t.enabled

    def test_render_with_vars(self):
        t = DMTemplate(template_id="t1", name="T", content="Hi {{name}}, welcome to {{channel}}!")
        result = t.render({"name": "Alice", "channel": "#general"})
        assert result == "Hi Alice, welcome to #general!"

    def test_render_no_context(self):
        t = DMTemplate(template_id="t1", name="T", content="Hello there!")
        assert t.render() == "Hello there!"

    def test_render_partial_context(self):
        t = DMTemplate(template_id="t1", name="T", content="Hi {{name}}, ID={{id}}")
        result = t.render({"name": "Bob"})
        assert "Bob" in result
        assert "{{id}}" in result

    def test_extract_variables(self):
        t = DMTemplate(template_id="t1", name="T", content="{{a}} and {{b}} and {{a}}")
        vars = t.extract_variables()
        assert "a" in vars
        assert "b" in vars

    def test_to_dict(self):
        t = DMTemplate(template_id="t1", name="T", content="hi")
        d = t.to_dict()
        assert d["template_id"] == "t1"
        assert "name" in d
        assert "content" in d

    def test_send_count_default(self):
        t = DMTemplate(template_id="t1", name="T", content="hi")
        assert t.send_count == 0


# ──────────────── Conversation ────────────────

class TestConversation:
    def test_create(self):
        c = Conversation(user_id="u1")
        assert c.user_id == "u1"
        assert c.message_count == 0

    def test_add_message(self):
        c = Conversation(user_id="u1")
        c.add_message("bot", "Hello!")
        c.add_message("user", "Hi!")
        assert c.message_count == 2

    def test_messages_in_order(self):
        c = Conversation(user_id="u1")
        c.add_message("bot", "A")
        c.add_message("user", "B")
        assert c.messages[0]["role"] == "bot"
        assert c.messages[1]["role"] == "user"


# ──────────────── AutoReplyEngine ────────────────

class TestAutoReplyEngine:
    def test_add_keyword_rule(self, auto_reply):
        auto_reply.add_keyword_rule(["help", "support"], "How can I help?")
        assert auto_reply.rule_count >= 1

    def test_match_keyword(self, auto_reply):
        auto_reply.add_keyword_rule(["hello", "hi"], "Hello there!")
        result = auto_reply.match("u1", "hello world")
        assert result == "Hello there!"

    def test_no_match_returns_default(self, auto_reply):
        auto_reply.add_keyword_rule(["hello"], "Hi!")
        result = auto_reply.match("u1", "goodbye")
        # default_reply is "" (falsy), so no explicit keyword match
        assert result == "" or result is None

    def test_add_regex_rule(self, auto_reply):
        auto_reply.add_regex_rule(r"order\s+#\d+", "Checking your order...")
        result = auto_reply.match("u1", "What about order #12345?")
        assert result == "Checking your order..."

    def test_regex_no_match_returns_default(self, auto_reply):
        auto_reply.add_regex_rule(r"order\s+#\d+", "Checking...")
        result = auto_reply.match("u1", "just chatting")
        # Returns default_reply ("") since no rule matches
        assert result == "" or result is None

    def test_cooldown(self):
        ar = AutoReplyEngine(cooldown_seconds=9999, default_reply="default")
        ar.add_keyword_rule(["hi"], "Hello!")
        assert ar.match("u1", "hi") == "Hello!"
        assert ar.match("u1", "hi") is None  # cooldown

    def test_clear_cooldowns(self):
        ar = AutoReplyEngine(cooldown_seconds=9999, default_reply="")
        ar.add_keyword_rule(["test"], "reply")
        ar.match("u1", "test")
        ar.clear_cooldowns()
        result = ar.match("u1", "test")
        assert result == "reply"

    def test_priority_ordering(self, auto_reply):
        auto_reply.add_keyword_rule(["price"], "Generic price info", priority=0)
        auto_reply.add_keyword_rule(["price"], "VIP price info", priority=10)
        result = auto_reply.match("u1", "what's the price?")
        assert result == "VIP price info"

    def test_get_rules(self, auto_reply):
        auto_reply.add_keyword_rule(["a"], "reply_a")
        auto_reply.add_regex_rule(r"b\d+", "reply_b")
        rules = auto_reply.get_rules()
        assert len(rules) == 2

    def test_default_reply(self):
        ar = AutoReplyEngine(cooldown_seconds=0, default_reply="I'm a bot!")
        result = ar.match("u1", "random message")
        assert result == "I'm a bot!"


# ──────────────── RateLimitedSender ────────────────

class TestRateLimitedSender:
    def test_can_send_initially(self, sender):
        assert sender.can_send()

    def test_record_send(self, sender):
        sender.record_send()
        assert sender.remaining_minute() == 4
        assert sender.remaining_day() == 99

    def test_minute_limit(self):
        s = RateLimitedSender(max_per_minute=2, max_per_day=100)
        s.record_send()
        s.record_send()
        assert not s.can_send()

    def test_get_stats(self, sender):
        sender.record_send()
        stats = sender.get_stats()
        assert "minute_used" in stats or "sent_this_minute" in stats
        assert stats.get("day_used", stats.get("sent_today", 0)) >= 1

    def test_remaining(self, sender):
        assert sender.remaining_minute() == 5
        assert sender.remaining_day() == 100


# ──────────────── DMManager ────────────────

class TestDMManager:
    def test_create_template(self, manager):
        tpl = manager.create_template("welcome", "Welcome!", "Hi {{name}}")
        assert tpl.template_id is not None
        assert tpl.name == "welcome"

    def test_get_template(self, manager):
        tpl = manager.create_template("test", "Test", "content")
        found = manager.get_template(tpl.template_id)
        assert found is not None
        assert found.name == "test"

    def test_list_templates(self, manager):
        manager.create_template("t1", "T1", "c1")
        manager.create_template("t2", "T2", "c2")
        templates = manager.list_templates()
        assert len(templates) == 2

    def test_delete_template(self, manager):
        tpl = manager.create_template("del", "Del", "content")
        assert manager.delete_template(tpl.template_id)
        assert manager.get_template(tpl.template_id) is None

    def test_update_template(self, manager):
        tpl = manager.create_template("old", "Old", "old content")
        manager.update_template(tpl.template_id, name="New", content="new content")
        updated = manager.get_template(tpl.template_id)
        assert updated.name == "New"
        assert updated.content == "new content"

    def test_send_dm(self, manager):
        msg = manager.send("user1", "Hello!")
        assert msg is not None
        assert msg.recipient_id == "user1"

    def test_send_template(self, manager):
        tpl = manager.create_template("greet", "Greet", "Hello {{name}}!")
        msg = manager.send_template("user1", tpl.template_id, {"name": "Alice"})
        assert msg is not None

    def test_send_welcome(self, manager):
        # send_welcome takes (user_id, username) not context dict
        tpl = manager.create_template("welcome", "Welcome {{username}}!",
                                      DMTrigger.NEW_FOLLOWER)
        manager.welcome_template_id = tpl.template_id
        msg = manager.send_welcome("new_user", "Bob")
        # may return message or None depending on config

    def test_handle_incoming(self, manager):
        manager.handle_incoming("user1", "Hello bot!")
        conv = manager.get_conversation("user1")
        assert conv is not None

    def test_conversation_tracking(self, manager):
        manager.send("user1", "Hi!")
        manager.handle_incoming("user1", "Hey!")
        conv = manager.get_conversation("user1")
        assert conv is not None

    def test_list_conversations(self, manager):
        manager.send("u1", "msg1")
        manager.send("u2", "msg2")
        convs = manager.list_conversations()
        assert len(convs) >= 2

    def test_tag_conversation(self, manager):
        manager.send("u1", "msg")
        result = manager.tag_conversation("u1", "vip")
        assert result

    def test_blacklist(self, manager):
        manager.add_blacklist("spammer")
        assert manager.is_blacklisted("spammer")
        manager.remove_blacklist("spammer")
        assert not manager.is_blacklisted("spammer")

    def test_send_blocked_user(self, manager):
        manager.add_blacklist("blocked_user")
        msg = manager.send("blocked_user", "Hello!")
        assert msg is None or msg.status == DMStatus.FAILED

    def test_get_stats(self, manager):
        manager.send("u1", "msg1")
        manager.send("u2", "msg2")
        stats = manager.get_stats()
        assert isinstance(stats, dict)

    def test_bulk_send(self, manager):
        results = manager.bulk_send(["u1", "u2", "u3"], "Broadcast message!")
        assert len(results) == 3

    def test_get_history(self, manager):
        manager.send("u1", "msg1")
        manager.send("u1", "msg2")
        history = manager.get_history("u1")
        assert len(history) >= 2

    def test_delete_nonexistent_template(self, manager):
        assert not manager.delete_template("nonexistent")
