"""Tests for DM Funnel Engine"""

import json
import pytest
from bot.dm_funnel import (
    FunnelStep, StepType, Trigger, TriggerType, Funnel, FunnelStatus,
    UserState, ConditionOp, TemplateEngine, ConditionEvaluator,
    DMFunnelEngine,
)


# ─── FunnelStep ───

class TestFunnelStep:
    def test_create_default(self):
        step = FunnelStep()
        assert step.id
        assert step.step_type == StepType.MESSAGE
        assert step.content == ""

    def test_create_message(self):
        step = FunnelStep(step_type=StepType.MESSAGE, content="Hello {{name}}!")
        assert step.content == "Hello {{name}}!"

    def test_create_delay(self):
        step = FunnelStep(step_type=StepType.DELAY, delay_seconds=3600)
        assert step.delay_seconds == 3600

    def test_create_condition(self):
        step = FunnelStep(
            step_type=StepType.CONDITION,
            condition_field="followers",
            condition_op=ConditionOp.GT,
            condition_value=1000,
            next_step_true="s2",
            next_step_false="s3",
        )
        assert step.condition_field == "followers"
        assert step.next_step_true == "s2"

    def test_to_dict(self):
        step = FunnelStep(step_type=StepType.TAG, tags_to_add=["vip"])
        d = step.to_dict()
        assert d["step_type"] == "tag"
        assert d["tags_to_add"] == ["vip"]

    def test_from_dict(self):
        d = {"id": "s1", "step_type": "message", "content": "Hi"}
        step = FunnelStep.from_dict(d)
        assert step.id == "s1"
        assert step.step_type == StepType.MESSAGE

    def test_roundtrip(self):
        step = FunnelStep(step_type=StepType.CONDITION, condition_op=ConditionOp.CONTAINS)
        d = step.to_dict()
        step2 = FunnelStep.from_dict(d)
        assert step2.step_type == step.step_type
        assert step2.condition_op == step.condition_op


# ─── Trigger ───

class TestTrigger:
    def test_create(self):
        t = Trigger(trigger_type=TriggerType.NEW_FOLLOWER)
        assert t.enabled is True
        assert t.max_fires_per_user == 1

    def test_keyword_trigger(self):
        t = Trigger(trigger_type=TriggerType.KEYWORD, keywords=["help", "pricing"])
        assert len(t.keywords) == 2

    def test_to_dict(self):
        t = Trigger(trigger_type=TriggerType.MENTION)
        d = t.to_dict()
        assert d["trigger_type"] == "mention"
        assert d["enabled"] is True

    def test_from_dict(self):
        d = {"trigger_type": "reply", "keywords": ["test"], "cooldown_seconds": 60}
        t = Trigger.from_dict(d)
        assert t.trigger_type == TriggerType.REPLY
        assert t.cooldown_seconds == 60


# ─── Funnel ───

class TestFunnel:
    def test_create(self):
        f = Funnel(name="Welcome Funnel")
        assert f.name == "Welcome Funnel"
        assert f.status == FunnelStatus.DRAFT
        assert f.steps == {}

    def test_add_step(self):
        f = Funnel(name="Test")
        step = FunnelStep(content="Hello")
        sid = f.add_step(step)
        assert sid == step.id
        assert f.entry_step_id == step.id

    def test_add_multiple_steps(self):
        f = Funnel(name="Test")
        s1 = FunnelStep(content="Step 1")
        s2 = FunnelStep(content="Step 2")
        f.add_step(s1)
        f.add_step(s2)
        assert f.entry_step_id == s1.id  # First step is entry
        assert len(f.steps) == 2

    def test_remove_step(self):
        f = Funnel(name="Test")
        s1 = FunnelStep(content="S1")
        f.add_step(s1)
        assert f.remove_step(s1.id) is True
        assert len(f.steps) == 0
        assert f.entry_step_id is None

    def test_remove_nonexistent_step(self):
        f = Funnel()
        assert f.remove_step("nope") is False

    def test_get_step(self):
        f = Funnel()
        s1 = FunnelStep(content="Test")
        f.add_step(s1)
        assert f.get_step(s1.id) == s1
        assert f.get_step("nope") is None

    def test_add_trigger(self):
        f = Funnel()
        t = Trigger(trigger_type=TriggerType.NEW_FOLLOWER)
        tid = f.add_trigger(t)
        assert tid == t.id
        assert len(f.triggers) == 1

    def test_chain_steps(self):
        f = Funnel()
        s1 = FunnelStep(content="1")
        s2 = FunnelStep(content="2")
        s3 = FunnelStep(content="3")
        f.add_step(s1)
        f.add_step(s2)
        f.add_step(s3)
        f.chain_steps([s1.id, s2.id, s3.id])
        assert s1.next_step == s2.id
        assert s2.next_step == s3.id
        assert s3.next_step is None
        assert f.entry_step_id == s1.id

    def test_to_dict(self):
        f = Funnel(name="Export Test")
        s = FunnelStep(content="Hi")
        f.add_step(s)
        d = f.to_dict()
        assert d["name"] == "Export Test"
        assert s.id in d["steps"]

    def test_from_dict(self):
        d = {
            "id": "f1",
            "name": "Imported",
            "status": "active",
            "triggers": [{"trigger_type": "new_follower"}],
            "steps": {"s1": {"id": "s1", "step_type": "message", "content": "Hi"}},
            "entry_step_id": "s1",
        }
        f = Funnel.from_dict(d)
        assert f.name == "Imported"
        assert f.status == FunnelStatus.ACTIVE
        assert len(f.triggers) == 1


# ─── UserState ───

class TestUserState:
    def test_create(self):
        us = UserState(user_id="u1", funnel_id="f1")
        assert us.status == "active"
        assert us.messages_sent == 0
        assert isinstance(us.tags, set)

    def test_to_dict(self):
        us = UserState(user_id="u1", funnel_id="f1")
        us.tags.add("vip")
        d = us.to_dict()
        assert d["user_id"] == "u1"
        assert "vip" in d["tags"]


# ─── TemplateEngine ───

class TestTemplateEngine:
    def test_render_simple(self):
        result = TemplateEngine.render("Hello {{name}}!", {"name": "Alice"})
        assert result == "Hello Alice!"

    def test_render_multiple_vars(self):
        result = TemplateEngine.render(
            "{{greeting}} {{name}}, welcome to {{product}}!",
            {"greeting": "Hi", "name": "Bob", "product": "TweetBot"},
        )
        assert result == "Hi Bob, welcome to TweetBot!"

    def test_render_no_vars(self):
        result = TemplateEngine.render("No variables here", {})
        assert result == "No variables here"

    def test_render_missing_var(self):
        result = TemplateEngine.render("Hello {{name}}!", {})
        assert result == "Hello {{name}}!"  # Unreplaced

    def test_extract_variables(self):
        vars_found = TemplateEngine.extract_variables("{{a}} and {{b}} and {{c}}")
        assert set(vars_found) == {"a", "b", "c"}

    def test_extract_no_variables(self):
        assert TemplateEngine.extract_variables("no vars") == []


# ─── ConditionEvaluator ───

class TestConditionEvaluator:
    def test_equals(self):
        assert ConditionEvaluator.evaluate("hello", ConditionOp.EQUALS, "hello") is True
        assert ConditionEvaluator.evaluate("hello", ConditionOp.EQUALS, "world") is False

    def test_contains(self):
        assert ConditionEvaluator.evaluate("hello world", ConditionOp.CONTAINS, "world") is True
        assert ConditionEvaluator.evaluate("hello", ConditionOp.CONTAINS, "xyz") is False
        assert ConditionEvaluator.evaluate(None, ConditionOp.CONTAINS, "x") is False

    def test_gt(self):
        assert ConditionEvaluator.evaluate(100, ConditionOp.GT, 50) is True
        assert ConditionEvaluator.evaluate(10, ConditionOp.GT, 50) is False
        assert ConditionEvaluator.evaluate("abc", ConditionOp.GT, 50) is False

    def test_lt(self):
        assert ConditionEvaluator.evaluate(10, ConditionOp.LT, 50) is True
        assert ConditionEvaluator.evaluate(100, ConditionOp.LT, 50) is False

    def test_in(self):
        assert ConditionEvaluator.evaluate("a", ConditionOp.IN, ["a", "b", "c"]) is True
        assert ConditionEvaluator.evaluate("x", ConditionOp.IN, ["a", "b"]) is False

    def test_not_in(self):
        assert ConditionEvaluator.evaluate("x", ConditionOp.NOT_IN, ["a", "b"]) is True
        assert ConditionEvaluator.evaluate("a", ConditionOp.NOT_IN, ["a", "b"]) is False

    def test_regex(self):
        assert ConditionEvaluator.evaluate("hello123", ConditionOp.REGEX, r"\d+") is True
        assert ConditionEvaluator.evaluate("hello", ConditionOp.REGEX, r"\d+") is False

    def test_regex_invalid(self):
        assert ConditionEvaluator.evaluate("test", ConditionOp.REGEX, "[invalid") is False

    def test_exists(self):
        assert ConditionEvaluator.evaluate("something", ConditionOp.EXISTS, None) is True
        assert ConditionEvaluator.evaluate(None, ConditionOp.EXISTS, None) is False


# ─── DMFunnelEngine ───

class TestDMFunnelEngine:
    def setup_method(self):
        self.engine = DMFunnelEngine()

    def _create_welcome_funnel(self) -> Funnel:
        funnel = self.engine.create_funnel("Welcome", "New follower welcome sequence")
        s1 = FunnelStep(step_type=StepType.MESSAGE, content="Welcome {{user_id}}!")
        s2 = FunnelStep(step_type=StepType.DELAY, delay_seconds=3600)
        s3 = FunnelStep(step_type=StepType.MESSAGE, content="Did you check our product?")
        funnel.add_step(s1)
        funnel.add_step(s2)
        funnel.add_step(s3)
        funnel.chain_steps([s1.id, s2.id, s3.id])
        trigger = Trigger(trigger_type=TriggerType.NEW_FOLLOWER)
        funnel.add_trigger(trigger)
        self.engine.update_funnel_status(funnel.id, FunnelStatus.ACTIVE)
        return funnel

    # CRUD
    def test_create_funnel(self):
        f = self.engine.create_funnel("Test")
        assert f.id
        assert f.name == "Test"

    def test_get_funnel(self):
        f = self.engine.create_funnel("Test")
        assert self.engine.get_funnel(f.id) == f

    def test_get_missing_funnel(self):
        assert self.engine.get_funnel("nope") is None

    def test_list_funnels(self):
        self.engine.create_funnel("A")
        self.engine.create_funnel("B")
        assert len(self.engine.list_funnels()) == 2

    def test_list_funnels_filter_status(self):
        f1 = self.engine.create_funnel("Active")
        self.engine.update_funnel_status(f1.id, FunnelStatus.ACTIVE)
        self.engine.create_funnel("Draft")
        assert len(self.engine.list_funnels(status=FunnelStatus.ACTIVE)) == 1

    def test_update_status(self):
        f = self.engine.create_funnel("T")
        assert self.engine.update_funnel_status(f.id, FunnelStatus.ACTIVE) is True
        assert f.status == FunnelStatus.ACTIVE

    def test_update_status_missing(self):
        assert self.engine.update_funnel_status("nope", FunnelStatus.ACTIVE) is False

    def test_delete_funnel(self):
        f = self.engine.create_funnel("T")
        assert self.engine.delete_funnel(f.id) is True
        assert self.engine.get_funnel(f.id) is None

    def test_delete_missing(self):
        assert self.engine.delete_funnel("nope") is False

    # Enter funnel
    def test_enter_funnel(self):
        funnel = self._create_welcome_funnel()
        state = self.engine.enter_funnel(funnel.id, "user1")
        assert state is not None
        assert state.user_id == "user1"
        assert state.current_step_id == funnel.entry_step_id

    def test_enter_inactive_funnel(self):
        f = self.engine.create_funnel("Draft")
        assert self.engine.enter_funnel(f.id, "u1") is None

    def test_enter_funnel_twice(self):
        funnel = self._create_welcome_funnel()
        s1 = self.engine.enter_funnel(funnel.id, "u1")
        s2 = self.engine.enter_funnel(funnel.id, "u1")
        assert s1 == s2  # Same state returned

    def test_enter_with_variables(self):
        funnel = self._create_welcome_funnel()
        state = self.engine.enter_funnel(funnel.id, "u1", {"name": "Alice"})
        assert state.variables["name"] == "Alice"

    # User state
    def test_get_user_state(self):
        funnel = self._create_welcome_funnel()
        self.engine.enter_funnel(funnel.id, "u1")
        state = self.engine.get_user_state(funnel.id, "u1")
        assert state is not None
        assert state.user_id == "u1"

    def test_get_missing_user_state(self):
        assert self.engine.get_user_state("f1", "u999") is None

    def test_list_users_in_funnel(self):
        funnel = self._create_welcome_funnel()
        self.engine.enter_funnel(funnel.id, "u1")
        self.engine.enter_funnel(funnel.id, "u2")
        users = self.engine.list_users_in_funnel(funnel.id)
        assert len(users) == 2

    def test_opt_out(self):
        funnel = self._create_welcome_funnel()
        self.engine.enter_funnel(funnel.id, "u1")
        assert self.engine.opt_out_user(funnel.id, "u1") is True
        state = self.engine.get_user_state(funnel.id, "u1")
        assert state.status == "opted_out"

    def test_opt_out_missing(self):
        assert self.engine.opt_out_user("f1", "u999") is False

    # Triggers
    def test_check_triggers(self):
        funnel = self._create_welcome_funnel()
        triggered = self.engine.check_triggers(TriggerType.NEW_FOLLOWER, "u1")
        assert len(triggered) == 1
        assert triggered[0].id == funnel.id

    def test_check_triggers_max_fires(self):
        funnel = self._create_welcome_funnel()
        self.engine.check_triggers(TriggerType.NEW_FOLLOWER, "u1")
        # Second fire should be blocked (max_fires_per_user=1)
        triggered = self.engine.check_triggers(TriggerType.NEW_FOLLOWER, "u1")
        assert len(triggered) == 0

    def test_check_triggers_keyword(self):
        funnel = self.engine.create_funnel("Keyword Funnel")
        trigger = Trigger(trigger_type=TriggerType.KEYWORD, keywords=["pricing", "cost"])
        funnel.add_trigger(trigger)
        self.engine.update_funnel_status(funnel.id, FunnelStatus.ACTIVE)

        triggered = self.engine.check_triggers(TriggerType.KEYWORD, "u1", {"text": "What's your pricing?"})
        assert len(triggered) == 1

    def test_check_triggers_keyword_no_match(self):
        funnel = self.engine.create_funnel("Keyword Funnel")
        trigger = Trigger(trigger_type=TriggerType.KEYWORD, keywords=["pricing"])
        funnel.add_trigger(trigger)
        self.engine.update_funnel_status(funnel.id, FunnelStatus.ACTIVE)

        triggered = self.engine.check_triggers(TriggerType.KEYWORD, "u1", {"text": "Hello there"})
        assert len(triggered) == 0

    def test_check_triggers_inactive_funnel(self):
        funnel = self.engine.create_funnel("Inactive")
        funnel.add_trigger(Trigger(trigger_type=TriggerType.NEW_FOLLOWER))
        # Status is still DRAFT
        triggered = self.engine.check_triggers(TriggerType.NEW_FOLLOWER, "u1")
        assert len(triggered) == 0

    # Step execution
    def test_execute_message_step(self):
        funnel = self._create_welcome_funnel()
        self.engine.enter_funnel(funnel.id, "u1")
        result = self.engine.execute_step(funnel.id, "u1")
        assert result["action"] == "message"
        assert "u1" in result["message"]

    def test_execute_delay_step(self):
        funnel = self._create_welcome_funnel()
        self.engine.enter_funnel(funnel.id, "u1")
        self.engine.execute_step(funnel.id, "u1")  # Message step
        result = self.engine.execute_step(funnel.id, "u1")  # Delay step
        assert result["action"] == "delay"
        assert result["delay_seconds"] == 3600
        assert "scheduled_at" in result

    def test_execute_condition_step(self):
        funnel = self.engine.create_funnel("Condition Test")
        s_cond = FunnelStep(
            step_type=StepType.CONDITION,
            condition_field="followers",
            condition_op=ConditionOp.GT,
            condition_value=1000,
            next_step_true="yes_id",
            next_step_false="no_id",
        )
        s_yes = FunnelStep(id="yes_id", step_type=StepType.MESSAGE, content="VIP!")
        s_no = FunnelStep(id="no_id", step_type=StepType.MESSAGE, content="Welcome!")
        funnel.add_step(s_cond)
        funnel.add_step(s_yes)
        funnel.add_step(s_no)
        funnel.entry_step_id = s_cond.id
        self.engine.update_funnel_status(funnel.id, FunnelStatus.ACTIVE)

        # Test condition met
        self.engine.enter_funnel(funnel.id, "u1", {"followers": 5000})
        result = self.engine.execute_step(funnel.id, "u1")
        assert result["condition_met"] is True
        state = self.engine.get_user_state(funnel.id, "u1")
        assert state.current_step_id == "yes_id"

    def test_execute_tag_step(self):
        funnel = self.engine.create_funnel("Tag Test")
        step = FunnelStep(step_type=StepType.TAG, tags_to_add=["vip", "premium"], tags_to_remove=["trial"])
        funnel.add_step(step)
        self.engine.update_funnel_status(funnel.id, FunnelStatus.ACTIVE)
        self.engine.enter_funnel(funnel.id, "u1")
        state = self.engine.get_user_state(funnel.id, "u1")
        state.tags.add("trial")

        result = self.engine.execute_step(funnel.id, "u1")
        assert "vip" in result["tags"]
        assert "premium" in result["tags"]
        assert "trial" not in result["tags"]

    def test_execute_completes_funnel(self):
        funnel = self.engine.create_funnel("Short")
        step = FunnelStep(step_type=StepType.MESSAGE, content="Only step")
        funnel.add_step(step)
        self.engine.update_funnel_status(funnel.id, FunnelStatus.ACTIVE)
        self.engine.enter_funnel(funnel.id, "u1")
        self.engine.execute_step(funnel.id, "u1")
        state = self.engine.get_user_state(funnel.id, "u1")
        assert state.status == "completed"

    def test_execute_inactive_user(self):
        funnel = self._create_welcome_funnel()
        assert self.engine.execute_step(funnel.id, "u999") is None

    def test_send_callback(self):
        sent = []
        self.engine.set_send_callback(lambda uid, msg: sent.append((uid, msg)))
        funnel = self._create_welcome_funnel()
        self.engine.enter_funnel(funnel.id, "u1")
        self.engine.execute_step(funnel.id, "u1")
        assert len(sent) == 1
        assert sent[0][0] == "u1"

    # User reply
    def test_process_user_reply(self):
        funnel = self._create_welcome_funnel()
        self.engine.enter_funnel(funnel.id, "u1")
        result = self.engine.process_user_reply(funnel.id, "u1", "Thanks!")
        assert result["received"] is True
        state = self.engine.get_user_state(funnel.id, "u1")
        assert state.variables["last_reply"] == "Thanks!"

    def test_process_reply_missing_user(self):
        assert self.engine.process_user_reply("f1", "u999", "Hi") is None

    # Analytics
    def test_funnel_stats(self):
        funnel = self._create_welcome_funnel()
        self.engine.enter_funnel(funnel.id, "u1")
        self.engine.enter_funnel(funnel.id, "u2")
        self.engine.execute_step(funnel.id, "u1")
        stats = self.engine.funnel_stats(funnel.id)
        assert stats["total_users"] == 2
        assert stats["messages_sent"] >= 1

    def test_funnel_stats_empty(self):
        funnel = self._create_welcome_funnel()
        stats = self.engine.funnel_stats(funnel.id)
        assert stats["total_users"] == 0
        assert stats["completion_rate"] == 0.0

    # Export/Import
    def test_export_funnel(self):
        funnel = self._create_welcome_funnel()
        data = self.engine.export_funnel(funnel.id)
        assert data is not None
        parsed = json.loads(data)
        assert parsed["name"] == "Welcome"

    def test_export_missing(self):
        assert self.engine.export_funnel("nope") is None

    def test_import_funnel(self):
        data = json.dumps({
            "id": "imported1",
            "name": "Imported Funnel",
            "status": "draft",
            "triggers": [],
            "steps": {},
        })
        funnel = self.engine.import_funnel(data)
        assert funnel is not None
        assert funnel.name == "Imported Funnel"
        assert self.engine.get_funnel("imported1") is not None

    def test_import_invalid(self):
        assert self.engine.import_funnel("{invalid json") is None

    # End-to-end
    def test_end_to_end_welcome_sequence(self):
        """完整欢迎序列: 触发→进入→执行3步"""
        sent_messages = []
        self.engine.set_send_callback(lambda uid, msg: sent_messages.append(msg))

        funnel = self._create_welcome_funnel()

        # 1. Check trigger
        triggered = self.engine.check_triggers(TriggerType.NEW_FOLLOWER, "new_user")
        assert len(triggered) == 1

        # 2. Enter funnel
        state = self.engine.enter_funnel(funnel.id, "new_user")
        assert state.status == "active"

        # 3. Execute message step
        r1 = self.engine.execute_step(funnel.id, "new_user")
        assert r1["action"] == "message"
        assert len(sent_messages) == 1

        # 4. Execute delay step
        r2 = self.engine.execute_step(funnel.id, "new_user")
        assert r2["action"] == "delay"

        # 5. Execute final message
        r3 = self.engine.execute_step(funnel.id, "new_user")
        assert r3["action"] == "message"
        assert len(sent_messages) == 2

        state = self.engine.get_user_state(funnel.id, "new_user")
        assert state.status == "completed"
        assert state.messages_sent == 2
