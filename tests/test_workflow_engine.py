"""
tests/test_workflow_engine.py - 工作流引擎测试
"""

import os
import pytest
import json
from bot.workflow_engine import (
    WorkflowEngine, Workflow, WorkflowNode, ConditionEvaluator,
    NodeType, ActionType, ConditionOp, WorkflowStatus, RunStatus,
)

TEST_DB = "/tmp/test_workflow_engine.db"


@pytest.fixture(autouse=True)
def cleanup():
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


@pytest.fixture
def engine():
    return WorkflowEngine(db_path=TEST_DB)


# ── WorkflowNode Tests ──────────────────────────────

class TestWorkflowNode:
    def test_create(self):
        node = WorkflowNode(name="Test", node_type="action")
        assert node.name == "Test"
        assert node.node_id != ""

    def test_to_dict(self):
        node = WorkflowNode(name="Test", node_type="condition")
        d = node.to_dict()
        assert d["name"] == "Test"
        assert d["node_type"] == "condition"

    def test_from_dict(self):
        data = {"name": "From Dict", "node_type": "action", "node_id": "abc123"}
        node = WorkflowNode.from_dict(data)
        assert node.name == "From Dict"
        assert node.node_id == "abc123"

    def test_default_values(self):
        node = WorkflowNode()
        assert node.timeout_seconds == 30
        assert node.retry_count == 0
        assert node.next_nodes == []

    def test_auto_id(self):
        n1 = WorkflowNode()
        n2 = WorkflowNode()
        assert n1.node_id != n2.node_id


# ── Workflow Tests ───────────────────────────────────

class TestWorkflow:
    def test_create(self):
        wf = Workflow(name="Test Workflow")
        assert wf.name == "Test Workflow"
        assert wf.workflow_id != ""
        assert wf.status == "draft"

    def test_to_dict(self):
        wf = Workflow(name="Test", description="A test workflow")
        d = wf.to_dict()
        assert d["name"] == "Test"
        assert "created_at" in d

    def test_from_dict(self):
        data = {"name": "From Dict", "trigger_type": "schedule"}
        wf = Workflow.from_dict(data)
        assert wf.name == "From Dict"
        assert wf.trigger_type == "schedule"


# ── ConditionEvaluator Tests ────────────────────────

class TestConditionEvaluator:
    def test_eq(self):
        assert ConditionEvaluator.evaluate(5, "eq", 5)
        assert not ConditionEvaluator.evaluate(5, "eq", 6)

    def test_ne(self):
        assert ConditionEvaluator.evaluate(5, "ne", 6)
        assert not ConditionEvaluator.evaluate(5, "ne", 5)

    def test_gt(self):
        assert ConditionEvaluator.evaluate(10, "gt", 5)
        assert not ConditionEvaluator.evaluate(5, "gt", 10)

    def test_gte(self):
        assert ConditionEvaluator.evaluate(10, "gte", 10)
        assert ConditionEvaluator.evaluate(11, "gte", 10)

    def test_lt(self):
        assert ConditionEvaluator.evaluate(5, "lt", 10)
        assert not ConditionEvaluator.evaluate(10, "lt", 5)

    def test_lte(self):
        assert ConditionEvaluator.evaluate(10, "lte", 10)
        assert ConditionEvaluator.evaluate(9, "lte", 10)

    def test_contains(self):
        assert ConditionEvaluator.evaluate("hello world", "contains", "world")
        assert not ConditionEvaluator.evaluate("hello", "contains", "world")

    def test_not_contains(self):
        assert ConditionEvaluator.evaluate("hello", "not_contains", "world")
        assert not ConditionEvaluator.evaluate("hello world", "not_contains", "world")

    def test_matches_regex(self):
        assert ConditionEvaluator.evaluate("test123", "matches", r"\d+")
        assert not ConditionEvaluator.evaluate("test", "matches", r"^\d+$")

    def test_in_list(self):
        assert ConditionEvaluator.evaluate("a", "in", ["a", "b", "c"])
        assert not ConditionEvaluator.evaluate("d", "in", ["a", "b", "c"])

    def test_not_in_list(self):
        assert ConditionEvaluator.evaluate("d", "not_in", ["a", "b", "c"])

    def test_exists(self):
        assert ConditionEvaluator.evaluate("something", "exists", None)
        assert not ConditionEvaluator.evaluate(None, "exists", None)

    def test_invalid_op(self):
        assert not ConditionEvaluator.evaluate(1, "invalid_op", 1)

    def test_type_error(self):
        assert not ConditionEvaluator.evaluate("abc", "gt", "def")

    def test_evaluate_multi_and(self):
        conditions = [
            {"field": "age", "op": "gt", "value": 18},
            {"field": "name", "op": "eq", "value": "Alice"},
        ]
        context = {"age": 25, "name": "Alice"}
        assert ConditionEvaluator.evaluate_multi(conditions, context, "and")

    def test_evaluate_multi_and_fail(self):
        conditions = [
            {"field": "age", "op": "gt", "value": 18},
            {"field": "name", "op": "eq", "value": "Bob"},
        ]
        context = {"age": 25, "name": "Alice"}
        assert not ConditionEvaluator.evaluate_multi(conditions, context, "and")

    def test_evaluate_multi_or(self):
        conditions = [
            {"field": "age", "op": "gt", "value": 30},
            {"field": "name", "op": "eq", "value": "Alice"},
        ]
        context = {"age": 25, "name": "Alice"}
        assert ConditionEvaluator.evaluate_multi(conditions, context, "or")

    def test_evaluate_multi_nested_field(self):
        conditions = [
            {"field": "user.profile.followers", "op": "gt", "value": 1000},
        ]
        context = {"user": {"profile": {"followers": 5000}}}
        assert ConditionEvaluator.evaluate_multi(conditions, context)

    def test_evaluate_multi_missing_field(self):
        conditions = [
            {"field": "nonexistent.field", "op": "eq", "value": "x"},
        ]
        assert not ConditionEvaluator.evaluate_multi(conditions, {})


# ── WorkflowEngine CRUD Tests ───────────────────────

class TestWorkflowEngineCRUD:
    def test_create_workflow(self, engine):
        wf = engine.create_workflow("Test WF", description="A test")
        assert wf.name == "Test WF"
        assert wf.workflow_id != ""
        assert wf.status == "draft"

    def test_get_workflow(self, engine):
        wf = engine.create_workflow("Get Test")
        fetched = engine.get_workflow(wf.workflow_id)
        assert fetched is not None
        assert fetched["name"] == "Get Test"

    def test_get_nonexistent(self, engine):
        assert engine.get_workflow("nonexistent") is None

    def test_list_workflows(self, engine):
        engine.create_workflow("WF 1")
        engine.create_workflow("WF 2")
        results = engine.list_workflows()
        assert len(results) >= 2

    def test_list_by_status(self, engine):
        wf = engine.create_workflow("Active WF")
        engine.activate_workflow(wf.workflow_id)
        results = engine.list_workflows(status="active")
        assert len(results) >= 1

    def test_list_by_tag(self, engine):
        engine.create_workflow("Tagged WF", tags=["engagement"])
        results = engine.list_workflows(tag="engagement")
        assert len(results) >= 1

    def test_update_workflow(self, engine):
        wf = engine.create_workflow("Original")
        result = engine.update_workflow(wf.workflow_id, name="Updated")
        assert result
        fetched = engine.get_workflow(wf.workflow_id)
        assert fetched["name"] == "Updated"

    def test_update_nonexistent(self, engine):
        assert not engine.update_workflow("fake", name="X")

    def test_update_no_fields(self, engine):
        wf = engine.create_workflow("Test")
        assert not engine.update_workflow(wf.workflow_id)

    def test_delete_workflow(self, engine):
        wf = engine.create_workflow("Delete Me")
        assert engine.delete_workflow(wf.workflow_id)
        assert engine.get_workflow(wf.workflow_id) is None

    def test_delete_nonexistent(self, engine):
        assert not engine.delete_workflow("fake")

    def test_activate_workflow(self, engine):
        wf = engine.create_workflow("To Activate")
        assert engine.activate_workflow(wf.workflow_id)
        fetched = engine.get_workflow(wf.workflow_id)
        assert fetched["status"] == "active"

    def test_pause_workflow(self, engine):
        wf = engine.create_workflow("To Pause")
        engine.activate_workflow(wf.workflow_id)
        assert engine.pause_workflow(wf.workflow_id)
        fetched = engine.get_workflow(wf.workflow_id)
        assert fetched["status"] == "paused"


# ── Execution Tests ──────────────────────────────────

class TestWorkflowExecution:
    def _create_simple_workflow(self, engine):
        nodes = [
            {"node_id": "start", "node_type": "action", "name": "Log start",
             "config": {"action_type": "log", "message": "Started"},
             "next_nodes": ["end"]},
            {"node_id": "end", "node_type": "end", "name": "Done"},
        ]
        wf = engine.create_workflow("Simple", nodes=nodes)
        engine.activate_workflow(wf.workflow_id)
        return wf

    def test_start_run(self, engine):
        wf = self._create_simple_workflow(engine)
        run_id = engine.start_run(wf.workflow_id)
        assert run_id is not None

    def test_start_run_nonexistent(self, engine):
        assert engine.start_run("fake") is None

    def test_execute_simple(self, engine):
        wf = self._create_simple_workflow(engine)
        run_id = engine.start_run(wf.workflow_id)
        result = engine.execute_run(run_id)
        assert result["status"] == "completed"
        assert result["steps"] >= 1

    def test_execute_with_condition_true(self, engine):
        nodes = [
            {"node_id": "check", "node_type": "condition", "name": "Check value",
             "config": {"conditions": [{"field": "score", "op": "gt", "value": 50}]},
             "on_true": "good", "on_false": "bad"},
            {"node_id": "good", "node_type": "action", "name": "Good path",
             "config": {"action_type": "log", "message": "Score is good!"},
             "next_nodes": ["end"]},
            {"node_id": "bad", "node_type": "action", "name": "Bad path",
             "config": {"action_type": "log", "message": "Score is bad"},
             "next_nodes": ["end"]},
            {"node_id": "end", "node_type": "end", "name": "Done"},
        ]
        wf = engine.create_workflow("Condition Test", nodes=nodes,
                                     variables={"score": 80})
        engine.activate_workflow(wf.workflow_id)
        run_id = engine.start_run(wf.workflow_id)
        result = engine.execute_run(run_id)
        assert result["status"] == "completed"
        # Should have taken the "good" path
        node_ids = [s["node_id"] for s in result["log"]]
        assert "good" in node_ids

    def test_execute_with_condition_false(self, engine):
        nodes = [
            {"node_id": "check", "node_type": "condition",
             "config": {"conditions": [{"field": "score", "op": "gt", "value": 50}]},
             "on_true": "good", "on_false": "bad"},
            {"node_id": "good", "node_type": "action",
             "config": {"action_type": "log", "message": "Good"},
             "next_nodes": ["end"]},
            {"node_id": "bad", "node_type": "action",
             "config": {"action_type": "log", "message": "Bad"},
             "next_nodes": ["end"]},
            {"node_id": "end", "node_type": "end"},
        ]
        wf = engine.create_workflow("Condition False", nodes=nodes,
                                     variables={"score": 30})
        engine.activate_workflow(wf.workflow_id)
        run_id = engine.start_run(wf.workflow_id)
        result = engine.execute_run(run_id)
        assert result["status"] == "completed"
        node_ids = [s["node_id"] for s in result["log"]]
        assert "bad" in node_ids

    def test_execute_with_branch(self, engine):
        nodes = [
            {"node_id": "branch", "node_type": "branch", "name": "Brancher",
             "config": {
                 "branches": [
                     {"name": "high", "conditions": [{"field": "level", "op": "eq", "value": "high"}],
                      "target_node": "action_high"},
                     {"name": "low", "conditions": [{"field": "level", "op": "eq", "value": "low"}],
                      "target_node": "action_low"},
                 ],
                 "default_node": "action_default",
             }},
            {"node_id": "action_high", "node_type": "action",
             "config": {"action_type": "log", "message": "High level"},
             "next_nodes": ["end"]},
            {"node_id": "action_low", "node_type": "action",
             "config": {"action_type": "log", "message": "Low level"},
             "next_nodes": ["end"]},
            {"node_id": "action_default", "node_type": "action",
             "config": {"action_type": "log", "message": "Default"},
             "next_nodes": ["end"]},
            {"node_id": "end", "node_type": "end"},
        ]
        wf = engine.create_workflow("Branch Test", nodes=nodes,
                                     variables={"level": "high"})
        engine.activate_workflow(wf.workflow_id)
        run_id = engine.start_run(wf.workflow_id)
        result = engine.execute_run(run_id)
        assert result["status"] == "completed"
        node_ids = [s["node_id"] for s in result["log"]]
        assert "action_high" in node_ids

    def test_execute_with_delay(self, engine):
        nodes = [
            {"node_id": "delay", "node_type": "delay",
             "config": {"seconds": 5},
             "next_nodes": ["action"]},
            {"node_id": "action", "node_type": "action",
             "config": {"action_type": "log", "message": "After delay"},
             "next_nodes": ["end"]},
            {"node_id": "end", "node_type": "end"},
        ]
        wf = engine.create_workflow("Delay Test", nodes=nodes)
        engine.activate_workflow(wf.workflow_id)
        run_id = engine.start_run(wf.workflow_id)
        result = engine.execute_run(run_id)
        assert result["status"] == "completed"

    def test_execute_update_var(self, engine):
        nodes = [
            {"node_id": "set", "node_type": "action",
             "config": {"action_type": "update_var", "variable": "greeting", "value": "Hello!"},
             "next_nodes": ["log"]},
            {"node_id": "log", "node_type": "action",
             "config": {"action_type": "log", "message": "Greeting: {{greeting}}"},
             "next_nodes": ["end"]},
            {"node_id": "end", "node_type": "end"},
        ]
        wf = engine.create_workflow("Var Test", nodes=nodes)
        engine.activate_workflow(wf.workflow_id)
        run_id = engine.start_run(wf.workflow_id)
        result = engine.execute_run(run_id)
        assert result["status"] == "completed"
        assert result["context"].get("greeting") == "Hello!"

    def test_execute_with_trigger_data(self, engine):
        nodes = [
            {"node_id": "check", "node_type": "condition",
             "config": {"conditions": [{"field": "trigger.type", "op": "eq", "value": "mention"}]},
             "on_true": "act", "on_false": "end"},
            {"node_id": "act", "node_type": "action",
             "config": {"action_type": "like"}, "next_nodes": ["end"]},
            {"node_id": "end", "node_type": "end"},
        ]
        wf = engine.create_workflow("Trigger Test", nodes=nodes)
        engine.activate_workflow(wf.workflow_id)
        run_id = engine.start_run(wf.workflow_id, trigger_data={"type": "mention"})
        result = engine.execute_run(run_id)
        assert result["status"] == "completed"
        node_ids = [s["node_id"] for s in result["log"]]
        assert "act" in node_ids

    def test_execute_tweet_action(self, engine):
        nodes = [
            {"node_id": "tweet", "node_type": "action",
             "config": {"action_type": "tweet", "text": "Hello Twitter!"},
             "next_nodes": ["end"]},
            {"node_id": "end", "node_type": "end"},
        ]
        wf = engine.create_workflow("Tweet Test", nodes=nodes)
        engine.activate_workflow(wf.workflow_id)
        run_id = engine.start_run(wf.workflow_id)
        result = engine.execute_run(run_id)
        assert result["status"] == "completed"

    def test_execute_empty_nodes(self, engine):
        wf = engine.create_workflow("Empty", nodes=[])
        engine.activate_workflow(wf.workflow_id)
        run_id = engine.start_run(wf.workflow_id)
        result = engine.execute_run(run_id)
        assert result["status"] == "completed"

    def test_execute_nonexistent_run(self, engine):
        result = engine.execute_run("fake_run")
        assert result["status"] == "error"

    def test_custom_action_handler(self):
        custom_results = []

        def my_handler(config, context):
            custom_results.append(config)
            return {"custom": True}

        eng = WorkflowEngine(db_path=TEST_DB, action_handlers={"custom_action": my_handler})
        nodes = [
            {"node_id": "custom", "node_type": "action",
             "config": {"action_type": "custom_action", "data": "test"},
             "next_nodes": ["end"]},
            {"node_id": "end", "node_type": "end"},
        ]
        wf = eng.create_workflow("Custom Action", nodes=nodes)
        eng.activate_workflow(wf.workflow_id)
        run_id = eng.start_run(wf.workflow_id)
        result = eng.execute_run(run_id)
        assert result["status"] == "completed"
        assert len(custom_results) == 1

    def test_execute_with_loop(self, engine):
        nodes = [
            {"node_id": "loop", "node_type": "loop",
             "config": {"variable": "items", "item_var": "current", "body_node": "action"},
             "next_nodes": ["end"]},
            {"node_id": "action", "node_type": "action",
             "config": {"action_type": "log", "message": "Processing item"},
             "next_nodes": ["loop"]},  # Loop back
            {"node_id": "end", "node_type": "end"},
        ]
        wf = engine.create_workflow("Loop Test", nodes=nodes,
                                     variables={"items": ["a", "b", "c"]})
        engine.activate_workflow(wf.workflow_id)
        run_id = engine.start_run(wf.workflow_id)
        result = engine.execute_run(run_id)
        assert result["status"] == "completed"
        # Should have processed 3 items
        action_steps = [s for s in result["log"] if s.get("node_id") == "action"]
        assert len(action_steps) == 3


# ── Run Management Tests ─────────────────────────────

class TestRunManagement:
    def test_get_run(self, engine):
        wf = engine.create_workflow("Run Test", nodes=[
            {"node_id": "end", "node_type": "end"}
        ])
        engine.activate_workflow(wf.workflow_id)
        run_id = engine.start_run(wf.workflow_id)
        run = engine.get_run(run_id)
        assert run is not None
        assert run["workflow_id"] == wf.workflow_id

    def test_get_nonexistent_run(self, engine):
        assert engine.get_run("fake") is None

    def test_list_runs(self, engine):
        wf = engine.create_workflow("List Runs", nodes=[
            {"node_id": "end", "node_type": "end"}
        ])
        engine.activate_workflow(wf.workflow_id)
        engine.start_run(wf.workflow_id)
        engine.start_run(wf.workflow_id)
        runs = engine.list_runs(workflow_id=wf.workflow_id)
        assert len(runs) >= 2

    def test_list_runs_by_status(self, engine):
        wf = engine.create_workflow("Status Runs", nodes=[
            {"node_id": "end", "node_type": "end"}
        ])
        engine.activate_workflow(wf.workflow_id)
        run_id = engine.start_run(wf.workflow_id)
        runs = engine.list_runs(status="running")
        assert len(runs) >= 1

    def test_cancel_run(self, engine):
        wf = engine.create_workflow("Cancel Test", nodes=[
            {"node_id": "end", "node_type": "end"}
        ])
        engine.activate_workflow(wf.workflow_id)
        run_id = engine.start_run(wf.workflow_id)
        assert engine.cancel_run(run_id)
        run = engine.get_run(run_id)
        assert run["status"] == "cancelled"

    def test_cancel_nonexistent(self, engine):
        assert not engine.cancel_run("fake")

    def test_rate_limit(self, engine):
        wf = engine.create_workflow("Rate Limit", nodes=[
            {"node_id": "end", "node_type": "end"}
        ], max_runs_per_hour=2)
        engine.activate_workflow(wf.workflow_id)
        engine.start_run(wf.workflow_id)
        engine.start_run(wf.workflow_id)
        # Third should fail
        result = engine.start_run(wf.workflow_id)
        assert result is None


# ── Audit Log Tests ──────────────────────────────────

class TestAuditLog:
    def test_audit_log_created(self, engine):
        wf = engine.create_workflow("Audit Test")
        logs = engine.get_audit_log(workflow_id=wf.workflow_id)
        assert len(logs) >= 1
        assert logs[0]["event_type"] == "workflow_created"

    def test_audit_log_run(self, engine):
        wf = engine.create_workflow("Audit Run", nodes=[
            {"node_id": "end", "node_type": "end"}
        ])
        engine.activate_workflow(wf.workflow_id)
        run_id = engine.start_run(wf.workflow_id)
        engine.execute_run(run_id)
        logs = engine.get_audit_log(run_id=run_id)
        assert len(logs) >= 1

    def test_audit_log_all(self, engine):
        engine.create_workflow("A1")
        engine.create_workflow("A2")
        logs = engine.get_audit_log()
        assert len(logs) >= 2


# ── Stats Tests ──────────────────────────────────────

class TestWorkflowStats:
    def test_stats_empty(self, engine):
        stats = engine.stats()
        assert stats["total_runs"] == 0
        assert stats["total_workflows"] == 0

    def test_stats_with_data(self, engine):
        wf = engine.create_workflow("Stats WF", nodes=[
            {"node_id": "end", "node_type": "end"}
        ])
        engine.activate_workflow(wf.workflow_id)
        engine.start_run(wf.workflow_id)
        stats = engine.stats()
        assert stats["total_runs"] >= 1
        assert stats["total_workflows"] >= 1
        assert stats["active_workflows"] >= 1

    def test_stats_per_workflow(self, engine):
        wf = engine.create_workflow("Specific Stats", nodes=[
            {"node_id": "end", "node_type": "end"}
        ])
        engine.activate_workflow(wf.workflow_id)
        run_id = engine.start_run(wf.workflow_id)
        engine.execute_run(run_id)
        stats = engine.stats(workflow_id=wf.workflow_id)
        assert stats["total_runs"] >= 1


# ── Template Tests ───────────────────────────────────

class TestWorkflowTemplates:
    def test_welcome_dm_template(self):
        t = WorkflowEngine.template_welcome_dm()
        assert t["name"] == "Welcome New Follower"
        assert len(t["nodes"]) >= 3

    def test_engagement_boost_template(self):
        t = WorkflowEngine.template_engagement_boost()
        assert t["name"] == "Engagement Boost"
        assert len(t["nodes"]) >= 4

    def test_content_pipeline_template(self):
        t = WorkflowEngine.template_content_pipeline()
        assert t["name"] == "Content Pipeline"
        assert t["trigger_type"] == "schedule"

    def test_create_from_template(self, engine):
        t = WorkflowEngine.template_welcome_dm()
        wf = engine.create_workflow(**t)
        assert wf.name == "Welcome New Follower"
        fetched = engine.get_workflow(wf.workflow_id)
        assert len(fetched["nodes"]) >= 3

    def test_execute_welcome_template(self, engine):
        t = WorkflowEngine.template_welcome_dm()
        wf = engine.create_workflow(**t)
        engine.activate_workflow(wf.workflow_id)
        run_id = engine.start_run(wf.workflow_id,
                                   trigger_data={"follower_count": 5000})
        result = engine.execute_run(run_id)
        assert result["status"] == "completed"
        # Should have taken the VIP path
        node_ids = [s["node_id"] for s in result["log"]]
        assert "dm_vip" in node_ids

    def test_execute_welcome_regular(self, engine):
        t = WorkflowEngine.template_welcome_dm()
        wf = engine.create_workflow(**t)
        engine.activate_workflow(wf.workflow_id)
        run_id = engine.start_run(wf.workflow_id,
                                   trigger_data={"follower_count": 50})
        result = engine.execute_run(run_id)
        assert result["status"] == "completed"
        node_ids = [s["node_id"] for s in result["log"]]
        assert "dm_regular" in node_ids
