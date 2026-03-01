"""
Workflow Automation Engine for Twitter/X Bot
自动化工作流引擎: 条件触发 → 多步骤执行 → 结果回调

Features:
- 可视化工作流定义 (JSON/Python DSL)
- 条件节点: follower_count, engagement_rate, keyword_match, time_window, sentiment
- 动作节点: tweet, reply, retweet, like, follow, unfollow, dm, webhook, delay
- 分支/循环/并行执行
- 执行历史 + 审计日志
- 触发器: schedule, event, webhook, manual
"""

import json
import sqlite3
import uuid
import time
import threading
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, Dict, List, Any, Callable
from dataclasses import dataclass, field, asdict


class NodeType(str, Enum):
    TRIGGER = "trigger"
    CONDITION = "condition"
    ACTION = "action"
    DELAY = "delay"
    BRANCH = "branch"
    LOOP = "loop"
    END = "end"


class TriggerType(str, Enum):
    SCHEDULE = "schedule"
    EVENT = "event"
    WEBHOOK = "webhook"
    MANUAL = "manual"
    FOLLOWER_MILESTONE = "follower_milestone"
    KEYWORD = "keyword"


class ActionType(str, Enum):
    TWEET = "tweet"
    REPLY = "reply"
    RETWEET = "retweet"
    LIKE = "like"
    FOLLOW = "follow"
    UNFOLLOW = "unfollow"
    DM = "dm"
    WEBHOOK = "webhook"
    LOG = "log"
    UPDATE_VAR = "update_var"
    ADD_TO_LIST = "add_to_list"
    REMOVE_FROM_LIST = "remove_from_list"


class ConditionOp(str, Enum):
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    MATCHES = "matches"  # regex
    IN = "in"
    NOT_IN = "not_in"
    EXISTS = "exists"


class WorkflowStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    ARCHIVED = "archived"
    ERROR = "error"


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    WAITING = "waiting"  # waiting for delay/condition


@dataclass
class WorkflowNode:
    """工作流节点"""
    node_id: str = ""
    node_type: str = "action"
    name: str = ""
    config: Dict[str, Any] = field(default_factory=dict)
    next_nodes: List[str] = field(default_factory=list)  # normal flow
    on_error: str = ""  # error handler node
    on_true: str = ""   # for condition nodes
    on_false: str = ""  # for condition nodes
    timeout_seconds: int = 30
    retry_count: int = 0
    retry_delay_seconds: int = 5

    def __post_init__(self):
        if not self.node_id:
            self.node_id = str(uuid.uuid4())[:8]

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "WorkflowNode":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class Workflow:
    """工作流定义"""
    workflow_id: str = ""
    name: str = ""
    description: str = ""
    trigger_type: str = "manual"
    trigger_config: Dict[str, Any] = field(default_factory=dict)
    nodes: List[Dict[str, Any]] = field(default_factory=list)
    variables: Dict[str, Any] = field(default_factory=dict)
    status: str = "draft"
    max_runs_per_hour: int = 10
    created_at: str = ""
    updated_at: str = ""
    tags: List[str] = field(default_factory=list)

    def __post_init__(self):
        if not self.workflow_id:
            self.workflow_id = str(uuid.uuid4())
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()
        self.updated_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Workflow":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


class ConditionEvaluator:
    """条件评估器"""

    @staticmethod
    def evaluate(value: Any, op: str, target: Any) -> bool:
        """评估条件"""
        try:
            if op == ConditionOp.EQ:
                return value == target
            elif op == ConditionOp.NE:
                return value != target
            elif op == ConditionOp.GT:
                return float(value) > float(target)
            elif op == ConditionOp.GTE:
                return float(value) >= float(target)
            elif op == ConditionOp.LT:
                return float(value) < float(target)
            elif op == ConditionOp.LTE:
                return float(value) <= float(target)
            elif op == ConditionOp.CONTAINS:
                return str(target) in str(value)
            elif op == ConditionOp.NOT_CONTAINS:
                return str(target) not in str(value)
            elif op == ConditionOp.MATCHES:
                import re
                return bool(re.search(str(target), str(value)))
            elif op == ConditionOp.IN:
                if isinstance(target, (list, tuple, set)):
                    return value in target
                return str(value) in str(target)
            elif op == ConditionOp.NOT_IN:
                if isinstance(target, (list, tuple, set)):
                    return value not in target
                return str(value) not in str(target)
            elif op == ConditionOp.EXISTS:
                return value is not None
            else:
                return False
        except (ValueError, TypeError):
            return False

    @staticmethod
    def evaluate_multi(conditions: List[dict], context: dict, logic: str = "and") -> bool:
        """评估多个条件 (AND/OR)"""
        results = []
        for cond in conditions:
            field_name = cond.get("field", "")
            op = cond.get("op", "eq")
            target = cond.get("value")

            # 支持嵌套字段 (e.g., "user.followers_count")
            value = context
            for key in field_name.split("."):
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    value = None
                    break

            results.append(ConditionEvaluator.evaluate(value, op, target))

        if logic == "or":
            return any(results)
        return all(results)


class WorkflowEngine:
    """工作流执行引擎"""

    def __init__(self, db_path: str = "twitterbot.db", action_handlers: Dict[str, Callable] = None):
        self.db_path = db_path
        self.action_handlers = action_handlers or {}
        self._init_tables()
        self._running = False
        self._thread = None

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_tables(self):
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS workflows (
                workflow_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT DEFAULT '',
                trigger_type TEXT DEFAULT 'manual',
                trigger_config TEXT DEFAULT '{}',
                nodes TEXT DEFAULT '[]',
                variables TEXT DEFAULT '{}',
                status TEXT DEFAULT 'draft',
                max_runs_per_hour INTEGER DEFAULT 10,
                created_at TEXT,
                updated_at TEXT,
                tags TEXT DEFAULT '[]'
            );
            CREATE TABLE IF NOT EXISTS workflow_runs (
                run_id TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                current_node TEXT DEFAULT '',
                context TEXT DEFAULT '{}',
                started_at TEXT,
                completed_at TEXT,
                error TEXT DEFAULT '',
                steps_log TEXT DEFAULT '[]',
                trigger_data TEXT DEFAULT '{}',
                FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id)
            );
            CREATE TABLE IF NOT EXISTS workflow_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_id TEXT NOT NULL,
                run_id TEXT,
                event_type TEXT NOT NULL,
                node_id TEXT,
                details TEXT DEFAULT '',
                created_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_runs_workflow ON workflow_runs(workflow_id);
            CREATE INDEX IF NOT EXISTS idx_runs_status ON workflow_runs(status);
            CREATE INDEX IF NOT EXISTS idx_audit_workflow ON workflow_audit_log(workflow_id);
        """)
        conn.commit()
        conn.close()

    # ── Workflow CRUD ────────────────────────────────────

    def create_workflow(self, name: str, description: str = "",
                        trigger_type: str = "manual",
                        trigger_config: dict = None,
                        nodes: List[dict] = None,
                        variables: dict = None,
                        tags: List[str] = None,
                        max_runs_per_hour: int = 10) -> Workflow:
        """创建工作流"""
        wf = Workflow(
            name=name, description=description,
            trigger_type=trigger_type,
            trigger_config=trigger_config or {},
            nodes=nodes or [],
            variables=variables or {},
            tags=tags or [],
            max_runs_per_hour=max_runs_per_hour,
        )
        conn = self._get_conn()
        conn.execute(
            "INSERT INTO workflows "
            "(workflow_id, name, description, trigger_type, trigger_config, "
            "nodes, variables, status, max_runs_per_hour, created_at, updated_at, tags) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (wf.workflow_id, wf.name, wf.description, wf.trigger_type,
             json.dumps(wf.trigger_config), json.dumps(wf.nodes),
             json.dumps(wf.variables), wf.status, wf.max_runs_per_hour,
             wf.created_at, wf.updated_at, json.dumps(wf.tags)),
        )
        conn.commit()
        conn.close()
        self._audit_log(wf.workflow_id, "", "workflow_created", details=f"Created: {name}")
        return wf

    def get_workflow(self, workflow_id: str) -> Optional[dict]:
        """获取工作流"""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM workflows WHERE workflow_id=?", (workflow_id,)
        ).fetchone()
        conn.close()
        if not row:
            return None
        d = dict(row)
        for field in ("trigger_config", "nodes", "variables", "tags"):
            if isinstance(d.get(field), str):
                try:
                    d[field] = json.loads(d[field])
                except (json.JSONDecodeError, TypeError):
                    pass
        return d

    def list_workflows(self, status: str = None, tag: str = None, limit: int = 50) -> List[dict]:
        """列出工作流"""
        conn = self._get_conn()
        query = "SELECT * FROM workflows"
        params = []
        conditions = []

        if status:
            conditions.append("status=?")
            params.append(status)
        if tag:
            conditions.append("tags LIKE ?")
            params.append(f'%"{tag}"%')

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += f" ORDER BY updated_at DESC LIMIT {limit}"

        rows = conn.execute(query, params).fetchall()
        conn.close()

        results = []
        for row in rows:
            d = dict(row)
            for field in ("trigger_config", "nodes", "variables", "tags"):
                if isinstance(d.get(field), str):
                    try:
                        d[field] = json.loads(d[field])
                    except (json.JSONDecodeError, TypeError):
                        pass
            results.append(d)
        return results

    def update_workflow(self, workflow_id: str, **kwargs) -> bool:
        """更新工作流"""
        allowed = {"name", "description", "trigger_type", "trigger_config",
                    "nodes", "variables", "status", "max_runs_per_hour", "tags"}
        updates = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
        if not updates:
            return False

        for field in ("trigger_config", "nodes", "variables", "tags"):
            if field in updates and not isinstance(updates[field], str):
                updates[field] = json.dumps(updates[field])

        updates["updated_at"] = datetime.now(timezone.utc).isoformat()
        set_clause = ", ".join(f"{k}=?" for k in updates)
        values = list(updates.values()) + [workflow_id]

        conn = self._get_conn()
        cursor = conn.execute(
            f"UPDATE workflows SET {set_clause} WHERE workflow_id=?", values,
        )
        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()
        if updated:
            self._audit_log(workflow_id, "", "workflow_updated",
                            details=f"Fields: {list(kwargs.keys())}")
        return updated

    def delete_workflow(self, workflow_id: str) -> bool:
        """删除工作流"""
        conn = self._get_conn()
        cursor = conn.execute(
            "DELETE FROM workflows WHERE workflow_id=?", (workflow_id,)
        )
        conn.commit()
        deleted = cursor.rowcount > 0
        conn.close()
        return deleted

    def activate_workflow(self, workflow_id: str) -> bool:
        """激活工作流"""
        return self.update_workflow(workflow_id, status="active")

    def pause_workflow(self, workflow_id: str) -> bool:
        """暂停工作流"""
        return self.update_workflow(workflow_id, status="paused")

    # ── Execution ────────────────────────────────────────

    def start_run(self, workflow_id: str, trigger_data: dict = None) -> Optional[str]:
        """启动工作流运行"""
        wf = self.get_workflow(workflow_id)
        if not wf:
            return None
        if wf["status"] not in ("active", "draft"):
            return None

        # Rate limit check
        if not self._check_rate_limit(workflow_id, wf.get("max_runs_per_hour", 10)):
            return None

        run_id = str(uuid.uuid4())
        nodes = wf.get("nodes", [])
        first_node = nodes[0]["node_id"] if nodes else ""

        context = dict(wf.get("variables", {}))
        if trigger_data:
            context["trigger"] = trigger_data

        conn = self._get_conn()
        conn.execute(
            "INSERT INTO workflow_runs "
            "(run_id, workflow_id, status, current_node, context, started_at, trigger_data, steps_log) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (run_id, workflow_id, "running", first_node,
             json.dumps(context), datetime.now(timezone.utc).isoformat(),
             json.dumps(trigger_data or {}), "[]"),
        )
        conn.commit()
        conn.close()

        self._audit_log(workflow_id, run_id, "run_started",
                        details=f"Trigger: {json.dumps(trigger_data or {})}")
        return run_id

    def execute_run(self, run_id: str) -> dict:
        """执行工作流 (同步)"""
        run = self.get_run(run_id)
        if not run or run["status"] != "running":
            return {"status": "error", "error": "Run not found or not running"}

        wf = self.get_workflow(run["workflow_id"])
        if not wf:
            return {"status": "error", "error": "Workflow not found"}

        nodes = wf.get("nodes", [])
        node_map = {n["node_id"]: n for n in nodes}
        context = run.get("context", {})
        if isinstance(context, str):
            context = json.loads(context)

        steps_log = run.get("steps_log", [])
        if isinstance(steps_log, str):
            steps_log = json.loads(steps_log)

        current = run.get("current_node", "")
        max_steps = 100  # safety limit

        for _ in range(max_steps):
            if not current or current not in node_map:
                break

            node = node_map[current]
            node_type = node.get("node_type", "action")
            config = node.get("config", {})

            step_result = {"node_id": current, "node_type": node_type,
                           "timestamp": datetime.now(timezone.utc).isoformat()}

            try:
                if node_type == NodeType.CONDITION:
                    conditions = config.get("conditions", [])
                    logic = config.get("logic", "and")
                    result = ConditionEvaluator.evaluate_multi(conditions, context, logic)
                    step_result["result"] = result
                    current = node.get("on_true", "") if result else node.get("on_false", "")

                elif node_type == NodeType.ACTION:
                    action_type = config.get("action_type", "log")
                    action_result = self._execute_action(action_type, config, context)
                    step_result["result"] = action_result
                    context["last_action_result"] = action_result
                    next_nodes = node.get("next_nodes", [])
                    current = next_nodes[0] if next_nodes else ""

                elif node_type == NodeType.DELAY:
                    delay_seconds = config.get("seconds", 0)
                    step_result["delay"] = delay_seconds
                    # In sync mode, just record the delay
                    next_nodes = node.get("next_nodes", [])
                    current = next_nodes[0] if next_nodes else ""

                elif node_type == NodeType.BRANCH:
                    branches = config.get("branches", [])
                    matched = False
                    for branch in branches:
                        conds = branch.get("conditions", [])
                        if ConditionEvaluator.evaluate_multi(conds, context):
                            current = branch.get("target_node", "")
                            step_result["branch"] = branch.get("name", "matched")
                            matched = True
                            break
                    if not matched:
                        default = config.get("default_node", "")
                        current = default
                        step_result["branch"] = "default"

                elif node_type == NodeType.LOOP:
                    loop_var = config.get("variable", "loop_items")
                    items = context.get(loop_var, [])
                    loop_idx = context.get(f"_loop_{current}_idx", 0)

                    if loop_idx < len(items):
                        context[config.get("item_var", "current_item")] = items[loop_idx]
                        context[f"_loop_{current}_idx"] = loop_idx + 1
                        step_result["iteration"] = loop_idx
                        body_node = config.get("body_node", "")
                        current = body_node if body_node else (node.get("next_nodes", [None])[0] or "")
                    else:
                        # Loop finished
                        context.pop(f"_loop_{current}_idx", None)
                        next_nodes = node.get("next_nodes", [])
                        current = next_nodes[0] if next_nodes else ""

                elif node_type == NodeType.END:
                    step_result["result"] = "workflow_complete"
                    steps_log.append(step_result)
                    current = ""
                    break

                else:
                    next_nodes = node.get("next_nodes", [])
                    current = next_nodes[0] if next_nodes else ""

                step_result["status"] = "success"

            except Exception as e:
                step_result["status"] = "error"
                step_result["error"] = str(e)
                error_node = node.get("on_error", "")
                if error_node:
                    current = error_node
                else:
                    self._update_run(run_id, status="failed",
                                     error=str(e), steps_log=steps_log,
                                     context=context)
                    self._audit_log(wf["workflow_id"], run_id, "run_failed",
                                    node_id=current, details=str(e))
                    return {"status": "failed", "error": str(e), "steps": steps_log}

            steps_log.append(step_result)
            self._audit_log(wf["workflow_id"], run_id, "step_executed",
                            node_id=step_result["node_id"])

        # Completed
        self._update_run(run_id, status="completed", steps_log=steps_log,
                         context=context, current_node=current)
        self._audit_log(wf["workflow_id"], run_id, "run_completed",
                        details=f"Steps: {len(steps_log)}")

        return {"status": "completed", "steps": len(steps_log),
                "context": context, "log": steps_log}

    def _execute_action(self, action_type: str, config: dict, context: dict) -> Any:
        """执行动作节点"""
        # Check for registered handler
        handler = self.action_handlers.get(action_type)
        if handler:
            return handler(config, context)

        # Built-in actions
        if action_type == ActionType.LOG:
            message = config.get("message", "")
            # Template variables
            for key, val in context.items():
                if isinstance(val, (str, int, float)):
                    message = message.replace(f"{{{{{key}}}}}", str(val))
            return {"logged": message}

        elif action_type == ActionType.UPDATE_VAR:
            var_name = config.get("variable", "")
            var_value = config.get("value")
            if var_name:
                context[var_name] = var_value
            return {"updated": var_name}

        elif action_type == ActionType.TWEET:
            return {"action": "tweet", "text": config.get("text", ""),
                    "simulated": True}

        elif action_type == ActionType.REPLY:
            return {"action": "reply", "text": config.get("text", ""),
                    "to": config.get("tweet_id", ""), "simulated": True}

        elif action_type == ActionType.LIKE:
            return {"action": "like", "tweet_id": config.get("tweet_id", ""),
                    "simulated": True}

        elif action_type == ActionType.FOLLOW:
            return {"action": "follow", "user": config.get("username", ""),
                    "simulated": True}

        elif action_type == ActionType.WEBHOOK:
            return {"action": "webhook", "url": config.get("url", ""),
                    "simulated": True}

        return {"action": action_type, "config": config, "simulated": True}

    # ── Run Management ───────────────────────────────────

    def get_run(self, run_id: str) -> Optional[dict]:
        """获取运行详情"""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM workflow_runs WHERE run_id=?", (run_id,)
        ).fetchone()
        conn.close()
        if not row:
            return None
        d = dict(row)
        for field in ("context", "steps_log", "trigger_data"):
            if isinstance(d.get(field), str):
                try:
                    d[field] = json.loads(d[field])
                except (json.JSONDecodeError, TypeError):
                    pass
        return d

    def list_runs(self, workflow_id: str = None, status: str = None,
                  limit: int = 50) -> List[dict]:
        """列出运行记录"""
        conn = self._get_conn()
        query = "SELECT * FROM workflow_runs"
        params = []
        conditions = []

        if workflow_id:
            conditions.append("workflow_id=?")
            params.append(workflow_id)
        if status:
            conditions.append("status=?")
            params.append(status)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += f" ORDER BY started_at DESC LIMIT {limit}"

        rows = conn.execute(query, params).fetchall()
        conn.close()

        results = []
        for row in rows:
            d = dict(row)
            for field in ("context", "steps_log", "trigger_data"):
                if isinstance(d.get(field), str):
                    try:
                        d[field] = json.loads(d[field])
                    except (json.JSONDecodeError, TypeError):
                        pass
            results.append(d)
        return results

    def cancel_run(self, run_id: str) -> bool:
        """取消运行"""
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE workflow_runs SET status='cancelled', "
            "completed_at=? WHERE run_id=? AND status IN ('pending', 'running', 'waiting')",
            (datetime.now(timezone.utc).isoformat(), run_id),
        )
        conn.commit()
        cancelled = cursor.rowcount > 0
        conn.close()
        return cancelled

    def _update_run(self, run_id: str, **kwargs):
        """内部: 更新运行状态"""
        updates = {}
        for k, v in kwargs.items():
            if k in ("status", "current_node", "error"):
                updates[k] = v
            elif k in ("context", "steps_log"):
                updates[k] = json.dumps(v) if not isinstance(v, str) else v

        if kwargs.get("status") in ("completed", "failed", "cancelled"):
            updates["completed_at"] = datetime.now(timezone.utc).isoformat()

        if not updates:
            return

        set_clause = ", ".join(f"{k}=?" for k in updates)
        values = list(updates.values()) + [run_id]

        conn = self._get_conn()
        conn.execute(f"UPDATE workflow_runs SET {set_clause} WHERE run_id=?", values)
        conn.commit()
        conn.close()

    def _check_rate_limit(self, workflow_id: str, max_per_hour: int) -> bool:
        """检查运行频率限制"""
        one_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        conn = self._get_conn()
        count = conn.execute(
            "SELECT COUNT(*) FROM workflow_runs WHERE workflow_id=? AND started_at > ?",
            (workflow_id, one_hour_ago),
        ).fetchone()[0]
        conn.close()
        return count < max_per_hour

    # ── Audit Log ────────────────────────────────────────

    def _audit_log(self, workflow_id: str, run_id: str, event_type: str,
                   node_id: str = "", details: str = ""):
        """记录审计日志"""
        conn = self._get_conn()
        conn.execute(
            "INSERT INTO workflow_audit_log "
            "(workflow_id, run_id, event_type, node_id, details, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (workflow_id, run_id, event_type, node_id,
             details, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        conn.close()

    def get_audit_log(self, workflow_id: str = None, run_id: str = None,
                      limit: int = 100) -> List[dict]:
        """获取审计日志"""
        conn = self._get_conn()
        query = "SELECT * FROM workflow_audit_log"
        params = []
        conditions = []

        if workflow_id:
            conditions.append("workflow_id=?")
            params.append(workflow_id)
        if run_id:
            conditions.append("run_id=?")
            params.append(run_id)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += f" ORDER BY created_at DESC LIMIT {limit}"

        rows = conn.execute(query, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Stats ────────────────────────────────────────────

    def stats(self, workflow_id: str = None) -> dict:
        """运行统计"""
        conn = self._get_conn()
        base = "SELECT {} FROM workflow_runs"
        params = []
        where = ""
        if workflow_id:
            where = " WHERE workflow_id=?"
            params.append(workflow_id)

        total = conn.execute(
            f"SELECT COUNT(*) FROM workflow_runs{where}", params
        ).fetchone()[0]

        by_status = {}
        for row in conn.execute(
            f"SELECT status, COUNT(*) as cnt FROM workflow_runs{where} GROUP BY status",
            params,
        ).fetchall():
            by_status[row["status"]] = row["cnt"]

        wf_count = conn.execute("SELECT COUNT(*) FROM workflows").fetchone()[0]
        active_wf = conn.execute(
            "SELECT COUNT(*) FROM workflows WHERE status='active'"
        ).fetchone()[0]

        conn.close()
        return {
            "total_runs": total,
            "by_status": by_status,
            "total_workflows": wf_count,
            "active_workflows": active_wf,
        }

    # ── Templates ────────────────────────────────────────

    @staticmethod
    def template_welcome_dm() -> dict:
        """模板: 新关注者欢迎DM"""
        return {
            "name": "Welcome New Follower",
            "description": "Send welcome DM to new followers",
            "trigger_type": "event",
            "trigger_config": {"event": "new_follower"},
            "nodes": [
                {"node_id": "check", "node_type": "condition", "name": "Check follower count",
                 "config": {"conditions": [{"field": "trigger.follower_count", "op": "gt", "value": 100}]},
                 "on_true": "dm_vip", "on_false": "dm_regular"},
                {"node_id": "dm_vip", "node_type": "action", "name": "DM VIP follower",
                 "config": {"action_type": "dm", "text": "Welcome! 🌟 As a valued member..."},
                 "next_nodes": ["end"]},
                {"node_id": "dm_regular", "node_type": "action", "name": "DM regular follower",
                 "config": {"action_type": "dm", "text": "Welcome! Thanks for following! 🎉"},
                 "next_nodes": ["end"]},
                {"node_id": "end", "node_type": "end", "name": "Done"},
            ],
        }

    @staticmethod
    def template_engagement_boost() -> dict:
        """模板: 互动率提升"""
        return {
            "name": "Engagement Boost",
            "description": "Auto-engage with users who interact with your content",
            "trigger_type": "event",
            "trigger_config": {"event": "mention"},
            "nodes": [
                {"node_id": "sentiment", "node_type": "condition", "name": "Check sentiment",
                 "config": {"conditions": [{"field": "trigger.sentiment", "op": "gte", "value": 0.5}]},
                 "on_true": "like_tweet", "on_false": "log_negative"},
                {"node_id": "like_tweet", "node_type": "action", "name": "Like the tweet",
                 "config": {"action_type": "like"}, "next_nodes": ["reply"]},
                {"node_id": "reply", "node_type": "action", "name": "Reply with thanks",
                 "config": {"action_type": "reply", "text": "Thanks for the mention! 🙏"},
                 "next_nodes": ["end"]},
                {"node_id": "log_negative", "node_type": "action", "name": "Log negative mention",
                 "config": {"action_type": "log", "message": "Negative mention detected"},
                 "next_nodes": ["end"]},
                {"node_id": "end", "node_type": "end", "name": "Done"},
            ],
        }

    @staticmethod
    def template_content_pipeline() -> dict:
        """模板: 内容发布管线"""
        return {
            "name": "Content Pipeline",
            "description": "Scheduled content publishing workflow",
            "trigger_type": "schedule",
            "trigger_config": {"cron": "0 9,13,17 * * 1-5"},
            "nodes": [
                {"node_id": "fetch", "node_type": "action", "name": "Fetch next content",
                 "config": {"action_type": "log", "message": "Fetching scheduled content"},
                 "next_nodes": ["check_content"]},
                {"node_id": "check_content", "node_type": "condition", "name": "Has content?",
                 "config": {"conditions": [{"field": "last_action_result.logged", "op": "exists", "value": True}]},
                 "on_true": "publish", "on_false": "end"},
                {"node_id": "publish", "node_type": "action", "name": "Publish tweet",
                 "config": {"action_type": "tweet", "text": "Automated content post"},
                 "next_nodes": ["log_success"]},
                {"node_id": "log_success", "node_type": "action", "name": "Log success",
                 "config": {"action_type": "log", "message": "Content published successfully"},
                 "next_nodes": ["end"]},
                {"node_id": "end", "node_type": "end", "name": "Done"},
            ],
        }
