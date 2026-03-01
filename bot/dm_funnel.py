"""
DM Funnel - 自动化私信漏斗引擎 v1.0
触发条件 + 延迟序列 + 分支逻辑 + 转化追踪 + 模板引擎
"""

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Dict, List, Optional, Callable, Any, Set

logger = logging.getLogger(__name__)


class FunnelStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    ARCHIVED = "archived"
    DRAFT = "draft"


class StepType(str, Enum):
    MESSAGE = "message"
    DELAY = "delay"
    CONDITION = "condition"
    ACTION = "action"
    TAG = "tag"


class TriggerType(str, Enum):
    NEW_FOLLOWER = "new_follower"
    KEYWORD = "keyword"
    REPLY = "reply"
    RETWEET = "retweet"
    LIKE = "like"
    MENTION = "mention"
    MANUAL = "manual"
    WEBHOOK = "webhook"


class ConditionOp(str, Enum):
    EQUALS = "equals"
    CONTAINS = "contains"
    GT = "gt"
    LT = "lt"
    IN = "in"
    NOT_IN = "not_in"
    REGEX = "regex"
    EXISTS = "exists"


@dataclass
class FunnelStep:
    """漏斗步骤"""
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    step_type: StepType = StepType.MESSAGE
    content: str = ""
    delay_seconds: int = 0
    condition_field: str = ""
    condition_op: ConditionOp = ConditionOp.EQUALS
    condition_value: Any = None
    next_step_true: Optional[str] = None
    next_step_false: Optional[str] = None
    next_step: Optional[str] = None
    tags_to_add: List[str] = field(default_factory=list)
    tags_to_remove: List[str] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "step_type": self.step_type.value,
            "content": self.content,
            "delay_seconds": self.delay_seconds,
            "condition_field": self.condition_field,
            "condition_op": self.condition_op.value,
            "condition_value": self.condition_value,
            "next_step_true": self.next_step_true,
            "next_step_false": self.next_step_false,
            "next_step": self.next_step,
            "tags_to_add": self.tags_to_add,
            "tags_to_remove": self.tags_to_remove,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "FunnelStep":
        d = dict(data)
        if "step_type" in d:
            d["step_type"] = StepType(d["step_type"])
        if "condition_op" in d:
            d["condition_op"] = ConditionOp(d["condition_op"])
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class Trigger:
    """漏斗触发器"""
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    trigger_type: TriggerType = TriggerType.MANUAL
    keywords: List[str] = field(default_factory=list)
    pattern: str = ""
    cooldown_seconds: int = 86400  # 24h default
    max_fires_per_user: int = 1
    enabled: bool = True

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "trigger_type": self.trigger_type.value,
            "keywords": self.keywords,
            "pattern": self.pattern,
            "cooldown_seconds": self.cooldown_seconds,
            "max_fires_per_user": self.max_fires_per_user,
            "enabled": self.enabled,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "Trigger":
        d = dict(data)
        if "trigger_type" in d:
            d["trigger_type"] = TriggerType(d["trigger_type"])
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class Funnel:
    """私信漏斗"""
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    name: str = ""
    description: str = ""
    status: FunnelStatus = FunnelStatus.DRAFT
    triggers: List[Trigger] = field(default_factory=list)
    steps: Dict[str, FunnelStep] = field(default_factory=dict)
    entry_step_id: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    metadata: Dict = field(default_factory=dict)

    def add_step(self, step: FunnelStep) -> str:
        self.steps[step.id] = step
        if self.entry_step_id is None:
            self.entry_step_id = step.id
        self.updated_at = datetime.now(timezone.utc).isoformat()
        return step.id

    def remove_step(self, step_id: str) -> bool:
        if step_id in self.steps:
            del self.steps[step_id]
            if self.entry_step_id == step_id:
                self.entry_step_id = next(iter(self.steps), None) if self.steps else None
            self.updated_at = datetime.now(timezone.utc).isoformat()
            return True
        return False

    def get_step(self, step_id: str) -> Optional[FunnelStep]:
        return self.steps.get(step_id)

    def add_trigger(self, trigger: Trigger) -> str:
        self.triggers.append(trigger)
        self.updated_at = datetime.now(timezone.utc).isoformat()
        return trigger.id

    def chain_steps(self, step_ids: List[str]):
        """链式连接步骤"""
        for i, sid in enumerate(step_ids):
            step = self.steps.get(sid)
            if step and i + 1 < len(step_ids):
                step.next_step = step_ids[i + 1]
        if step_ids:
            self.entry_step_id = step_ids[0]

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "status": self.status.value,
            "triggers": [t.to_dict() for t in self.triggers],
            "steps": {k: v.to_dict() for k, v in self.steps.items()},
            "entry_step_id": self.entry_step_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "Funnel":
        d = dict(data)
        d["status"] = FunnelStatus(d.get("status", "draft"))
        d["triggers"] = [Trigger.from_dict(t) for t in d.get("triggers", [])]
        d["steps"] = {k: FunnelStep.from_dict(v) for k, v in d.get("steps", {}).items()}
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class UserState:
    """用户在漏斗中的状态"""
    user_id: str = ""
    funnel_id: str = ""
    current_step_id: Optional[str] = None
    status: str = "active"  # active, completed, opted_out, error
    tags: Set[str] = field(default_factory=set)
    variables: Dict[str, Any] = field(default_factory=dict)
    messages_sent: int = 0
    messages_received: int = 0
    entered_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    last_action_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    scheduled_at: Optional[str] = None
    history: List[Dict] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "user_id": self.user_id,
            "funnel_id": self.funnel_id,
            "current_step_id": self.current_step_id,
            "status": self.status,
            "tags": list(self.tags),
            "variables": self.variables,
            "messages_sent": self.messages_sent,
            "messages_received": self.messages_received,
            "entered_at": self.entered_at,
            "last_action_at": self.last_action_at,
            "scheduled_at": self.scheduled_at,
        }


class TemplateEngine:
    """DM模板引擎"""

    @staticmethod
    def render(template: str, variables: Dict[str, Any]) -> str:
        """渲染模板，替换 {{var}} 占位符"""
        result = template
        for key, value in variables.items():
            result = result.replace("{{" + key + "}}", str(value))
        return result

    @staticmethod
    def extract_variables(template: str) -> List[str]:
        """提取模板中的变量名"""
        import re
        return re.findall(r'\{\{(\w+)\}\}', template)


class ConditionEvaluator:
    """条件评估器"""

    @staticmethod
    def evaluate(field_value: Any, op: ConditionOp, target: Any) -> bool:
        """评估条件"""
        import re as re_mod
        if op == ConditionOp.EQUALS:
            return field_value == target
        elif op == ConditionOp.CONTAINS:
            return str(target) in str(field_value) if field_value else False
        elif op == ConditionOp.GT:
            try:
                return float(field_value) > float(target)
            except (ValueError, TypeError):
                return False
        elif op == ConditionOp.LT:
            try:
                return float(field_value) < float(target)
            except (ValueError, TypeError):
                return False
        elif op == ConditionOp.IN:
            if isinstance(target, (list, set, tuple)):
                return field_value in target
            return str(field_value) in str(target)
        elif op == ConditionOp.NOT_IN:
            if isinstance(target, (list, set, tuple)):
                return field_value not in target
            return str(field_value) not in str(target)
        elif op == ConditionOp.REGEX:
            try:
                return bool(re_mod.search(str(target), str(field_value or "")))
            except re_mod.error:
                return False
        elif op == ConditionOp.EXISTS:
            return field_value is not None
        return False


class DMFunnelEngine:
    """DM漏斗执行引擎"""

    def __init__(self):
        self._funnels: Dict[str, Funnel] = {}
        self._user_states: Dict[str, UserState] = {}  # key: f"{funnel_id}:{user_id}"
        self._trigger_fires: Dict[str, Dict[str, int]] = {}  # trigger_id -> {user_id: count}
        self._send_callback: Optional[Callable] = None
        self.template_engine = TemplateEngine()
        self.condition_eval = ConditionEvaluator()

    def set_send_callback(self, callback: Callable):
        """设置发送DM回调"""
        self._send_callback = callback

    # --- Funnel CRUD ---

    def create_funnel(self, name: str, description: str = "") -> Funnel:
        funnel = Funnel(name=name, description=description)
        self._funnels[funnel.id] = funnel
        return funnel

    def get_funnel(self, funnel_id: str) -> Optional[Funnel]:
        return self._funnels.get(funnel_id)

    def list_funnels(self, status: Optional[FunnelStatus] = None) -> List[Funnel]:
        funnels = list(self._funnels.values())
        if status:
            funnels = [f for f in funnels if f.status == status]
        return funnels

    def update_funnel_status(self, funnel_id: str, status: FunnelStatus) -> bool:
        funnel = self._funnels.get(funnel_id)
        if funnel:
            funnel.status = status
            funnel.updated_at = datetime.now(timezone.utc).isoformat()
            return True
        return False

    def delete_funnel(self, funnel_id: str) -> bool:
        if funnel_id in self._funnels:
            del self._funnels[funnel_id]
            # Clean up user states
            to_remove = [k for k in self._user_states if k.startswith(f"{funnel_id}:")]
            for k in to_remove:
                del self._user_states[k]
            return True
        return False

    # --- User State ---

    def _state_key(self, funnel_id: str, user_id: str) -> str:
        return f"{funnel_id}:{user_id}"

    def enter_funnel(self, funnel_id: str, user_id: str, variables: Optional[Dict] = None) -> Optional[UserState]:
        """用户进入漏斗"""
        funnel = self._funnels.get(funnel_id)
        if not funnel or funnel.status != FunnelStatus.ACTIVE:
            return None

        key = self._state_key(funnel_id, user_id)
        if key in self._user_states:
            existing = self._user_states[key]
            if existing.status == "active":
                return existing  # Already in funnel

        state = UserState(
            user_id=user_id,
            funnel_id=funnel_id,
            current_step_id=funnel.entry_step_id,
            variables=variables or {},
        )
        self._user_states[key] = state
        return state

    def get_user_state(self, funnel_id: str, user_id: str) -> Optional[UserState]:
        return self._user_states.get(self._state_key(funnel_id, user_id))

    def list_users_in_funnel(self, funnel_id: str, status: Optional[str] = None) -> List[UserState]:
        states = [
            s for k, s in self._user_states.items()
            if k.startswith(f"{funnel_id}:")
        ]
        if status:
            states = [s for s in states if s.status == status]
        return states

    def opt_out_user(self, funnel_id: str, user_id: str) -> bool:
        key = self._state_key(funnel_id, user_id)
        state = self._user_states.get(key)
        if state:
            state.status = "opted_out"
            state.last_action_at = datetime.now(timezone.utc).isoformat()
            return True
        return False

    # --- Trigger Matching ---

    def check_triggers(self, event_type: TriggerType, user_id: str, data: Dict = None) -> List[Funnel]:
        """检查事件是否触发任何漏斗"""
        data = data or {}
        triggered = []

        for funnel in self._funnels.values():
            if funnel.status != FunnelStatus.ACTIVE:
                continue
            for trigger in funnel.triggers:
                if not trigger.enabled or trigger.trigger_type != event_type:
                    continue

                # Check fire count
                fires = self._trigger_fires.get(trigger.id, {})
                user_fires = fires.get(user_id, 0)
                if user_fires >= trigger.max_fires_per_user:
                    continue

                # Keyword matching
                if trigger.keywords and event_type == TriggerType.KEYWORD:
                    text = data.get("text", "").lower()
                    if not any(kw.lower() in text for kw in trigger.keywords):
                        continue

                triggered.append(funnel)
                # Record fire
                self._trigger_fires.setdefault(trigger.id, {})[user_id] = user_fires + 1

        return triggered

    # --- Step Execution ---

    def execute_step(self, funnel_id: str, user_id: str) -> Optional[Dict]:
        """执行当前步骤并推进"""
        key = self._state_key(funnel_id, user_id)
        state = self._user_states.get(key)
        if not state or state.status != "active":
            return None

        funnel = self._funnels.get(funnel_id)
        if not funnel or not state.current_step_id:
            return None

        step = funnel.get_step(state.current_step_id)
        if not step:
            state.status = "completed"
            return {"action": "completed", "step_id": None}

        result = {"action": step.step_type.value, "step_id": step.id}

        if step.step_type == StepType.MESSAGE:
            message = self.template_engine.render(step.content, {
                **state.variables,
                "user_id": user_id,
            })
            result["message"] = message
            state.messages_sent += 1
            if self._send_callback:
                self._send_callback(user_id, message)
            next_id = step.next_step

        elif step.step_type == StepType.DELAY:
            # Schedule next execution
            delay = timedelta(seconds=step.delay_seconds)
            scheduled = datetime.now(timezone.utc) + delay
            state.scheduled_at = scheduled.isoformat()
            result["delay_seconds"] = step.delay_seconds
            result["scheduled_at"] = state.scheduled_at
            next_id = step.next_step

        elif step.step_type == StepType.CONDITION:
            field_value = state.variables.get(step.condition_field)
            met = self.condition_eval.evaluate(field_value, step.condition_op, step.condition_value)
            result["condition_met"] = met
            next_id = step.next_step_true if met else step.next_step_false

        elif step.step_type == StepType.TAG:
            for tag in step.tags_to_add:
                state.tags.add(tag)
            for tag in step.tags_to_remove:
                state.tags.discard(tag)
            result["tags"] = list(state.tags)
            next_id = step.next_step

        elif step.step_type == StepType.ACTION:
            result["action_content"] = step.content
            next_id = step.next_step

        else:
            next_id = step.next_step

        # Record history
        state.history.append({
            "step_id": step.id,
            "step_type": step.step_type.value,
            "at": datetime.now(timezone.utc).isoformat(),
        })
        state.last_action_at = datetime.now(timezone.utc).isoformat()

        # Advance
        if next_id and next_id in funnel.steps:
            state.current_step_id = next_id
        else:
            state.current_step_id = None
            if step.step_type != StepType.DELAY:
                state.status = "completed"

        return result

    def process_user_reply(self, funnel_id: str, user_id: str, reply_text: str) -> Optional[Dict]:
        """处理用户回复"""
        state = self.get_user_state(funnel_id, user_id)
        if not state or state.status != "active":
            return None
        state.messages_received += 1
        state.variables["last_reply"] = reply_text
        state.last_action_at = datetime.now(timezone.utc).isoformat()
        return {"received": True, "messages_received": state.messages_received}

    # --- Analytics ---

    def funnel_stats(self, funnel_id: str) -> Dict[str, Any]:
        """漏斗统计"""
        users = self.list_users_in_funnel(funnel_id)
        funnel = self._funnels.get(funnel_id)

        by_status: Dict[str, int] = {}
        by_step: Dict[str, int] = {}
        total_msgs_sent = 0
        total_msgs_received = 0

        for u in users:
            by_status[u.status] = by_status.get(u.status, 0) + 1
            if u.current_step_id:
                by_step[u.current_step_id] = by_step.get(u.current_step_id, 0) + 1
            total_msgs_sent += u.messages_sent
            total_msgs_received += u.messages_received

        completion_rate = (
            by_status.get("completed", 0) / len(users) * 100
            if users else 0.0
        )
        opt_out_rate = (
            by_status.get("opted_out", 0) / len(users) * 100
            if users else 0.0
        )

        return {
            "funnel_id": funnel_id,
            "funnel_name": funnel.name if funnel else "",
            "total_users": len(users),
            "by_status": by_status,
            "by_step": by_step,
            "completion_rate": round(completion_rate, 2),
            "opt_out_rate": round(opt_out_rate, 2),
            "messages_sent": total_msgs_sent,
            "messages_received": total_msgs_received,
            "reply_rate": round(
                total_msgs_received / total_msgs_sent * 100, 2
            ) if total_msgs_sent > 0 else 0.0,
        }

    def export_funnel(self, funnel_id: str) -> Optional[str]:
        """导出漏斗配置为JSON"""
        funnel = self._funnels.get(funnel_id)
        if not funnel:
            return None
        return json.dumps(funnel.to_dict(), indent=2, ensure_ascii=False)

    def import_funnel(self, data: str) -> Optional[Funnel]:
        """从JSON导入漏斗"""
        try:
            d = json.loads(data)
            funnel = Funnel.from_dict(d)
            self._funnels[funnel.id] = funnel
            return funnel
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Import funnel failed: {e}")
            return None
