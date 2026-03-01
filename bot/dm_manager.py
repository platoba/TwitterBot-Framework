"""
DM Manager - 私信自动化引擎 v1.0
模板私信 + 自动回复 + 欢迎消息 + 批量发送 + 对话追踪
"""

import re
import time
import json
import logging
import hashlib
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Callable, Any
from collections import defaultdict
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class DMStatus(str, Enum):
    DRAFT = "draft"
    QUEUED = "queued"
    SENT = "sent"
    DELIVERED = "delivered"
    READ = "read"
    FAILED = "failed"


class DMTrigger(str, Enum):
    NEW_FOLLOWER = "new_follower"
    KEYWORD = "keyword"
    MENTION = "mention"
    REPLY = "reply"
    MANUAL = "manual"
    SCHEDULED = "scheduled"


@dataclass
class DMTemplate:
    """私信模板"""
    template_id: str
    name: str
    content: str
    variables: List[str] = field(default_factory=list)
    trigger: DMTrigger = DMTrigger.MANUAL
    trigger_config: Dict = field(default_factory=dict)
    enabled: bool = True
    send_count: int = 0
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def render(self, context: Optional[Dict[str, str]] = None) -> str:
        """渲染模板变量"""
        text = self.content
        if context:
            for key, value in context.items():
                text = text.replace(f"{{{{{key}}}}}", str(value))
        return text

    def extract_variables(self) -> List[str]:
        """提取模板变量"""
        return re.findall(r"\{\{(\w+)\}\}", self.content)

    def to_dict(self) -> Dict:
        return {
            "template_id": self.template_id,
            "name": self.name,
            "content": self.content,
            "variables": self.variables,
            "trigger": self.trigger.value,
            "trigger_config": self.trigger_config,
            "enabled": self.enabled,
            "send_count": self.send_count,
        }


@dataclass
class DMMessage:
    """私信记录"""
    message_id: str
    recipient_id: str
    content: str
    template_id: Optional[str] = None
    status: DMStatus = DMStatus.DRAFT
    trigger: DMTrigger = DMTrigger.MANUAL
    error: str = ""
    sent_at: float = 0.0
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict:
        return {
            "message_id": self.message_id,
            "recipient_id": self.recipient_id,
            "content": self.content[:50] + "..." if len(self.content) > 50 else self.content,
            "template_id": self.template_id,
            "status": self.status.value,
            "trigger": self.trigger.value,
        }


@dataclass
class Conversation:
    """对话记录"""
    user_id: str
    messages: List[Dict] = field(default_factory=list)
    last_activity: float = field(default_factory=time.time)
    tags: List[str] = field(default_factory=list)
    is_active: bool = True

    @property
    def message_count(self) -> int:
        return len(self.messages)

    def add_message(self, role: str, content: str):
        self.messages.append({
            "role": role,
            "content": content,
            "timestamp": time.time(),
        })
        self.last_activity = time.time()


class AutoReplyEngine:
    """
    自动回复引擎

    支持:
    - 关键词匹配回复
    - 正则模式匹配
    - 默认fallback回复
    - 回复冷却 (防止重复回复)
    """

    def __init__(
        self,
        cooldown_seconds: float = 300.0,
        default_reply: str = "Thanks for your message! I'll get back to you soon. 🙏",
    ):
        self.cooldown_seconds = cooldown_seconds
        self.default_reply = default_reply
        self._rules: List[Dict] = []
        self._last_reply: Dict[str, float] = {}

    def add_keyword_rule(
        self,
        keywords: List[str],
        reply: str,
        case_sensitive: bool = False,
        priority: int = 0,
    ):
        """添加关键词触发规则"""
        self._rules.append({
            "type": "keyword",
            "keywords": keywords if case_sensitive else [k.lower() for k in keywords],
            "reply": reply,
            "case_sensitive": case_sensitive,
            "priority": priority,
        })
        self._rules.sort(key=lambda r: -r["priority"])

    def add_regex_rule(self, pattern: str, reply: str, priority: int = 0):
        """添加正则匹配规则"""
        try:
            compiled = re.compile(pattern, re.IGNORECASE)
        except re.error as e:
            raise ValueError(f"Invalid regex: {e}")

        self._rules.append({
            "type": "regex",
            "pattern": compiled,
            "pattern_str": pattern,
            "reply": reply,
            "priority": priority,
        })
        self._rules.sort(key=lambda r: -r["priority"])

    def match(self, user_id: str, text: str) -> Optional[str]:
        """
        匹配消息并返回回复

        Returns:
            回复文本，None如果在冷却期内
        """
        now = time.time()

        # 冷却检查
        last = self._last_reply.get(user_id, 0)
        if now - last < self.cooldown_seconds:
            return None

        reply = self._find_reply(text)
        if reply:
            self._last_reply[user_id] = now
            return reply

        return None

    def _find_reply(self, text: str) -> Optional[str]:
        """查找匹配的回复"""
        for rule in self._rules:
            if rule["type"] == "keyword":
                check_text = text if rule["case_sensitive"] else text.lower()
                for kw in rule["keywords"]:
                    if kw in check_text:
                        return rule["reply"]

            elif rule["type"] == "regex":
                match = rule["pattern"].search(text)
                if match:
                    reply = rule["reply"]
                    # 支持正则分组替换
                    for i, group in enumerate(match.groups(), 1):
                        if group:
                            reply = reply.replace(f"${i}", group)
                    return reply

        return self.default_reply

    @property
    def rule_count(self) -> int:
        return len(self._rules)

    def get_rules(self) -> List[Dict]:
        result = []
        for r in self._rules:
            entry = {"type": r["type"], "reply": r["reply"], "priority": r["priority"]}
            if r["type"] == "keyword":
                entry["keywords"] = r["keywords"]
            elif r["type"] == "regex":
                entry["pattern"] = r["pattern_str"]
            result.append(entry)
        return result

    def clear_cooldowns(self):
        self._last_reply.clear()


class RateLimitedSender:
    """限速发送器 (防止API限流)"""

    def __init__(
        self,
        max_per_minute: int = 15,
        max_per_day: int = 500,
    ):
        self.max_per_minute = max_per_minute
        self.max_per_day = max_per_day
        self._minute_window: List[float] = []
        self._day_window: List[float] = []

    def can_send(self) -> bool:
        now = time.time()
        self._minute_window = [t for t in self._minute_window if now - t < 60]
        self._day_window = [t for t in self._day_window if now - t < 86400]
        return (
            len(self._minute_window) < self.max_per_minute
            and len(self._day_window) < self.max_per_day
        )

    def record_send(self):
        now = time.time()
        self._minute_window.append(now)
        self._day_window.append(now)

    def remaining_minute(self) -> int:
        now = time.time()
        active = sum(1 for t in self._minute_window if now - t < 60)
        return max(0, self.max_per_minute - active)

    def remaining_day(self) -> int:
        now = time.time()
        active = sum(1 for t in self._day_window if now - t < 86400)
        return max(0, self.max_per_day - active)

    def get_stats(self) -> Dict:
        return {
            "minute_used": len(self._minute_window),
            "minute_limit": self.max_per_minute,
            "day_used": len(self._day_window),
            "day_limit": self.max_per_day,
            "remaining_minute": self.remaining_minute(),
            "remaining_day": self.remaining_day(),
        }


class DMManager:
    """
    私信管理器

    Features:
    - 模板管理 (创建/编辑/删除)
    - 自动回复 (关键词+正则)
    - 欢迎私信 (新关注者自动发送)
    - 批量发送 (限速保护)
    - 对话追踪 (上下文管理)
    - 黑名单 (忽略特定用户)
    - 发送统计
    """

    def __init__(
        self,
        auto_reply_engine: Optional[AutoReplyEngine] = None,
        rate_limiter: Optional[RateLimitedSender] = None,
        welcome_template_id: Optional[str] = None,
    ):
        self.auto_reply = auto_reply_engine or AutoReplyEngine()
        self.rate_limiter = rate_limiter or RateLimitedSender()
        self.welcome_template_id = welcome_template_id

        self._templates: Dict[str, DMTemplate] = {}
        self._conversations: Dict[str, Conversation] = {}
        self._blacklist: Set[str] = set()
        self._sent_history: List[DMMessage] = []
        self._counter = 0

    # ─── Templates ───────────────────────────────────

    def create_template(
        self,
        name: str,
        content: str,
        trigger: DMTrigger = DMTrigger.MANUAL,
        trigger_config: Optional[Dict] = None,
    ) -> DMTemplate:
        """创建模板"""
        template_id = f"tpl-{hashlib.md5(name.encode()).hexdigest()[:8]}"
        template = DMTemplate(
            template_id=template_id,
            name=name,
            content=content,
            variables=re.findall(r"\{\{(\w+)\}\}", content),
            trigger=trigger,
            trigger_config=trigger_config or {},
        )
        self._templates[template_id] = template
        return template

    def get_template(self, template_id: str) -> Optional[DMTemplate]:
        return self._templates.get(template_id)

    def list_templates(self) -> List[DMTemplate]:
        return list(self._templates.values())

    def delete_template(self, template_id: str) -> bool:
        if template_id in self._templates:
            del self._templates[template_id]
            return True
        return False

    def update_template(
        self, template_id: str, **kwargs
    ) -> Optional[DMTemplate]:
        tpl = self._templates.get(template_id)
        if not tpl:
            return None
        for key, value in kwargs.items():
            if hasattr(tpl, key):
                setattr(tpl, key, value)
        if "content" in kwargs:
            tpl.variables = tpl.extract_variables()
        return tpl

    # ─── Sending ─────────────────────────────────────

    def send(
        self,
        recipient_id: str,
        content: str,
        template_id: Optional[str] = None,
        trigger: DMTrigger = DMTrigger.MANUAL,
    ) -> DMMessage:
        """
        发送私信

        Returns:
            DMMessage with status
        """
        self._counter += 1
        msg = DMMessage(
            message_id=f"dm-{int(time.time())}-{self._counter}",
            recipient_id=recipient_id,
            content=content,
            template_id=template_id,
            trigger=trigger,
        )

        # 黑名单检查
        if recipient_id in self._blacklist:
            msg.status = DMStatus.FAILED
            msg.error = "User blacklisted"
            self._sent_history.append(msg)
            return msg

        # 限速检查
        if not self.rate_limiter.can_send():
            msg.status = DMStatus.FAILED
            msg.error = "Rate limit exceeded"
            self._sent_history.append(msg)
            return msg

        # 发送 (模拟)
        msg.status = DMStatus.SENT
        msg.sent_at = time.time()
        self.rate_limiter.record_send()

        # 更新模板计数
        if template_id and template_id in self._templates:
            self._templates[template_id].send_count += 1

        # 更新对话
        conv = self._get_conversation(recipient_id)
        conv.add_message("sent", content)

        self._sent_history.append(msg)
        return msg

    def send_template(
        self,
        recipient_id: str,
        template_id: str,
        context: Optional[Dict[str, str]] = None,
        trigger: DMTrigger = DMTrigger.MANUAL,
    ) -> DMMessage:
        """使用模板发送私信"""
        tpl = self._templates.get(template_id)
        if not tpl:
            msg = DMMessage(
                message_id=f"dm-err-{self._counter}",
                recipient_id=recipient_id,
                content="",
                status=DMStatus.FAILED,
            )
            msg.error = f"Template {template_id} not found"
            return msg

        content = tpl.render(context)
        return self.send(recipient_id, content, template_id=template_id, trigger=trigger)

    def send_welcome(
        self, user_id: str, username: str = ""
    ) -> Optional[DMMessage]:
        """发送欢迎私信"""
        if not self.welcome_template_id:
            return None
        return self.send_template(
            user_id,
            self.welcome_template_id,
            context={"username": username, "user_id": user_id},
            trigger=DMTrigger.NEW_FOLLOWER,
        )

    def bulk_send(
        self,
        user_ids: List[str],
        content: str,
        template_id: Optional[str] = None,
    ) -> List[DMMessage]:
        """批量发送"""
        results = []
        for uid in user_ids:
            msg = self.send(uid, content, template_id=template_id)
            results.append(msg)
        return results

    # ─── Auto Reply ──────────────────────────────────

    def handle_incoming(
        self, user_id: str, text: str
    ) -> Optional[DMMessage]:
        """处理收到的私信 (自动回复)"""
        if user_id in self._blacklist:
            return None

        conv = self._get_conversation(user_id)
        conv.add_message("received", text)

        reply = self.auto_reply.match(user_id, text)
        if reply:
            return self.send(user_id, reply, trigger=DMTrigger.KEYWORD)
        return None

    # ─── Conversations ───────────────────────────────

    def _get_conversation(self, user_id: str) -> Conversation:
        if user_id not in self._conversations:
            self._conversations[user_id] = Conversation(user_id=user_id)
        return self._conversations[user_id]

    def get_conversation(self, user_id: str) -> Optional[Conversation]:
        return self._conversations.get(user_id)

    def list_conversations(
        self, active_only: bool = False, limit: int = 50
    ) -> List[Conversation]:
        convs = list(self._conversations.values())
        if active_only:
            convs = [c for c in convs if c.is_active]
        convs.sort(key=lambda c: -c.last_activity)
        return convs[:limit]

    def tag_conversation(self, user_id: str, tag: str) -> bool:
        conv = self._conversations.get(user_id)
        if conv and tag not in conv.tags:
            conv.tags.append(tag)
            return True
        return False

    # ─── Blacklist ───────────────────────────────────

    def add_blacklist(self, user_id: str):
        self._blacklist.add(user_id)

    def remove_blacklist(self, user_id: str):
        self._blacklist.discard(user_id)

    def is_blacklisted(self, user_id: str) -> bool:
        return user_id in self._blacklist

    # ─── Stats ───────────────────────────────────────

    def get_stats(self) -> Dict[str, Any]:
        sent = [m for m in self._sent_history if m.status == DMStatus.SENT]
        failed = [m for m in self._sent_history if m.status == DMStatus.FAILED]

        return {
            "templates": len(self._templates),
            "conversations": len(self._conversations),
            "total_sent": len(sent),
            "total_failed": len(failed),
            "blacklist_size": len(self._blacklist),
            "auto_reply_rules": self.auto_reply.rule_count,
            "rate_limiter": self.rate_limiter.get_stats(),
            "success_rate": (
                round(len(sent) / (len(sent) + len(failed)) * 100, 1)
                if (len(sent) + len(failed)) > 0 else 0
            ),
        }

    def get_history(
        self,
        user_id: Optional[str] = None,
        status: Optional[DMStatus] = None,
        limit: int = 100,
    ) -> List[DMMessage]:
        msgs = self._sent_history
        if user_id:
            msgs = [m for m in msgs if m.recipient_id == user_id]
        if status:
            msgs = [m for m in msgs if m.status == status]
        return msgs[-limit:]
