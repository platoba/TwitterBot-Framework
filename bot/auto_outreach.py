"""
Auto Outreach v1.0
自动化外展引擎 — 冷启动外展 + 模板个性化 + 跟进序列 + 回复追踪 + 转化分析

Features:
- ProspectStore: 潜在客户存储 + 评分 + 标签 + 状态管理
- OutreachTemplate: 多阶段消息模板 + {{变量}} 渲染 + A/B版本
- SequenceEngine: 跟进序列 (delay + condition + branch)
- ResponseTracker: 回复检测 + 情感分析 + 自动分类
- OutreachAnalytics: 转化漏斗 + 回复率 + 最佳时间分析
- AutoOutreach: 统一入口
"""

import json
import sqlite3
import threading
import hashlib
import re
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple, Callable


# ── Enums ─────────────────────────────────────────────────────────

class ProspectStatus(Enum):
    """潜在客户状态"""
    NEW = "new"
    CONTACTED = "contacted"
    REPLIED = "replied"
    INTERESTED = "interested"
    NOT_INTERESTED = "not_interested"
    CONVERTED = "converted"
    BOUNCED = "bounced"
    BLOCKED = "blocked"
    UNSUBSCRIBED = "unsubscribed"


class SequenceStepType(Enum):
    """序列步骤类型"""
    MESSAGE = "message"          # 发送消息
    DELAY = "delay"              # 等待时间
    CONDITION = "condition"      # 条件分支
    TAG = "tag"                  # 打标签
    NOTIFY = "notify"           # 通知
    WEBHOOK = "webhook"         # 外部回调


class ResponseCategory(Enum):
    """回复分类"""
    POSITIVE = "positive"        # 感兴趣
    NEGATIVE = "negative"        # 不感兴趣
    QUESTION = "question"        # 有疑问
    NEUTRAL = "neutral"          # 中性
    OUT_OF_OFFICE = "out_of_office"  # 自动回复
    SPAM = "spam"                # 垃圾
    UNSUBSCRIBE = "unsubscribe"  # 退订


class OutreachChannel(Enum):
    """外展渠道"""
    DM = "dm"                    # Twitter DM
    REPLY = "reply"              # 公开回复
    QUOTE = "quote"              # 引用推文
    MENTION = "mention"          # @提及


# ── Dataclasses ───────────────────────────────────────────────────

@dataclass
class Prospect:
    """潜在客户"""
    prospect_id: str
    username: str
    display_name: str = ""
    bio: str = ""
    follower_count: int = 0
    following_count: int = 0
    tweet_count: int = 0
    status: ProspectStatus = ProspectStatus.NEW
    score: float = 0.0
    tags: List[str] = field(default_factory=list)
    variables: Dict[str, Any] = field(default_factory=dict)
    source: str = ""
    notes: str = ""
    contacted_at: str = ""
    replied_at: str = ""
    created_at: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()

    @property
    def engagement_rate(self) -> float:
        if self.follower_count == 0:
            return 0.0
        return min(self.tweet_count / max(self.follower_count, 1) * 100, 100.0)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["status"] = self.status.value
        return d


@dataclass
class OutreachMessage:
    """外展消息"""
    message_id: str
    prospect_id: str
    sequence_id: str
    step_index: int
    channel: OutreachChannel
    content: str
    sent_at: str = ""
    delivered: bool = False
    opened: bool = False
    replied: bool = False
    reply_text: str = ""
    reply_category: Optional[ResponseCategory] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.sent_at:
            self.sent_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["channel"] = self.channel.value
        d["reply_category"] = self.reply_category.value if self.reply_category else None
        return d


@dataclass
class SequenceStep:
    """序列步骤"""
    step_type: SequenceStepType
    content: str = ""
    delay_hours: float = 0
    channel: OutreachChannel = OutreachChannel.DM
    condition_field: str = ""
    condition_op: str = ""
    condition_value: str = ""
    ab_variants: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["step_type"] = self.step_type.value
        d["channel"] = self.channel.value
        return d


@dataclass
class Sequence:
    """外展序列"""
    sequence_id: str
    name: str
    steps: List[SequenceStep] = field(default_factory=list)
    enabled: bool = True
    created_at: str = ""
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()

    def add_step(self, step: SequenceStep):
        self.steps.append(step)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sequence_id": self.sequence_id,
            "name": self.name,
            "steps": [s.to_dict() for s in self.steps],
            "enabled": self.enabled,
            "created_at": self.created_at,
            "tags": self.tags,
        }


# ── ProspectScorer ────────────────────────────────────────────────

class ProspectScorer:
    """潜在客户评分器"""

    # Weight configuration
    WEIGHTS = {
        "follower_score": 0.25,
        "engagement_score": 0.20,
        "bio_relevance": 0.20,
        "activity_score": 0.15,
        "ratio_score": 0.10,
        "completeness": 0.10,
    }

    NICHE_KEYWORDS: List[str] = []

    def __init__(self, niche_keywords: Optional[List[str]] = None,
                 weights: Optional[Dict[str, float]] = None):
        if niche_keywords:
            self.NICHE_KEYWORDS = niche_keywords
        if weights:
            self.WEIGHTS = weights

    def score(self, prospect: Prospect) -> float:
        """Calculate composite prospect score (0-100)"""
        scores = {
            "follower_score": self._follower_score(prospect),
            "engagement_score": self._engagement_score(prospect),
            "bio_relevance": self._bio_relevance(prospect),
            "activity_score": self._activity_score(prospect),
            "ratio_score": self._ratio_score(prospect),
            "completeness": self._completeness_score(prospect),
        }
        total = sum(scores[k] * self.WEIGHTS.get(k, 0) for k in scores)
        return round(min(max(total, 0), 100), 1)

    def score_detailed(self, prospect: Prospect) -> Dict[str, Any]:
        """Return detailed scoring breakdown"""
        scores = {
            "follower_score": self._follower_score(prospect),
            "engagement_score": self._engagement_score(prospect),
            "bio_relevance": self._bio_relevance(prospect),
            "activity_score": self._activity_score(prospect),
            "ratio_score": self._ratio_score(prospect),
            "completeness": self._completeness_score(prospect),
        }
        weighted = {k: round(v * self.WEIGHTS.get(k, 0), 1) for k, v in scores.items()}
        total = sum(weighted.values())
        return {
            "raw_scores": scores,
            "weighted_scores": weighted,
            "total": round(min(max(total, 0), 100), 1),
            "grade": self._grade(total),
        }

    def _follower_score(self, p: Prospect) -> float:
        if p.follower_count <= 0:
            return 0
        import math
        # Sweet spot: 1k-100k followers
        log_f = math.log10(max(p.follower_count, 1))
        if log_f < 2:     # < 100
            return log_f * 15
        elif log_f < 3:   # 100-1k
            return 30 + (log_f - 2) * 30
        elif log_f < 5:   # 1k-100k
            return 60 + (log_f - 3) * 20
        else:             # > 100k
            return 100

    def _engagement_score(self, p: Prospect) -> float:
        rate = p.engagement_rate
        if rate > 10:
            return 100
        elif rate > 5:
            return 80
        elif rate > 2:
            return 60
        elif rate > 1:
            return 40
        return max(rate * 40, 0)

    def _bio_relevance(self, p: Prospect) -> float:
        if not p.bio or not self.NICHE_KEYWORDS:
            return 50  # neutral if no data
        bio_lower = p.bio.lower()
        matches = sum(1 for kw in self.NICHE_KEYWORDS if kw.lower() in bio_lower)
        if matches == 0:
            return 10
        ratio = matches / len(self.NICHE_KEYWORDS)
        return min(ratio * 200, 100)

    def _activity_score(self, p: Prospect) -> float:
        if p.tweet_count > 10000:
            return 100
        elif p.tweet_count > 5000:
            return 80
        elif p.tweet_count > 1000:
            return 60
        elif p.tweet_count > 100:
            return 40
        return max(p.tweet_count / 100 * 40, 0)

    def _ratio_score(self, p: Prospect) -> float:
        if p.following_count == 0:
            return 50
        ratio = p.follower_count / max(p.following_count, 1)
        if ratio > 10:
            return 100
        elif ratio > 2:
            return 80
        elif ratio > 1:
            return 60
        elif ratio > 0.5:
            return 40
        return 20

    def _completeness_score(self, p: Prospect) -> float:
        checks = [
            bool(p.display_name),
            bool(p.bio),
            p.follower_count > 0,
            p.tweet_count > 0,
            bool(p.source),
        ]
        return sum(checks) / len(checks) * 100

    @staticmethod
    def _grade(score: float) -> str:
        if score >= 90:
            return "S"
        elif score >= 80:
            return "A"
        elif score >= 70:
            return "B+"
        elif score >= 60:
            return "B"
        elif score >= 50:
            return "C"
        elif score >= 40:
            return "D"
        return "F"


# ── TemplateEngine ────────────────────────────────────────────────

class TemplateEngine:
    """消息模板引擎"""

    # Built-in templates
    TEMPLATES = {
        "cold_dm": "Hey {{name}}! I noticed you're into {{niche}}. {{hook}} Would love to connect! 🚀",
        "follow_up_1": "Hi {{name}}, just following up on my earlier message. {{value_prop}} Let me know if you're interested!",
        "follow_up_2": "{{name}}, last check-in! I thought this might help: {{resource}}. No pressure either way 🙏",
        "collab_pitch": "{{name}}, I love your content about {{topic}}! Would you be open to a quick collab? {{details}}",
        "product_intro": "Hey {{name}}! We built {{product}} for people like you who {{pain_point}}. Quick demo? {{link}}",
        "thank_you": "Thanks for connecting, {{name}}! Really appreciate your work on {{topic}}. 🙌",
        "referral_ask": "{{name}}, quick question — do you know anyone who might benefit from {{offer}}? Happy to return the favor!",
        "engagement_first": "Great thread on {{topic}}, {{name}}! Especially the part about {{detail}}. Mind if I DM you about a related idea?",
    }

    def __init__(self, custom_templates: Optional[Dict[str, str]] = None):
        self._templates = dict(self.TEMPLATES)
        if custom_templates:
            self._templates.update(custom_templates)

    def render(self, template_name: str, variables: Dict[str, Any]) -> str:
        """Render template with variables"""
        tpl = self._templates.get(template_name, "")
        if not tpl:
            return ""
        result = tpl
        for key, value in variables.items():
            result = result.replace("{{" + key + "}}", str(value))
        # Remove unreplaced variables
        result = re.sub(r'\{\{[^}]+\}\}', '', result)
        return result.strip()

    def render_raw(self, template_text: str, variables: Dict[str, Any]) -> str:
        """Render arbitrary template text"""
        result = template_text
        for key, value in variables.items():
            result = result.replace("{{" + key + "}}", str(value))
        result = re.sub(r'\{\{[^}]+\}\}', '', result)
        return result.strip()

    def add_template(self, name: str, template: str):
        self._templates[name] = template

    def get_template(self, name: str) -> Optional[str]:
        return self._templates.get(name)

    def list_templates(self) -> List[str]:
        return list(self._templates.keys())

    def get_variables(self, template_name: str) -> List[str]:
        """Extract variable names from template"""
        tpl = self._templates.get(template_name, "")
        return re.findall(r'\{\{(\w+)\}\}', tpl)

    def preview(self, template_name: str) -> str:
        """Show template with variable placeholders"""
        return self._templates.get(template_name, "")


# ── OutreachDB ────────────────────────────────────────────────────

class OutreachDB:
    """外展数据持久化"""

    def __init__(self, db_path: str = "outreach.db"):
        self._db_path = db_path
        self._local = threading.local()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self._db_path)
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn

    def _init_db(self):
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS prospects (
                prospect_id TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                display_name TEXT DEFAULT '',
                bio TEXT DEFAULT '',
                follower_count INTEGER DEFAULT 0,
                following_count INTEGER DEFAULT 0,
                tweet_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'new',
                score REAL DEFAULT 0.0,
                tags TEXT DEFAULT '[]',
                variables TEXT DEFAULT '{}',
                source TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                contacted_at TEXT DEFAULT '',
                replied_at TEXT DEFAULT '',
                created_at TEXT DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS sequences (
                sequence_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                steps TEXT DEFAULT '[]',
                enabled INTEGER DEFAULT 1,
                created_at TEXT DEFAULT '',
                tags TEXT DEFAULT '[]'
            );
            CREATE TABLE IF NOT EXISTS messages (
                message_id TEXT PRIMARY KEY,
                prospect_id TEXT NOT NULL,
                sequence_id TEXT DEFAULT '',
                step_index INTEGER DEFAULT 0,
                channel TEXT DEFAULT 'dm',
                content TEXT DEFAULT '',
                sent_at TEXT DEFAULT '',
                delivered INTEGER DEFAULT 0,
                opened INTEGER DEFAULT 0,
                replied INTEGER DEFAULT 0,
                reply_text TEXT DEFAULT '',
                reply_category TEXT DEFAULT '',
                FOREIGN KEY (prospect_id) REFERENCES prospects(prospect_id)
            );
            CREATE TABLE IF NOT EXISTS enrollments (
                prospect_id TEXT NOT NULL,
                sequence_id TEXT NOT NULL,
                current_step INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active',
                enrolled_at TEXT DEFAULT '',
                completed_at TEXT DEFAULT '',
                paused_at TEXT DEFAULT '',
                PRIMARY KEY (prospect_id, sequence_id)
            );
            CREATE INDEX IF NOT EXISTS idx_prospects_status ON prospects(status);
            CREATE INDEX IF NOT EXISTS idx_messages_prospect ON messages(prospect_id);
            CREATE INDEX IF NOT EXISTS idx_enrollments_status ON enrollments(status);
        """)
        conn.commit()

    def save_prospect(self, prospect: Prospect):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO prospects
            (prospect_id, username, display_name, bio, follower_count, following_count,
             tweet_count, status, score, tags, variables, source, notes,
             contacted_at, replied_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (prospect.prospect_id, prospect.username, prospect.display_name,
              prospect.bio, prospect.follower_count, prospect.following_count,
              prospect.tweet_count, prospect.status.value, prospect.score,
              json.dumps(prospect.tags), json.dumps(prospect.variables),
              prospect.source, prospect.notes, prospect.contacted_at,
              prospect.replied_at, prospect.created_at))
        conn.commit()

    def save_prospects_batch(self, prospects: List[Prospect]):
        conn = self._get_conn()
        for p in prospects:
            conn.execute("""
                INSERT OR REPLACE INTO prospects
                (prospect_id, username, display_name, bio, follower_count, following_count,
                 tweet_count, status, score, tags, variables, source, notes,
                 contacted_at, replied_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (p.prospect_id, p.username, p.display_name, p.bio,
                  p.follower_count, p.following_count, p.tweet_count,
                  p.status.value, p.score, json.dumps(p.tags),
                  json.dumps(p.variables), p.source, p.notes,
                  p.contacted_at, p.replied_at, p.created_at))
        conn.commit()

    def get_prospect(self, prospect_id: str) -> Optional[Dict[str, Any]]:
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM prospects WHERE prospect_id = ?",
                           (prospect_id,)).fetchone()
        if row:
            return dict(row)
        return None

    def get_prospects_by_status(self, status: ProspectStatus,
                                 limit: int = 50) -> List[Dict]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM prospects WHERE status = ? ORDER BY score DESC LIMIT ?",
            (status.value, limit)).fetchall()
        return [dict(r) for r in rows]

    def get_top_prospects(self, limit: int = 20) -> List[Dict]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM prospects WHERE status = 'new' ORDER BY score DESC LIMIT ?",
            (limit,)).fetchall()
        return [dict(r) for r in rows]

    def update_prospect_status(self, prospect_id: str, status: ProspectStatus):
        conn = self._get_conn()
        conn.execute("UPDATE prospects SET status = ? WHERE prospect_id = ?",
                     (status.value, prospect_id))
        conn.commit()

    def count_prospects(self) -> Dict[str, int]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT status, COUNT(*) as cnt FROM prospects GROUP BY status"
        ).fetchall()
        return {r["status"]: r["cnt"] for r in rows}

    def save_sequence(self, sequence: Sequence):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO sequences
            (sequence_id, name, steps, enabled, created_at, tags)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (sequence.sequence_id, sequence.name,
              json.dumps([s.to_dict() for s in sequence.steps]),
              1 if sequence.enabled else 0,
              sequence.created_at, json.dumps(sequence.tags)))
        conn.commit()

    def get_sequence(self, sequence_id: str) -> Optional[Dict]:
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM sequences WHERE sequence_id = ?",
                           (sequence_id,)).fetchone()
        if row:
            return dict(row)
        return None

    def get_all_sequences(self) -> List[Dict]:
        conn = self._get_conn()
        rows = conn.execute("SELECT * FROM sequences").fetchall()
        return [dict(r) for r in rows]

    def save_message(self, message: OutreachMessage):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO messages
            (message_id, prospect_id, sequence_id, step_index, channel,
             content, sent_at, delivered, opened, replied, reply_text, reply_category)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (message.message_id, message.prospect_id, message.sequence_id,
              message.step_index, message.channel.value, message.content,
              message.sent_at, 1 if message.delivered else 0,
              1 if message.opened else 0, 1 if message.replied else 0,
              message.reply_text,
              message.reply_category.value if message.reply_category else ""))
        conn.commit()

    def get_prospect_messages(self, prospect_id: str) -> List[Dict]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM messages WHERE prospect_id = ? ORDER BY sent_at",
            (prospect_id,)).fetchall()
        return [dict(r) for r in rows]

    def get_message_stats(self) -> Dict[str, Any]:
        conn = self._get_conn()
        total = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        delivered = conn.execute("SELECT COUNT(*) FROM messages WHERE delivered = 1").fetchone()[0]
        replied = conn.execute("SELECT COUNT(*) FROM messages WHERE replied = 1").fetchone()[0]
        return {
            "total_sent": total,
            "delivered": delivered,
            "replied": replied,
            "delivery_rate": delivered / max(total, 1) * 100,
            "reply_rate": replied / max(total, 1) * 100,
        }

    def enroll_prospect(self, prospect_id: str, sequence_id: str):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO enrollments
            (prospect_id, sequence_id, current_step, status, enrolled_at)
            VALUES (?, ?, 0, 'active', ?)
        """, (prospect_id, sequence_id,
              datetime.now(timezone.utc).isoformat()))
        conn.commit()

    def get_active_enrollments(self, sequence_id: str) -> List[Dict]:
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT e.*, p.username, p.display_name, p.score
            FROM enrollments e
            JOIN prospects p ON e.prospect_id = p.prospect_id
            WHERE e.sequence_id = ? AND e.status = 'active'
        """, (sequence_id,)).fetchall()
        return [dict(r) for r in rows]

    def advance_enrollment(self, prospect_id: str, sequence_id: str):
        conn = self._get_conn()
        conn.execute("""
            UPDATE enrollments SET current_step = current_step + 1
            WHERE prospect_id = ? AND sequence_id = ?
        """, (prospect_id, sequence_id))
        conn.commit()

    def complete_enrollment(self, prospect_id: str, sequence_id: str):
        conn = self._get_conn()
        conn.execute("""
            UPDATE enrollments SET status = 'completed',
            completed_at = ?
            WHERE prospect_id = ? AND sequence_id = ?
        """, (datetime.now(timezone.utc).isoformat(), prospect_id, sequence_id))
        conn.commit()

    def pause_enrollment(self, prospect_id: str, sequence_id: str):
        conn = self._get_conn()
        conn.execute("""
            UPDATE enrollments SET status = 'paused',
            paused_at = ?
            WHERE prospect_id = ? AND sequence_id = ?
        """, (datetime.now(timezone.utc).isoformat(), prospect_id, sequence_id))
        conn.commit()

    def close(self):
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None


# ── ResponseClassifier ────────────────────────────────────────────

class ResponseClassifier:
    """回复自动分类器"""

    POSITIVE_PATTERNS = [
        r'\b(yes|sure|absolutely|definitely|love|great|awesome|interested|tell me more)\b',
        r'\b(sounds good|let\'s do it|count me in|sign me up|i\'m in)\b',
        r'\b(好的|可以|感兴趣|不错|合作|聊聊)\b',
    ]

    NEGATIVE_PATTERNS = [
        r'\b(no thanks|not interested|stop|unsubscribe|remove me|don\'t contact)\b',
        r'\b(no|nope|pass|decline|不用了|不需要|别发了)\b',
    ]

    OOO_PATTERNS = [
        r'\b(out of office|on vacation|away|auto.?reply|automatic response)\b',
        r'\b(will be back|returning on|limited access)\b',
    ]

    QUESTION_PATTERNS = [
        r'\?$',
        r'\b(how|what|when|where|who|why|which|can you|could you|tell me)\b.*\?',
        r'\b(怎么|什么|多少|哪里|为什么)\b',
    ]

    def classify(self, text: str) -> ResponseCategory:
        """Classify reply text into category"""
        text_lower = text.lower().strip()

        if not text_lower:
            return ResponseCategory.NEUTRAL

        # Check OOO first (auto-replies)
        for pattern in self.OOO_PATTERNS:
            if re.search(pattern, text_lower, re.IGNORECASE):
                return ResponseCategory.OUT_OF_OFFICE

        # Check negative / unsubscribe
        for pattern in self.NEGATIVE_PATTERNS:
            if re.search(pattern, text_lower, re.IGNORECASE):
                if any(w in text_lower for w in ["unsubscribe", "remove", "stop", "别发"]):
                    return ResponseCategory.UNSUBSCRIBE
                return ResponseCategory.NEGATIVE

        # Check positive
        for pattern in self.POSITIVE_PATTERNS:
            if re.search(pattern, text_lower, re.IGNORECASE):
                return ResponseCategory.POSITIVE

        # Check questions
        for pattern in self.QUESTION_PATTERNS:
            if re.search(pattern, text_lower, re.IGNORECASE):
                return ResponseCategory.QUESTION

        return ResponseCategory.NEUTRAL

    def classify_with_confidence(self, text: str) -> Tuple[ResponseCategory, float]:
        """Classify with confidence score"""
        text_lower = text.lower().strip()
        if not text_lower:
            return ResponseCategory.NEUTRAL, 0.5

        scores = {
            ResponseCategory.POSITIVE: 0,
            ResponseCategory.NEGATIVE: 0,
            ResponseCategory.QUESTION: 0,
            ResponseCategory.OUT_OF_OFFICE: 0,
            ResponseCategory.UNSUBSCRIBE: 0,
        }

        for pattern in self.POSITIVE_PATTERNS:
            if re.search(pattern, text_lower, re.IGNORECASE):
                scores[ResponseCategory.POSITIVE] += 1

        for pattern in self.NEGATIVE_PATTERNS:
            if re.search(pattern, text_lower, re.IGNORECASE):
                scores[ResponseCategory.NEGATIVE] += 1
                if any(w in text_lower for w in ["unsubscribe", "remove", "stop"]):
                    scores[ResponseCategory.UNSUBSCRIBE] += 2

        for pattern in self.OOO_PATTERNS:
            if re.search(pattern, text_lower, re.IGNORECASE):
                scores[ResponseCategory.OUT_OF_OFFICE] += 2

        for pattern in self.QUESTION_PATTERNS:
            if re.search(pattern, text_lower, re.IGNORECASE):
                scores[ResponseCategory.QUESTION] += 1

        max_cat = max(scores, key=scores.get)
        max_score = scores[max_cat]
        total = sum(scores.values())

        if total == 0:
            return ResponseCategory.NEUTRAL, 0.5

        confidence = min(max_score / max(total, 1), 1.0)
        return max_cat, round(confidence, 2)


# ── SequenceExecutor ──────────────────────────────────────────────

class SequenceExecutor:
    """序列执行引擎"""

    def __init__(self, db: OutreachDB, template_engine: TemplateEngine,
                 classifier: Optional[ResponseClassifier] = None):
        self.db = db
        self.template = template_engine
        self.classifier = classifier or ResponseClassifier()

    def enroll(self, prospect_id: str, sequence_id: str):
        """Enroll prospect in sequence"""
        self.db.enroll_prospect(prospect_id, sequence_id)

    def get_next_messages(self, sequence_id: str) -> List[Dict[str, Any]]:
        """Get next messages to send for all active enrollments"""
        seq_data = self.db.get_sequence(sequence_id)
        if not seq_data:
            return []

        steps = json.loads(seq_data.get("steps", "[]"))
        enrollments = self.db.get_active_enrollments(sequence_id)
        messages = []

        for enrollment in enrollments:
            step_idx = enrollment["current_step"]
            if step_idx >= len(steps):
                self.db.complete_enrollment(enrollment["prospect_id"], sequence_id)
                continue

            step = steps[step_idx]
            if step.get("step_type") == "message":
                prospect = self.db.get_prospect(enrollment["prospect_id"])
                if prospect:
                    variables = json.loads(prospect.get("variables", "{}"))
                    variables["name"] = prospect.get("display_name") or prospect.get("username", "")
                    content = self.template.render_raw(step.get("content", ""), variables)
                    messages.append({
                        "prospect_id": enrollment["prospect_id"],
                        "username": prospect.get("username", ""),
                        "channel": step.get("channel", "dm"),
                        "content": content,
                        "step_index": step_idx,
                    })

        return messages

    def record_sent(self, prospect_id: str, sequence_id: str,
                    step_index: int, content: str,
                    channel: OutreachChannel = OutreachChannel.DM):
        """Record that a message was sent"""
        msg_id = hashlib.md5(f"{prospect_id}:{sequence_id}:{step_index}".encode()).hexdigest()[:12]
        message = OutreachMessage(
            message_id=msg_id,
            prospect_id=prospect_id,
            sequence_id=sequence_id,
            step_index=step_index,
            channel=channel,
            content=content,
            delivered=True,
        )
        self.db.save_message(message)
        self.db.update_prospect_status(prospect_id, ProspectStatus.CONTACTED)
        self.db.advance_enrollment(prospect_id, sequence_id)

    def record_reply(self, prospect_id: str, reply_text: str):
        """Record a reply from prospect"""
        category = self.classifier.classify(reply_text)
        # Update latest message
        messages = self.db.get_prospect_messages(prospect_id)
        if messages:
            last = messages[-1]
            conn = self.db._get_conn()
            conn.execute("""
                UPDATE messages SET replied = 1, reply_text = ?, reply_category = ?
                WHERE message_id = ?
            """, (reply_text, category.value, last["message_id"]))
            conn.commit()

        # Update prospect status based on category
        status_map = {
            ResponseCategory.POSITIVE: ProspectStatus.INTERESTED,
            ResponseCategory.NEGATIVE: ProspectStatus.NOT_INTERESTED,
            ResponseCategory.UNSUBSCRIBE: ProspectStatus.UNSUBSCRIBED,
            ResponseCategory.QUESTION: ProspectStatus.REPLIED,
            ResponseCategory.NEUTRAL: ProspectStatus.REPLIED,
            ResponseCategory.OUT_OF_OFFICE: ProspectStatus.REPLIED,
        }
        new_status = status_map.get(category, ProspectStatus.REPLIED)
        self.db.update_prospect_status(prospect_id, new_status)

        # Pause sequence if negative/unsubscribe
        if category in [ResponseCategory.NEGATIVE, ResponseCategory.UNSUBSCRIBE]:
            conn = self.db._get_conn()
            conn.execute("""
                UPDATE enrollments SET status = 'paused'
                WHERE prospect_id = ? AND status = 'active'
            """, (prospect_id,))
            conn.commit()


# ── OutreachAnalytics ─────────────────────────────────────────────

class OutreachAnalytics:
    """外展分析引擎"""

    def __init__(self, db: OutreachDB):
        self.db = db

    def funnel_report(self) -> Dict[str, Any]:
        """Generate conversion funnel report"""
        counts = self.db.count_prospects()
        total = sum(counts.values())
        return {
            "total_prospects": total,
            "status_breakdown": counts,
            "funnel": {
                "contacted": counts.get("contacted", 0),
                "replied": counts.get("replied", 0),
                "interested": counts.get("interested", 0),
                "converted": counts.get("converted", 0),
            },
            "rates": {
                "contact_rate": counts.get("contacted", 0) / max(total, 1) * 100,
                "reply_rate": counts.get("replied", 0) / max(total, 1) * 100,
                "interest_rate": counts.get("interested", 0) / max(total, 1) * 100,
                "conversion_rate": counts.get("converted", 0) / max(total, 1) * 100,
            },
        }

    def message_report(self) -> Dict[str, Any]:
        """Generate message performance report"""
        return self.db.get_message_stats()

    def sequence_report(self, sequence_id: str) -> Dict[str, Any]:
        """Report for a specific sequence"""
        conn = self.db._get_conn()
        total = conn.execute(
            "SELECT COUNT(*) FROM enrollments WHERE sequence_id = ?",
            (sequence_id,)).fetchone()[0]
        active = conn.execute(
            "SELECT COUNT(*) FROM enrollments WHERE sequence_id = ? AND status = 'active'",
            (sequence_id,)).fetchone()[0]
        completed = conn.execute(
            "SELECT COUNT(*) FROM enrollments WHERE sequence_id = ? AND status = 'completed'",
            (sequence_id,)).fetchone()[0]
        paused = conn.execute(
            "SELECT COUNT(*) FROM enrollments WHERE sequence_id = ? AND status = 'paused'",
            (sequence_id,)).fetchone()[0]
        msgs_sent = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE sequence_id = ?",
            (sequence_id,)).fetchone()[0]
        msgs_replied = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE sequence_id = ? AND replied = 1",
            (sequence_id,)).fetchone()[0]

        return {
            "sequence_id": sequence_id,
            "total_enrolled": total,
            "active": active,
            "completed": completed,
            "paused": paused,
            "completion_rate": completed / max(total, 1) * 100,
            "messages_sent": msgs_sent,
            "messages_replied": msgs_replied,
            "reply_rate": msgs_replied / max(msgs_sent, 1) * 100,
        }

    def channel_report(self) -> Dict[str, Any]:
        """Performance by outreach channel"""
        conn = self.db._get_conn()
        rows = conn.execute("""
            SELECT channel,
                   COUNT(*) as total,
                   SUM(replied) as replied,
                   SUM(delivered) as delivered
            FROM messages GROUP BY channel
        """).fetchall()
        channels = {}
        for r in rows:
            ch = r["channel"]
            channels[ch] = {
                "total": r["total"],
                "delivered": r["delivered"],
                "replied": r["replied"],
                "reply_rate": r["replied"] / max(r["total"], 1) * 100,
            }
        return channels

    def generate_text_report(self) -> str:
        """Generate full text report"""
        funnel = self.funnel_report()
        msgs = self.message_report()
        lines = [
            "=== Outreach Analytics Report ===",
            "",
            f"Total Prospects: {funnel['total_prospects']}",
            "",
            "--- Funnel ---",
        ]
        for status, count in funnel["status_breakdown"].items():
            lines.append(f"  {status}: {count}")
        lines.append("")
        lines.append("--- Rates ---")
        for rate_name, rate_val in funnel["rates"].items():
            lines.append(f"  {rate_name}: {rate_val:.1f}%")
        lines.append("")
        lines.append("--- Messages ---")
        lines.append(f"  Total Sent: {msgs['total_sent']}")
        lines.append(f"  Delivered: {msgs['delivered']}")
        lines.append(f"  Replied: {msgs['replied']}")
        lines.append(f"  Reply Rate: {msgs['reply_rate']:.1f}%")
        return "\n".join(lines)

    def export_json(self, filepath: str):
        """Export full analytics to JSON"""
        data = {
            "funnel": self.funnel_report(),
            "messages": self.message_report(),
            "channels": self.channel_report(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)


# ── AutoOutreach (统一入口) ───────────────────────────────────────

class AutoOutreach:
    """自动化外展引擎 — 统一入口"""

    def __init__(self, db_path: str = "outreach.db",
                 niche_keywords: Optional[List[str]] = None):
        self.db = OutreachDB(db_path)
        self.scorer = ProspectScorer(niche_keywords=niche_keywords)
        self.template = TemplateEngine()
        self.classifier = ResponseClassifier()
        self.executor = SequenceExecutor(self.db, self.template, self.classifier)
        self.analytics = OutreachAnalytics(self.db)

    def add_prospect(self, username: str, display_name: str = "",
                     bio: str = "", follower_count: int = 0,
                     following_count: int = 0, tweet_count: int = 0,
                     source: str = "", tags: Optional[List[str]] = None,
                     variables: Optional[Dict[str, Any]] = None) -> Prospect:
        """Add and score a prospect"""
        prospect_id = hashlib.md5(username.encode()).hexdigest()[:12]
        prospect = Prospect(
            prospect_id=prospect_id,
            username=username,
            display_name=display_name,
            bio=bio,
            follower_count=follower_count,
            following_count=following_count,
            tweet_count=tweet_count,
            source=source,
            tags=tags or [],
            variables=variables or {},
        )
        prospect.score = self.scorer.score(prospect)
        self.db.save_prospect(prospect)
        return prospect

    def add_prospects_batch(self, prospects_data: List[Dict[str, Any]]) -> List[Prospect]:
        """Batch add and score prospects"""
        prospects = []
        for data in prospects_data:
            username = data.get("username", "")
            prospect_id = hashlib.md5(username.encode()).hexdigest()[:12]
            p = Prospect(
                prospect_id=prospect_id,
                username=username,
                display_name=data.get("display_name", ""),
                bio=data.get("bio", ""),
                follower_count=data.get("follower_count", 0),
                following_count=data.get("following_count", 0),
                tweet_count=data.get("tweet_count", 0),
                source=data.get("source", ""),
                tags=data.get("tags", []),
                variables=data.get("variables", {}),
            )
            p.score = self.scorer.score(p)
            prospects.append(p)
        self.db.save_prospects_batch(prospects)
        return prospects

    def create_sequence(self, sequence_id: str, name: str,
                        steps: Optional[List[Dict[str, Any]]] = None) -> Sequence:
        """Create an outreach sequence"""
        seq = Sequence(sequence_id=sequence_id, name=name)
        if steps:
            for s in steps:
                step = SequenceStep(
                    step_type=SequenceStepType(s.get("step_type", "message")),
                    content=s.get("content", ""),
                    delay_hours=s.get("delay_hours", 0),
                    channel=OutreachChannel(s.get("channel", "dm")),
                    ab_variants=s.get("ab_variants", []),
                )
                seq.add_step(step)
        self.db.save_sequence(seq)
        return seq

    def enroll(self, prospect_id: str, sequence_id: str):
        """Enroll prospect in sequence"""
        self.executor.enroll(prospect_id, sequence_id)

    def get_pending_messages(self, sequence_id: str) -> List[Dict]:
        """Get messages ready to send"""
        return self.executor.get_next_messages(sequence_id)

    def mark_sent(self, prospect_id: str, sequence_id: str,
                  step_index: int, content: str,
                  channel: OutreachChannel = OutreachChannel.DM):
        """Mark message as sent"""
        self.executor.record_sent(prospect_id, sequence_id, step_index, content, channel)

    def process_reply(self, prospect_id: str, reply_text: str):
        """Process incoming reply"""
        self.executor.record_reply(prospect_id, reply_text)

    def get_funnel(self) -> Dict[str, Any]:
        """Get conversion funnel"""
        return self.analytics.funnel_report()

    def get_report(self) -> str:
        """Get text report"""
        return self.analytics.generate_text_report()

    def get_top_prospects(self, limit: int = 20) -> List[Dict]:
        """Get top-scored new prospects"""
        return self.db.get_top_prospects(limit)

    def close(self):
        self.db.close()
