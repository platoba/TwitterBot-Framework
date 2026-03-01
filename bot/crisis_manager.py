"""
Crisis Manager v1.0
品牌危机检测与响应 — 负面情绪飙升检测 + 阈值告警 + 预置响应模板 + 升级流程 + 危机后分析

Features:
- CrisisAlert: real-time negative mention spike detection
- SentimentThreshold: configurable alert thresholds per keyword
- ResponseTemplate: pre-built crisis response templates
- EscalationWorkflow: multi-level escalation (auto → team → executive)
- CrisisTimeline: full event timeline tracking
- PostCrisisAnalysis: impact assessment + recovery metrics
"""

import json
import math
import sqlite3
import threading
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any, Callable


class CrisisSeverity(Enum):
    """危机严重级别"""
    LOW = "low"              # Minor negative trend
    MEDIUM = "medium"        # Notable spike in negativity
    HIGH = "high"            # Significant brand threat
    CRITICAL = "critical"    # Full crisis — immediate action required


class CrisisStatus(Enum):
    """危机状态"""
    DETECTED = "detected"
    ACKNOWLEDGED = "acknowledged"
    RESPONDING = "responding"
    MONITORING = "monitoring"
    ESCALATED = "escalated"
    RESOLVED = "resolved"
    POST_MORTEM = "post_mortem"


class EscalationLevel(Enum):
    """升级级别"""
    AUTO = "auto"            # Automated response
    TEAM = "team"            # Team notification
    MANAGER = "manager"      # Management alert
    EXECUTIVE = "executive"  # C-level escalation
    EXTERNAL = "external"    # PR agency / legal


class ResponseTone(Enum):
    """响应语气"""
    EMPATHETIC = "empathetic"
    FACTUAL = "factual"
    APOLOGETIC = "apologetic"
    PROFESSIONAL = "professional"
    TRANSPARENT = "transparent"


@dataclass
class Mention:
    """提及记录"""
    tweet_id: str
    author_id: str
    author_username: str
    text: str
    sentiment_score: float  # -1.0 to 1.0
    created_at: str
    like_count: int = 0
    retweet_count: int = 0
    reply_count: int = 0
    reach: int = 0
    keywords: List[str] = field(default_factory=list)
    is_influencer: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def virality_score(self) -> float:
        """病毒传播分数"""
        return (
            self.like_count * 1.0 +
            self.retweet_count * 3.0 +
            self.reply_count * 2.0 +
            self.reach * 0.01
        )

    @property
    def is_negative(self) -> bool:
        return self.sentiment_score < -0.3

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CrisisEvent:
    """危机事件"""
    event_id: str
    title: str
    severity: CrisisSeverity
    status: CrisisStatus = CrisisStatus.DETECTED
    escalation_level: EscalationLevel = EscalationLevel.AUTO
    trigger_mention: Optional[Mention] = None
    keywords: List[str] = field(default_factory=list)
    mention_count: int = 0
    negative_count: int = 0
    peak_virality: float = 0.0
    avg_sentiment: float = 0.0
    detected_at: str = ""
    acknowledged_at: str = ""
    resolved_at: str = ""
    assigned_to: str = ""
    notes: List[str] = field(default_factory=list)
    responses_sent: int = 0
    timeline: List[Dict[str, Any]] = field(default_factory=list)

    def __post_init__(self):
        if not self.detected_at:
            self.detected_at = datetime.now(timezone.utc).isoformat()
        self._add_timeline("Crisis detected", f"Severity: {self.severity.value}")

    def _add_timeline(self, action: str, detail: str = ""):
        self.timeline.append({
            "action": action,
            "detail": detail,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def acknowledge(self, assigned_to: str = ""):
        self.status = CrisisStatus.ACKNOWLEDGED
        self.acknowledged_at = datetime.now(timezone.utc).isoformat()
        self.assigned_to = assigned_to
        self._add_timeline("Acknowledged", f"Assigned to: {assigned_to}")

    def escalate(self, level: EscalationLevel, reason: str = ""):
        self.escalation_level = level
        self.status = CrisisStatus.ESCALATED
        self._add_timeline("Escalated", f"Level: {level.value}, Reason: {reason}")

    def respond(self, response_text: str):
        self.status = CrisisStatus.RESPONDING
        self.responses_sent += 1
        self._add_timeline("Response sent", response_text[:100])

    def resolve(self, notes: str = ""):
        self.status = CrisisStatus.RESOLVED
        self.resolved_at = datetime.now(timezone.utc).isoformat()
        if notes:
            self.notes.append(notes)
        self._add_timeline("Resolved", notes)

    @property
    def duration_minutes(self) -> float:
        """危机持续时间（分钟）"""
        start = datetime.fromisoformat(self.detected_at)
        if self.resolved_at:
            end = datetime.fromisoformat(self.resolved_at)
        else:
            end = datetime.now(timezone.utc)
        return (end - start).total_seconds() / 60

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "event_id": self.event_id,
            "title": self.title,
            "severity": self.severity.value,
            "status": self.status.value,
            "escalation_level": self.escalation_level.value,
            "keywords": self.keywords,
            "mention_count": self.mention_count,
            "negative_count": self.negative_count,
            "peak_virality": round(self.peak_virality, 2),
            "avg_sentiment": round(self.avg_sentiment, 4),
            "detected_at": self.detected_at,
            "acknowledged_at": self.acknowledged_at,
            "resolved_at": self.resolved_at,
            "assigned_to": self.assigned_to,
            "responses_sent": self.responses_sent,
            "duration_minutes": round(self.duration_minutes, 1),
            "timeline": self.timeline,
            "notes": self.notes,
        }
        if self.trigger_mention:
            d["trigger_mention"] = self.trigger_mention.to_dict()
        return d


@dataclass
class AlertThreshold:
    """告警阈值配置"""
    keyword: str
    negative_count_1h: int = 10     # 1小时内负面提及
    negative_count_24h: int = 50    # 24小时内负面提及
    sentiment_floor: float = -0.5   # 平均情绪下限
    virality_threshold: float = 100  # 单条传播阈值
    influencer_mentions: int = 2    # 大V提及数
    severity_override: Optional[CrisisSeverity] = None


# Built-in response templates
RESPONSE_TEMPLATES: Dict[str, Dict[str, str]] = {
    "product_issue": {
        ResponseTone.EMPATHETIC.value: (
            "We hear you and understand your frustration with {issue}. "
            "Our team is actively investigating this and we'll share an update soon. "
            "Please DM us your details so we can help directly."
        ),
        ResponseTone.FACTUAL.value: (
            "We're aware of reports regarding {issue}. "
            "Our engineering team identified the cause and a fix is being deployed. "
            "Expected resolution: {eta}."
        ),
        ResponseTone.APOLOGETIC.value: (
            "We sincerely apologize for the experience with {issue}. "
            "This is not the standard we hold ourselves to. "
            "We're making it right — please check your DMs for next steps."
        ),
    },
    "service_outage": {
        ResponseTone.FACTUAL.value: (
            "We're experiencing a service disruption affecting {affected_area}. "
            "Our team is working to restore full functionality. "
            "Current status: {status}. Updates at {status_page}."
        ),
        ResponseTone.TRANSPARENT.value: (
            "Full transparency: {service} is down due to {cause}. "
            "We take this seriously. Here's what happened and what we're doing: {details}. "
            "ETA for resolution: {eta}."
        ),
    },
    "pr_scandal": {
        ResponseTone.PROFESSIONAL.value: (
            "We're aware of the concerns raised about {topic}. "
            "We take these matters seriously and are conducting a thorough review. "
            "We'll share our findings and any actions taken."
        ),
        ResponseTone.TRANSPARENT.value: (
            "Addressing the conversation around {topic}: "
            "Here are the facts as we know them: {facts}. "
            "We commit to {commitment} and will keep you updated."
        ),
    },
    "misinformation": {
        ResponseTone.FACTUAL.value: (
            "We've seen claims about {claim}. "
            "To set the record straight: {correction}. "
            "For verified information, please refer to {source}."
        ),
    },
    "customer_complaint": {
        ResponseTone.EMPATHETIC.value: (
            "We're sorry to hear about your experience, @{username}. "
            "That's not what we want for our customers. "
            "Please DM us — we'd love to make this right."
        ),
    },
}


class CrisisDB:
    """危机管理数据库"""

    def __init__(self, db_path: str = "crisis_manager.db"):
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path)
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
        return self._local.conn

    def _init_db(self):
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS mentions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tweet_id TEXT UNIQUE,
                author_id TEXT,
                author_username TEXT,
                text TEXT,
                sentiment_score REAL,
                created_at TEXT,
                like_count INTEGER DEFAULT 0,
                retweet_count INTEGER DEFAULT 0,
                reply_count INTEGER DEFAULT 0,
                reach INTEGER DEFAULT 0,
                keywords TEXT DEFAULT '[]',
                is_influencer INTEGER DEFAULT 0,
                virality_score REAL DEFAULT 0,
                collected_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS crisis_events (
                event_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                severity TEXT NOT NULL,
                status TEXT DEFAULT 'detected',
                escalation_level TEXT DEFAULT 'auto',
                keywords TEXT DEFAULT '[]',
                mention_count INTEGER DEFAULT 0,
                negative_count INTEGER DEFAULT 0,
                peak_virality REAL DEFAULT 0,
                avg_sentiment REAL DEFAULT 0,
                detected_at TEXT,
                acknowledged_at TEXT,
                resolved_at TEXT,
                assigned_to TEXT DEFAULT '',
                notes TEXT DEFAULT '[]',
                responses_sent INTEGER DEFAULT 0,
                timeline TEXT DEFAULT '[]',
                trigger_tweet_id TEXT
            );

            CREATE TABLE IF NOT EXISTS alert_thresholds (
                keyword TEXT PRIMARY KEY,
                negative_count_1h INTEGER DEFAULT 10,
                negative_count_24h INTEGER DEFAULT 50,
                sentiment_floor REAL DEFAULT -0.5,
                virality_threshold REAL DEFAULT 100,
                influencer_mentions INTEGER DEFAULT 2,
                severity_override TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_mention_sentiment
                ON mentions(sentiment_score);
            CREATE INDEX IF NOT EXISTS idx_mention_created
                ON mentions(created_at);
            CREATE INDEX IF NOT EXISTS idx_crisis_status
                ON crisis_events(status);
        """)
        conn.commit()

    def save_mention(self, mention: Mention):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO mentions
            (tweet_id, author_id, author_username, text, sentiment_score,
             created_at, like_count, retweet_count, reply_count, reach,
             keywords, is_influencer, virality_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            mention.tweet_id, mention.author_id, mention.author_username,
            mention.text, mention.sentiment_score, mention.created_at,
            mention.like_count, mention.retweet_count, mention.reply_count,
            mention.reach, json.dumps(mention.keywords),
            int(mention.is_influencer), mention.virality_score,
        ))
        conn.commit()

    def save_mentions_batch(self, mentions: List[Mention]):
        conn = self._get_conn()
        for m in mentions:
            conn.execute("""
                INSERT OR REPLACE INTO mentions
                (tweet_id, author_id, author_username, text, sentiment_score,
                 created_at, like_count, retweet_count, reply_count, reach,
                 keywords, is_influencer, virality_score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                m.tweet_id, m.author_id, m.author_username,
                m.text, m.sentiment_score, m.created_at,
                m.like_count, m.retweet_count, m.reply_count,
                m.reach, json.dumps(m.keywords),
                int(m.is_influencer), m.virality_score,
            ))
        conn.commit()

    def get_negative_count(self, hours: int = 1,
                           keyword: Optional[str] = None) -> int:
        conn = self._get_conn()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        if keyword:
            row = conn.execute("""
                SELECT COUNT(*) FROM mentions
                WHERE sentiment_score < -0.3 AND created_at >= ?
                AND keywords LIKE ?
            """, (cutoff, f"%{keyword}%")).fetchone()
        else:
            row = conn.execute("""
                SELECT COUNT(*) FROM mentions
                WHERE sentiment_score < -0.3 AND created_at >= ?
            """, (cutoff,)).fetchone()
        return row[0]

    def get_avg_sentiment(self, hours: int = 24) -> float:
        conn = self._get_conn()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        row = conn.execute("""
            SELECT AVG(sentiment_score) FROM mentions
            WHERE created_at >= ?
        """, (cutoff,)).fetchone()
        return row[0] or 0.0

    def get_influencer_mentions(self, hours: int = 24) -> List[Dict]:
        conn = self._get_conn()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        rows = conn.execute("""
            SELECT * FROM mentions
            WHERE is_influencer = 1 AND sentiment_score < -0.3
            AND created_at >= ?
            ORDER BY virality_score DESC
        """, (cutoff,)).fetchall()
        return [dict(r) for r in rows]

    def get_top_viral_negative(self, hours: int = 24,
                                limit: int = 10) -> List[Dict]:
        conn = self._get_conn()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        rows = conn.execute("""
            SELECT * FROM mentions
            WHERE sentiment_score < -0.3 AND created_at >= ?
            ORDER BY virality_score DESC LIMIT ?
        """, (cutoff, limit)).fetchall()
        return [dict(r) for r in rows]

    def save_crisis(self, event: CrisisEvent):
        conn = self._get_conn()
        trigger_id = event.trigger_mention.tweet_id if event.trigger_mention else ""
        conn.execute("""
            INSERT OR REPLACE INTO crisis_events
            (event_id, title, severity, status, escalation_level,
             keywords, mention_count, negative_count, peak_virality,
             avg_sentiment, detected_at, acknowledged_at, resolved_at,
             assigned_to, notes, responses_sent, timeline, trigger_tweet_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event.event_id, event.title, event.severity.value,
            event.status.value, event.escalation_level.value,
            json.dumps(event.keywords), event.mention_count,
            event.negative_count, event.peak_virality, event.avg_sentiment,
            event.detected_at, event.acknowledged_at, event.resolved_at,
            event.assigned_to, json.dumps(event.notes),
            event.responses_sent, json.dumps(event.timeline),
            trigger_id,
        ))
        conn.commit()

    def get_active_crises(self) -> List[Dict]:
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT * FROM crisis_events
            WHERE status NOT IN ('resolved', 'post_mortem')
            ORDER BY
                CASE severity
                    WHEN 'critical' THEN 0
                    WHEN 'high' THEN 1
                    WHEN 'medium' THEN 2
                    WHEN 'low' THEN 3
                END
        """).fetchall()
        return [dict(r) for r in rows]

    def get_crisis(self, event_id: str) -> Optional[Dict]:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM crisis_events WHERE event_id = ?", (event_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_crisis_history(self, days: int = 90) -> List[Dict]:
        conn = self._get_conn()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = conn.execute("""
            SELECT * FROM crisis_events
            WHERE detected_at >= ?
            ORDER BY detected_at DESC
        """, (cutoff,)).fetchall()
        return [dict(r) for r in rows]

    def save_threshold(self, threshold: AlertThreshold):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO alert_thresholds
            (keyword, negative_count_1h, negative_count_24h,
             sentiment_floor, virality_threshold, influencer_mentions,
             severity_override)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            threshold.keyword, threshold.negative_count_1h,
            threshold.negative_count_24h, threshold.sentiment_floor,
            threshold.virality_threshold, threshold.influencer_mentions,
            threshold.severity_override.value if threshold.severity_override else None,
        ))
        conn.commit()

    def get_thresholds(self) -> List[AlertThreshold]:
        conn = self._get_conn()
        rows = conn.execute("SELECT * FROM alert_thresholds").fetchall()
        results = []
        for r in rows:
            sev = CrisisSeverity(r["severity_override"]) if r["severity_override"] else None
            results.append(AlertThreshold(
                keyword=r["keyword"],
                negative_count_1h=r["negative_count_1h"],
                negative_count_24h=r["negative_count_24h"],
                sentiment_floor=r["sentiment_floor"],
                virality_threshold=r["virality_threshold"],
                influencer_mentions=r["influencer_mentions"],
                severity_override=sev,
            ))
        return results

    def close(self):
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None


class SpikeDetector:
    """负面情绪飙升检测"""

    def __init__(self, db: CrisisDB):
        self.db = db

    def check_spikes(self, thresholds: Optional[List[AlertThreshold]] = None
                     ) -> List[Dict[str, Any]]:
        """检查所有阈值是否触发"""
        if thresholds is None:
            thresholds = self.db.get_thresholds()

        alerts = []
        for t in thresholds:
            alert = self._check_threshold(t)
            if alert:
                alerts.append(alert)

        # Also check global metrics
        global_alert = self._check_global()
        if global_alert:
            alerts.append(global_alert)

        return alerts

    def _check_threshold(self, threshold: AlertThreshold) -> Optional[Dict[str, Any]]:
        """检查单个阈值"""
        neg_1h = self.db.get_negative_count(hours=1, keyword=threshold.keyword)
        neg_24h = self.db.get_negative_count(hours=24, keyword=threshold.keyword)

        triggered = False
        reasons = []

        if neg_1h >= threshold.negative_count_1h:
            triggered = True
            reasons.append(f"1h negative mentions: {neg_1h} >= {threshold.negative_count_1h}")

        if neg_24h >= threshold.negative_count_24h:
            triggered = True
            reasons.append(f"24h negative mentions: {neg_24h} >= {threshold.negative_count_24h}")

        if not triggered:
            return None

        severity = threshold.severity_override or self._infer_severity(neg_1h, neg_24h)
        return {
            "keyword": threshold.keyword,
            "severity": severity.value,
            "negative_1h": neg_1h,
            "negative_24h": neg_24h,
            "reasons": reasons,
            "detected_at": datetime.now(timezone.utc).isoformat(),
        }

    def _check_global(self) -> Optional[Dict[str, Any]]:
        """检查全局负面趋势"""
        avg_sentiment = self.db.get_avg_sentiment(hours=1)
        if avg_sentiment < -0.5:
            return {
                "keyword": "__global__",
                "severity": CrisisSeverity.HIGH.value,
                "avg_sentiment_1h": round(avg_sentiment, 4),
                "reasons": [f"Global avg sentiment dropped to {avg_sentiment:.4f}"],
                "detected_at": datetime.now(timezone.utc).isoformat(),
            }
        return None

    def _infer_severity(self, neg_1h: int, neg_24h: int) -> CrisisSeverity:
        """根据数据推断严重级别"""
        if neg_1h >= 50 or neg_24h >= 500:
            return CrisisSeverity.CRITICAL
        elif neg_1h >= 25 or neg_24h >= 200:
            return CrisisSeverity.HIGH
        elif neg_1h >= 10 or neg_24h >= 50:
            return CrisisSeverity.MEDIUM
        return CrisisSeverity.LOW


class ResponseEngine:
    """危机响应引擎"""

    def __init__(self, custom_templates: Optional[Dict] = None):
        self.templates = {**RESPONSE_TEMPLATES}
        if custom_templates:
            self.templates.update(custom_templates)

    def get_template(self, category: str,
                     tone: ResponseTone = ResponseTone.EMPATHETIC) -> Optional[str]:
        """获取响应模板"""
        cat_templates = self.templates.get(category, {})
        return cat_templates.get(tone.value)

    def render_response(self, category: str,
                        tone: ResponseTone = ResponseTone.EMPATHETIC,
                        **kwargs) -> str:
        """渲染响应文本"""
        template = self.get_template(category, tone)
        if not template:
            # Fallback to any available tone
            cat_templates = self.templates.get(category, {})
            if cat_templates:
                template = next(iter(cat_templates.values()))
            else:
                return f"We're aware of the situation regarding {kwargs.get('issue', 'this matter')} and are looking into it."

        try:
            return template.format(**kwargs)
        except KeyError:
            # Return template with unfilled placeholders marked
            return template

    def list_categories(self) -> List[str]:
        return list(self.templates.keys())

    def list_tones(self, category: str) -> List[str]:
        return list(self.templates.get(category, {}).keys())

    def add_template(self, category: str, tone: ResponseTone, template: str):
        """添加自定义模板"""
        if category not in self.templates:
            self.templates[category] = {}
        self.templates[category][tone.value] = template


class EscalationManager:
    """升级流程管理器"""

    DEFAULT_RULES = {
        CrisisSeverity.LOW: EscalationLevel.AUTO,
        CrisisSeverity.MEDIUM: EscalationLevel.TEAM,
        CrisisSeverity.HIGH: EscalationLevel.MANAGER,
        CrisisSeverity.CRITICAL: EscalationLevel.EXECUTIVE,
    }

    TIME_ESCALATION = {
        # If unresolved after N minutes, escalate
        30: EscalationLevel.TEAM,
        60: EscalationLevel.MANAGER,
        120: EscalationLevel.EXECUTIVE,
        240: EscalationLevel.EXTERNAL,
    }

    def __init__(self, rules: Optional[Dict[CrisisSeverity, EscalationLevel]] = None,
                 callbacks: Optional[Dict[EscalationLevel, Callable]] = None):
        self.rules = rules or self.DEFAULT_RULES
        self.callbacks = callbacks or {}

    def determine_level(self, event: CrisisEvent) -> EscalationLevel:
        """根据严重性和时间确定升级级别"""
        base_level = self.rules.get(event.severity, EscalationLevel.AUTO)

        # Time-based escalation
        if event.status not in (CrisisStatus.RESOLVED, CrisisStatus.POST_MORTEM):
            minutes = event.duration_minutes
            for threshold_min, level in sorted(self.TIME_ESCALATION.items(), reverse=True):
                if minutes >= threshold_min:
                    if self._level_rank(level) > self._level_rank(base_level):
                        base_level = level
                    break

        return base_level

    def should_escalate(self, event: CrisisEvent) -> bool:
        """是否需要升级"""
        recommended = self.determine_level(event)
        return self._level_rank(recommended) > self._level_rank(event.escalation_level)

    def escalate(self, event: CrisisEvent, reason: str = "") -> EscalationLevel:
        """执行升级"""
        new_level = self.determine_level(event)
        event.escalate(new_level, reason)

        # Fire callback if registered
        callback = self.callbacks.get(new_level)
        if callback:
            try:
                callback(event)
            except Exception:
                pass

        return new_level

    @staticmethod
    def _level_rank(level: EscalationLevel) -> int:
        ranks = {
            EscalationLevel.AUTO: 0,
            EscalationLevel.TEAM: 1,
            EscalationLevel.MANAGER: 2,
            EscalationLevel.EXECUTIVE: 3,
            EscalationLevel.EXTERNAL: 4,
        }
        return ranks.get(level, 0)


class PostCrisisAnalyzer:
    """危机后分析"""

    @classmethod
    def analyze(cls, event: CrisisEvent,
                mentions_during: Optional[List[Mention]] = None,
                mentions_after: Optional[List[Mention]] = None
                ) -> Dict[str, Any]:
        """生成危机后分析报告"""
        report = {
            "event_id": event.event_id,
            "title": event.title,
            "severity": event.severity.value,
            "duration_minutes": round(event.duration_minutes, 1),
            "response_time": cls._response_time(event),
            "escalation_path": cls._escalation_path(event),
            "total_mentions": event.mention_count,
            "negative_mentions": event.negative_count,
            "peak_virality": event.peak_virality,
            "responses_sent": event.responses_sent,
        }

        if mentions_during:
            report["during_crisis"] = {
                "avg_sentiment": round(
                    sum(m.sentiment_score for m in mentions_during) / len(mentions_during), 4
                ),
                "total_reach": sum(m.reach for m in mentions_during),
                "influencer_mentions": sum(1 for m in mentions_during if m.is_influencer),
                "top_viral": max(m.virality_score for m in mentions_during),
            }

        if mentions_after:
            report["post_crisis"] = {
                "avg_sentiment": round(
                    sum(m.sentiment_score for m in mentions_after) / len(mentions_after), 4
                ),
                "sentiment_recovery": cls._sentiment_recovery(mentions_during, mentions_after),
                "mention_volume_change": cls._volume_change(mentions_during, mentions_after),
            }

        report["lessons_learned"] = cls._generate_lessons(event, report)
        return report

    @classmethod
    def _response_time(cls, event: CrisisEvent) -> Optional[float]:
        """检测到确认的时间（分钟）"""
        if not event.acknowledged_at:
            return None
        detected = datetime.fromisoformat(event.detected_at)
        acknowledged = datetime.fromisoformat(event.acknowledged_at)
        return round((acknowledged - detected).total_seconds() / 60, 1)

    @classmethod
    def _escalation_path(cls, event: CrisisEvent) -> List[str]:
        """升级路径"""
        path = []
        for entry in event.timeline:
            if "Escalated" in entry.get("action", ""):
                path.append(entry["detail"])
        return path

    @classmethod
    def _sentiment_recovery(cls,
                             during: Optional[List[Mention]],
                             after: Optional[List[Mention]]) -> float:
        """情绪恢复率"""
        if not during or not after:
            return 0.0
        avg_during = sum(m.sentiment_score for m in during) / len(during)
        avg_after = sum(m.sentiment_score for m in after) / len(after)
        if avg_during == 0:
            return 0.0
        return round((avg_after - avg_during) / abs(avg_during) * 100, 2)

    @classmethod
    def _volume_change(cls,
                       during: Optional[List[Mention]],
                       after: Optional[List[Mention]]) -> float:
        """提及量变化百分比"""
        if not during:
            return 0.0
        vol_during = len(during)
        vol_after = len(after) if after else 0
        return round((vol_after - vol_during) / vol_during * 100, 2)

    @classmethod
    def _generate_lessons(cls, event: CrisisEvent,
                          report: Dict[str, Any]) -> List[str]:
        """自动生成教训总结"""
        lessons = []

        # Response time
        rt = report.get("response_time")
        if rt is not None:
            if rt > 60:
                lessons.append(f"Response time was {rt:.0f} min — target < 30 min for {event.severity.value} severity")
            elif rt < 15:
                lessons.append("Fast response time — maintain this speed")

        # Escalation
        if event.escalation_level in (EscalationLevel.EXECUTIVE, EscalationLevel.EXTERNAL):
            lessons.append("Crisis required high-level escalation — review prevention measures")

        # Volume
        if event.negative_count > 100:
            lessons.append("High negative volume — consider proactive monitoring keywords")

        # Recovery
        post = report.get("post_crisis", {})
        recovery = post.get("sentiment_recovery", 0)
        if recovery > 50:
            lessons.append("Strong sentiment recovery — response strategy was effective")
        elif recovery < 0:
            lessons.append("Sentiment continued declining post-crisis — review response strategy")

        if not lessons:
            lessons.append("No critical issues identified in crisis handling")

        return lessons


class CrisisManager:
    """统一危机管理器"""

    def __init__(self, db_path: str = "crisis_manager.db",
                 custom_templates: Optional[Dict] = None):
        self.db = CrisisDB(db_path)
        self.spike_detector = SpikeDetector(self.db)
        self.response_engine = ResponseEngine(custom_templates)
        self.escalation_manager = EscalationManager()
        self._event_counter = 0

    def ingest_mention(self, mention: Mention):
        """接收提及"""
        self.db.save_mention(mention)

    def ingest_mentions(self, mentions: List[Mention]):
        """批量接收"""
        self.db.save_mentions_batch(mentions)

    def check_alerts(self) -> List[Dict[str, Any]]:
        """检查告警"""
        return self.spike_detector.check_spikes()

    def create_crisis(self, title: str, severity: CrisisSeverity,
                      trigger: Optional[Mention] = None,
                      keywords: Optional[List[str]] = None) -> CrisisEvent:
        """创建危机事件"""
        self._event_counter += 1
        event_id = f"CRISIS-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{self._event_counter:04d}"

        event = CrisisEvent(
            event_id=event_id,
            title=title,
            severity=severity,
            trigger_mention=trigger,
            keywords=keywords or [],
        )

        self.db.save_crisis(event)
        return event

    def get_response(self, category: str,
                     tone: ResponseTone = ResponseTone.EMPATHETIC,
                     **kwargs) -> str:
        """获取危机响应文本"""
        return self.response_engine.render_response(category, tone, **kwargs)

    def set_threshold(self, keyword: str, **kwargs) -> AlertThreshold:
        """设置告警阈值"""
        threshold = AlertThreshold(keyword=keyword, **kwargs)
        self.db.save_threshold(threshold)
        return threshold

    def get_active_crises(self) -> List[Dict]:
        """获取活跃危机"""
        return self.db.get_active_crises()

    def get_dashboard(self) -> Dict[str, Any]:
        """获取危机仪表板"""
        active = self.db.get_active_crises()
        avg_sentiment = self.db.get_avg_sentiment(hours=1)
        neg_1h = self.db.get_negative_count(hours=1)
        neg_24h = self.db.get_negative_count(hours=24)
        top_viral = self.db.get_top_viral_negative(hours=24, limit=5)

        return {
            "status": "crisis" if active else "normal",
            "active_crises": len(active),
            "crises": active,
            "sentiment_1h": round(avg_sentiment, 4),
            "negative_mentions_1h": neg_1h,
            "negative_mentions_24h": neg_24h,
            "top_viral_negative": top_viral,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    def close(self):
        self.db.close()
