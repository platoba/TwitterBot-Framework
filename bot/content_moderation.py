"""
内容安全 & 审核管道
发布前自动检测: 毒性/PII/品牌安全/链接安全/平台合规
"""

import hashlib
import json
import re
import sqlite3
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Any


class RiskLevel(Enum):
    """风险等级"""
    SAFE = "safe"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ViolationType(Enum):
    """违规类型"""
    TOXICITY = "toxicity"
    PII_LEAK = "pii_leak"
    BRAND_UNSAFE = "brand_unsafe"
    LINK_UNSAFE = "link_unsafe"
    SPAM = "spam"
    PLATFORM_VIOLATION = "platform_violation"
    SENSITIVE_TOPIC = "sensitive_topic"
    COPYRIGHT = "copyright"


@dataclass
class ModerationFlag:
    """审核标记"""
    violation_type: ViolationType
    risk_level: RiskLevel
    description: str
    matched_text: str = ""
    confidence: float = 1.0
    suggestion: str = ""

    def to_dict(self) -> Dict:
        d = asdict(self)
        d["violation_type"] = self.violation_type.value
        d["risk_level"] = self.risk_level.value
        return d


@dataclass
class ModerationResult:
    """审核结果"""
    content: str
    approved: bool
    overall_risk: RiskLevel
    flags: List[ModerationFlag] = field(default_factory=list)
    score: float = 1.0  # 0=最差, 1=最安全
    reviewed_at: str = ""
    auto_fix: Optional[str] = None  # 自动修复后的内容

    def to_dict(self) -> Dict:
        d = {
            "content": self.content[:100] + "..." if len(self.content) > 100 else self.content,
            "approved": self.approved,
            "overall_risk": self.overall_risk.value,
            "flags": [f.to_dict() for f in self.flags],
            "score": round(self.score, 3),
            "reviewed_at": self.reviewed_at,
        }
        if self.auto_fix:
            d["auto_fix"] = self.auto_fix
        return d


class ToxicityDetector:
    """毒性检测器 (基于关键词+模式)"""

    # 分级关键词库
    TOXIC_PATTERNS = {
        RiskLevel.CRITICAL: [
            r"\b(kill|murder|assassinate)\s+(yourself|himself|herself|them)\b",
            r"\b(bomb|terrorist|attack)\s+(threat|plan|target)\b",
            r"\bsuicid(e|al)\b",
        ],
        RiskLevel.HIGH: [
            r"\b(hate|despise)\s+(all|every)\s+\w+\b",
            r"\b(racist|sexist|bigot)\b",
            r"\bslur[s]?\b",
        ],
        RiskLevel.MEDIUM: [
            r"\b(stupid|idiot|moron|dumb)\b",
            r"\b(shut\s+up|stfu)\b",
            r"\b(loser|pathetic|worthless)\b",
            r"\b(trash|garbage)\s+(person|people|human)\b",
        ],
        RiskLevel.LOW: [
            r"\b(suck[s]?|crap|damn|hell)\b",
            r"\b(wtf|omfg)\b",
        ],
    }

    @classmethod
    def scan(cls, text: str) -> List[ModerationFlag]:
        flags = []
        text_lower = text.lower()
        for level, patterns in cls.TOXIC_PATTERNS.items():
            for pattern in patterns:
                matches = re.findall(pattern, text_lower)
                if matches:
                    matched = matches[0] if isinstance(matches[0], str) else " ".join(matches[0])
                    flags.append(ModerationFlag(
                        violation_type=ViolationType.TOXICITY,
                        risk_level=level,
                        description=f"Toxic content detected ({level.value})",
                        matched_text=matched,
                        confidence=0.85,
                        suggestion="Remove or rephrase toxic language",
                    ))
        return flags


class PIIDetector:
    """个人信息泄露检测器"""

    PATTERNS = {
        "email": (r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b", RiskLevel.HIGH),
        "phone_us": (r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b", RiskLevel.HIGH),
        "phone_cn": (r"\b(?:\+?86[-.\s]?)?1[3-9]\d{9}\b", RiskLevel.HIGH),
        "ssn": (r"\b\d{3}-\d{2}-\d{4}\b", RiskLevel.CRITICAL),
        "credit_card": (r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13})\b", RiskLevel.CRITICAL),
        "ip_address": (r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b", RiskLevel.MEDIUM),
        "passport": (r"\b[A-Z]{1,2}\d{6,9}\b", RiskLevel.HIGH),
        "id_card_cn": (r"\b\d{17}[\dXx]\b", RiskLevel.CRITICAL),
    }

    @classmethod
    def scan(cls, text: str) -> List[ModerationFlag]:
        flags = []
        for pii_type, (pattern, level) in cls.PATTERNS.items():
            matches = re.findall(pattern, text)
            for m in matches:
                masked = cls._mask(m)
                flags.append(ModerationFlag(
                    violation_type=ViolationType.PII_LEAK,
                    risk_level=level,
                    description=f"PII detected: {pii_type}",
                    matched_text=masked,
                    confidence=0.9,
                    suggestion=f"Remove {pii_type} before posting",
                ))
        return flags

    @classmethod
    def _mask(cls, text: str) -> str:
        """遮罩敏感信息"""
        if len(text) <= 4:
            return "***"
        return text[:2] + "*" * (len(text) - 4) + text[-2:]

    @classmethod
    def redact(cls, text: str) -> str:
        """自动脱敏"""
        result = text
        for pii_type, (pattern, _) in cls.PATTERNS.items():
            result = re.sub(pattern, f"[{pii_type.upper()}_REDACTED]", result)
        return result


class BrandSafetyChecker:
    """品牌安全关键词检查"""

    DEFAULT_UNSAFE_TOPICS = [
        "drugs", "gambling", "violence", "weapons", "adult",
        "tobacco", "alcohol abuse", "hate speech", "terrorism",
        "child exploitation", "self harm",
    ]

    DEFAULT_UNSAFE_KEYWORDS = {
        RiskLevel.HIGH: [
            "cocaine", "heroin", "meth", "illegal drugs",
            "child porn", "underage", "exploit",
        ],
        RiskLevel.MEDIUM: [
            "gambling", "casino", "bet365", "slot machine",
            "gun", "rifle", "ammunition", "weapon",
            "nude", "porn", "xxx",
        ],
        RiskLevel.LOW: [
            "beer", "wine", "whiskey", "vodka",
            "cigarette", "vape", "smoking",
            "controversial", "scandal",
        ],
    }

    def __init__(self, custom_keywords: Optional[Dict[str, List[str]]] = None,
                 brand_name: str = ""):
        self.keywords = dict(self.DEFAULT_UNSAFE_KEYWORDS)
        if custom_keywords:
            for level_str, words in custom_keywords.items():
                level = RiskLevel(level_str) if isinstance(level_str, str) else level_str
                if level in self.keywords:
                    self.keywords[level].extend(words)
                else:
                    self.keywords[level] = words
        self.brand_name = brand_name

    def scan(self, text: str) -> List[ModerationFlag]:
        flags = []
        text_lower = text.lower()
        for level, keywords in self.keywords.items():
            for kw in keywords:
                if kw.lower() in text_lower:
                    flags.append(ModerationFlag(
                        violation_type=ViolationType.BRAND_UNSAFE,
                        risk_level=level,
                        description=f"Brand-unsafe keyword: '{kw}'",
                        matched_text=kw,
                        confidence=0.8,
                        suggestion=f"Remove or replace '{kw}' to maintain brand safety",
                    ))
        return flags


class SpamDetector:
    """垃圾内容检测"""

    SPAM_INDICATORS = {
        "excessive_hashtags": (r"(#\w+\s*){6,}", RiskLevel.MEDIUM,
                               "Too many hashtags (>5)"),
        "excessive_caps": (r"[A-Z]{10,}", RiskLevel.LOW,
                          "Excessive capitalization"),
        "repeated_chars": (r"(.)\1{4,}", RiskLevel.LOW,
                          "Repeated characters"),
        "money_bait": (r"\$\d{3,}.*(?:free|earn|make|win)", RiskLevel.HIGH,
                       "Money/earnings bait"),
        "url_flood": (r"(https?://\S+\s*){3,}", RiskLevel.MEDIUM,
                      "Too many URLs"),
        "follow_beg": (r"(?:follow|subscribe|like)\s+(?:me|my|us)\s+(?:back|for|to)", RiskLevel.LOW,
                       "Follow/subscribe begging"),
        "dm_bait": (r"(?:dm|message)\s+(?:me|us)\s+(?:for|to)\s+(?:free|earn|win|make)", RiskLevel.HIGH,
                    "DM bait/scam pattern"),
    }

    @classmethod
    def scan(cls, text: str) -> List[ModerationFlag]:
        flags = []
        text_lower = text.lower()
        for indicator, (pattern, level, desc) in cls.SPAM_INDICATORS.items():
            if re.search(pattern, text_lower if "A-Z" not in pattern else text):
                flags.append(ModerationFlag(
                    violation_type=ViolationType.SPAM,
                    risk_level=level,
                    description=desc,
                    matched_text=indicator,
                    confidence=0.75,
                    suggestion="Revise content to appear more authentic",
                ))
        return flags


class PlatformComplianceChecker:
    """Twitter/X平台合规检查"""

    RULES = {
        "max_length": 280,
        "max_hashtags": 10,
        "max_mentions": 10,
        "max_urls": 4,
        "reserved_words": ["twitter", "tweet", "retweet"],  # 不能冒充官方
    }

    @classmethod
    def scan(cls, text: str) -> List[ModerationFlag]:
        flags = []

        # 长度检查
        if len(text) > cls.RULES["max_length"]:
            flags.append(ModerationFlag(
                violation_type=ViolationType.PLATFORM_VIOLATION,
                risk_level=RiskLevel.MEDIUM,
                description=f"Tweet exceeds {cls.RULES['max_length']} chars ({len(text)})",
                matched_text=f"length={len(text)}",
                suggestion=f"Trim to {cls.RULES['max_length']} characters",
            ))

        # hashtag数量
        hashtags = re.findall(r"#\w+", text)
        if len(hashtags) > cls.RULES["max_hashtags"]:
            flags.append(ModerationFlag(
                violation_type=ViolationType.PLATFORM_VIOLATION,
                risk_level=RiskLevel.LOW,
                description=f"Too many hashtags ({len(hashtags)})",
                matched_text=str(len(hashtags)),
                suggestion=f"Use max {cls.RULES['max_hashtags']} hashtags",
            ))

        # mention数量
        mentions = re.findall(r"@\w+", text)
        if len(mentions) > cls.RULES["max_mentions"]:
            flags.append(ModerationFlag(
                violation_type=ViolationType.PLATFORM_VIOLATION,
                risk_level=RiskLevel.MEDIUM,
                description=f"Too many mentions ({len(mentions)})",
                matched_text=str(len(mentions)),
                suggestion=f"Use max {cls.RULES['max_mentions']} mentions",
            ))

        return flags


class LinkSafetyChecker:
    """链接安全检查"""

    SUSPICIOUS_TLDS = {".xyz", ".tk", ".ml", ".ga", ".cf", ".gq", ".top", ".work", ".click"}
    KNOWN_SHORTENERS = {"bit.ly", "tinyurl.com", "t.co", "goo.gl", "ow.ly", "is.gd",
                        "buff.ly", "rebrand.ly", "cutt.ly"}
    PHISHING_PATTERNS = [
        r"paypal.*login",
        r"google.*verify",
        r"apple.*confirm",
        r"bank.*update",
        r"amazon.*secure",
    ]

    @classmethod
    def scan(cls, text: str) -> List[ModerationFlag]:
        flags = []
        urls = re.findall(r"https?://([^\s]+)", text.lower())

        for url in urls:
            domain = url.split("/")[0]

            # 检查可疑TLD
            for tld in cls.SUSPICIOUS_TLDS:
                if domain.endswith(tld):
                    flags.append(ModerationFlag(
                        violation_type=ViolationType.LINK_UNSAFE,
                        risk_level=RiskLevel.MEDIUM,
                        description=f"Suspicious TLD: {tld}",
                        matched_text=domain,
                        confidence=0.6,
                        suggestion="Consider using a more trusted domain",
                    ))

            # 检查钓鱼模式
            for pattern in cls.PHISHING_PATTERNS:
                if re.search(pattern, url):
                    flags.append(ModerationFlag(
                        violation_type=ViolationType.LINK_UNSAFE,
                        risk_level=RiskLevel.HIGH,
                        description="Potential phishing URL",
                        matched_text=domain,
                        confidence=0.7,
                        suggestion="This URL matches a phishing pattern",
                    ))

        return flags


class ModerationStore:
    """审核记录持久化"""

    def __init__(self, db_path: str = "moderation.db"):
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
            CREATE TABLE IF NOT EXISTS moderation_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content_hash TEXT,
                content_preview TEXT,
                approved INTEGER,
                overall_risk TEXT,
                score REAL,
                flags_json TEXT,
                reviewed_at TEXT DEFAULT (datetime('now')),
                auto_fixed INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS moderation_stats (
                date TEXT PRIMARY KEY,
                total_scanned INTEGER DEFAULT 0,
                approved INTEGER DEFAULT 0,
                rejected INTEGER DEFAULT 0,
                auto_fixed INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_mod_risk
                ON moderation_log(overall_risk);
            CREATE INDEX IF NOT EXISTS idx_mod_date
                ON moderation_log(reviewed_at);
        """)
        conn.commit()

    def log_result(self, result: ModerationResult):
        conn = self._get_conn()
        content_hash = hashlib.sha256(result.content.encode()).hexdigest()[:16]
        preview = result.content[:200]
        conn.execute(
            "INSERT INTO moderation_log "
            "(content_hash, content_preview, approved, overall_risk, score, flags_json, auto_fixed) "
            "VALUES (?,?,?,?,?,?,?)",
            (content_hash, preview, int(result.approved),
             result.overall_risk.value, result.score,
             json.dumps([f.to_dict() for f in result.flags]),
             int(bool(result.auto_fix)))
        )
        # 更新日统计
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        conn.execute("""
            INSERT INTO moderation_stats (date, total_scanned, approved, rejected, auto_fixed)
            VALUES (?, 1, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                total_scanned = total_scanned + 1,
                approved = approved + excluded.approved,
                rejected = rejected + excluded.rejected,
                auto_fixed = auto_fixed + excluded.auto_fixed
        """, (today, int(result.approved), int(not result.approved),
              int(bool(result.auto_fix))))
        conn.commit()

    def get_stats(self, days: int = 7) -> Dict[str, Any]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM moderation_stats ORDER BY date DESC LIMIT ?",
            (days,)
        ).fetchall()
        return {
            "daily": [dict(r) for r in rows],
            "total": sum(r["total_scanned"] for r in rows),
            "approval_rate": (
                sum(r["approved"] for r in rows) /
                max(sum(r["total_scanned"] for r in rows), 1)
            ),
        }

    def close(self):
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None


class ContentModerator:
    """
    统一内容审核管道

    集成所有检测器, 输出综合审核结果
    支持自动修复 (脱敏PII等)
    """

    # 风险等级权重
    RISK_WEIGHTS = {
        RiskLevel.CRITICAL: 1.0,
        RiskLevel.HIGH: 0.7,
        RiskLevel.MEDIUM: 0.4,
        RiskLevel.LOW: 0.15,
        RiskLevel.SAFE: 0.0,
    }

    # 自动拒绝阈值
    AUTO_REJECT_SCORE = 0.3

    def __init__(self,
                 brand_checker: Optional[BrandSafetyChecker] = None,
                 store: Optional[ModerationStore] = None,
                 auto_fix: bool = False):
        self.toxicity = ToxicityDetector()
        self.pii = PIIDetector()
        self.brand = brand_checker or BrandSafetyChecker()
        self.spam = SpamDetector()
        self.compliance = PlatformComplianceChecker()
        self.link_safety = LinkSafetyChecker()
        self.store = store or ModerationStore()
        self.auto_fix = auto_fix

    def moderate(self, content: str) -> ModerationResult:
        """
        执行完整审核管道

        Returns: ModerationResult
        """
        all_flags: List[ModerationFlag] = []

        # 依次执行所有检测器
        all_flags.extend(ToxicityDetector.scan(content))
        all_flags.extend(PIIDetector.scan(content))
        all_flags.extend(self.brand.scan(content))
        all_flags.extend(SpamDetector.scan(content))
        all_flags.extend(PlatformComplianceChecker.scan(content))
        all_flags.extend(LinkSafetyChecker.scan(content))

        # 计算综合分数
        score = self._calculate_score(all_flags)
        overall_risk = self._determine_risk(all_flags)
        approved = score >= self.AUTO_REJECT_SCORE and overall_risk != RiskLevel.CRITICAL

        # 自动修复
        fixed = None
        if self.auto_fix and not approved:
            fixed = self._auto_fix(content, all_flags)
            if fixed and fixed != content:
                # 重新评估修复后的内容
                recheck = self._quick_check(fixed)
                if recheck >= self.AUTO_REJECT_SCORE:
                    approved = True

        result = ModerationResult(
            content=content,
            approved=approved,
            overall_risk=overall_risk,
            flags=all_flags,
            score=round(score, 4),
            reviewed_at=datetime.now(timezone.utc).isoformat(),
            auto_fix=fixed,
        )
        self.store.log_result(result)
        return result

    def _calculate_score(self, flags: List[ModerationFlag]) -> float:
        """基于flags计算安全分数 (0=最差, 1=安全)"""
        if not flags:
            return 1.0
        total_penalty = 0.0
        for f in flags:
            weight = self.RISK_WEIGHTS.get(f.risk_level, 0)
            total_penalty += weight * f.confidence
        return max(0.0, 1.0 - total_penalty)

    def _determine_risk(self, flags: List[ModerationFlag]) -> RiskLevel:
        """确定最高风险等级"""
        if not flags:
            return RiskLevel.SAFE
        risk_order = [RiskLevel.CRITICAL, RiskLevel.HIGH,
                      RiskLevel.MEDIUM, RiskLevel.LOW]
        for level in risk_order:
            if any(f.risk_level == level for f in flags):
                return level
        return RiskLevel.SAFE

    def _auto_fix(self, content: str, flags: List[ModerationFlag]) -> Optional[str]:
        """尝试自动修复"""
        fixed = content
        for f in flags:
            if f.violation_type == ViolationType.PII_LEAK:
                fixed = PIIDetector.redact(fixed)
        return fixed if fixed != content else None

    def _quick_check(self, content: str) -> float:
        """快速检查分数 (不记录)"""
        flags = []
        flags.extend(ToxicityDetector.scan(content))
        flags.extend(PIIDetector.scan(content))
        return self._calculate_score(flags)

    def moderate_batch(self, contents: List[str]) -> List[ModerationResult]:
        """批量审核"""
        return [self.moderate(c) for c in contents]

    def get_stats(self, days: int = 7) -> Dict[str, Any]:
        """获取审核统计"""
        return self.store.get_stats(days)
