"""
Compliance & Safety Module v1.0
Twitter合规与安全检查 — 内容审核 + Spam评分 + 操作频率合规 + 关注比检测

Features:
- ContentPolicy: banned words, safe words, link density check
- SpamScorer: multi-dimensional spam scoring (0-100)
- ComplianceChecker: rate compliance, following ratio, content policy
- Violation history tracking with SQLite persistence
- Compliance report generation (text/json)
"""

import re
import time
import json
import sqlite3
import threading
from collections import Counter
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple, Set


class ViolationType(Enum):
    BANNED_WORD = "banned_word"
    SPAM = "spam"
    LINK_DENSITY = "link_density"
    HASHTAG_SPAM = "hashtag_spam"
    MENTION_SPAM = "mention_spam"
    CAPS_ABUSE = "caps_abuse"
    RATE_LIMIT = "rate_limit"
    FOLLOWING_RATIO = "following_ratio"
    DUPLICATE_CONTENT = "duplicate_content"
    SHORT_CONTENT = "short_content"


class Severity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class Violation:
    """违规记录"""
    type: ViolationType
    severity: Severity
    description: str
    detail: str = ""
    score_impact: float = 0.0


@dataclass
class ComplianceResult:
    """合规检查结果"""
    passed: bool
    score: float  # 0-100, higher = cleaner
    violations: List[Violation] = field(default_factory=list)
    checked_at: str = ""

    def __post_init__(self):
        if not self.checked_at:
            self.checked_at = datetime.now(timezone.utc).isoformat()


class ContentPolicy:
    """内容策略管理器"""

    # 默认敏感词(通用)
    DEFAULT_BANNED = {
        "scam", "free money", "guaranteed profit", "click here now",
        "act now", "limited time", "100% free", "no risk",
        "earn $$", "make money fast", "double your",
    }

    def __init__(self):
        self._banned_words: Set[str] = set(self.DEFAULT_BANNED)
        self._safe_words: Set[str] = set()
        self._banned_patterns: List[re.Pattern] = []

    def add_banned_words(self, words: List[str]):
        """添加敏感词"""
        self._banned_words.update(w.lower() for w in words)

    def remove_banned_words(self, words: List[str]):
        """移除敏感词"""
        for w in words:
            self._banned_words.discard(w.lower())

    def add_safe_words(self, words: List[str]):
        """添加安全词(白名单)"""
        self._safe_words.update(w.lower() for w in words)

    def add_banned_pattern(self, pattern: str):
        """添加敏感正则"""
        try:
            self._banned_patterns.append(re.compile(pattern, re.IGNORECASE))
        except re.error:
            pass

    def check(self, text: str) -> List[Violation]:
        """检查内容合规"""
        violations = []
        text_lower = text.lower()

        # 敏感词检查
        for word in self._banned_words:
            if word in text_lower:
                # 检查是否在安全词中
                is_safe = any(safe in text_lower for safe in self._safe_words if word in safe)
                if not is_safe:
                    violations.append(Violation(
                        type=ViolationType.BANNED_WORD,
                        severity=Severity.HIGH,
                        description=f"Banned word detected: '{word}'",
                        detail=word,
                        score_impact=15.0,
                    ))

        # 正则检查
        for pattern in self._banned_patterns:
            m = pattern.search(text)
            if m:
                violations.append(Violation(
                    type=ViolationType.BANNED_WORD,
                    severity=Severity.MEDIUM,
                    description=f"Banned pattern matched: '{m.group(0)}'",
                    detail=m.group(0),
                    score_impact=10.0,
                ))

        return violations

    @property
    def banned_count(self) -> int:
        return len(self._banned_words)

    @property
    def safe_count(self) -> int:
        return len(self._safe_words)


class SpamScorer:
    """Spam评分器 (0-100, 越高越可能是spam)"""

    # Twitter limits
    MAX_HASHTAGS = 5
    MAX_MENTIONS = 5
    MAX_LINKS = 2
    MAX_CAPS_RATIO = 0.5
    MIN_CONTENT_LENGTH = 10

    def score(self, text: str) -> Tuple[float, List[Violation]]:
        """计算spam分数"""
        violations = []
        total_score = 0.0

        if not text.strip():
            return 100.0, [Violation(
                type=ViolationType.SHORT_CONTENT,
                severity=Severity.HIGH,
                description="Empty content",
                score_impact=100.0,
            )]

        # 1. 标签堆砌检测
        hashtags = re.findall(r"#\w+", text)
        if len(hashtags) > self.MAX_HASHTAGS:
            impact = min((len(hashtags) - self.MAX_HASHTAGS) * 5, 25)
            total_score += impact
            violations.append(Violation(
                type=ViolationType.HASHTAG_SPAM,
                severity=Severity.MEDIUM,
                description=f"Too many hashtags: {len(hashtags)} (max {self.MAX_HASHTAGS})",
                detail=str(len(hashtags)),
                score_impact=impact,
            ))

        # 2. @mention轰炸
        mentions = re.findall(r"@\w+", text)
        if len(mentions) > self.MAX_MENTIONS:
            impact = min((len(mentions) - self.MAX_MENTIONS) * 5, 25)
            total_score += impact
            violations.append(Violation(
                type=ViolationType.MENTION_SPAM,
                severity=Severity.MEDIUM,
                description=f"Too many mentions: {len(mentions)} (max {self.MAX_MENTIONS})",
                detail=str(len(mentions)),
                score_impact=impact,
            ))

        # 3. 链接密度
        links = re.findall(r"https?://\S+", text)
        if len(links) > self.MAX_LINKS:
            impact = min((len(links) - self.MAX_LINKS) * 10, 30)
            total_score += impact
            violations.append(Violation(
                type=ViolationType.LINK_DENSITY,
                severity=Severity.HIGH,
                description=f"Too many links: {len(links)} (max {self.MAX_LINKS})",
                detail=str(len(links)),
                score_impact=impact,
            ))

        # 链接占比
        link_chars = sum(len(l) for l in links)
        if len(text) > 0:
            link_ratio = link_chars / len(text)
            if link_ratio > 0.5:
                impact = 15.0
                total_score += impact
                violations.append(Violation(
                    type=ViolationType.LINK_DENSITY,
                    severity=Severity.MEDIUM,
                    description=f"High link density: {link_ratio:.1%}",
                    score_impact=impact,
                ))

        # 4. 大写比例
        alpha_chars = [c for c in text if c.isalpha()]
        if alpha_chars:
            caps_ratio = sum(1 for c in alpha_chars if c.isupper()) / len(alpha_chars)
            if caps_ratio > self.MAX_CAPS_RATIO:
                impact = min((caps_ratio - self.MAX_CAPS_RATIO) * 40, 20)
                total_score += impact
                violations.append(Violation(
                    type=ViolationType.CAPS_ABUSE,
                    severity=Severity.LOW,
                    description=f"High caps ratio: {caps_ratio:.1%}",
                    score_impact=impact,
                ))

        # 5. 内容太短
        clean_text = re.sub(r"[@#]\w+|https?://\S+", "", text).strip()
        if len(clean_text) < self.MIN_CONTENT_LENGTH:
            impact = 10.0
            total_score += impact
            violations.append(Violation(
                type=ViolationType.SHORT_CONTENT,
                severity=Severity.LOW,
                description=f"Content too short after removing links/tags: {len(clean_text)} chars",
                score_impact=impact,
            ))

        # 6. 重复字符检测
        if len(text) > 20:
            char_counts = Counter(text.lower())
            most_common_ratio = char_counts.most_common(1)[0][1] / len(text) if text else 0
            if most_common_ratio > 0.3 and text[0] != " ":
                impact = 10.0
                total_score += impact
                violations.append(Violation(
                    type=ViolationType.DUPLICATE_CONTENT,
                    severity=Severity.LOW,
                    description=f"Repetitive character pattern detected",
                    score_impact=impact,
                ))

        return min(total_score, 100.0), violations


class ComplianceChecker:
    """合规检查器"""

    # Twitter rate limits (approximate public API)
    RATE_LIMITS = {
        "tweets_per_hour": 50,
        "follows_per_day": 400,
        "likes_per_day": 1000,
        "dms_per_day": 500,
        "retweets_per_hour": 50,
    }

    FOLLOWING_RATIO_LIMITS = {
        "max_ratio_below_5k": 1.1,   # following/followers < 1.1 when followers < 5000
        "max_ratio_above_5k": 1.0,   # following/followers < 1.0 when followers >= 5000
        "safe_following": 5000,       # absolute following cap for unverified
    }

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._policy = ContentPolicy()
        self._scorer = SpamScorer()
        self._init_db()

    def _init_db(self):
        with self._lock:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS violation_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id TEXT,
                    type TEXT,
                    severity TEXT,
                    description TEXT,
                    detail TEXT,
                    score_impact REAL,
                    checked_at TEXT
                );
                CREATE TABLE IF NOT EXISTS rate_tracking (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id TEXT,
                    action_type TEXT,
                    timestamp TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_rate_account ON rate_tracking(account_id, action_type, timestamp);
            """)
            self._conn.commit()

    @property
    def policy(self) -> ContentPolicy:
        return self._policy

    @property
    def scorer(self) -> SpamScorer:
        return self._scorer

    def check_content(self, text: str) -> ComplianceResult:
        """全面内容合规检查"""
        violations = []

        # 内容策略检查
        violations.extend(self._policy.check(text))

        # Spam评分
        spam_score, spam_violations = self._scorer.score(text)
        if spam_score > 50:
            violations.extend(spam_violations)

        # 计算综合分数 (100 = 完全合规)
        total_impact = sum(v.score_impact for v in violations)
        score = max(0.0, 100.0 - total_impact)

        return ComplianceResult(
            passed=score >= 50.0,
            score=score,
            violations=violations,
        )

    def record_action(self, account_id: str, action_type: str):
        """记录操作"""
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self._conn.execute(
                "INSERT INTO rate_tracking (account_id, action_type, timestamp) VALUES (?, ?, ?)",
                (account_id, action_type, now)
            )
            self._conn.commit()

    def check_rate_compliance(self, account_id: str) -> ComplianceResult:
        """检查操作频率合规"""
        violations = []
        now = datetime.now(timezone.utc)
        hour_ago = (now - timedelta(hours=1)).isoformat()
        day_ago = (now - timedelta(days=1)).isoformat()

        # 每小时推文数
        tweet_count = self._conn.execute(
            "SELECT COUNT(*) as c FROM rate_tracking WHERE account_id = ? AND action_type = 'tweet' AND timestamp > ?",
            (account_id, hour_ago)
        ).fetchone()["c"]
        limit = self.RATE_LIMITS["tweets_per_hour"]
        if tweet_count > limit:
            violations.append(Violation(
                type=ViolationType.RATE_LIMIT,
                severity=Severity.CRITICAL,
                description=f"Tweet rate exceeded: {tweet_count}/{limit} per hour",
                detail=f"tweets_per_hour:{tweet_count}",
                score_impact=30.0,
            ))

        # 每日关注数
        follow_count = self._conn.execute(
            "SELECT COUNT(*) as c FROM rate_tracking WHERE account_id = ? AND action_type = 'follow' AND timestamp > ?",
            (account_id, day_ago)
        ).fetchone()["c"]
        limit = self.RATE_LIMITS["follows_per_day"]
        if follow_count > limit:
            violations.append(Violation(
                type=ViolationType.RATE_LIMIT,
                severity=Severity.HIGH,
                description=f"Follow rate exceeded: {follow_count}/{limit} per day",
                detail=f"follows_per_day:{follow_count}",
                score_impact=25.0,
            ))

        # 每日点赞数
        like_count = self._conn.execute(
            "SELECT COUNT(*) as c FROM rate_tracking WHERE account_id = ? AND action_type = 'like' AND timestamp > ?",
            (account_id, day_ago)
        ).fetchone()["c"]
        limit = self.RATE_LIMITS["likes_per_day"]
        if like_count > limit:
            violations.append(Violation(
                type=ViolationType.RATE_LIMIT,
                severity=Severity.MEDIUM,
                description=f"Like rate exceeded: {like_count}/{limit} per day",
                detail=f"likes_per_day:{like_count}",
                score_impact=15.0,
            ))

        total_impact = sum(v.score_impact for v in violations)
        score = max(0.0, 100.0 - total_impact)

        return ComplianceResult(passed=len(violations) == 0, score=score, violations=violations)

    def check_following_ratio(self, followers: int, following: int) -> ComplianceResult:
        """检查关注比合规"""
        violations = []

        if followers == 0:
            ratio = float(following) if following > 0 else 0.0
        else:
            ratio = following / followers

        if followers < 5000:
            max_ratio = self.FOLLOWING_RATIO_LIMITS["max_ratio_below_5k"]
        else:
            max_ratio = self.FOLLOWING_RATIO_LIMITS["max_ratio_above_5k"]

        if ratio > max_ratio and following > 100:
            violations.append(Violation(
                type=ViolationType.FOLLOWING_RATIO,
                severity=Severity.MEDIUM,
                description=f"Following ratio too high: {ratio:.2f} (max {max_ratio})",
                detail=f"following={following},followers={followers},ratio={ratio:.2f}",
                score_impact=20.0,
            ))

        if following > self.FOLLOWING_RATIO_LIMITS["safe_following"]:
            violations.append(Violation(
                type=ViolationType.FOLLOWING_RATIO,
                severity=Severity.LOW,
                description=f"Following count high: {following} (safe limit: {self.FOLLOWING_RATIO_LIMITS['safe_following']})",
                score_impact=10.0,
            ))

        total_impact = sum(v.score_impact for v in violations)
        score = max(0.0, 100.0 - total_impact)
        return ComplianceResult(passed=len(violations) == 0, score=score, violations=violations)

    def add_banned_words(self, words: List[str]):
        """添加敏感词"""
        self._policy.add_banned_words(words)

    def add_safe_words(self, words: List[str]):
        """添加安全词"""
        self._policy.add_safe_words(words)

    def record_violation(self, account_id: str, violation: Violation):
        """记录违规"""
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self._conn.execute(
                """INSERT INTO violation_history
                   (account_id, type, severity, description, detail, score_impact, checked_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (account_id, violation.type.value, violation.severity.value,
                 violation.description, violation.detail, violation.score_impact, now)
            )
            self._conn.commit()

    def get_violation_history(self, account_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """获取违规历史"""
        if account_id:
            rows = self._conn.execute(
                "SELECT * FROM violation_history WHERE account_id = ? ORDER BY id DESC LIMIT ?",
                (account_id, limit)
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM violation_history ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def generate_compliance_report(self, account_id: str, format: str = "text") -> str:
        """生成合规报告"""
        violations = self.get_violation_history(account_id)
        total = len(violations)

        by_type = Counter(v["type"] for v in violations)
        by_severity = Counter(v["severity"] for v in violations)
        avg_impact = sum(v["score_impact"] for v in violations) / total if total else 0

        report_data = {
            "account_id": account_id,
            "total_violations": total,
            "by_type": dict(by_type),
            "by_severity": dict(by_severity),
            "avg_score_impact": round(avg_impact, 2),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

        if format == "json":
            return json.dumps(report_data, indent=2)

        lines = [
            f"═══ Compliance Report: {account_id} ═══",
            f"Total Violations: {total}",
            f"Average Impact: {avg_impact:.1f}",
            "",
            "By Type:",
        ]
        for t, c in by_type.most_common():
            lines.append(f"  {t}: {c}")
        lines.append("")
        lines.append("By Severity:")
        for s, c in by_severity.most_common():
            lines.append(f"  {s}: {c}")

        return "\n".join(lines)

    def close(self):
        self._conn.close()
