"""
Viral Score Predictor v1.0
推文病毒传播预测引擎
基于历史数据的轻量级传播预测 + 内容优化建议
"""

import logging
import re
import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from bot.database import Database

logger = logging.getLogger(__name__)


# ── 内容特征权重 ──
FEATURE_WEIGHTS = {
    "has_hashtags": 1.2,
    "has_mention": 1.1,
    "has_url": 0.9,
    "has_emoji": 1.3,
    "has_question": 1.4,
    "has_number": 1.15,
    "has_thread_hook": 1.5,
    "optimal_length": 1.25,
    "has_cta": 1.35,
    "has_media_hint": 1.4,
}

# CTA关键词
CTA_PATTERNS = [
    r"\breply\b", r"\bcomment\b", r"\bshare\b", r"\bretweet\b", r"\brt\b",
    r"\bfollow\b", r"\blike\b", r"\bcheck out\b", r"\bclick\b", r"\blink\b",
    r"\bjoin\b", r"\bsubscribe\b", r"\bsign up\b", r"\bget\b", r"\btry\b",
    r"\bthread\b", r"\b👇\b", r"\b⬇️\b",
]

MEDIA_HINTS = [
    r"\b(pic|photo|image|screenshot|video|gif|meme)\b",
    r"📸|📷|🎥|🎬|📹",
]

THREAD_HOOKS = [
    r"^(1/|🧵|thread|a thread)",
    r"\bhere'?s? (what|how|why|the)\b",
    r"\bmost people don'?t\b",
    r"\byou (need|should|must|have to)\b",
    r"\b(hot take|unpopular opinion|controversial)\b",
]

ENGAGEMENT_TRIGGERS = [
    r"\b(agree|disagree)\b",
    r"\b(what do you think|thoughts\?|opinions\?)\b",
    r"\b(wrong|right|true|false)\b",
    r"\b(best|worst|top \d+)\b",
]


@dataclass
class ContentFeatures:
    """内容特征分析"""
    has_hashtags: bool = False
    hashtag_count: int = 0
    has_mention: bool = False
    mention_count: int = 0
    has_url: bool = False
    has_emoji: bool = False
    emoji_count: int = 0
    has_question: bool = False
    has_number: bool = False
    has_thread_hook: bool = False
    has_cta: bool = False
    has_media_hint: bool = False
    optimal_length: bool = False
    char_count: int = 0
    word_count: int = 0
    engagement_trigger_count: int = 0

    def to_dict(self) -> Dict:
        return {
            "has_hashtags": self.has_hashtags,
            "hashtag_count": self.hashtag_count,
            "has_mention": self.has_mention,
            "mention_count": self.mention_count,
            "has_url": self.has_url,
            "has_emoji": self.has_emoji,
            "emoji_count": self.emoji_count,
            "has_question": self.has_question,
            "has_number": self.has_number,
            "has_thread_hook": self.has_thread_hook,
            "has_cta": self.has_cta,
            "has_media_hint": self.has_media_hint,
            "optimal_length": self.optimal_length,
            "char_count": self.char_count,
            "word_count": self.word_count,
            "engagement_trigger_count": self.engagement_trigger_count,
        }


@dataclass
class ViralPrediction:
    """病毒传播预测结果"""
    content: str
    viral_score: float = 0.0   # 0-100
    predicted_impressions: int = 0
    predicted_engagements: int = 0
    predicted_engagement_rate: float = 0.0
    confidence: float = 0.0    # 0-1
    features: ContentFeatures = field(default_factory=ContentFeatures)
    suggestions: List[str] = field(default_factory=list)
    category: str = "normal"   # low, normal, potential, viral

    @property
    def grade(self) -> str:
        """病毒传播评级"""
        if self.viral_score >= 80:
            return "S"
        elif self.viral_score >= 60:
            return "A"
        elif self.viral_score >= 40:
            return "B"
        elif self.viral_score >= 20:
            return "C"
        return "D"

    def to_dict(self) -> Dict:
        return {
            "content_preview": self.content[:100],
            "viral_score": self.viral_score,
            "grade": self.grade,
            "predicted_impressions": self.predicted_impressions,
            "predicted_engagements": self.predicted_engagements,
            "predicted_engagement_rate": self.predicted_engagement_rate,
            "confidence": self.confidence,
            "category": self.category,
            "features": self.features.to_dict(),
            "suggestions": self.suggestions,
        }


class ViralPredictor:
    """病毒传播预测器"""

    def __init__(self, db: Database):
        self.db = db
        self._ensure_table()
        self._baseline = self._load_baseline()

    def _ensure_table(self):
        conn = self.db._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS viral_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tweet_id TEXT,
                content TEXT NOT NULL,
                features TEXT DEFAULT '{}',
                viral_score REAL DEFAULT 0,
                actual_impressions INTEGER DEFAULT 0,
                actual_engagements INTEGER DEFAULT 0,
                actual_engagement_rate REAL DEFAULT 0,
                predicted_score REAL DEFAULT 0,
                accuracy REAL DEFAULT 0,
                recorded_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_viral_score ON viral_history(viral_score);
        """)
        conn.commit()

    def _load_baseline(self) -> Dict:
        """加载历史基线数据"""
        conn = self.db._get_conn()
        row = conn.execute("""
            SELECT AVG(actual_impressions) as avg_imp,
                   AVG(actual_engagements) as avg_eng,
                   AVG(actual_engagement_rate) as avg_er,
                   COUNT(*) as total
            FROM viral_history
            WHERE actual_impressions > 0
        """).fetchone()

        if row and row["total"] and row["total"] > 0:
            return {
                "avg_impressions": row["avg_imp"] or 100,
                "avg_engagements": row["avg_eng"] or 5,
                "avg_engagement_rate": row["avg_er"] or 2.0,
                "sample_size": row["total"],
            }
        return {
            "avg_impressions": 100,
            "avg_engagements": 5,
            "avg_engagement_rate": 2.0,
            "sample_size": 0,
        }

    # ── 特征提取 ──

    def extract_features(self, content: str) -> ContentFeatures:
        """提取推文内容特征"""
        f = ContentFeatures()
        f.char_count = len(content)
        f.word_count = len(content.split())

        # 标签
        hashtags = re.findall(r'#\w+', content)
        f.has_hashtags = len(hashtags) > 0
        f.hashtag_count = len(hashtags)

        # @提及
        mentions = re.findall(r'@\w+', content)
        f.has_mention = len(mentions) > 0
        f.mention_count = len(mentions)

        # URL
        f.has_url = bool(re.search(r'https?://\S+', content))

        # Emoji (Unicode emoji ranges)
        emoji_pattern = re.compile(
            "[\U0001F300-\U0001F9FF\U00002600-\U000027BF\U0001FA00-\U0001FA6F"
            "\U0001FA70-\U0001FAFF\U00002702-\U000027B0]+",
            re.UNICODE
        )
        emojis = emoji_pattern.findall(content)
        f.has_emoji = len(emojis) > 0
        f.emoji_count = len(emojis)

        # 疑问句
        f.has_question = "?" in content

        # 数字
        f.has_number = bool(re.search(r'\d+', content))

        # Thread hook
        content_lower = content.lower()
        f.has_thread_hook = any(
            re.search(p, content_lower) for p in THREAD_HOOKS
        )

        # CTA
        f.has_cta = any(
            re.search(p, content_lower) for p in CTA_PATTERNS
        )

        # 媒体提示
        f.has_media_hint = any(
            re.search(p, content_lower) for p in MEDIA_HINTS
        )

        # 最优长度 (71-280字符)
        f.optimal_length = 71 <= f.char_count <= 280

        # 互动触发器
        f.engagement_trigger_count = sum(
            1 for p in ENGAGEMENT_TRIGGERS if re.search(p, content_lower)
        )

        return f

    # ── 预测 ──

    def predict(self, content: str) -> ViralPrediction:
        """预测推文的病毒传播潜力"""
        features = self.extract_features(content)

        # 计算基础分
        base_score = 30.0  # 基础分30

        # 特征加权
        multiplier = 1.0
        for feat_name, weight in FEATURE_WEIGHTS.items():
            if getattr(features, feat_name, False):
                multiplier *= weight

        # 标签数量惩罚（过多标签降分）
        if features.hashtag_count > 5:
            multiplier *= 0.7
        elif features.hashtag_count > 3:
            multiplier *= 0.85

        # 互动触发器加分
        multiplier *= (1 + features.engagement_trigger_count * 0.15)

        # 长度惩罚
        if features.char_count < 20:
            multiplier *= 0.5
        elif features.char_count > 270:
            multiplier *= 0.9

        # 计算最终分
        viral_score = min(base_score * multiplier, 100)
        viral_score = round(viral_score, 1)

        # 预测指标
        baseline_imp = self._baseline["avg_impressions"]
        baseline_eng = self._baseline["avg_engagements"]

        score_ratio = viral_score / 50  # 50分为基准
        predicted_imp = int(baseline_imp * score_ratio)
        predicted_eng = int(baseline_eng * score_ratio)
        predicted_er = round(predicted_eng / max(predicted_imp, 1) * 100, 2)

        # 置信度
        sample = self._baseline["sample_size"]
        confidence = min(math.log1p(sample) / math.log1p(100), 1.0)
        confidence = round(confidence, 2)

        # 分类
        if viral_score >= 75:
            category = "viral"
        elif viral_score >= 50:
            category = "potential"
        elif viral_score >= 25:
            category = "normal"
        else:
            category = "low"

        # 优化建议
        suggestions = self._generate_suggestions(features)

        return ViralPrediction(
            content=content,
            viral_score=viral_score,
            predicted_impressions=predicted_imp,
            predicted_engagements=predicted_eng,
            predicted_engagement_rate=predicted_er,
            confidence=confidence,
            features=features,
            suggestions=suggestions,
            category=category,
        )

    def _generate_suggestions(self, features: ContentFeatures) -> List[str]:
        """基于特征生成优化建议"""
        suggestions = []

        if not features.has_emoji:
            suggestions.append("添加1-2个相关emoji提升视觉吸引力")

        if not features.has_question:
            suggestions.append("加入提问或投票引导互动")

        if not features.optimal_length:
            if features.char_count < 71:
                suggestions.append("内容偏短，建议扩展到71-280字符")
            else:
                suggestions.append("内容过长，可能影响阅读完成率")

        if not features.has_cta:
            suggestions.append("添加行动号召(CTA)如'Reply with your...'")

        if features.hashtag_count == 0:
            suggestions.append("添加2-3个相关标签增加可发现性")
        elif features.hashtag_count > 5:
            suggestions.append("标签过多(>5个)，建议精简到2-3个")

        if not features.has_thread_hook and features.word_count > 40:
            suggestions.append("长内容可改为thread格式，以hook开头")

        if features.engagement_trigger_count == 0:
            suggestions.append("添加互动触发词如'agree or disagree?'")

        return suggestions[:5]  # 最多5条建议

    # ── 历史记录 ──

    def record_actual(self, content: str, tweet_id: str = None,
                      impressions: int = 0, engagements: int = 0):
        """记录实际表现，用于校准模型"""
        features = self.extract_features(content)
        prediction = self.predict(content)
        eng_rate = engagements / max(impressions, 1) * 100

        # 准确度
        if impressions > 0:
            pred_imp = prediction.predicted_impressions
            accuracy = 1 - abs(pred_imp - impressions) / max(impressions, 1)
            accuracy = max(round(accuracy, 2), 0)
        else:
            accuracy = 0

        import json
        conn = self.db._get_conn()
        conn.execute("""
            INSERT INTO viral_history (tweet_id, content, features, viral_score,
                actual_impressions, actual_engagements, actual_engagement_rate,
                predicted_score, accuracy)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (tweet_id, content, json.dumps(features.to_dict()),
              prediction.viral_score, impressions, engagements,
              round(eng_rate, 2), prediction.viral_score, accuracy))
        conn.commit()

        # 刷新基线
        self._baseline = self._load_baseline()

    def batch_predict(self, contents: List[str]) -> List[ViralPrediction]:
        """批量预测"""
        results = [self.predict(c) for c in contents]
        results.sort(key=lambda p: p.viral_score, reverse=True)
        return results

    def model_accuracy(self) -> Dict:
        """模型准确度报告"""
        conn = self.db._get_conn()
        row = conn.execute("""
            SELECT AVG(accuracy) as avg_acc,
                   MIN(accuracy) as min_acc,
                   MAX(accuracy) as max_acc,
                   COUNT(*) as total
            FROM viral_history
            WHERE actual_impressions > 0
        """).fetchone()

        if not row or not row["total"]:
            return {"accuracy": 0, "samples": 0, "calibrated": False}

        return {
            "accuracy": round(row["avg_acc"] or 0, 2),
            "min_accuracy": round(row["min_acc"] or 0, 2),
            "max_accuracy": round(row["max_acc"] or 0, 2),
            "samples": row["total"],
            "calibrated": row["total"] >= 20,
        }

    # ── 报告 ──

    def format_prediction(self, prediction: ViralPrediction) -> str:
        """格式化预测报告"""
        grade_emoji = {"S": "🔥", "A": "⭐", "B": "👍", "C": "😐", "D": "👎"}

        lines = [
            f"{grade_emoji.get(prediction.grade, '❓')} *Viral Score: {prediction.viral_score}/100* [{prediction.grade}]",
            f"Category: {prediction.category}\n",
            "📊 *Predictions*",
            f"  Impressions: ~{prediction.predicted_impressions:,}",
            f"  Engagements: ~{prediction.predicted_engagements:,}",
            f"  Eng Rate: ~{prediction.predicted_engagement_rate}%",
            f"  Confidence: {prediction.confidence * 100:.0f}%\n",
            "📝 *Content Analysis*",
            f"  Length: {prediction.features.char_count} chars ({prediction.features.word_count} words)",
            f"  Hashtags: {prediction.features.hashtag_count}",
            f"  Has emoji: {'✅' if prediction.features.has_emoji else '❌'}",
            f"  Has CTA: {'✅' if prediction.features.has_cta else '❌'}",
            f"  Has question: {'✅' if prediction.features.has_question else '❌'}",
        ]

        if prediction.suggestions:
            lines.append("\n💡 *Optimization Tips*")
            for i, s in enumerate(prediction.suggestions, 1):
                lines.append(f"  {i}. {s}")

        return "\n".join(lines)
