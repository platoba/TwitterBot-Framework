"""
Profile Optimizer - AI驱动的Twitter资料优化器 v1.0
Bio评分 + 关键词密度 + CTA检测 + 竞品对比 + 优化建议
"""

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger(__name__)


class ProfileGrade(str, Enum):
    S = "S"  # 95-100 完美
    A_PLUS = "A+"  # 90-94
    A = "A"  # 85-89
    B_PLUS = "B+"  # 80-84
    B = "B"  # 70-79
    C = "C"  # 60-69
    D = "D"  # 40-59
    F = "F"  # 0-39


# Bio quality patterns
POWER_WORDS = [
    "founder", "ceo", "cto", "creator", "builder", "engineer",
    "developer", "designer", "writer", "author", "speaker",
    "helping", "building", "teaching", "growing", "scaling",
    "expert", "specialist", "consultant", "advisor", "mentor",
    "passionate", "innovative", "creative", "strategic",
]

CTA_PATTERNS = [
    re.compile(r"(?:dm|message|reach out|contact|email)\s+(?:me|us)", re.I),
    re.compile(r"(?:link|👇|⬇️|below|click|check out)", re.I),
    re.compile(r"(?:subscribe|join|sign up|follow|download|get)", re.I),
    re.compile(r"(?:book|schedule|hire|work with)", re.I),
    re.compile(r"(?:free|bonus|exclusive|limited)", re.I),
]

EMOJI_PATTERN = re.compile(
    "["
    "\U0001F300-\U0001F9FF"
    "\U0001FA00-\U0001FA6F"
    "\U0001FA70-\U0001FAFF"
    "\U00002702-\U000027B0"
    "\U000024C2-\U0001F251"
    "]+",
    flags=re.UNICODE,
)

URL_PATTERN = re.compile(r"https?://\S+", re.I)

SOCIAL_PROOF_PATTERNS = [
    re.compile(r"\b\d+[kKmM]?\+?\s*(?:followers|subs|subscribers|users|customers|clients|students)\b", re.I),
    re.compile(r"(?:featured|seen|as seen)\s+(?:in|on|at)\b", re.I),
    re.compile(r"\b(?:forbes|techcrunch|hacker news|product hunt|y combinator|ycombinator)\b", re.I),
    re.compile(r"\b(?:ex-?|formerly?\s+(?:at\s+)?)(google|meta|facebook|apple|amazon|microsoft|netflix)\b", re.I),
    re.compile(r"\$\d+[kKmMbB]\+?\s*(?:revenue|arr|mrr|raised)\b", re.I),
]


@dataclass
class ProfileData:
    """Twitter Profile"""
    username: str = ""
    display_name: str = ""
    bio: str = ""
    location: str = ""
    website: str = ""
    followers_count: int = 0
    following_count: int = 0
    tweet_count: int = 0
    listed_count: int = 0
    created_at: str = ""
    pinned_tweet: str = ""
    banner_url: str = ""
    avatar_url: str = ""

    def to_dict(self) -> Dict:
        return {
            "username": self.username,
            "display_name": self.display_name,
            "bio": self.bio,
            "location": self.location,
            "website": self.website,
            "followers_count": self.followers_count,
            "following_count": self.following_count,
            "tweet_count": self.tweet_count,
            "listed_count": self.listed_count,
            "created_at": self.created_at,
            "pinned_tweet": self.pinned_tweet,
        }


@dataclass
class ScoreBreakdown:
    """评分明细"""
    category: str = ""
    score: float = 0.0
    max_score: float = 0.0
    details: str = ""
    suggestions: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "category": self.category,
            "score": self.score,
            "max_score": self.max_score,
            "percentage": round(self.score / self.max_score * 100, 1) if self.max_score > 0 else 0,
            "details": self.details,
            "suggestions": self.suggestions,
        }


class BioAnalyzer:
    """Bio文本分析器"""

    @staticmethod
    def word_count(bio: str) -> int:
        return len(bio.split()) if bio else 0

    @staticmethod
    def char_count(bio: str) -> int:
        return len(bio) if bio else 0

    @staticmethod
    def has_emoji(bio: str) -> bool:
        return bool(EMOJI_PATTERN.search(bio)) if bio else False

    @staticmethod
    def emoji_count(bio: str) -> int:
        if not bio:
            return 0
        return len(EMOJI_PATTERN.findall(bio))

    @staticmethod
    def has_url(bio: str) -> bool:
        return bool(URL_PATTERN.search(bio)) if bio else False

    @staticmethod
    def has_hashtag(bio: str) -> bool:
        return bool(re.search(r"#\w+", bio)) if bio else False

    @staticmethod
    def hashtag_count(bio: str) -> int:
        if not bio:
            return 0
        return len(re.findall(r"#\w+", bio))

    @staticmethod
    def has_mention(bio: str) -> bool:
        return bool(re.search(r"@\w+", bio)) if bio else False

    @staticmethod
    def power_word_count(bio: str) -> int:
        if not bio:
            return 0
        bio_lower = bio.lower()
        return sum(1 for w in POWER_WORDS if w in bio_lower)

    @staticmethod
    def cta_count(bio: str) -> int:
        if not bio:
            return 0
        return sum(1 for p in CTA_PATTERNS if p.search(bio))

    @staticmethod
    def social_proof_count(bio: str) -> int:
        if not bio:
            return 0
        return sum(1 for p in SOCIAL_PROOF_PATTERNS if p.search(bio))

    @staticmethod
    def line_count(bio: str) -> int:
        if not bio:
            return 0
        return len([l for l in bio.split("\n") if l.strip()])

    @staticmethod
    def readability_score(bio: str) -> float:
        """简易可读性评分 0-100"""
        if not bio:
            return 0.0
        words = bio.split()
        if not words:
            return 0.0
        avg_word_len = sum(len(w) for w in words) / len(words)
        # Optimal: 4-6 chars per word
        if 4 <= avg_word_len <= 6:
            word_score = 100
        elif avg_word_len < 4:
            word_score = 70
        else:
            word_score = max(0, 100 - (avg_word_len - 6) * 15)

        # Sentence variety
        lines = [l.strip() for l in bio.split("\n") if l.strip()]
        variety = min(100, len(lines) * 25)

        return round((word_score * 0.6 + variety * 0.4), 1)


class ProfileScorer:
    """Profile评分引擎"""

    def __init__(self):
        self.analyzer = BioAnalyzer()

    def score_bio_length(self, bio: str) -> ScoreBreakdown:
        """Bio长度评分 (max 15)"""
        chars = self.analyzer.char_count(bio)
        suggestions = []

        if chars == 0:
            score = 0
            details = "Bio is empty"
            suggestions.append("Add a bio describing who you are and what you do")
        elif chars < 30:
            score = 5
            details = f"Bio too short ({chars} chars)"
            suggestions.append("Expand bio to at least 100 characters for better discoverability")
        elif chars < 80:
            score = 8
            details = f"Bio could be longer ({chars} chars)"
            suggestions.append("Add more detail (aim for 120-160 chars)")
        elif chars <= 160:
            score = 15
            details = f"Optimal bio length ({chars}/160 chars)"
        else:
            score = 12
            details = f"Bio at max ({chars} chars)"

        return ScoreBreakdown("Bio Length", score, 15, details, suggestions)

    def score_power_words(self, bio: str) -> ScoreBreakdown:
        """Power词评分 (max 15)"""
        count = self.analyzer.power_word_count(bio)
        suggestions = []

        if count == 0:
            score = 0
            details = "No power words found"
            suggestions.append("Add role/action words (e.g., 'Building X', 'Founder of Y')")
        elif count == 1:
            score = 8
            details = f"{count} power word found"
            suggestions.append("Add 1-2 more action words for impact")
        elif count <= 3:
            score = 15
            details = f"{count} power words (optimal)"
        else:
            score = 12
            details = f"{count} power words (may be too many)"
            suggestions.append("Focus on 2-3 strongest power words")

        return ScoreBreakdown("Power Words", score, 15, details, suggestions)

    def score_cta(self, bio: str) -> ScoreBreakdown:
        """CTA评分 (max 10)"""
        count = self.analyzer.cta_count(bio)
        suggestions = []

        if count == 0:
            score = 0
            details = "No call-to-action found"
            suggestions.append("Add a CTA (e.g., 'DM me for...', 'Link below 👇')")
        elif count == 1:
            score = 10
            details = "1 CTA found (optimal)"
        else:
            score = 7
            details = f"{count} CTAs found"
            suggestions.append("Keep to 1 clear CTA to avoid confusion")

        return ScoreBreakdown("Call to Action", score, 10, details, suggestions)

    def score_social_proof(self, bio: str) -> ScoreBreakdown:
        """社交证明评分 (max 10)"""
        count = self.analyzer.social_proof_count(bio)
        suggestions = []

        if count == 0:
            score = 0
            details = "No social proof"
            suggestions.append("Add credibility (followers count, featured in, ex-company)")
        elif count == 1:
            score = 8
            details = "1 social proof element"
        else:
            score = 10
            details = f"{count} social proof elements"

        return ScoreBreakdown("Social Proof", score, 10, details, suggestions)

    def score_emoji(self, bio: str) -> ScoreBreakdown:
        """Emoji使用评分 (max 10)"""
        count = self.analyzer.emoji_count(bio)
        suggestions = []

        if count == 0:
            score = 3
            details = "No emojis"
            suggestions.append("Add 1-3 relevant emojis for visual breaks")
        elif 1 <= count <= 3:
            score = 10
            details = f"{count} emojis (optimal)"
        elif count <= 5:
            score = 7
            details = f"{count} emojis (slightly high)"
        else:
            score = 3
            details = f"{count} emojis (too many)"
            suggestions.append("Reduce to 2-3 emojis max for professional look")

        return ScoreBreakdown("Emoji Usage", score, 10, details, suggestions)

    def score_formatting(self, bio: str) -> ScoreBreakdown:
        """格式评分 (max 10)"""
        lines = self.analyzer.line_count(bio)
        has_pipe = "|" in bio if bio else False
        has_bullet = "•" in bio or "·" in bio or "▪" in bio if bio else False
        suggestions = []

        score = 0
        if lines >= 2 or has_pipe or has_bullet:
            score += 5
        elif bio:
            suggestions.append("Break bio into multiple lines or use separators (|, •)")

        if has_pipe or has_bullet:
            score += 3
        if lines >= 2:
            score += 2

        score = min(score, 10)
        details = f"{lines} lines, {'has' if has_pipe or has_bullet else 'no'} separators"

        return ScoreBreakdown("Formatting", score, 10, details, suggestions)

    def score_completeness(self, profile: ProfileData) -> ScoreBreakdown:
        """完整度评分 (max 15)"""
        score = 0
        missing = []

        if profile.bio:
            score += 3
        else:
            missing.append("bio")
        if profile.display_name:
            score += 2
        else:
            missing.append("display name")
        if profile.location:
            score += 2
        else:
            missing.append("location")
        if profile.website:
            score += 3
        else:
            missing.append("website URL")
        if profile.pinned_tweet:
            score += 3
        else:
            missing.append("pinned tweet")
        if profile.banner_url:
            score += 2
        else:
            missing.append("banner image")

        suggestions = [f"Add: {', '.join(missing)}"] if missing else []
        details = f"{score}/15 profile fields filled"

        return ScoreBreakdown("Profile Completeness", score, 15, details, suggestions)

    def score_engagement_ratio(self, profile: ProfileData) -> ScoreBreakdown:
        """互动比评分 (max 15)"""
        suggestions = []
        if profile.followers_count == 0:
            score = 5
            details = "No followers data"
            suggestions.append("Build followers through consistent posting")
        else:
            # Following/Follower ratio
            ratio = profile.following_count / profile.followers_count if profile.followers_count > 0 else 999
            if ratio <= 0.5:
                score = 15
                details = f"Excellent ratio ({ratio:.2f} following/followers)"
            elif ratio <= 1.0:
                score = 12
                details = f"Good ratio ({ratio:.2f})"
            elif ratio <= 2.0:
                score = 8
                details = f"Fair ratio ({ratio:.2f})"
                suggestions.append("Unfollow inactive accounts to improve ratio")
            else:
                score = 3
                details = f"Poor ratio ({ratio:.2f})"
                suggestions.append("Following too many accounts vs followers")

        return ScoreBreakdown("Engagement Ratio", score, 15, details, suggestions)

    def full_score(self, profile: ProfileData) -> Dict[str, Any]:
        """完整评分"""
        breakdowns = [
            self.score_bio_length(profile.bio),
            self.score_power_words(profile.bio),
            self.score_cta(profile.bio),
            self.score_social_proof(profile.bio),
            self.score_emoji(profile.bio),
            self.score_formatting(profile.bio),
            self.score_completeness(profile),
            self.score_engagement_ratio(profile),
        ]

        total = sum(b.score for b in breakdowns)
        max_total = sum(b.max_score for b in breakdowns)

        # Grade
        pct = total / max_total * 100 if max_total > 0 else 0
        if pct >= 95:
            grade = ProfileGrade.S
        elif pct >= 90:
            grade = ProfileGrade.A_PLUS
        elif pct >= 85:
            grade = ProfileGrade.A
        elif pct >= 80:
            grade = ProfileGrade.B_PLUS
        elif pct >= 70:
            grade = ProfileGrade.B
        elif pct >= 60:
            grade = ProfileGrade.C
        elif pct >= 40:
            grade = ProfileGrade.D
        else:
            grade = ProfileGrade.F

        # Collect all suggestions
        all_suggestions = []
        for b in breakdowns:
            all_suggestions.extend(b.suggestions)

        return {
            "username": profile.username,
            "total_score": round(total, 1),
            "max_score": max_total,
            "percentage": round(pct, 1),
            "grade": grade.value,
            "breakdowns": [b.to_dict() for b in breakdowns],
            "top_suggestions": all_suggestions[:5],
            "all_suggestions": all_suggestions,
        }


class ProfileComparator:
    """竞品Profile对比"""

    def __init__(self):
        self.scorer = ProfileScorer()

    def compare(self, profiles: List[ProfileData]) -> Dict[str, Any]:
        """对比多个Profile"""
        if not profiles:
            return {"profiles": [], "ranking": []}

        scores = []
        for p in profiles:
            result = self.scorer.full_score(p)
            scores.append(result)

        # Rank
        ranking = sorted(scores, key=lambda x: x["total_score"], reverse=True)
        for i, r in enumerate(ranking):
            r["rank"] = i + 1

        # Best practices from top performer
        best = ranking[0] if ranking else None
        insights = []
        if best and len(ranking) > 1:
            worst = ranking[-1]
            diff = best["total_score"] - worst["total_score"]
            insights.append(f"Score gap between #{1} and #{len(ranking)}: {diff:.1f} points")

        return {
            "profiles": scores,
            "ranking": [{"username": r["username"], "rank": r["rank"],
                         "score": r["total_score"], "grade": r["grade"]} for r in ranking],
            "insights": insights,
        }


class BioGenerator:
    """Bio生成辅助器"""

    TEMPLATES = {
        "founder": "{emoji1} {role} of {project} | {value_prop} | {cta} {emoji2}",
        "creator": "{emoji1} {role} | {niche} | {achievement} | {cta} {emoji2}",
        "developer": "{emoji1} {role} | Building {project} | {tech_stack} | {cta}",
        "marketer": "{emoji1} {value_prop} | {achievement} | {cta} {emoji2}",
        "generic": "{emoji1} {role} | {value_prop} | {cta}",
    }

    @classmethod
    def suggest_bios(cls, role: str, niche: str = "", project: str = "",
                     achievement: str = "", cta: str = "DM me 📩") -> List[str]:
        """生成Bio建议"""
        suggestions = []
        vars_dict = {
            "role": role,
            "niche": niche or "growth",
            "project": project or "something cool",
            "achievement": achievement or "",
            "value_prop": f"Helping you with {niche}" if niche else "Building cool stuff",
            "cta": cta,
            "emoji1": "🚀",
            "emoji2": "👇",
            "tech_stack": "Python • TypeScript • AI",
        }

        for name, template in cls.TEMPLATES.items():
            try:
                bio = template.format(**vars_dict)
                bio = bio.replace(" |  |", " |").replace("| |", "|").strip(" |")
                if bio and len(bio) <= 160:
                    suggestions.append(bio)
            except (KeyError, IndexError):
                continue

        return suggestions

    @staticmethod
    def optimize_length(bio: str, max_chars: int = 160) -> str:
        """优化Bio长度"""
        if len(bio) <= max_chars:
            return bio

        # Try removing trailing spaces
        bio = bio.strip()
        if len(bio) <= max_chars:
            return bio

        # Try removing extra spaces
        bio = re.sub(r"\s+", " ", bio)
        if len(bio) <= max_chars:
            return bio

        # Truncate at word boundary
        truncated = bio[:max_chars]
        last_space = truncated.rfind(" ")
        if last_space > max_chars * 0.7:
            return truncated[:last_space]
        return truncated


class ProfileOptimizer:
    """Profile优化引擎 - 统一入口"""

    def __init__(self):
        self.scorer = ProfileScorer()
        self.comparator = ProfileComparator()
        self.analyzer = BioAnalyzer()
        self.generator = BioGenerator()

    def analyze(self, profile: ProfileData) -> Dict[str, Any]:
        """分析并评分Profile"""
        return self.scorer.full_score(profile)

    def compare(self, profiles: List[ProfileData]) -> Dict[str, Any]:
        """对比多个Profile"""
        return self.comparator.compare(profiles)

    def suggest_bios(self, **kwargs) -> List[str]:
        """生成Bio建议"""
        return self.generator.suggest_bios(**kwargs)

    def optimize_bio(self, bio: str, max_chars: int = 160) -> str:
        """优化Bio"""
        return self.generator.optimize_length(bio, max_chars)

    def text_report(self, profile: ProfileData) -> str:
        """文本报告"""
        result = self.analyze(profile)
        lines = [
            f"📊 Profile Analysis: @{result['username']}",
            f"Overall: {result['grade']} ({result['total_score']}/{result['max_score']} = {result['percentage']}%)",
            "",
            "Breakdown:",
        ]
        for b in result["breakdowns"]:
            bar = "█" * int(b["percentage"] / 10) + "░" * (10 - int(b["percentage"] / 10))
            lines.append(f"  {b['category']:20s} {bar} {b['score']:.0f}/{b['max_score']:.0f}")

        if result["top_suggestions"]:
            lines.append("")
            lines.append("💡 Top Suggestions:")
            for i, s in enumerate(result["top_suggestions"], 1):
                lines.append(f"  {i}. {s}")

        return "\n".join(lines)
