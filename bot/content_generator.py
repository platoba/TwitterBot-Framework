"""
内容生成器 - 基于模板的推文生成 + A/B测试
"""

import random
import string
from datetime import datetime
from typing import Dict, List, Optional, Tuple


# ── 默认模板库 ──

DEFAULT_TEMPLATES = {
    "announcement": [
        "🚀 {title}\n\n{body}\n\n{hashtags}",
        "📢 {title}\n\n{body}\n\n{call_to_action}\n{hashtags}",
        "🔥 Breaking: {title}\n\n{body}\n\n👉 {call_to_action}\n{hashtags}",
    ],
    "engagement": [
        "💭 {question}\n\nDrop your thoughts below 👇\n{hashtags}",
        "🤔 What do you think about {topic}?\n\nRT if you agree! 🔄\n{hashtags}",
        "📊 Quick poll: {question}\n\n👍 = Yes\n👎 = No\n\n{hashtags}",
    ],
    "thread_hook": [
        "🧵 Thread: {title}\n\n{hook}\n\n(1/{total})",
        "Here's what most people get wrong about {topic}:\n\n{hook}\n\n🧵👇 (1/{total})",
    ],
    "promotion": [
        "Check out {product}! 🎯\n\n{benefit}\n\n{link}\n{hashtags}",
        "If you're into {topic}, you'll love {product}.\n\n{benefit}\n\n{link}",
    ],
    "insight": [
        "💡 {insight}\n\nHere's why this matters:\n{explanation}\n\n{hashtags}",
        "📈 Data point: {insight}\n\n{explanation}\n\nThoughts? 🤔\n{hashtags}",
    ],
    "daily": [
        "☀️ Good morning! Today's focus: {topic}\n\n{body}\n\n{hashtags}",
        "🌙 Evening wrap-up: {summary}\n\nWhat did you accomplish today? 👇\n{hashtags}",
    ],
}


class ContentGenerator:
    """基于模板的推文内容生成器"""

    MAX_TWEET_LENGTH = 280

    def __init__(self, templates: Dict[str, List[str]] = None):
        self.templates = templates or dict(DEFAULT_TEMPLATES)
        self._custom_vars: Dict[str, str] = {}

    def set_variable(self, key: str, value: str):
        """设置全局变量"""
        self._custom_vars[key] = value

    def set_variables(self, variables: Dict[str, str]):
        """批量设置变量"""
        self._custom_vars.update(variables)

    def add_template(self, category: str, template: str):
        """添加自定义模板"""
        if category not in self.templates:
            self.templates[category] = []
        self.templates[category].append(template)

    def get_categories(self) -> List[str]:
        return list(self.templates.keys())

    def generate(self, category: str, variables: Dict[str, str] = None,
                 template_index: int = None) -> Optional[str]:
        """从模板生成推文"""
        templates = self.templates.get(category)
        if not templates:
            return None

        if template_index is not None and 0 <= template_index < len(templates):
            template = templates[template_index]
        else:
            template = random.choice(templates)

        merged = {**self._custom_vars, **(variables or {})}

        # 填充缺失变量为空字符串
        try:
            # 提取模板中的变量名
            field_names = [
                fname for _, fname, _, _
                in string.Formatter().parse(template)
                if fname is not None
            ]
            for fn in field_names:
                if fn not in merged:
                    merged[fn] = ""

            result = template.format(**merged)
        except (KeyError, IndexError, ValueError):
            result = template

        # 清理多余空行
        lines = [line for line in result.split("\n")]
        result = "\n".join(lines).strip()

        return self.truncate(result)

    def generate_variants(self, category: str, variables: Dict[str, str] = None,
                           count: int = 2) -> List[str]:
        """生成多个变体(用于A/B测试)"""
        templates = self.templates.get(category, [])
        if not templates:
            return []

        variants = []
        indices = list(range(len(templates)))
        random.shuffle(indices)

        for i in range(min(count, len(templates))):
            result = self.generate(category, variables, template_index=indices[i])
            if result:
                variants.append(result)

        return variants

    def generate_ab_pair(self, category: str,
                          variables: Dict[str, str] = None) -> Tuple[str, str]:
        """生成A/B测试对"""
        variants = self.generate_variants(category, variables, count=2)
        if len(variants) < 2:
            base = variants[0] if variants else "Hello! 👋"
            return base, base
        return variants[0], variants[1]

    def generate_hashtags(self, topics: List[str], max_tags: int = 5) -> str:
        """生成hashtag字符串"""
        tags = []
        for topic in topics[:max_tags]:
            clean = topic.strip().replace(" ", "").replace("#", "")
            if clean:
                tags.append(f"#{clean}")
        return " ".join(tags)

    def generate_thread(self, category: str, variables: Dict[str, str] = None,
                         body_parts: List[str] = None) -> List[str]:
        """生成推文线程"""
        body_parts = body_parts or []
        total = len(body_parts) + 1

        variables = variables or {}
        variables["total"] = str(total)

        hook = self.generate(category, variables)
        if not hook:
            return []

        tweets = [hook]
        for i, part in enumerate(body_parts, 2):
            tweet = f"({i}/{total}) {part}"
            tweets.append(self.truncate(tweet))

        return tweets

    @classmethod
    def truncate(cls, text: str) -> str:
        if len(text) <= cls.MAX_TWEET_LENGTH:
            return text
        return text[:cls.MAX_TWEET_LENGTH - 3] + "..."

    def estimate_engagement(self, text: str) -> Dict[str, float]:
        """估算互动率(启发式)"""
        score = 50.0

        if "?" in text:
            score += 10
        if any(word in text.lower() for word in ["thread", "🧵"]):
            score += 8
        if any(e in text for e in ["🔥", "🚀", "💡", "📊"]):
            score += 5
        if "#" in text:
            count = text.count("#")
            score += min(count * 2, 10)
        if text.count("\n") >= 2:
            score += 5

        word_count = len(text.split())
        if 20 <= word_count <= 40:
            score += 5
        elif word_count > 60:
            score -= 5

        if any(cta in text.lower() for cta in ["rt ", "retweet", "share", "like if"]):
            score += 7
        if any(cta in text.lower() for cta in ["reply", "comment", "thoughts", "👇"]):
            score += 8

        return {
            "estimated_score": min(max(score, 0), 100),
            "has_question": "?" in text,
            "has_hashtags": "#" in text,
            "has_emoji": any(ord(c) > 0x1F00 for c in text),
            "has_cta": any(w in text.lower() for w in ["reply", "rt", "share", "👇"]),
            "word_count": word_count,
            "char_count": len(text)
        }
