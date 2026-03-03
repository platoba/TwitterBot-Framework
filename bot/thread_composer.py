"""
Thread Composer - 推文线程创建引擎 v3.0
自动分割长文 + 编号 + 媒体附件 + 引用嵌套 + 预览
"""

import re
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

TWEET_MAX_CHARS = 280
THREAD_NUMBERING = "({current}/{total})"
URL_PLACEHOLDER_LEN = 23  # Twitter t.co wraps all URLs to 23 chars


@dataclass
class ThreadTweet:
    """线程中的单条推文"""
    index: int
    text: str
    media_urls: List[str] = field(default_factory=list)
    quote_tweet_id: Optional[str] = None
    reply_to_id: Optional[str] = None
    char_count: int = 0

    def __post_init__(self):
        self.char_count = self._calc_chars()

    def _calc_chars(self) -> int:
        """计算实际字符数(考虑URL压缩)"""
        text = self.text
        # Twitter wraps all URLs to 23 chars
        url_pattern = re.compile(r'https?://\S+')
        urls = url_pattern.findall(text)
        adjusted = len(url_pattern.sub('', text))
        adjusted += len(urls) * URL_PLACEHOLDER_LEN
        return adjusted

    @property
    def is_valid(self) -> bool:
        return 0 < self.char_count <= TWEET_MAX_CHARS

    def to_dict(self) -> Dict:
        return {
            "index": self.index,
            "text": self.text,
            "media_urls": self.media_urls,
            "quote_tweet_id": self.quote_tweet_id,
            "reply_to_id": self.reply_to_id,
            "char_count": self.char_count,
            "is_valid": self.is_valid,
        }


@dataclass
class Thread:
    """完整推文线程"""
    title: str
    tweets: List[ThreadTweet] = field(default_factory=list)
    hashtags: str = ""
    hook: str = ""
    cta: str = ""

    @property
    def total(self) -> int:
        return len(self.tweets)

    @property
    def total_chars(self) -> int:
        return sum(t.char_count for t in self.tweets)

    @property
    def is_valid(self) -> bool:
        return all(t.is_valid for t in self.tweets) and self.total > 0

    @property
    def invalid_tweets(self) -> List[ThreadTweet]:
        return [t for t in self.tweets if not t.is_valid]

    def to_dict(self) -> Dict:
        return {
            "title": self.title,
            "total_tweets": self.total,
            "total_chars": self.total_chars,
            "is_valid": self.is_valid,
            "hashtags": self.hashtags,
            "tweets": [t.to_dict() for t in self.tweets],
        }


class ThreadComposer:
    """推文线程创建引擎"""

    def __init__(self, max_chars: int = TWEET_MAX_CHARS,
                 numbering: bool = True,
                 numbering_format: str = THREAD_NUMBERING,
                 add_hook: bool = True,
                 add_cta: bool = True):
        self.max_chars = max_chars
        self.numbering = numbering
        self.numbering_format = numbering_format
        self.add_hook = add_hook
        self.add_cta = add_cta

    def _numbering_len(self, current: int, total: int) -> int:
        """计算编号占用字符数"""
        if not self.numbering:
            return 0
        return len(self.numbering_format.format(current=current, total=total)) + 1  # +1 for newline/space

    def _split_text(self, text: str, reserve_chars: int = 0) -> List[str]:
        """智能分割文本到多条推文"""
        available = self.max_chars - reserve_chars
        if available <= 0:
            available = self.max_chars

        if len(text) <= available:
            return [text]

        chunks = []
        remaining = text

        while remaining:
            if len(remaining) <= available:
                chunks.append(remaining)
                break

            # 优先在段落边界分割
            split_pos = self._find_split_point(remaining, available)
            chunk = remaining[:split_pos].rstrip()
            remaining = remaining[split_pos:].lstrip()

            if chunk:
                chunks.append(chunk)

        return chunks

    def _find_split_point(self, text: str, max_len: int) -> int:
        """找到最佳分割点: 段落 > 句子 > 逗号 > 空格 > 硬切"""
        if len(text) <= max_len:
            return len(text)

        # 1. 段落边界 (\n\n)
        pos = text.rfind('\n\n', 0, max_len)
        if pos > max_len * 0.3:
            return pos + 2

        # 2. 换行
        pos = text.rfind('\n', 0, max_len)
        if pos > max_len * 0.3:
            return pos + 1

        # 3. 句子结束 (. ! ? 。！？)
        for sep in ['. ', '! ', '? ', '。', '！', '？']:
            pos = text.rfind(sep, 0, max_len)
            if pos > max_len * 0.3:
                return pos + len(sep)

        # 4. 逗号/分号
        for sep in [', ', '; ', '，', '；']:
            pos = text.rfind(sep, 0, max_len)
            if pos > max_len * 0.3:
                return pos + len(sep)

        # 5. 空格
        pos = text.rfind(' ', 0, max_len)
        if pos > max_len * 0.2:
            return pos + 1

        # 6. 硬切
        return max_len

    def compose(self, title: str, body: str,
                hashtags: str = "",
                hook: str = "",
                cta: str = "",
                media_map: Dict[int, List[str]] = None) -> Thread:
        """
        从长文本组装线程

        Args:
            title: 线程标题
            body: 长文本主体
            hashtags: 尾部标签
            hook: 首条钩子文案(覆盖自动生成)
            cta: 尾条CTA
            media_map: {tweet_index: [media_urls]} 附件映射
        """
        media_map = media_map or {}

        # 估算编号占位(先按10条预估)
        numbering_reserve = self._numbering_len(1, 10) if self.numbering else 0

        # 首条: hook + 标题
        if hook:
            first_text = hook
        elif self.add_hook:
            first_text = f"🧵 {title}"
        else:
            first_text = title

        # 分割主体
        body_chunks = self._split_text(body, reserve_chars=numbering_reserve)

        # 尾条: CTA + hashtags
        tail_parts = []
        if cta and self.add_cta:
            tail_parts.append(cta)
        if hashtags:
            tail_parts.append(hashtags)
        tail_text = "\n\n".join(tail_parts) if tail_parts else ""

        # 组装线程
        raw_tweets = [first_text] + body_chunks
        if tail_text:
            # 尝试合并到最后一条
            last = raw_tweets[-1]
            combined = f"{last}\n\n{tail_text}"
            if len(combined) <= self.max_chars - numbering_reserve:
                raw_tweets[-1] = combined
            else:
                raw_tweets.append(tail_text)

        total = len(raw_tweets)

        # 重新计算编号(用实际total)
        tweets = []
        for i, text in enumerate(raw_tweets):
            idx = i + 1
            if self.numbering:
                numbering_str = self.numbering_format.format(current=idx, total=total)
                full_text = f"{text}\n\n{numbering_str}"
            else:
                full_text = text

            tweet = ThreadTweet(
                index=idx,
                text=full_text,
                media_urls=media_map.get(idx, []),
            )
            tweets.append(tweet)

        thread = Thread(
            title=title,
            tweets=tweets,
            hashtags=hashtags,
            hook=hook or first_text,
            cta=cta,
        )

        return thread

    def compose_from_points(self, title: str,
                             points: List[str],
                             hashtags: str = "",
                             hook: str = "",
                             cta: str = "",
                             one_per_tweet: bool = True) -> Thread:
        """
        从要点列表组装线程(每点一条推)

        Args:
            title: 标题
            points: 要点列表
            one_per_tweet: True=每点一条; False=尽量合并
        """
        if one_per_tweet:
            body = "\n\n---SPLIT---\n\n".join(points)
            # 用特殊分隔符确保不合并
            chunks = points
        else:
            body = "\n\n".join(points)
            return self.compose(title, body, hashtags, hook, cta)

        # 手动组装
        numbering_reserve = self._numbering_len(1, len(chunks) + 2) if self.numbering else 0

        # 首条
        first = hook or f"🧵 {title}"

        # 处理每个要点(可能需要再分割)
        all_parts = [first]
        for point in chunks:
            if len(point) <= self.max_chars - numbering_reserve:
                all_parts.append(point)
            else:
                sub_parts = self._split_text(point, numbering_reserve)
                all_parts.extend(sub_parts)

        # CTA尾条
        tail_parts = []
        if cta:
            tail_parts.append(cta)
        if hashtags:
            tail_parts.append(hashtags)
        if tail_parts:
            tail = "\n\n".join(tail_parts)
            last = all_parts[-1]
            combined = f"{last}\n\n{tail}"
            if len(combined) <= self.max_chars - numbering_reserve:
                all_parts[-1] = combined
            else:
                all_parts.append(tail)

        total = len(all_parts)
        tweets = []
        for i, text in enumerate(all_parts):
            idx = i + 1
            if self.numbering:
                numbering_str = self.numbering_format.format(current=idx, total=total)
                full_text = f"{text}\n\n{numbering_str}"
            else:
                full_text = text
            tweets.append(ThreadTweet(index=idx, text=full_text))

        return Thread(title=title, tweets=tweets, hashtags=hashtags, hook=first, cta=cta)

    def preview(self, thread: Thread) -> str:
        """生成线程预览文本"""
        lines = [
            f"📋 *Thread Preview: {thread.title}*",
            f"📝 {thread.total} tweets | {thread.total_chars} total chars",
            f"✅ Valid: {thread.is_valid}\n",
        ]

        for tweet in thread.tweets:
            status = "✅" if tweet.is_valid else "❌"
            lines.append(f"── Tweet {tweet.index}/{thread.total} {status} ({tweet.char_count} chars) ──")
            lines.append(tweet.text)
            if tweet.media_urls:
                lines.append(f"  📎 Media: {', '.join(tweet.media_urls)}")
            lines.append("")

        invalid = thread.invalid_tweets
        if invalid:
            lines.append(f"⚠️ {len(invalid)} tweets exceed {self.max_chars} chars!")
            for t in invalid:
                lines.append(f"  Tweet {t.index}: {t.char_count} chars (over by {t.char_count - self.max_chars})")

        return "\n".join(lines)

    def validate(self, thread: Thread) -> Dict:
        """验证线程"""
        issues = []

        if thread.total == 0:
            issues.append("Thread is empty")

        if thread.total > 25:
            issues.append(f"Thread too long: {thread.total} tweets (max recommended: 25)")

        for tweet in thread.tweets:
            if not tweet.is_valid:
                issues.append(f"Tweet {tweet.index}: {tweet.char_count} chars (max {self.max_chars})")

            if tweet.text.strip() == "":
                issues.append(f"Tweet {tweet.index}: empty text")

        # 检查重复
        texts = [t.text for t in thread.tweets]
        seen = set()
        for i, t in enumerate(texts):
            if t in seen:
                issues.append(f"Tweet {i+1}: duplicate content")
            seen.add(t)

        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "total_tweets": thread.total,
            "total_chars": thread.total_chars,
        }

    def estimate_read_time(self, thread: Thread) -> Dict:
        """估算阅读时间"""
        total_words = sum(len(t.text.split()) for t in thread.tweets)
        # Average reading speed: 200-250 words/min for social media
        read_minutes = total_words / 225
        scroll_time = thread.total * 3  # ~3 seconds per tweet scroll
        total_seconds = read_minutes * 60 + scroll_time

        return {
            "total_words": total_words,
            "read_minutes": round(read_minutes, 1),
            "scroll_seconds": scroll_time,
            "total_seconds": round(total_seconds),
            "formatted": f"{int(total_seconds // 60)}m {int(total_seconds % 60)}s",
        }
