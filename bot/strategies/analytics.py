"""
分析面板策略
互动率/粉丝增长/最佳发帖时间/热门推文分析
"""

import logging
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from bot.twitter_api import TwitterAPI
from bot.database import Database
from bot.webhook import TelegramWebhook

logger = logging.getLogger(__name__)


class AnalyticsStrategy:
    """分析面板引擎"""

    def __init__(self, api: TwitterAPI, db: Database,
                 webhook: TelegramWebhook = None):
        self.api = api
        self.db = db
        self.webhook = webhook

    # ── 互动率分析 ──

    def calculate_engagement_rate(self, username: str,
                                    tweets: List[Dict] = None) -> Dict:
        """计算互动率"""
        if tweets is None:
            tweets = self.db.get_tweet_history(username, limit=50)

        if not tweets:
            return {"engagement_rate": 0, "total_tweets": 0}

        total_likes = sum(t.get("like_count", 0) for t in tweets)
        total_retweets = sum(t.get("retweet_count", 0) for t in tweets)
        total_replies = sum(t.get("reply_count", 0) for t in tweets)
        total_quotes = sum(t.get("quote_count", 0) for t in tweets)
        total_impressions = sum(t.get("impression_count", 0) for t in tweets)

        total_engagement = total_likes + total_retweets + total_replies + total_quotes
        count = len(tweets)

        if total_impressions > 0:
            engagement_rate = (total_engagement / total_impressions) * 100
        elif count > 0:
            engagement_rate = total_engagement / count
        else:
            engagement_rate = 0

        return {
            "engagement_rate": round(engagement_rate, 4),
            "total_tweets": count,
            "total_likes": total_likes,
            "total_retweets": total_retweets,
            "total_replies": total_replies,
            "total_quotes": total_quotes,
            "total_impressions": total_impressions,
            "avg_likes": round(total_likes / max(count, 1), 1),
            "avg_retweets": round(total_retweets / max(count, 1), 1),
            "avg_replies": round(total_replies / max(count, 1), 1),
        }

    # ── 粉丝增长分析 ──

    def get_follower_growth(self, username: str, days: int = 7) -> Optional[Dict]:
        return self.db.get_follower_growth(username, days)

    def track_user(self, username: str) -> bool:
        """获取并保存当前用户指标快照"""
        data = self.api.get_user(username)
        if not data or "data" not in data:
            return False

        metrics = data["data"].get("public_metrics", {})
        return self.db.save_analytics_snapshot(username, metrics)

    # ── 最佳发帖时间 ──

    def best_posting_times(self, username: str = "",
                            tweets: List[Dict] = None) -> Dict:
        """分析最佳发帖时间"""
        if tweets is None:
            tweets = self.db.get_tweet_history(username, limit=200)

        if not tweets:
            return {"best_hours": [], "best_days": [], "total_analyzed": 0}

        hour_engagement = defaultdict(list)
        day_engagement = defaultdict(list)

        for t in tweets:
            created = t.get("created_at", "")
            if not created:
                continue

            try:
                dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue

            engagement = (t.get("like_count", 0) + t.get("retweet_count", 0) +
                          t.get("reply_count", 0))

            hour_engagement[dt.hour].append(engagement)
            day_engagement[dt.strftime("%A")].append(engagement)

        best_hours = []
        for hour, engagements in sorted(hour_engagement.items()):
            avg = sum(engagements) / len(engagements)
            best_hours.append({"hour": hour, "avg_engagement": round(avg, 1),
                                "tweet_count": len(engagements)})
        best_hours.sort(key=lambda x: x["avg_engagement"], reverse=True)

        best_days = []
        for day, engagements in day_engagement.items():
            avg = sum(engagements) / len(engagements)
            best_days.append({"day": day, "avg_engagement": round(avg, 1),
                               "tweet_count": len(engagements)})
        best_days.sort(key=lambda x: x["avg_engagement"], reverse=True)

        return {
            "best_hours": best_hours[:5],
            "best_days": best_days,
            "total_analyzed": len(tweets),
        }

    # ── 热门推文分析 ──

    def top_tweets(self, username: str = "", limit: int = 10,
                    metric: str = "like_count") -> List[Dict]:
        return self.db.get_top_tweets(username, limit, metric)

    def analyze_top_content(self, username: str = "",
                             limit: int = 20) -> Dict:
        """分析热门内容特征"""
        tweets = self.db.get_top_tweets(username, limit, "like_count")
        if not tweets:
            return {"patterns": [], "avg_length": 0}

        lengths = []
        has_emoji = 0
        has_hashtag = 0
        has_url = 0
        has_question = 0
        has_media = 0
        hashtag_counter = Counter()

        for t in tweets:
            text = t.get("text", "")
            lengths.append(len(text))

            if any(ord(c) > 0x1F00 for c in text):
                has_emoji += 1
            if "#" in text:
                has_hashtag += 1
                for word in text.split():
                    if word.startswith("#"):
                        hashtag_counter[word.lower()] += 1
            if "http" in text:
                has_url += 1
            if "?" in text:
                has_question += 1

        total = len(tweets)
        return {
            "total_analyzed": total,
            "avg_length": round(sum(lengths) / max(total, 1), 0),
            "emoji_pct": round(has_emoji / max(total, 1) * 100, 1),
            "hashtag_pct": round(has_hashtag / max(total, 1) * 100, 1),
            "url_pct": round(has_url / max(total, 1) * 100, 1),
            "question_pct": round(has_question / max(total, 1) * 100, 1),
            "top_hashtags": hashtag_counter.most_common(10),
            "patterns": self._identify_patterns(tweets),
        }

    def _identify_patterns(self, tweets: List[Dict]) -> List[str]:
        """识别热门推文模式"""
        patterns = []
        if not tweets:
            return patterns

        avg_likes = sum(t.get("like_count", 0) for t in tweets) / len(tweets)

        threads = [t for t in tweets if "🧵" in t.get("text", "") or
                   "thread" in t.get("text", "").lower()]
        if len(threads) > len(tweets) * 0.2:
            patterns.append("线程推文表现好")

        questions = [t for t in tweets if "?" in t.get("text", "")]
        if len(questions) > len(tweets) * 0.3:
            patterns.append("提问推文互动高")

        short = [t for t in tweets if len(t.get("text", "")) < 100]
        if len(short) > len(tweets) * 0.5:
            patterns.append("短推文更受欢迎")

        return patterns

    # ── 综合报告 ──

    def generate_report(self, username: str) -> str:
        """生成综合分析报告"""
        self.track_user(username)

        engagement = self.calculate_engagement_rate(username)
        growth = self.get_follower_growth(username)
        best_times = self.best_posting_times(username)
        top = self.top_tweets(username, limit=5)
        content_analysis = self.analyze_top_content(username)

        lines = [f"📊 *@{username} 分析报告*\n"]

        lines.append("*互动率*")
        lines.append(f"  📈 互动率: {engagement['engagement_rate']:.2f}%")
        lines.append(f"  ❤️ 平均点赞: {engagement['avg_likes']}")
        lines.append(f"  🔄 平均转推: {engagement['avg_retweets']}")
        lines.append(f"  💬 平均回复: {engagement['avg_replies']}")
        lines.append(f"  📝 分析推文数: {engagement['total_tweets']}\n")

        if growth:
            emoji = "📈" if growth["growth"] >= 0 else "📉"
            lines.append("*粉丝增长*")
            lines.append(f"  {emoji} {growth['growth']:+,} ({growth['growth_rate']:+.2f}%)")
            lines.append(f"  👥 当前: {growth['current']:,}\n")

        if best_times["best_hours"]:
            lines.append("*最佳发帖时间*")
            for h in best_times["best_hours"][:3]:
                lines.append(f"  🕐 {h['hour']:02d}:00 UTC (⚡{h['avg_engagement']})")
            lines.append("")

        if top:
            lines.append("*热门推文 Top 5*")
            for i, t in enumerate(top[:5], 1):
                text = t.get("text", "")[:60]
                likes = t.get("like_count", 0)
                lines.append(f"  {i}. ❤️{likes} | {text}")
            lines.append("")

        if content_analysis.get("patterns"):
            lines.append("*内容洞察*")
            for p in content_analysis["patterns"]:
                lines.append(f"  💡 {p}")

        report = "\n".join(lines)

        if self.webhook:
            self.webhook.notify_analytics(username, report)

        return report

    def format_engagement_summary(self, stats: Dict) -> str:
        """格式化互动统计"""
        lines = ["⚡ *互动统计 (7天)*\n"]
        for action, count in stats.items():
            emoji = {"reply": "💬", "like": "❤️", "retweet": "🔄"}.get(action, "📌")
            lines.append(f"  {emoji} {action}: {count}")
        return "\n".join(lines) if len(lines) > 1 else "暂无互动数据"
