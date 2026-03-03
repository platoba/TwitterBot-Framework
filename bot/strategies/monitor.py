"""
话题监控 + 竞品追踪策略
实时监控关键词、用户、竞品账号
"""

import logging
from typing import Dict, List

from bot.twitter_api import TwitterAPI
from bot.database import Database
from bot.webhook import TelegramWebhook

logger = logging.getLogger(__name__)


class MonitorStrategy:
    """话题/竞品监控引擎"""

    def __init__(self, api: TwitterAPI, db: Database,
                 webhook: TelegramWebhook = None):
        self.api = api
        self.db = db
        self.webhook = webhook
        self.check_interval: int = 300  # 5分钟

    # ── 关键词监控 ──

    def add_keyword_monitor(self, keyword: str, chat_id: str,
                             config: Dict = None) -> int:
        """添加关键词监控"""
        return self.db.add_monitor(keyword, chat_id, "keyword", config)

    def add_competitor_monitor(self, username: str, chat_id: str,
                                config: Dict = None) -> int:
        """添加竞品账号监控"""
        return self.db.add_monitor(username, chat_id, "competitor", config)

    def remove_monitor(self, keyword: str) -> bool:
        return self.db.deactivate_monitor(keyword)

    def get_active_monitors(self, monitor_type: str = None) -> List[Dict]:
        return self.db.get_active_monitors(monitor_type)

    # ── 检查逻辑 ──

    def check_keyword(self, monitor: Dict) -> List[Dict]:
        """检查关键词新推文"""
        keyword = monitor["keyword"]
        since_id = monitor.get("last_tweet_id")

        data = self.api.search_recent(keyword, max_results=10, since_id=since_id)
        if not data or "data" not in data:
            return []

        tweets = data["data"]
        users = {u["id"]: u for u in data.get("includes", {}).get("users", [])}

        new_tweets = []
        latest_id = since_id

        for tweet in tweets:
            author = users.get(tweet.get("author_id"), {})
            tweet["author_username"] = author.get("username", "")
            new_tweets.append(tweet)

            self.db.save_tweet(tweet, source_query=keyword)

            tid = tweet.get("id", "")
            if latest_id is None or tid > latest_id:
                latest_id = tid

        if latest_id and latest_id != since_id:
            self.db.update_monitor(monitor["id"], latest_id)

        return new_tweets

    def check_competitor(self, monitor: Dict) -> List[Dict]:
        """检查竞品账号新推文"""
        username = monitor["keyword"]
        since_id = monitor.get("last_tweet_id")

        user_id = self.api.resolve_username(username)
        if not user_id:
            return []

        data = self.api.get_user_tweets(user_id, max_results=10, since_id=since_id)
        if not data or "data" not in data:
            return []

        tweets = data["data"]
        new_tweets = []
        latest_id = since_id

        for tweet in tweets:
            tweet["author_username"] = username
            new_tweets.append(tweet)

            self.db.save_tweet(tweet, source_query=f"@{username}")

            tid = tweet.get("id", "")
            if latest_id is None or tid > latest_id:
                latest_id = tid

        if latest_id and latest_id != since_id:
            self.db.update_monitor(monitor["id"], latest_id)

        return new_tweets

    def check_all(self) -> Dict[str, List[Dict]]:
        """检查所有活跃监控"""
        results = {}
        monitors = self.db.get_active_monitors()

        for monitor in monitors:
            keyword = monitor["keyword"]
            monitor_type = monitor.get("monitor_type", "keyword")
            chat_id = monitor.get("chat_id", "")

            try:
                if monitor_type == "keyword":
                    tweets = self.check_keyword(monitor)
                elif monitor_type == "competitor":
                    tweets = self.check_competitor(monitor)
                else:
                    continue

                if tweets:
                    results[keyword] = tweets
                    if self.webhook and chat_id:
                        self.webhook.notify_new_tweets(keyword, tweets, chat_id)

            except Exception as e:
                logger.error(f"Monitor check failed for '{keyword}': {e}")

        return results

    # ── 竞品分析 ──

    def compare_competitors(self, usernames: List[str]) -> List[Dict]:
        """对比竞品账号数据"""
        results = []
        for username in usernames:
            data = self.api.get_user(username)
            if not data or "data" not in data:
                continue

            user = data["data"]
            metrics = user.get("public_metrics", {})

            recent = self.api.get_user_tweets(user["id"], max_results=10)
            avg_engagement = 0
            if recent and "data" in recent:
                total_eng = sum(
                    t.get("public_metrics", {}).get("like_count", 0) +
                    t.get("public_metrics", {}).get("retweet_count", 0)
                    for t in recent["data"]
                )
                avg_engagement = total_eng / max(len(recent["data"]), 1)

            results.append({
                "username": username,
                "followers": metrics.get("followers_count", 0),
                "following": metrics.get("following_count", 0),
                "tweets": metrics.get("tweet_count", 0),
                "avg_engagement": round(avg_engagement, 1),
                "description": user.get("description", ""),
            })

            self.db.save_analytics_snapshot(username, metrics)

        results.sort(key=lambda x: x["followers"], reverse=True)
        return results

    def format_comparison(self, comparisons: List[Dict]) -> str:
        """格式化竞品对比"""
        if not comparisons:
            return "❌ 无竞品数据"

        lines = ["📊 *竞品对比*\n"]
        for i, c in enumerate(comparisons, 1):
            lines.append(
                f"{i}. @{c['username']}\n"
                f"   👥 {c['followers']:,} | 📝 {c['tweets']:,} | "
                f"⚡ {c['avg_engagement']:.0f}/推\n"
            )
        return "\n".join(lines)

    def get_monitor_summary(self) -> str:
        """获取监控摘要"""
        monitors = self.db.get_active_monitors()
        if not monitors:
            return "📋 暂无活跃监控"

        kw_monitors = [m for m in monitors if m.get("monitor_type") == "keyword"]
        comp_monitors = [m for m in monitors if m.get("monitor_type") == "competitor"]

        lines = ["📋 *监控列表*\n"]
        if kw_monitors:
            lines.append("🔍 *关键词*")
            for m in kw_monitors:
                checked = m.get("last_checked", "从未") or "从未"
                lines.append(f"  • `{m['keyword']}` (最后检查: {checked})")

        if comp_monitors:
            lines.append("\n👀 *竞品*")
            for m in comp_monitors:
                checked = m.get("last_checked", "从未") or "从未"
                lines.append(f"  • @{m['keyword']} (最后检查: {checked})")

        return "\n".join(lines)
