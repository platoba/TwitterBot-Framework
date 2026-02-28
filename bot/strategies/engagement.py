"""
自动互动策略
自动回复 + 自动点赞 + 竞品追踪互动
"""

import logging
import re
from typing import Dict, List, Optional, Callable

from bot.twitter_api import TwitterAPI
from bot.database import Database
from bot.webhook import TelegramWebhook

logger = logging.getLogger(__name__)


class EngagementRule:
    """互动规则"""

    def __init__(self, name: str, keyword_pattern: str,
                 reply_template: str = "",
                 action: str = "reply",
                 min_followers: int = 0,
                 max_followers: int = 0,
                 enabled: bool = True):
        self.name = name
        self.keyword_pattern = keyword_pattern
        self.reply_template = reply_template
        self.action = action  # reply, like, retweet, like_and_reply
        self.min_followers = min_followers
        self.max_followers = max_followers
        self.enabled = enabled
        self._compiled = re.compile(keyword_pattern, re.IGNORECASE)

    def matches(self, text: str) -> bool:
        return bool(self._compiled.search(text))

    def check_follower_filter(self, follower_count: int) -> bool:
        if self.min_followers > 0 and follower_count < self.min_followers:
            return False
        if self.max_followers > 0 and follower_count > self.max_followers:
            return False
        return True

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "pattern": self.keyword_pattern,
            "reply_template": self.reply_template,
            "action": self.action,
            "min_followers": self.min_followers,
            "max_followers": self.max_followers,
            "enabled": self.enabled,
        }


class EngagementStrategy:
    """自动互动策略引擎"""

    def __init__(self, api: TwitterAPI, db: Database,
                 webhook: TelegramWebhook = None):
        self.api = api
        self.db = db
        self.webhook = webhook
        self.rules: List[EngagementRule] = []
        self._reply_formatters: Dict[str, Callable] = {}
        self.my_user_id: Optional[str] = None
        self.dry_run: bool = False

    def add_rule(self, rule: EngagementRule):
        self.rules.append(rule)

    def remove_rule(self, name: str) -> bool:
        before = len(self.rules)
        self.rules = [r for r in self.rules if r.name != name]
        return len(self.rules) < before

    def get_rules(self) -> List[Dict]:
        return [r.to_dict() for r in self.rules]

    def register_reply_formatter(self, name: str, func: Callable):
        """注册自定义回复格式化函数"""
        self._reply_formatters[name] = func

    def format_reply(self, rule: EngagementRule, tweet: Dict,
                      author: Dict = None) -> str:
        """格式化回复内容"""
        template = rule.reply_template
        if rule.name in self._reply_formatters:
            return self._reply_formatters[rule.name](tweet, author)

        username = author.get("username", "") if author else ""
        return template.format(
            username=username,
            tweet_text=tweet.get("text", "")[:50],
            author_name=author.get("name", "") if author else ""
        )

    def process_tweet(self, tweet: Dict, author: Dict = None) -> List[Dict]:
        """处理单条推文, 返回执行的操作列表"""
        results = []
        text = tweet.get("text", "")

        for rule in self.rules:
            if not rule.enabled:
                continue

            if not rule.matches(text):
                continue

            if author and not rule.check_follower_filter(
                author.get("public_metrics", {}).get("followers_count", 0)
            ):
                continue

            action_result = self._execute_action(rule, tweet, author)
            if action_result:
                results.append(action_result)

        return results

    def _execute_action(self, rule: EngagementRule, tweet: Dict,
                         author: Dict = None) -> Optional[Dict]:
        """执行互动操作"""
        tweet_id = tweet.get("id", "")
        username = author.get("username", "") if author else ""

        result = {
            "rule": rule.name,
            "action": rule.action,
            "tweet_id": tweet_id,
            "username": username,
        }

        if self.dry_run:
            result["status"] = "dry_run"
            return result

        try:
            if rule.action in ("reply", "like_and_reply"):
                reply_text = self.format_reply(rule, tweet, author)
                if reply_text:
                    resp = self.api.post_tweet(reply_text, reply_to=tweet_id)
                    result["reply_text"] = reply_text
                    result["reply_response"] = resp
                    self.db.log_engagement("reply", tweet_id, username, reply_text)

            if rule.action in ("like", "like_and_reply"):
                if self.my_user_id:
                    self.api.like_tweet(self.my_user_id, tweet_id)
                    self.db.log_engagement("like", tweet_id, username)

            if rule.action == "retweet":
                if self.my_user_id:
                    self.api.retweet(self.my_user_id, tweet_id)
                    self.db.log_engagement("retweet", tweet_id, username)

            result["status"] = "success"

            if self.webhook:
                self.webhook.notify_engagement(
                    rule.action, f"@{username}", rule.name
                )

        except Exception as e:
            logger.error(f"Engagement action failed: {e}")
            result["status"] = "error"
            result["error"] = str(e)
            self.db.log_engagement(rule.action, tweet_id, username, status="error")

        return result

    def process_search_results(self, query: str, max_results: int = 10) -> List[Dict]:
        """搜索并处理推文"""
        data = self.api.search_recent(query, max_results)
        if not data or "data" not in data:
            return []

        users = {}
        for u in data.get("includes", {}).get("users", []):
            users[u["id"]] = u

        all_results = []
        for tweet in data["data"]:
            author = users.get(tweet.get("author_id"))
            results = self.process_tweet(tweet, author)
            all_results.extend(results)
            self.db.save_tweet({**tweet, "author_username": author.get("username", "") if author else ""}, query)

        return all_results

    def get_engagement_stats(self, days: int = 7) -> Dict:
        return self.db.get_engagement_stats(days)
