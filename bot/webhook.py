"""
Webhook模块 - 实时推送通知到Telegram
"""

import os
import logging
from typing import Optional, Dict, Any, List

import requests

logger = logging.getLogger(__name__)


class TelegramWebhook:
    """Telegram Bot API 封装 + Webhook通知推送"""

    def __init__(self, bot_token: str = None, default_chat_id: str = None):
        self.bot_token = bot_token or os.environ.get("BOT_TOKEN", "")
        self.default_chat_id = default_chat_id or os.environ.get("TG_CHAT_ID", "")
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}"
        self._session = requests.Session()

    @property
    def is_configured(self) -> bool:
        return bool(self.bot_token)

    def _call(self, method: str, params: Dict = None,
              json_data: Dict = None) -> Optional[Dict]:
        try:
            if json_data:
                resp = self._session.post(
                    f"{self.api_url}/{method}",
                    json=json_data, timeout=30
                )
            else:
                resp = self._session.get(
                    f"{self.api_url}/{method}",
                    params=params, timeout=30
                )
            data = resp.json()
            if not data.get("ok"):
                logger.warning(f"TG API {method} failed: {data}")
            return data
        except Exception as e:
            logger.error(f"TG API error: {e}")
            return None

    def get_me(self) -> Optional[Dict]:
        return self._call("getMe")

    def send_message(self, chat_id: str, text: str,
                     reply_to: int = None,
                     parse_mode: str = "Markdown",
                     disable_preview: bool = True) -> Optional[Dict]:
        params = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": disable_preview
        }
        if reply_to:
            params["reply_to_message_id"] = reply_to
        if parse_mode:
            params["parse_mode"] = parse_mode

        result = self._call("sendMessage", params=params)

        if not result or not result.get("ok"):
            params.pop("parse_mode", None)
            result = self._call("sendMessage", params=params)

        return result

    def get_updates(self, offset: int = None, timeout: int = 30) -> Optional[Dict]:
        params = {"timeout": timeout}
        if offset is not None:
            params["offset"] = offset
        return self._call("getUpdates", params=params)

    # ── 通知推送 ──

    def notify(self, text: str, chat_id: str = None) -> bool:
        target = chat_id or self.default_chat_id
        if not target:
            logger.warning("No chat_id for notification")
            return False
        result = self.send_message(target, text)
        return bool(result and result.get("ok"))

    def notify_new_tweets(self, query: str, tweets: List[Dict],
                           chat_id: str = None) -> bool:
        if not tweets:
            return False
        lines = [f"🔔 *新推文匹配*: `{query}`\n"]
        for t in tweets[:5]:
            username = t.get("author_username", "?")
            text = t.get("text", "")[:100]
            metrics = t.get("public_metrics", {})
            likes = metrics.get("like_count", 0)
            rts = metrics.get("retweet_count", 0)
            lines.append(f"@{username} | ❤️{likes} 🔄{rts}")
            lines.append(f"  {text}\n")
        if len(tweets) > 5:
            lines.append(f"_...还有 {len(tweets) - 5} 条_")
        return self.notify("\n".join(lines), chat_id)

    def notify_engagement(self, action: str, target: str,
                           detail: str = "", chat_id: str = None) -> bool:
        text = f"⚡ *自动互动*\n操作: {action}\n目标: {target}"
        if detail:
            text += f"\n详情: {detail}"
        return self.notify(text, chat_id)

    def notify_scheduled_tweet(self, content: str, status: str,
                                tweet_id: str = None,
                                chat_id: str = None) -> bool:
        emoji = "✅" if status == "sent" else "❌"
        text = f"{emoji} *定时推文*\n状态: {status}\n内容: {content[:100]}"
        if tweet_id:
            text += f"\nID: `{tweet_id}`"
        return self.notify(text, chat_id)

    def notify_analytics(self, username: str, summary: str,
                          chat_id: str = None) -> bool:
        text = f"📊 *分析报告* @{username}\n\n{summary}"
        return self.notify(text, chat_id)

    def notify_alert(self, title: str, message: str,
                      level: str = "info", chat_id: str = None) -> bool:
        emojis = {"info": "ℹ️", "warning": "⚠️", "error": "🚨", "success": "✅"}
        emoji = emojis.get(level, "ℹ️")
        text = f"{emoji} *{title}*\n{message}"
        return self.notify(text, chat_id)

    def notify_ab_test_result(self, test_name: str, winner: str,
                               metrics_a: Dict, metrics_b: Dict,
                               chat_id: str = None) -> bool:
        text = (
            f"🧪 *A/B测试结果*: {test_name}\n\n"
            f"🅰️ 变体A: ❤️{metrics_a.get('likes', 0)} 🔄{metrics_a.get('retweets', 0)}\n"
            f"🅱️ 变体B: ❤️{metrics_b.get('likes', 0)} 🔄{metrics_b.get('retweets', 0)}\n\n"
            f"🏆 胜出: *{winner}*"
        )
        return self.notify(text, chat_id)
