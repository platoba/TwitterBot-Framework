"""
定时发推调度器
支持定时发推 + 队列管理 + A/B测试发布
"""

import logging
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Callable

from bot.twitter_api import TwitterAPI
from bot.database import Database
from bot.webhook import TelegramWebhook
from bot.content_generator import ContentGenerator

logger = logging.getLogger(__name__)


class SchedulerStrategy:
    """定时发推调度器"""

    def __init__(self, api: TwitterAPI, db: Database,
                 webhook: TelegramWebhook = None,
                 content_generator: ContentGenerator = None):
        self.api = api
        self.db = db
        self.webhook = webhook
        self.generator = content_generator or ContentGenerator()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self.poll_interval: int = 60
        self._pre_send_hook: Optional[Callable] = None
        self._post_send_hook: Optional[Callable] = None

    # ── 调度管理 ──

    def schedule_tweet(self, content: str, scheduled_at: str,
                        metadata: Dict = None) -> int:
        """添加定时推文"""
        return self.db.add_scheduled_tweet(content, scheduled_at, metadata)

    def schedule_at(self, content: str, dt: datetime,
                     metadata: Dict = None) -> int:
        """使用datetime对象调度"""
        scheduled_at = dt.strftime("%Y-%m-%dT%H:%M:%S")
        return self.schedule_tweet(content, scheduled_at, metadata)

    def schedule_in(self, content: str, minutes: int = 0,
                     hours: int = 0, metadata: Dict = None) -> int:
        """延迟N分钟/小时后发送"""
        dt = datetime.now(timezone.utc) + timedelta(minutes=minutes, hours=hours)
        return self.schedule_at(content, dt, metadata)

    def schedule_generated(self, category: str, variables: Dict[str, str],
                            scheduled_at: str) -> int:
        """生成内容并调度"""
        content = self.generator.generate(category, variables)
        if not content:
            raise ValueError(f"Failed to generate content for category: {category}")
        return self.schedule_tweet(content, scheduled_at,
                                    {"category": category, "generated": True})

    def schedule_ab_test(self, test_name: str, category: str,
                          variables: Dict[str, str],
                          scheduled_at: str,
                          delay_minutes: int = 60) -> Dict:
        """调度A/B测试: 两个变体在不同时间发送"""
        variant_a, variant_b = self.generator.generate_ab_pair(category, variables)

        dt_a = datetime.fromisoformat(scheduled_at)
        dt_b = dt_a + timedelta(minutes=delay_minutes)

        test_id = self.db.create_ab_test(test_name, variant_a, variant_b)

        sched_a = self.schedule_tweet(variant_a, scheduled_at,
                                       {"ab_test_id": test_id, "variant": "A"})
        sched_b = self.schedule_tweet(variant_b, dt_b.strftime("%Y-%m-%dT%H:%M:%S"),
                                       {"ab_test_id": test_id, "variant": "B"})

        return {
            "test_id": test_id,
            "variant_a": variant_a,
            "variant_b": variant_b,
            "schedule_a_id": sched_a,
            "schedule_b_id": sched_b,
        }

    # ── 队列操作 ──

    def get_queue(self, status: str = None, limit: int = 20) -> List[Dict]:
        return self.db.get_schedule_queue(status, limit)

    def get_pending(self) -> List[Dict]:
        return self.db.get_pending_tweets()

    def cancel_tweet(self, schedule_id: int) -> bool:
        """取消定时推文"""
        self.db.update_schedule_status(schedule_id, "cancelled")
        return True

    # ── 执行 ──

    def send_tweet(self, schedule_item: Dict) -> Dict:
        """发送单条定时推文"""
        content = schedule_item["content"]
        schedule_id = schedule_item["id"]

        if self._pre_send_hook:
            content = self._pre_send_hook(content) or content

        result = self.api.post_tweet(content)

        if result and "data" in result:
            tweet_id = result["data"].get("id", "")
            self.db.update_schedule_status(schedule_id, "sent", tweet_id=tweet_id)

            if self.webhook:
                self.webhook.notify_scheduled_tweet(content, "sent", tweet_id)

            if self._post_send_hook:
                self._post_send_hook(schedule_item, result)

            return {"status": "sent", "tweet_id": tweet_id}
        else:
            error = str(result) if result else "No response"
            self.db.update_schedule_status(schedule_id, "failed", error=error)

            if self.webhook:
                self.webhook.notify_scheduled_tweet(content, "failed")

            return {"status": "failed", "error": error}

    def process_pending(self) -> List[Dict]:
        """处理所有到期的定时推文"""
        pending = self.get_pending()
        results = []

        for item in pending:
            result = self.send_tweet(item)
            result["schedule_id"] = item["id"]
            results.append(result)

            time.sleep(2)

        return results

    # ── 后台运行 ──

    def start(self):
        """启动后台调度循环"""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("Scheduler started")

    def stop(self):
        """停止调度循环"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("Scheduler stopped")

    @property
    def is_running(self) -> bool:
        return self._running

    def _run_loop(self):
        while self._running:
            try:
                results = self.process_pending()
                if results:
                    logger.info(f"Processed {len(results)} scheduled tweets")
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
            time.sleep(self.poll_interval)

    # ── Hooks ──

    def set_pre_send_hook(self, func: Callable):
        self._pre_send_hook = func

    def set_post_send_hook(self, func: Callable):
        self._post_send_hook = func

    # ── 格式化 ──

    def format_queue(self, items: List[Dict] = None) -> str:
        """格式化队列显示"""
        if items is None:
            items = self.get_queue("pending")

        if not items:
            return "📋 调度队列为空"

        lines = ["📋 *调度队列*\n"]
        for item in items[:20]:
            status_emoji = {
                "pending": "⏳", "sent": "✅", "failed": "❌", "cancelled": "🚫"
            }.get(item.get("status", ""), "❓")

            content = item.get("content", "")[:50]
            sched_at = item.get("scheduled_at", "?")
            lines.append(f"{status_emoji} #{item['id']} | {sched_at}")
            lines.append(f"   {content}\n")

        return "\n".join(lines)
