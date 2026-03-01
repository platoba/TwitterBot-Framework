"""
TwitterBot Framework v3.0 — Twitter/X全栈自动化工具箱
集成: 策略引擎 + 分析面板 + 内容生成 + 定时调度 + A/B测试
通过Telegram Bot控制
"""

import os
import sys
import time
import json
import logging
from datetime import datetime, timezone

from bot.twitter_api import TwitterAPI
from bot.database import Database
from bot.webhook import TelegramWebhook
from bot.content_generator import ContentGenerator
from bot.strategies.analytics import AnalyticsStrategy
from bot.strategies.engagement import EngagementStrategy, EngagementRule
from bot.strategies.monitor import MonitorStrategy
from bot.strategies.scheduler import SchedulerStrategy

# ── 配置 ──

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger("twitterbot")

TOKEN = os.environ.get("BOT_TOKEN", "")
DB_PATH = os.environ.get("DB_PATH", "twitterbot.db")

if not TOKEN:
    print("❌ 未设置 BOT_TOKEN!")
    sys.exit(1)


class TwitterBot:
    """TwitterBot主控制器"""

    def __init__(self):
        # 核心组件
        self.api = TwitterAPI()
        self.db = Database(DB_PATH)
        self.webhook = TelegramWebhook(bot_token=TOKEN)
        self.generator = ContentGenerator()

        # 策略引擎
        self.analytics = AnalyticsStrategy(self.api, self.db, self.webhook)
        self.engagement = EngagementStrategy(self.api, self.db, self.webhook)
        self.monitor = MonitorStrategy(self.api, self.db, self.webhook)
        self.scheduler = SchedulerStrategy(
            self.api, self.db, self.webhook, self.generator
        )

        # 调度器启动
        if os.environ.get("SCHEDULER_ENABLED", "true").lower() == "true":
            self.scheduler.start()
            logger.info("Scheduler started")

    def handle(self, chat_id: int, msg_id: int, text: str):
        """处理Telegram消息"""
        parts = text.split(maxsplit=1)
        cmd = parts[0].lower()
        args = parts[1].strip() if len(parts) > 1 else ""

        handlers = {
            "/start": self._cmd_start,
            "/help": self._cmd_start,
            "/search": self._cmd_search,
            "/user": self._cmd_user,
            "/tweets": self._cmd_tweets,
            "/monitor": self._cmd_monitor,
            "/monitors": self._cmd_monitors,
            "/unmonitor": self._cmd_unmonitor,
            "/analyze": self._cmd_analyze,
            "/report": self._cmd_report,
            "/schedule": self._cmd_schedule,
            "/queue": self._cmd_queue,
            "/cancel": self._cmd_cancel,
            "/generate": self._cmd_generate,
            "/ab": self._cmd_ab_test,
            "/engage": self._cmd_engage,
            "/rules": self._cmd_rules,
            "/addrule": self._cmd_add_rule,
            "/rmrule": self._cmd_remove_rule,
            "/competitors": self._cmd_competitors,
            "/checkall": self._cmd_check_all,
            "/stats": self._cmd_stats,
            "/besttimes": self._cmd_best_times,
            "/toptweets": self._cmd_top_tweets,
            "/status": self._cmd_status,
        }

        handler = handlers.get(cmd)
        if handler:
            handler(chat_id, msg_id, args)
        elif text.startswith("/"):
            self._send(chat_id, "❓ 未知命令，输入 /help 查看帮助", msg_id)

    def _send(self, chat_id, text, reply_to=None):
        self.webhook.send_message(str(chat_id), text, reply_to=reply_to)

    # ── 基础命令 ──

    def _cmd_start(self, chat_id, msg_id, args):
        status = "✅" if self.api.is_configured else "⚠️"
        sched = "✅ 运行中" if self.scheduler.is_running else "⏸ 已停止"
        self._send(chat_id,
            "🐦 *TwitterBot Framework v3.0*\n\n"
            "Twitter/X全栈自动化工具箱\n\n"
            "🔍 *搜索*\n"
            "  /search <关键词> — 搜索推文\n"
            "  /user <用户名> — 用户信息\n"
            "  /tweets <用户名> — 最近推文\n\n"
            "📊 *分析*\n"
            "  /analyze <用户名> — 快速分析\n"
            "  /report <用户名> — 综合报告\n"
            "  /besttimes <用户名> — 最佳发帖时间\n"
            "  /toptweets — 热门推文\n"
            "  /stats — 互动统计\n\n"
            "👀 *监控*\n"
            "  /monitor <关键词> — 添加关键词监控\n"
            "  /monitors — 查看监控列表\n"
            "  /unmonitor <关键词> — 取消监控\n"
            "  /competitors <用户1> <用户2> — 竞品对比\n"
            "  /checkall — 立即检查所有监控\n\n"
            "📝 *内容*\n"
            "  /generate <类型> — 生成推文\n"
            "  /ab <类型> — A/B测试内容\n\n"
            "⏰ *调度*\n"
            "  /schedule <时间> <内容> — 定时推文\n"
            "  /queue — 查看调度队列\n"
            "  /cancel <ID> — 取消定时推文\n\n"
            "⚡ *自动互动*\n"
            "  /engage <查询> — 按规则自动互动\n"
            "  /rules — 查看互动规则\n"
            "  /addrule <名称> <模式> <动作> — 添加规则\n"
            "  /rmrule <名称> — 删除规则\n\n"
            "ℹ️ *系统*\n"
            "  /status — 系统状态\n"
            f"\nTwitter API: {status}\n调度器: {sched}", msg_id)

    def _cmd_search(self, chat_id, msg_id, args):
        if not args:
            return self._send(chat_id, "用法: /search <关键词>", msg_id)
        data = self.api.search_recent(args)
        if not data or "data" not in data:
            return self._send(chat_id, "❌ 搜索失败或无结果", msg_id)

        users = {u["id"]: u for u in data.get("includes", {}).get("users", [])}
        lines = [f"🔍 搜索: `{args}` ({len(data['data'])}条)\n"]
        for t in data["data"][:10]:
            m = t.get("public_metrics", {})
            user = users.get(t.get("author_id"), {})
            username = user.get("username", "?")
            lines.append(f"@{username} | ❤️{m.get('like_count',0)} 🔄{m.get('retweet_count',0)}")
            lines.append(f"  {t['text'][:80]}\n")
            # 保存到数据库
            t["author_username"] = username
            self.db.save_tweet(t, args)
        self._send(chat_id, "\n".join(lines), msg_id)

    def _cmd_user(self, chat_id, msg_id, args):
        if not args:
            return self._send(chat_id, "用法: /user <用户名>", msg_id)
        username = args.lstrip("@")
        data = self.api.get_user(username)
        if not data or "data" not in data:
            return self._send(chat_id, f"❌ 用户 @{username} 不存在", msg_id)

        d = data["data"]
        m = d.get("public_metrics", {})
        self._send(chat_id,
            f"🐦 *@{d['username']}* ({d.get('name', '')})\n\n"
            f"👥 粉丝: {m.get('followers_count', 0):,}\n"
            f"👤 关注: {m.get('following_count', 0):,}\n"
            f"📝 推文: {m.get('tweet_count', 0):,}\n"
            f"📋 简介: {d.get('description', '')[:150]}\n"
            f"📅 注册: {d.get('created_at', '')[:10]}", msg_id)

    def _cmd_tweets(self, chat_id, msg_id, args):
        if not args:
            return self._send(chat_id, "用法: /tweets <用户名>", msg_id)
        username = args.lstrip("@")
        user_id = self.api.resolve_username(username)
        if not user_id:
            return self._send(chat_id, f"❌ 用户 @{username} 不存在", msg_id)

        data = self.api.get_user_tweets(user_id, max_results=10)
        if not data or "data" not in data:
            return self._send(chat_id, "❌ 无推文", msg_id)

        lines = [f"📝 @{username} 最近推文\n"]
        for t in data["data"]:
            m = t.get("public_metrics", {})
            lines.append(f"❤️{m.get('like_count',0)} 🔄{m.get('retweet_count',0)} 💬{m.get('reply_count',0)}")
            lines.append(f"  {t['text'][:100]}\n")
        self._send(chat_id, "\n".join(lines), msg_id)

    # ── 监控 ──

    def _cmd_monitor(self, chat_id, msg_id, args):
        if not args:
            return self._send(chat_id, "用法: /monitor <关键词>", msg_id)
        mid = self.monitor.add_keyword_monitor(args, str(chat_id))
        self._send(chat_id, f"👀 已添加监控: `{args}` (ID: {mid})", msg_id)

    def _cmd_monitors(self, chat_id, msg_id, args):
        self._send(chat_id, self.monitor.get_monitor_summary(), msg_id)

    def _cmd_unmonitor(self, chat_id, msg_id, args):
        if not args:
            return self._send(chat_id, "用法: /unmonitor <关键词>", msg_id)
        if self.monitor.remove_monitor(args):
            self._send(chat_id, f"✅ 已取消监控: `{args}`", msg_id)
        else:
            self._send(chat_id, f"⚠️ 未找到监控: `{args}`", msg_id)

    def _cmd_check_all(self, chat_id, msg_id, args):
        results = self.monitor.check_all()
        if not results:
            return self._send(chat_id, "✅ 所有监控已检查，暂无新推文", msg_id)
        total = sum(len(v) for v in results.values())
        lines = [f"🔔 发现 {total} 条新推文\n"]
        for keyword, tweets in results.items():
            lines.append(f"*{keyword}*: {len(tweets)}条新推文")
        self._send(chat_id, "\n".join(lines), msg_id)

    def _cmd_competitors(self, chat_id, msg_id, args):
        if not args:
            return self._send(chat_id, "用法: /competitors <用户1> <用户2> ...", msg_id)
        usernames = [u.lstrip("@") for u in args.split()]
        results = self.monitor.compare_competitors(usernames)
        self._send(chat_id, self.monitor.format_comparison(results), msg_id)

    # ── 分析 ──

    def _cmd_analyze(self, chat_id, msg_id, args):
        if not args:
            return self._send(chat_id, "用法: /analyze <用户名>", msg_id)
        username = args.lstrip("@")
        self._cmd_user(chat_id, msg_id, username)
        self._cmd_tweets(chat_id, None, username)

    def _cmd_report(self, chat_id, msg_id, args):
        if not args:
            return self._send(chat_id, "用法: /report <用户名>", msg_id)
        username = args.lstrip("@")
        report = self.analytics.generate_report(username)
        self._send(chat_id, report, msg_id)

    def _cmd_stats(self, chat_id, msg_id, args):
        stats = self.engagement.get_engagement_stats(7)
        self._send(chat_id, self.analytics.format_engagement_summary(stats), msg_id)

    def _cmd_best_times(self, chat_id, msg_id, args):
        username = args.lstrip("@") if args else ""
        result = self.analytics.best_posting_times(username)
        if not result["best_hours"]:
            return self._send(chat_id, "📊 数据不足，需要更多推文历史", msg_id)
        lines = [f"🕐 *最佳发帖时间*\n"]
        for h in result["best_hours"][:5]:
            lines.append(f"  {h['hour']:02d}:00 UTC — ⚡{h['avg_engagement']:.1f} ({h['tweet_count']}条)")
        self._send(chat_id, "\n".join(lines), msg_id)

    def _cmd_top_tweets(self, chat_id, msg_id, args):
        username = args.lstrip("@") if args else ""
        top = self.analytics.top_tweets(username, limit=10)
        if not top:
            return self._send(chat_id, "📊 暂无推文数据", msg_id)
        lines = ["🏆 *热门推文 Top 10*\n"]
        for i, t in enumerate(top[:10], 1):
            text = t.get("text", "")[:60]
            likes = t.get("like_count", 0)
            lines.append(f"  {i}. ❤️{likes} | {text}")
        self._send(chat_id, "\n".join(lines), msg_id)

    # ── 内容生成 ──

    def _cmd_generate(self, chat_id, msg_id, args):
        if not args:
            cats = ", ".join(self.generator.get_categories())
            return self._send(chat_id, f"用法: /generate <类型>\n可用类型: {cats}", msg_id)
        parts = args.split(maxsplit=1)
        category = parts[0]
        extra = parts[1] if len(parts) > 1 else ""
        variables = {"topic": extra, "title": extra, "body": extra,
                     "question": extra, "hashtags": ""}
        result = self.generator.generate(category, variables)
        if not result:
            return self._send(chat_id, f"❌ 未知类型: {category}", msg_id)

        score = self.generator.estimate_engagement(result)
        self._send(chat_id,
            f"📝 *生成推文* ({category})\n\n{result}\n\n"
            f"⚡ 预估互动分: {score['estimated_score']:.0f}/100", msg_id)

    def _cmd_ab_test(self, chat_id, msg_id, args):
        if not args:
            return self._send(chat_id, "用法: /ab <类型> <主题>", msg_id)
        parts = args.split(maxsplit=1)
        category = parts[0]
        topic = parts[1] if len(parts) > 1 else ""
        variables = {"topic": topic, "title": topic, "body": topic,
                     "question": topic, "hashtags": ""}
        a, b = self.generator.generate_ab_pair(category, variables)
        score_a = self.generator.estimate_engagement(a)
        score_b = self.generator.estimate_engagement(b)
        self._send(chat_id,
            f"🧪 *A/B测试* ({category})\n\n"
            f"🅰️ 变体A (⚡{score_a['estimated_score']:.0f})\n{a}\n\n"
            f"🅱️ 变体B (⚡{score_b['estimated_score']:.0f})\n{b}", msg_id)

    # ── 调度 ──

    def _cmd_schedule(self, chat_id, msg_id, args):
        if not args:
            return self._send(chat_id,
                "用法: /schedule <ISO时间> <内容>\n"
                "例: /schedule 2026-03-01T10:00:00 Hello world!", msg_id)
        parts = args.split(maxsplit=1)
        if len(parts) < 2:
            return self._send(chat_id, "❌ 需要时间和内容", msg_id)
        scheduled_at, content = parts
        try:
            sid = self.scheduler.schedule_tweet(content, scheduled_at)
            self._send(chat_id,
                f"⏰ 已调度推文 (ID: {sid})\n"
                f"📅 {scheduled_at}\n"
                f"📝 {content[:100]}", msg_id)
        except Exception as e:
            self._send(chat_id, f"❌ 调度失败: {e}", msg_id)

    def _cmd_queue(self, chat_id, msg_id, args):
        self._send(chat_id, self.scheduler.format_queue(), msg_id)

    def _cmd_cancel(self, chat_id, msg_id, args):
        if not args or not args.isdigit():
            return self._send(chat_id, "用法: /cancel <ID>", msg_id)
        self.scheduler.cancel_tweet(int(args))
        self._send(chat_id, f"✅ 已取消调度 #{args}", msg_id)

    # ── 自动互动 ──

    def _cmd_engage(self, chat_id, msg_id, args):
        if not args:
            return self._send(chat_id, "用法: /engage <搜索查询>", msg_id)
        results = self.engagement.process_search_results(args)
        if not results:
            return self._send(chat_id, "ℹ️ 无匹配规则触发", msg_id)
        lines = [f"⚡ 自动互动结果: {len(results)}个操作\n"]
        for r in results[:10]:
            status = "✅" if r["status"] == "success" else "🏷️" if r["status"] == "dry_run" else "❌"
            lines.append(f"  {status} {r['action']} → @{r.get('username', '?')}")
        self._send(chat_id, "\n".join(lines), msg_id)

    def _cmd_rules(self, chat_id, msg_id, args):
        rules = self.engagement.get_rules()
        if not rules:
            return self._send(chat_id, "📋 暂无互动规则\n用 /addrule 添加", msg_id)
        lines = ["📋 *互动规则*\n"]
        for r in rules:
            status = "✅" if r["enabled"] else "⏸"
            lines.append(f"  {status} *{r['name']}* | {r['action']} | `{r['pattern']}`")
        self._send(chat_id, "\n".join(lines), msg_id)

    def _cmd_add_rule(self, chat_id, msg_id, args):
        parts = args.split(maxsplit=2)
        if len(parts) < 3:
            return self._send(chat_id,
                "用法: /addrule <名称> <模式> <动作>\n"
                "动作: reply, like, retweet, like_and_reply", msg_id)
        name, pattern, action = parts
        rule = EngagementRule(name, pattern, action=action)
        self.engagement.add_rule(rule)
        self._send(chat_id, f"✅ 已添加规则: *{name}* ({action})", msg_id)

    def _cmd_remove_rule(self, chat_id, msg_id, args):
        if not args:
            return self._send(chat_id, "用法: /rmrule <名称>", msg_id)
        if self.engagement.remove_rule(args):
            self._send(chat_id, f"✅ 已删除规则: {args}", msg_id)
        else:
            self._send(chat_id, f"⚠️ 未找到规则: {args}", msg_id)

    # ── 状态 ──

    def _cmd_status(self, chat_id, msg_id, args):
        monitors = len(self.monitor.get_active_monitors())
        rules = len(self.engagement.get_rules())
        pending = len(self.scheduler.get_pending())
        rate_status = self.api.get_rate_limit_status()

        self._send(chat_id,
            "ℹ️ *系统状态*\n\n"
            f"🐦 Twitter API: {'✅ 已连接' if self.api.is_configured else '❌ 未配置'}\n"
            f"✍️ 写操作: {'✅' if self.api.can_write else '❌ 需要OAuth'}\n"
            f"⏰ 调度器: {'✅ 运行中' if self.scheduler.is_running else '⏸ 已停止'}\n"
            f"👀 活跃监控: {monitors}\n"
            f"📋 互动规则: {rules}\n"
            f"⏳ 待发推文: {pending}\n"
            f"🔄 API端点状态: {len(rate_status)}个追踪中", msg_id)

    def run(self):
        """启动Bot主循环"""
        me = self.webhook.get_me()
        if me and me.get("ok"):
            bot_name = me["result"]["username"]
            logger.info(f"✅ @{bot_name} 已上线!")
        else:
            logger.error("❌ 无法连接Telegram!")
            return

        offset = None
        while True:
            try:
                result = self.webhook.get_updates(offset=offset)
                if not result or not result.get("ok"):
                    time.sleep(5)
                    continue
                for update in result.get("result", []):
                    offset = update["update_id"] + 1
                    msg = update.get("message")
                    if not msg:
                        continue
                    text = (msg.get("text") or "").strip()
                    if text:
                        self.handle(msg["chat"]["id"], msg["message_id"], text)
            except KeyboardInterrupt:
                logger.info("Shutting down...")
                self.scheduler.stop()
                self.db.close()
                break
            except Exception as e:
                logger.error(f"Main loop error: {e}")
                time.sleep(5)

    def cleanup(self):
        """清理资源"""
        self.scheduler.stop()
        self.db.close()


def main():
    print(f"\n{'='*50}")
    print(f"  TwitterBot Framework v3.0")
    print(f"{'='*50}")
    bot = TwitterBot()
    try:
        bot.run()
    finally:
        bot.cleanup()


if __name__ == "__main__":
    main()
