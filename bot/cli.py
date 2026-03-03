"""
TwitterBot Framework CLI v1.0
命令行工具 - 推文搜索/发布/分析/竞品/情感/导出
"""

import argparse
import os

from bot.twitter_api import TwitterAPI
from bot.database import Database
from bot.content_generator import ContentGenerator
from bot.thread_composer import ThreadComposer
from bot.export import ExportEngine
from bot.sentiment import SentimentAnalyzer
from bot.competitor import CompetitorAnalyzer
from bot.audience import AudienceAnalyzer


def get_api() -> TwitterAPI:
    return TwitterAPI()


def get_db(path: str = None) -> Database:
    return Database(path or os.environ.get("DB_PATH", "twitterbot.db"))


# ── 子命令 ──

def cmd_search(args):
    """搜索推文"""
    api = get_api()
    db = get_db(args.db)
    data = api.search_recent(args.query, max_results=args.limit)
    if not data or "data" not in data:
        print("No results found.")
        return

    tweets = data["data"]
    users = {u["id"]: u for u in data.get("includes", {}).get("users", [])}

    for tweet in tweets:
        author = users.get(tweet.get("author_id"), {})
        username = author.get("username", "?")
        metrics = tweet.get("public_metrics", {})
        print(f"\n@{username} | ❤️ {metrics.get('like_count', 0)} "
              f"🔄 {metrics.get('retweet_count', 0)} "
              f"💬 {metrics.get('reply_count', 0)}")
        print(f"  {tweet.get('text', '')[:200]}")

        tweet["author_username"] = username
        db.save_tweet(tweet, source_query=args.query)

    print(f"\n✅ {len(tweets)} tweets found and saved.")


def cmd_user(args):
    """查看用户信息"""
    api = get_api()
    data = api.get_user(args.username)
    if not data or "data" not in data:
        print(f"User @{args.username} not found.")
        return

    user = data["data"]
    metrics = user.get("public_metrics", {})
    print(f"\n👤 @{user.get('username')} ({user.get('name')})")
    print(f"📝 {user.get('description', '')[:200]}")
    print(f"👥 Followers: {metrics.get('followers_count', 0):,}")
    print(f"➡️  Following: {metrics.get('following_count', 0):,}")
    print(f"📊 Tweets: {metrics.get('tweet_count', 0):,}")
    print(f"📋 Listed: {metrics.get('listed_count', 0):,}")
    print(f"✅ Verified: {user.get('verified', False)}")
    print(f"📅 Joined: {user.get('created_at', '')[:10]}")


def cmd_tweet(args):
    """发送推文"""
    api = get_api()
    if not api.can_write:
        print("❌ OAuth credentials required for posting.")
        return

    result = api.post_tweet(args.text, reply_to=args.reply_to)
    if result and "data" in result:
        tweet_id = result["data"].get("id", "")
        print(f"✅ Tweet posted: https://twitter.com/i/status/{tweet_id}")
    else:
        print(f"❌ Failed to post tweet: {result}")


def cmd_thread(args):
    """创建线程"""
    composer = ThreadComposer(numbering=not args.no_numbering)

    if args.file:
        with open(args.file, "r", encoding="utf-8") as f:
            body = f.read()
    else:
        body = args.body or ""

    thread = composer.compose(
        title=args.title,
        body=body,
        hashtags=args.hashtags or "",
        hook=args.hook or "",
        cta=args.cta or "",
    )

    print(composer.preview(thread))

    validation = composer.validate(thread)
    if not validation["valid"]:
        print("\n⚠️ Issues:")
        for issue in validation["issues"]:
            print(f"  - {issue}")

    read_time = composer.estimate_read_time(thread)
    print(f"\n⏱️ Estimated read time: {read_time['formatted']}")


def cmd_analyze(args):
    """分析账号"""
    api = get_api()
    db = get_db(args.db)
    audience = AudienceAnalyzer(api, db)

    activity = audience.analyze_activity_patterns(args.username, tweet_limit=args.limit)
    if not activity:
        print(f"Cannot analyze @{args.username}")
        return

    print(f"\n📊 Activity Analysis: @{args.username}")
    print(f"  Analyzed: {activity.get('total_analyzed', 0)} tweets\n")

    if activity.get("best_posting_hours"):
        print("⏰ Best Posting Hours (UTC):")
        for h in activity["best_posting_hours"]:
            print(f"  {h['hour']:02d}:00 - Avg engagement: {h['avg_engagement']:.1f} "
                  f"(n={h['sample_size']})")

    if activity.get("day_distribution"):
        print("\n📅 Day Distribution:")
        for day, count in activity["day_distribution"].items():
            bar = "█" * count
            print(f"  {day:10s} {bar} ({count})")


def cmd_sentiment(args):
    """情感分析"""
    db = get_db(args.db)
    analyzer = SentimentAnalyzer(db)

    if args.text:
        result = analyzer.analyze(args.text)
        emoji = "😊" if result.is_positive else "😠" if result.is_negative else "😐"
        print(f"\n{emoji} Score: {result.score:.3f} | Label: {result.label}")
        print(f"   Confidence: {result.confidence:.1%}")
        if result.positive_words:
            print(f"   ✅ Positive: {', '.join(result.positive_words)}")
        if result.negative_words:
            print(f"   ❌ Negative: {', '.join(result.negative_words)}")
    elif args.username:
        tweets = db.get_tweet_history(args.username, limit=args.limit)
        if not tweets:
            print(f"No tweets found for @{args.username}")
            return
        results = analyzer.analyze_tweets(tweets)
        summary = analyzer.summarize(results)
        print(analyzer.format_summary(summary))

        crisis = analyzer.detect_brand_crisis(results)
        if crisis["crisis"]:
            print(f"\n🚨 Crisis Level: {crisis['level']}")
            print(f"   {crisis['recommendation']}")


def cmd_competitor(args):
    """竞品分析"""
    api = get_api()
    db = get_db(args.db)
    comp = CompetitorAnalyzer(api, db)

    if args.action == "add":
        profile = comp.add_competitor(args.username)
        if profile:
            print(f"✅ Added competitor: @{args.username}")
            print(f"   Followers: {profile.followers:,}")
            print(f"   Engagement: {profile.engagement_rate:.3f}%")
        else:
            print(f"❌ Failed to add @{args.username}")

    elif args.action == "remove":
        if comp.remove_competitor(args.username):
            print(f"✅ Removed: @{args.username}")
        else:
            print(f"❌ Not found: @{args.username}")

    elif args.action == "list":
        competitors = comp.list_competitors()
        if not competitors:
            print("No competitors tracked.")
            return
        print(f"\n📋 Tracked Competitors ({len(competitors)}):\n")
        for c in competitors:
            print(f"  @{c.get('username', '?')}: "
                  f"{c.get('followers', 0):,} followers | "
                  f"Eng: {c.get('engagement_rate', 0):.3f}%")

    elif args.action == "compare":
        if not args.username:
            print("❌ --username required for compare")
            return
        comparison = comp.compare(args.username)
        print(comp.format_comparison(comparison))


def cmd_export(args):
    """导出数据"""
    db = get_db(args.db)
    engine = ExportEngine(db)

    if args.format == "all":
        results = engine.batch_export(args.username or "", args.output or "./exports")
        print(f"\n✅ Exported {len(results)} files:")
        for name, path in results.items():
            print(f"  📄 {path}")
    else:
        export_map = {
            "csv": engine.tweets_to_csv,
            "json": engine.tweets_to_json,
            "markdown": engine.tweets_to_markdown,
            "html": engine.tweets_to_html,
        }
        func = export_map.get(args.format)
        if not func:
            print(f"❌ Unknown format: {args.format}")
            return

        content = func(args.username or "", args.limit)

        if args.output:
            engine.export_to_file(content, args.output)
            print(f"✅ Exported to {args.output}")
        else:
            print(content)


def cmd_stats(args):
    """查看统计"""
    db = get_db(args.db)
    engagement = db.get_engagement_stats(args.days)
    schedule = db.get_schedule_queue(limit=5)
    ab_tests = db.get_ab_tests(limit=5)

    print(f"\n📊 Stats (last {args.days} days)\n")

    print("⚡ Engagement Actions:")
    if engagement:
        for action, count in engagement.items():
            print(f"  {action}: {count}")
    else:
        print("  No actions recorded")

    print(f"\n📅 Schedule Queue ({len(schedule)} upcoming):")
    for item in schedule:
        status_emoji = "⏳" if item["status"] == "pending" else "✅"
        print(f"  {status_emoji} {item.get('scheduled_at', '')[:16]} | "
              f"{item.get('content', '')[:50]}")

    print(f"\n🧪 Recent A/B Tests ({len(ab_tests)}):")
    for test in ab_tests:
        winner = test.get("winner", "pending")
        print(f"  {test.get('test_name', '?')}: {winner}")


def cmd_generate(args):
    """生成内容"""
    gen = ContentGenerator()

    if args.list_categories:
        print("📝 Available categories:")
        for cat in gen.get_categories():
            print(f"  - {cat}")
        return

    variables = {}
    if args.vars:
        for pair in args.vars:
            if "=" in pair:
                k, v = pair.split("=", 1)
                variables[k] = v

    if args.ab:
        a, b = gen.generate_ab_pair(args.category, variables)
        print(f"\n🅰️ Variant A:\n{a}\n")
        print(f"🅱️ Variant B:\n{b}")

        score_a = gen.estimate_engagement(a)
        score_b = gen.estimate_engagement(b)
        print(f"\n📊 Engagement Scores: A={score_a['estimated_score']:.0f} "
              f"B={score_b['estimated_score']:.0f}")
    else:
        count = args.count or 1
        for i in range(count):
            content = gen.generate(args.category, variables)
            if content:
                score = gen.estimate_engagement(content)
                print(f"\n--- [{i+1}] Score: {score['estimated_score']:.0f} ---")
                print(content)


# ── 主入口 ──

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="twitterbot",
        description="TwitterBot Framework CLI - Twitter/X automation toolkit"
    )
    parser.add_argument("--db", default=None, help="Database path")
    sub = parser.add_subparsers(dest="command", help="Available commands")

    # search
    p_search = sub.add_parser("search", help="Search recent tweets")
    p_search.add_argument("query", help="Search query")
    p_search.add_argument("-n", "--limit", type=int, default=10)

    # user
    p_user = sub.add_parser("user", help="Get user info")
    p_user.add_argument("username", help="Twitter username")

    # tweet
    p_tweet = sub.add_parser("tweet", help="Post a tweet")
    p_tweet.add_argument("text", help="Tweet text")
    p_tweet.add_argument("--reply-to", help="Reply to tweet ID")

    # thread
    p_thread = sub.add_parser("thread", help="Compose a thread")
    p_thread.add_argument("title", help="Thread title")
    p_thread.add_argument("--body", help="Thread body text")
    p_thread.add_argument("--file", help="Read body from file")
    p_thread.add_argument("--hashtags", help="Hashtags")
    p_thread.add_argument("--hook", help="Custom hook text")
    p_thread.add_argument("--cta", help="Call to action")
    p_thread.add_argument("--no-numbering", action="store_true")

    # analyze
    p_analyze = sub.add_parser("analyze", help="Analyze account activity")
    p_analyze.add_argument("username", help="Username to analyze")
    p_analyze.add_argument("-n", "--limit", type=int, default=50)

    # sentiment
    p_sent = sub.add_parser("sentiment", help="Sentiment analysis")
    p_sent.add_argument("--text", help="Analyze single text")
    p_sent.add_argument("--username", help="Analyze user's tweets")
    p_sent.add_argument("-n", "--limit", type=int, default=50)

    # competitor
    p_comp = sub.add_parser("competitor", help="Competitor analysis")
    p_comp.add_argument("action", choices=["add", "remove", "list", "compare"])
    p_comp.add_argument("--username", help="Username")

    # export
    p_export = sub.add_parser("export", help="Export data")
    p_export.add_argument("--format", choices=["csv", "json", "markdown", "html", "all"],
                           default="csv")
    p_export.add_argument("--username", help="Filter by username")
    p_export.add_argument("-n", "--limit", type=int, default=500)
    p_export.add_argument("-o", "--output", help="Output file/directory")

    # stats
    p_stats = sub.add_parser("stats", help="View statistics")
    p_stats.add_argument("--days", type=int, default=7)

    # generate
    p_gen = sub.add_parser("generate", help="Generate tweet content")
    p_gen.add_argument("category", nargs="?", default="engagement")
    p_gen.add_argument("--vars", nargs="*", help="Variables: key=value")
    p_gen.add_argument("--ab", action="store_true", help="Generate A/B pair")
    p_gen.add_argument("-n", "--count", type=int, default=1)
    p_gen.add_argument("--list-categories", action="store_true")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    commands = {
        "search": cmd_search,
        "user": cmd_user,
        "tweet": cmd_tweet,
        "thread": cmd_thread,
        "analyze": cmd_analyze,
        "sentiment": cmd_sentiment,
        "competitor": cmd_competitor,
        "export": cmd_export,
        "stats": cmd_stats,
        "generate": cmd_generate,
    }

    func = commands.get(args.command)
    if func:
        func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
