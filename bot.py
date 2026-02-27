"""
TwitterBot Framework - Twitter/X自动化框架
支持搜索、监控、自动回复、数据分析
通过Telegram Bot控制
"""

import os
import time
import json
import requests
from datetime import datetime

TOKEN = os.environ.get("BOT_TOKEN", "")
TW_BEARER = os.environ.get("TW_BEARER_TOKEN", "")

if not TOKEN:
    raise ValueError("未设置 BOT_TOKEN!")

API_URL = f"https://api.telegram.org/bot{TOKEN}"
TW_API = "https://api.twitter.com/2"


def tg_get(method, params=None):
    try:
        r = requests.get(f"{API_URL}/{method}", params=params, timeout=35)
        return r.json()
    except:
        return None


def tg_send(chat_id, text, reply_to=None, parse_mode="Markdown"):
    params = {"chat_id": chat_id, "text": text}
    if reply_to: params["reply_to_message_id"] = reply_to
    if parse_mode: params["parse_mode"] = parse_mode
    result = tg_get("sendMessage", params)
    if not result or not result.get("ok"):
        params.pop("parse_mode", None)
        result = tg_get("sendMessage", params)
    return result


def tw_get(endpoint, params=None):
    if not TW_BEARER:
        return None
    r = requests.get(f"{TW_API}{endpoint}",
        params=params, headers={"Authorization": f"Bearer {TW_BEARER}"}, timeout=15)
    return r.json() if r.ok else None


def search_tweets(query, max_results=10):
    data = tw_get("/tweets/search/recent", {
        "query": query, "max_results": max_results,
        "tweet.fields": "public_metrics,created_at,author_id",
        "expansions": "author_id",
        "user.fields": "username,public_metrics"
    })
    if not data or "data" not in data:
        return "❌ 搜索失败或无结果"

    users = {u["id"]: u for u in data.get("includes", {}).get("users", [])}
    lines = [f"🔍 搜索: `{query}` ({len(data['data'])}条)\n"]
    for t in data["data"][:10]:
        m = t.get("public_metrics", {})
        user = users.get(t.get("author_id"), {})
        username = user.get("username", "?")
        lines.append(f"@{username} | ❤️{m.get('like_count',0)} 🔄{m.get('retweet_count',0)}")
        lines.append(f"  {t['text'][:80]}\n")
    return "\n".join(lines)


def get_user_info(username):
    data = tw_get(f"/users/by/username/{username}", {
        "user.fields": "public_metrics,description,created_at,profile_image_url"
    })
    if not data or "data" not in data:
        return f"❌ 用户 @{username} 不存在"

    d = data["data"]
    m = d.get("public_metrics", {})
    return (f"🐦 *@{d['username']}* ({d.get('name', '')})\n\n"
            f"👥 粉丝: {m.get('followers_count', 0):,}\n"
            f"👤 关注: {m.get('following_count', 0):,}\n"
            f"📝 推文: {m.get('tweet_count', 0):,}\n"
            f"📋 简介: {d.get('description', '')[:150]}\n"
            f"📅 注册: {d.get('created_at', '')[:10]}")


def get_user_tweets(username, max_results=5):
    user_data = tw_get(f"/users/by/username/{username}")
    if not user_data or "data" not in user_data:
        return f"❌ 用户 @{username} 不存在"

    user_id = user_data["data"]["id"]
    data = tw_get(f"/users/{user_id}/tweets", {
        "max_results": max_results,
        "tweet.fields": "public_metrics,created_at"
    })
    if not data or "data" not in data:
        return "❌ 无推文"

    lines = [f"📝 @{username} 最近推文\n"]
    for t in data["data"]:
        m = t.get("public_metrics", {})
        lines.append(f"❤️{m.get('like_count',0)} 🔄{m.get('retweet_count',0)} 💬{m.get('reply_count',0)}")
        lines.append(f"  {t['text'][:100]}\n")
    return "\n".join(lines)


# 监控列表
monitors = {}


def handle(chat_id, msg_id, text):
    cmd = text.split()[0].lower()
    args = text[len(cmd):].strip()

    if cmd == "/start":
        tg_send(chat_id,
            "🐦 *TwitterBot Framework*\n\n"
            "Twitter/X数据分析+自动化工具\n\n"
            "🔍 *搜索*\n"
            "  /search <关键词> — 搜索推文\n"
            "  /user <用户名> — 用户信息\n"
            "  /tweets <用户名> — 最近推文\n\n"
            "👀 *监控*\n"
            "  /monitor <关键词> — 添加关键词监控\n"
            "  /monitors — 查看监控列表\n"
            "  /unmonitor <关键词> — 取消监控\n\n"
            "📊 *分析*\n"
            "  /analyze <用户名> — 账号分析\n"
            f"\n{'✅ Twitter API已连接' if TW_BEARER else '⚠️ 未配置TW_BEARER_TOKEN'}", msg_id)

    elif cmd == "/search":
        if not args:
            tg_send(chat_id, "用法: /search <关键词>", msg_id)
        else:
            tg_send(chat_id, search_tweets(args), msg_id)

    elif cmd == "/user":
        if not args:
            tg_send(chat_id, "用法: /user <用户名>", msg_id)
        else:
            tg_send(chat_id, get_user_info(args.lstrip("@")), msg_id)

    elif cmd == "/tweets":
        if not args:
            tg_send(chat_id, "用法: /tweets <用户名>", msg_id)
        else:
            tg_send(chat_id, get_user_tweets(args.lstrip("@")), msg_id)

    elif cmd == "/monitor":
        if not args:
            tg_send(chat_id, "用法: /monitor <关键词>", msg_id)
        else:
            monitors[args] = {"chat_id": chat_id, "added": datetime.now().isoformat()}
            tg_send(chat_id, f"👀 已添加监控: `{args}`", msg_id)

    elif cmd == "/monitors":
        if not monitors:
            tg_send(chat_id, "📋 暂无监控", msg_id)
        else:
            lines = ["📋 监控列表\n"]
            for k in monitors:
                lines.append(f"  • `{k}`")
            tg_send(chat_id, "\n".join(lines), msg_id)

    elif cmd == "/unmonitor":
        if args in monitors:
            del monitors[args]
            tg_send(chat_id, f"✅ 已取消监控: `{args}`", msg_id)
        else:
            tg_send(chat_id, f"⚠️ 未找到监控: `{args}`", msg_id)

    elif cmd == "/analyze":
        if not args:
            tg_send(chat_id, "用法: /analyze <用户名>", msg_id)
        else:
            username = args.lstrip("@")
            info = get_user_info(username)
            tweets = get_user_tweets(username, 10)
            tg_send(chat_id, info, msg_id)
            tg_send(chat_id, tweets)


def main():
    print(f"\n{'='*50}")
    print(f"  TwitterBot Framework")
    print(f"  Twitter API: {'✅' if TW_BEARER else '❌'}")
    print(f"{'='*50}")

    me = tg_get("getMe")
    if me and me.get("ok"):
        print(f"\n✅ @{me['result']['username']} 已上线!")
    else:
        print("\n❌ 无法连接Telegram!")
        return

    offset = None
    while True:
        try:
            result = tg_get("getUpdates", {"timeout": 30, **({"offset": offset} if offset else {})})
            if not result or not result.get("ok"):
                time.sleep(5)
                continue
            for update in result.get("result", []):
                offset = update["update_id"] + 1
                msg = update.get("message")
                if not msg: continue
                text = (msg.get("text") or "").strip()
                if text:
                    handle(msg["chat"]["id"], msg["message_id"], text)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"[错误] {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
