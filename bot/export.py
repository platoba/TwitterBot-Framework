"""
Export Engine - 报告导出引擎 v3.0
CSV/JSON/HTML/Markdown报告导出 + 可视化图表 + 定时导出
"""

import csv
import io
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from bot.database import Database

logger = logging.getLogger(__name__)


class ExportEngine:
    """多格式报告导出引擎"""

    def __init__(self, db: Database):
        self.db = db

    # ── CSV导出 ──

    def tweets_to_csv(self, username: str = "", limit: int = 500) -> str:
        """导出推文历史为CSV"""
        tweets = self.db.get_tweet_history(username, limit)
        return self._dicts_to_csv(tweets, [
            "tweet_id", "author_username", "text", "like_count",
            "retweet_count", "reply_count", "quote_count",
            "impression_count", "created_at", "source_query"
        ])

    def analytics_to_csv(self, username: str, limit: int = 90) -> str:
        """导出分析快照为CSV"""
        snapshots = self.db.get_analytics_history(username, limit)
        return self._dicts_to_csv(snapshots, [
            "username", "followers_count", "following_count",
            "tweet_count", "listed_count", "snapshot_at"
        ])

    def engagement_to_csv(self, days: int = 30) -> str:
        """导出互动日志为CSV"""
        conn = self.db._get_conn()
        rows = conn.execute("""
            SELECT * FROM engagement_log
            WHERE created_at >= datetime('now', ?)
            ORDER BY created_at DESC
        """, (f"-{days} days",)).fetchall()
        data = [dict(r) for r in rows]
        return self._dicts_to_csv(data, [
            "id", "action_type", "target_tweet_id", "target_username",
            "reply_text", "status", "created_at"
        ])

    def schedule_to_csv(self, status: str = None) -> str:
        """导出调度队列为CSV"""
        items = self.db.get_schedule_queue(status, limit=500)
        return self._dicts_to_csv(items, [
            "id", "content", "scheduled_at", "status",
            "tweet_id", "error", "created_at", "executed_at"
        ])

    def ab_tests_to_csv(self) -> str:
        """导出A/B测试为CSV"""
        tests = self.db.get_ab_tests(limit=100)
        return self._dicts_to_csv(tests, [
            "id", "test_name", "variant_a", "variant_b",
            "variant_a_tweet_id", "variant_b_tweet_id",
            "winner", "status", "created_at", "evaluated_at"
        ])

    def _dicts_to_csv(self, data: List[Dict], columns: List[str]) -> str:
        """字典列表转CSV字符串"""
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        for row in data:
            writer.writerow(row)
        return output.getvalue()

    # ── JSON导出 ──

    def tweets_to_json(self, username: str = "", limit: int = 500) -> str:
        tweets = self.db.get_tweet_history(username, limit)
        return json.dumps({"tweets": tweets, "count": len(tweets),
                           "exported_at": self._now()}, indent=2, ensure_ascii=False)

    def analytics_to_json(self, username: str, limit: int = 90) -> str:
        snapshots = self.db.get_analytics_history(username, limit)
        return json.dumps({"analytics": snapshots, "username": username,
                           "count": len(snapshots), "exported_at": self._now()},
                          indent=2, ensure_ascii=False)

    def full_report_json(self, username: str = "") -> str:
        """全量报告JSON"""
        report = {
            "generated_at": self._now(),
            "tweets": self.db.get_tweet_history(username, 200),
            "top_tweets": self.db.get_top_tweets(username, 10),
            "schedule_queue": self.db.get_schedule_queue(limit=50),
            "ab_tests": self.db.get_ab_tests(limit=20),
            "engagement_stats": self.db.get_engagement_stats(30),
        }
        if username:
            report["analytics"] = self.db.get_analytics_history(username, 30)
            report["follower_growth"] = self.db.get_follower_growth(username, 7)
        return json.dumps(report, indent=2, ensure_ascii=False)

    # ── Markdown导出 ──

    def tweets_to_markdown(self, username: str = "", limit: int = 50) -> str:
        """推文历史Markdown报告"""
        tweets = self.db.get_tweet_history(username, limit)
        lines = [
            f"# Tweet History Report",
            f"Generated: {self._now()}",
            f"Username: {username or 'all'}",
            f"Total: {len(tweets)}\n",
            "| # | Tweet | Likes | RTs | Replies | Impressions | Date |",
            "|---|-------|-------|-----|---------|-------------|------|",
        ]
        for i, t in enumerate(tweets, 1):
            text = t.get("text", "")[:60].replace("|", "\\|").replace("\n", " ")
            lines.append(
                f"| {i} | {text} | {t.get('like_count', 0)} | "
                f"{t.get('retweet_count', 0)} | {t.get('reply_count', 0)} | "
                f"{t.get('impression_count', 0)} | {t.get('created_at', '')[:10]} |"
            )
        return "\n".join(lines)

    def analytics_to_markdown(self, username: str, limit: int = 30) -> str:
        """分析快照Markdown报告"""
        snapshots = self.db.get_analytics_history(username, limit)
        lines = [
            f"# Analytics Report: @{username}",
            f"Generated: {self._now()}\n",
            "| Date | Followers | Following | Tweets | Listed |",
            "|------|-----------|-----------|--------|--------|",
        ]
        for s in snapshots:
            lines.append(
                f"| {s.get('snapshot_at', '')[:10]} | "
                f"{s.get('followers_count', 0):,} | "
                f"{s.get('following_count', 0):,} | "
                f"{s.get('tweet_count', 0):,} | "
                f"{s.get('listed_count', 0):,} |"
            )

        growth = self.db.get_follower_growth(username, 7)
        if growth:
            lines.extend([
                f"\n## 7-Day Growth",
                f"- Current: {growth['current']:,}",
                f"- Previous: {growth['previous']:,}",
                f"- Growth: {growth['growth']:+,} ({growth['growth_rate']:+.2f}%)",
            ])

        return "\n".join(lines)

    # ── HTML导出 ──

    def tweets_to_html(self, username: str = "", limit: int = 50) -> str:
        """推文历史HTML报告"""
        tweets = self.db.get_tweet_history(username, limit)
        top_tweets = self.db.get_top_tweets(username, 5)
        engagement_stats = self.db.get_engagement_stats(7)

        rows_html = ""
        for t in tweets:
            text = t.get("text", "")[:80].replace("<", "&lt;").replace(">", "&gt;")
            rows_html += f"""
            <tr>
                <td>{t.get('author_username', '')}</td>
                <td>{text}</td>
                <td>{t.get('like_count', 0):,}</td>
                <td>{t.get('retweet_count', 0):,}</td>
                <td>{t.get('reply_count', 0):,}</td>
                <td>{t.get('impression_count', 0):,}</td>
                <td>{t.get('created_at', '')[:10]}</td>
            </tr>"""

        top_html = ""
        for i, t in enumerate(top_tweets, 1):
            text = t.get("text", "")[:60].replace("<", "&lt;").replace(">", "&gt;")
            top_html += f"""
            <div class="top-tweet">
                <strong>#{i}</strong> @{t.get('author_username', '')}
                <p>{text}</p>
                <span>❤️ {t.get('like_count', 0):,} | 🔄 {t.get('retweet_count', 0):,} |
                      💬 {t.get('reply_count', 0):,}</span>
            </div>"""

        stats_html = ""
        for action, count in engagement_stats.items():
            stats_html += f"<li>{action}: <strong>{count}</strong></li>"

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TwitterBot Report - {username or 'All'}</title>
<style>
    body {{ font-family: -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; margin:0; padding:20px; background:#0f172a; color:#e2e8f0; }}
    .container {{ max-width:1200px; margin:0 auto; }}
    h1 {{ color:#38bdf8; border-bottom:2px solid #1e3a5f; padding-bottom:10px; }}
    h2 {{ color:#7dd3fc; margin-top:30px; }}
    .stats {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:15px; margin:20px 0; }}
    .stat-card {{ background:#1e293b; border-radius:8px; padding:20px; text-align:center; border:1px solid #334155; }}
    .stat-card .value {{ font-size:2em; color:#38bdf8; font-weight:bold; }}
    .stat-card .label {{ color:#94a3b8; font-size:0.9em; }}
    table {{ width:100%; border-collapse:collapse; margin:20px 0; }}
    th {{ background:#1e3a5f; color:#38bdf8; padding:12px; text-align:left; }}
    td {{ padding:10px; border-bottom:1px solid #334155; }}
    tr:hover {{ background:#1e293b; }}
    .top-tweet {{ background:#1e293b; border-radius:8px; padding:15px; margin:10px 0; border-left:3px solid #38bdf8; }}
    .top-tweet p {{ color:#cbd5e1; margin:5px 0; }}
    .top-tweet span {{ color:#94a3b8; font-size:0.85em; }}
    ul {{ list-style:none; padding:0; }}
    li {{ padding:5px 0; }}
    .footer {{ margin-top:40px; padding-top:20px; border-top:1px solid #334155; color:#64748b; font-size:0.85em; text-align:center; }}
</style>
</head>
<body>
<div class="container">
    <h1>📊 TwitterBot Report</h1>
    <p>Generated: {self._now()} | Username: {username or 'All'}</p>

    <div class="stats">
        <div class="stat-card">
            <div class="value">{len(tweets)}</div>
            <div class="label">Total Tweets</div>
        </div>
        <div class="stat-card">
            <div class="value">{sum(t.get('like_count',0) for t in tweets):,}</div>
            <div class="label">Total Likes</div>
        </div>
        <div class="stat-card">
            <div class="value">{sum(t.get('retweet_count',0) for t in tweets):,}</div>
            <div class="label">Total Retweets</div>
        </div>
        <div class="stat-card">
            <div class="value">{sum(t.get('impression_count',0) for t in tweets):,}</div>
            <div class="label">Total Impressions</div>
        </div>
    </div>

    <h2>🏆 Top Tweets</h2>
    {top_html}

    <h2>📋 Tweet History</h2>
    <table>
        <thead>
            <tr><th>Author</th><th>Text</th><th>❤️</th><th>🔄</th><th>💬</th><th>👀</th><th>Date</th></tr>
        </thead>
        <tbody>{rows_html}</tbody>
    </table>

    <h2>⚡ Engagement Stats (7d)</h2>
    <ul>{stats_html}</ul>

    <div class="footer">
        Generated by TwitterBot Framework v3.0 | {self._now()}
    </div>
</div>
</body>
</html>"""

    def analytics_to_html(self, username: str, limit: int = 30) -> str:
        """分析快照HTML报告"""
        snapshots = self.db.get_analytics_history(username, limit)
        growth = self.db.get_follower_growth(username, 7)

        rows_html = ""
        for s in snapshots:
            rows_html += f"""
            <tr>
                <td>{s.get('snapshot_at', '')[:10]}</td>
                <td>{s.get('followers_count', 0):,}</td>
                <td>{s.get('following_count', 0):,}</td>
                <td>{s.get('tweet_count', 0):,}</td>
                <td>{s.get('listed_count', 0):,}</td>
            </tr>"""

        growth_html = ""
        if growth:
            growth_html = f"""
            <div class="stats">
                <div class="stat-card">
                    <div class="value">{growth['current']:,}</div>
                    <div class="label">Current Followers</div>
                </div>
                <div class="stat-card">
                    <div class="value" style="color:{'#22c55e' if growth['growth']>=0 else '#ef4444'}">{growth['growth']:+,}</div>
                    <div class="label">7-Day Growth</div>
                </div>
                <div class="stat-card">
                    <div class="value">{growth['growth_rate']:+.2f}%</div>
                    <div class="label">Growth Rate</div>
                </div>
            </div>"""

        # Chart data (JSON for inline JS sparkline)
        chart_labels = json.dumps([s.get('snapshot_at', '')[:10] for s in reversed(snapshots)])
        chart_data = json.dumps([s.get('followers_count', 0) for s in reversed(snapshots)])

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Analytics Report - @{username}</title>
<style>
    body {{ font-family: -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; margin:0; padding:20px; background:#0f172a; color:#e2e8f0; }}
    .container {{ max-width:1000px; margin:0 auto; }}
    h1 {{ color:#38bdf8; }}
    h2 {{ color:#7dd3fc; margin-top:30px; }}
    .stats {{ display:grid; grid-template-columns:repeat(3,1fr); gap:15px; margin:20px 0; }}
    .stat-card {{ background:#1e293b; border-radius:8px; padding:20px; text-align:center; border:1px solid #334155; }}
    .stat-card .value {{ font-size:2em; color:#38bdf8; font-weight:bold; }}
    .stat-card .label {{ color:#94a3b8; font-size:0.9em; }}
    table {{ width:100%; border-collapse:collapse; margin:20px 0; }}
    th {{ background:#1e3a5f; color:#38bdf8; padding:12px; text-align:left; }}
    td {{ padding:10px; border-bottom:1px solid #334155; }}
    tr:hover {{ background:#1e293b; }}
    .chart-container {{ background:#1e293b; border-radius:8px; padding:20px; margin:20px 0; min-height:200px; }}
    .footer {{ margin-top:40px; padding-top:20px; border-top:1px solid #334155; color:#64748b; font-size:0.85em; text-align:center; }}
</style>
</head>
<body>
<div class="container">
    <h1>📈 Analytics Report: @{username}</h1>
    <p>Generated: {self._now()}</p>

    {growth_html}

    <h2>📊 Follower Trend</h2>
    <div class="chart-container">
        <canvas id="chart" width="900" height="200"></canvas>
    </div>

    <h2>📋 History</h2>
    <table>
        <thead>
            <tr><th>Date</th><th>Followers</th><th>Following</th><th>Tweets</th><th>Listed</th></tr>
        </thead>
        <tbody>{rows_html}</tbody>
    </table>

    <div class="footer">
        Generated by TwitterBot Framework v3.0 | {self._now()}
    </div>
</div>

<script>
// Minimal inline sparkline chart (no external deps)
(function() {{
    var canvas = document.getElementById('chart');
    if (!canvas) return;
    var ctx = canvas.getContext('2d');
    var labels = {chart_labels};
    var data = {chart_data};
    if (data.length < 2) return;

    var w = canvas.width, h = canvas.height;
    var padding = 40;
    var min = Math.min(...data), max = Math.max(...data);
    var range = max - min || 1;

    ctx.strokeStyle = '#38bdf8';
    ctx.lineWidth = 2;
    ctx.beginPath();

    for (var i = 0; i < data.length; i++) {{
        var x = padding + (i / (data.length - 1)) * (w - 2*padding);
        var y = h - padding - ((data[i] - min) / range) * (h - 2*padding);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
    }}
    ctx.stroke();

    // Gradient fill
    var grad = ctx.createLinearGradient(0, 0, 0, h);
    grad.addColorStop(0, 'rgba(56,189,248,0.3)');
    grad.addColorStop(1, 'rgba(56,189,248,0)');
    ctx.lineTo(padding + (w - 2*padding), h - padding);
    ctx.lineTo(padding, h - padding);
    ctx.fillStyle = grad;
    ctx.fill();

    // Dots
    ctx.fillStyle = '#38bdf8';
    for (var i = 0; i < data.length; i++) {{
        var x = padding + (i / (data.length - 1)) * (w - 2*padding);
        var y = h - padding - ((data[i] - min) / range) * (h - 2*padding);
        ctx.beginPath();
        ctx.arc(x, y, 3, 0, 2*Math.PI);
        ctx.fill();
    }}
}})();
</script>
</body>
</html>"""

    # ── 文件导出 ──

    def export_to_file(self, content: str, filepath: str) -> bool:
        """写入文件"""
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content)
            logger.info(f"Exported to {filepath}")
            return True
        except IOError as e:
            logger.error(f"Export failed: {e}")
            return False

    def batch_export(self, username: str = "", output_dir: str = "./exports") -> Dict[str, str]:
        """批量导出所有格式"""
        import os
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results = {}

        exports = [
            (f"tweets_{timestamp}.csv", self.tweets_to_csv(username)),
            (f"tweets_{timestamp}.json", self.tweets_to_json(username)),
            (f"tweets_{timestamp}.md", self.tweets_to_markdown(username)),
            (f"tweets_{timestamp}.html", self.tweets_to_html(username)),
        ]

        if username:
            exports.extend([
                (f"analytics_{username}_{timestamp}.csv", self.analytics_to_csv(username)),
                (f"analytics_{username}_{timestamp}.json", self.analytics_to_json(username)),
                (f"analytics_{username}_{timestamp}.md", self.analytics_to_markdown(username)),
                (f"analytics_{username}_{timestamp}.html", self.analytics_to_html(username)),
            ])

        exports.extend([
            (f"engagement_{timestamp}.csv", self.engagement_to_csv()),
            (f"schedule_{timestamp}.csv", self.schedule_to_csv()),
            (f"ab_tests_{timestamp}.csv", self.ab_tests_to_csv()),
            (f"full_report_{timestamp}.json", self.full_report_json(username)),
        ])

        for filename, content in exports:
            filepath = os.path.join(output_dir, filename)
            if self.export_to_file(content, filepath):
                results[filename] = filepath

        return results

    def _now(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
