"""
Report generator for Twitter analytics.

Generates HTML, JSON, and CSV reports with:
- Account performance overview
- Engagement metrics and trends
- Content performance rankings
- Audience growth analysis
- Hashtag effectiveness
- Best posting times heatmap
- Competitor comparison
"""

import csv
import io
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Optional


class ReportFormat(str, Enum):
    """Report output format."""
    HTML = "html"
    JSON = "json"
    CSV = "csv"
    MARKDOWN = "markdown"


class ReportPeriod(str, Enum):
    """Report time period."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"
    CUSTOM = "custom"


class MetricType(str, Enum):
    """Types of metrics tracked."""
    IMPRESSIONS = "impressions"
    ENGAGEMENTS = "engagements"
    LIKES = "likes"
    RETWEETS = "retweets"
    REPLIES = "replies"
    QUOTES = "quotes"
    CLICKS = "clicks"
    FOLLOWERS = "followers"
    FOLLOWING = "following"
    TWEETS = "tweets"
    ENGAGEMENT_RATE = "engagement_rate"
    CLICK_THROUGH_RATE = "click_through_rate"


@dataclass
class MetricSnapshot:
    """Point-in-time metric value."""
    metric: MetricType
    value: float
    timestamp: str
    delta: float = 0.0
    delta_pct: float = 0.0

    def to_dict(self) -> dict:
        return {
            "metric": self.metric.value,
            "value": self.value,
            "timestamp": self.timestamp,
            "delta": self.delta,
            "delta_pct": self.delta_pct,
        }


@dataclass
class TweetPerformance:
    """Individual tweet performance data."""
    tweet_id: str
    text: str
    created_at: str
    impressions: int = 0
    likes: int = 0
    retweets: int = 0
    replies: int = 0
    quotes: int = 0
    clicks: int = 0
    engagement_rate: float = 0.0
    hashtags: list = field(default_factory=list)
    media_type: Optional[str] = None

    @property
    def total_engagements(self) -> int:
        return self.likes + self.retweets + self.replies + self.quotes + self.clicks

    def to_dict(self) -> dict:
        return {
            "tweet_id": self.tweet_id,
            "text": self.text[:100],
            "created_at": self.created_at,
            "impressions": self.impressions,
            "likes": self.likes,
            "retweets": self.retweets,
            "replies": self.replies,
            "quotes": self.quotes,
            "clicks": self.clicks,
            "engagement_rate": round(self.engagement_rate, 4),
            "total_engagements": self.total_engagements,
            "hashtags": self.hashtags,
            "media_type": self.media_type,
        }


@dataclass
class AudienceSegment:
    """Audience demographic segment."""
    name: str
    count: int
    percentage: float
    growth_rate: float = 0.0

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "count": self.count,
            "percentage": round(self.percentage, 2),
            "growth_rate": round(self.growth_rate, 2),
        }


@dataclass
class HashtagStat:
    """Hashtag performance statistics."""
    hashtag: str
    usage_count: int
    avg_engagement: float
    avg_impressions: float
    best_tweet_id: Optional[str] = None

    @property
    def effectiveness_score(self) -> float:
        """Score combining usage and engagement."""
        if self.usage_count == 0:
            return 0
        return (self.avg_engagement * 0.7 + self.avg_impressions * 0.3) / 100

    def to_dict(self) -> dict:
        return {
            "hashtag": self.hashtag,
            "usage_count": self.usage_count,
            "avg_engagement": round(self.avg_engagement, 2),
            "avg_impressions": round(self.avg_impressions, 2),
            "effectiveness_score": round(self.effectiveness_score, 2),
            "best_tweet_id": self.best_tweet_id,
        }


@dataclass
class CompetitorProfile:
    """Competitor comparison data."""
    username: str
    followers: int
    following: int
    tweets_count: int
    avg_engagement_rate: float
    top_hashtags: list = field(default_factory=list)
    posting_frequency: float = 0.0  # tweets per day

    def to_dict(self) -> dict:
        return {
            "username": self.username,
            "followers": self.followers,
            "following": self.following,
            "tweets_count": self.tweets_count,
            "avg_engagement_rate": round(self.avg_engagement_rate, 4),
            "top_hashtags": self.top_hashtags[:10],
            "posting_frequency": round(self.posting_frequency, 2),
        }


@dataclass
class PostingHeatmap:
    """Posting time performance heatmap."""
    data: dict = field(default_factory=dict)  # {day: {hour: engagement_rate}}

    def add_datapoint(self, day: int, hour: int, engagement: float):
        """Add engagement data for a time slot (day 0=Mon, hour 0-23)."""
        if day not in self.data:
            self.data[day] = {}
        if hour not in self.data[day]:
            self.data[day][hour] = []
        if isinstance(self.data[day][hour], list):
            self.data[day][hour].append(engagement)

    def finalize(self):
        """Convert lists to averages."""
        for day in self.data:
            for hour in self.data[day]:
                values = self.data[day][hour]
                if isinstance(values, list) and values:
                    self.data[day][hour] = sum(values) / len(values)
                elif isinstance(values, list):
                    self.data[day][hour] = 0

    def get_best_times(self, top_n: int = 5) -> list[tuple[int, int, float]]:
        """Get top N best posting times."""
        self.finalize()
        times = []
        for day in self.data:
            for hour in self.data[day]:
                val = self.data[day][hour]
                if isinstance(val, (int, float)):
                    times.append((day, hour, val))
        times.sort(key=lambda x: x[2], reverse=True)
        return times[:top_n]

    def to_dict(self) -> dict:
        self.finalize()
        days = ["Monday", "Tuesday", "Wednesday", "Thursday",
                "Friday", "Saturday", "Sunday"]
        result = {}
        for day_idx, day_name in enumerate(days):
            if day_idx in self.data:
                result[day_name] = {
                    str(h): round(v, 4) if isinstance(v, (int, float)) else 0
                    for h, v in sorted(self.data[day_idx].items())
                }
        return result


@dataclass
class ReportData:
    """Complete report data container."""
    account_username: str
    period: ReportPeriod
    start_date: str
    end_date: str
    generated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    # Overview metrics
    metrics: list[MetricSnapshot] = field(default_factory=list)

    # Content
    top_tweets: list[TweetPerformance] = field(default_factory=list)
    worst_tweets: list[TweetPerformance] = field(default_factory=list)
    total_tweets: int = 0

    # Audience
    audience_segments: list[AudienceSegment] = field(default_factory=list)
    follower_growth: list[dict] = field(default_factory=list)

    # Hashtags
    hashtag_stats: list[HashtagStat] = field(default_factory=list)

    # Timing
    posting_heatmap: Optional[PostingHeatmap] = None

    # Competitors
    competitors: list[CompetitorProfile] = field(default_factory=list)

    # Recommendations
    recommendations: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "account": self.account_username,
            "period": self.period.value,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "generated_at": self.generated_at,
            "overview": {
                "total_tweets": self.total_tweets,
                "metrics": [m.to_dict() for m in self.metrics],
            },
            "top_tweets": [t.to_dict() for t in self.top_tweets[:10]],
            "worst_tweets": [t.to_dict() for t in self.worst_tweets[:5]],
            "audience": {
                "segments": [s.to_dict() for s in self.audience_segments],
                "growth": self.follower_growth,
            },
            "hashtags": [h.to_dict() for h in self.hashtag_stats[:20]],
            "posting_heatmap": self.posting_heatmap.to_dict() if self.posting_heatmap else {},
            "best_posting_times": [
                {"day": d, "hour": h, "engagement": round(e, 4)}
                for d, h, e in (self.posting_heatmap.get_best_times() if self.posting_heatmap else [])
            ],
            "competitors": [c.to_dict() for c in self.competitors],
            "recommendations": self.recommendations,
        }


class ReportGenerator:
    """
    Generate comprehensive Twitter analytics reports.

    Supports HTML, JSON, CSV, and Markdown output formats.
    Includes account overview, content performance, audience analysis,
    hashtag effectiveness, posting time optimization, and competitor benchmarking.
    """

    # Color scheme for HTML reports
    COLORS = {
        "primary": "#1DA1F2",
        "secondary": "#14171A",
        "success": "#17BF63",
        "warning": "#FFAD1F",
        "danger": "#E0245E",
        "light": "#AAB8C2",
        "bg": "#F5F8FA",
        "card_bg": "#FFFFFF",
    }

    def __init__(self, report_data: Optional[ReportData] = None):
        self.data = report_data

    def set_data(self, data: ReportData):
        """Set report data."""
        self.data = data

    def generate(self, fmt: ReportFormat = ReportFormat.HTML) -> str:
        """
        Generate report in specified format.

        Args:
            fmt: Output format (HTML, JSON, CSV, MARKDOWN)

        Returns:
            Report content as string
        """
        if not self.data:
            raise ValueError("No report data set")

        generators = {
            ReportFormat.HTML: self._generate_html,
            ReportFormat.JSON: self._generate_json,
            ReportFormat.CSV: self._generate_csv,
            ReportFormat.MARKDOWN: self._generate_markdown,
        }

        generator = generators.get(fmt)
        if not generator:
            raise ValueError(f"Unsupported format: {fmt}")

        return generator()

    def save(self, filepath: str, fmt: Optional[ReportFormat] = None):
        """Save report to file."""
        if fmt is None:
            ext = filepath.rsplit(".", 1)[-1].lower()
            fmt = {
                "html": ReportFormat.HTML,
                "json": ReportFormat.JSON,
                "csv": ReportFormat.CSV,
                "md": ReportFormat.MARKDOWN,
            }.get(ext, ReportFormat.HTML)

        content = self.generate(fmt)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)

    def generate_recommendations(self) -> list[str]:
        """Generate actionable recommendations from data."""
        if not self.data:
            return []

        recs = []

        # Analyze top tweet patterns
        if self.data.top_tweets:
            media_tweets = [t for t in self.data.top_tweets if t.media_type]
            if len(media_tweets) > len(self.data.top_tweets) / 2:
                recs.append("📸 Visual content performs well — include images or videos in more tweets.")

            avg_len = sum(len(t.text) for t in self.data.top_tweets) / len(self.data.top_tweets)
            if avg_len < 140:
                recs.append("📝 Shorter tweets tend to perform better for your account. Keep it concise.")
            elif avg_len > 200:
                recs.append("📝 Longer, detailed tweets resonate with your audience. Keep providing depth.")

        # Hashtag recommendations
        if self.data.hashtag_stats:
            top_tags = sorted(self.data.hashtag_stats,
                            key=lambda h: h.effectiveness_score, reverse=True)[:3]
            tag_names = ", ".join(f"#{t.hashtag}" for t in top_tags)
            recs.append(f"#️⃣ Your most effective hashtags: {tag_names}. Use them more frequently.")

            low_tags = [h for h in self.data.hashtag_stats
                       if h.usage_count >= 3 and h.effectiveness_score < 0.5]
            if low_tags:
                drop_names = ", ".join(f"#{t.hashtag}" for t in low_tags[:3])
                recs.append(f"🗑️ Consider dropping low-performing hashtags: {drop_names}")

        # Posting time recommendations
        if self.data.posting_heatmap:
            best_times = self.data.posting_heatmap.get_best_times(3)
            if best_times:
                days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                time_strs = [f"{days[d]} {h}:00" for d, h, _ in best_times]
                recs.append(f"⏰ Best posting times: {', '.join(time_strs)}")

        # Engagement rate check
        eng_metrics = [m for m in self.data.metrics
                      if m.metric == MetricType.ENGAGEMENT_RATE]
        if eng_metrics:
            rate = eng_metrics[0].value
            if rate < 0.01:
                recs.append("📉 Engagement rate is below 1%. Try more questions, polls, or threads.")
            elif rate > 0.05:
                recs.append("🔥 Engagement rate above 5% — excellent! Maintain your current strategy.")

        # Growth check
        if self.data.follower_growth:
            recent = self.data.follower_growth[-7:]  # last 7 data points
            if recent:
                deltas = [g.get("delta", 0) for g in recent]
                avg_growth = sum(deltas) / len(deltas) if deltas else 0
                if avg_growth < 0:
                    recs.append("⚠️ Follower count is declining. Review content strategy and engagement.")
                elif avg_growth > 50:
                    recs.append("🚀 Strong follower growth! Capitalize with consistent posting.")

        # Competitor insights
        if self.data.competitors:
            better = [c for c in self.data.competitors
                     if c.avg_engagement_rate > (
                         eng_metrics[0].value if eng_metrics else 0
                     )]
            if better:
                recs.append(
                    f"👀 {len(better)} competitor(s) have higher engagement rates. "
                    "Study their content patterns."
                )

        self.data.recommendations = recs
        return recs

    # === Format Generators ===

    def _generate_json(self) -> str:
        """Generate JSON report."""
        return json.dumps(self.data.to_dict(), indent=2, ensure_ascii=False)

    def _generate_csv(self) -> str:
        """Generate CSV report (tweet performance table)."""
        output = io.StringIO()
        writer = csv.writer(output)

        # Header
        writer.writerow([
            "Tweet ID", "Text", "Created At", "Impressions",
            "Likes", "Retweets", "Replies", "Quotes", "Clicks",
            "Engagement Rate", "Hashtags", "Media Type",
        ])

        # All tweets
        all_tweets = self.data.top_tweets + self.data.worst_tweets
        seen = set()
        for t in all_tweets:
            if t.tweet_id in seen:
                continue
            seen.add(t.tweet_id)
            writer.writerow([
                t.tweet_id, t.text[:100], t.created_at, t.impressions,
                t.likes, t.retweets, t.replies, t.quotes, t.clicks,
                f"{t.engagement_rate:.4f}", " ".join(t.hashtags), t.media_type or "",
            ])

        return output.getvalue()

    def _generate_markdown(self) -> str:
        """Generate Markdown report."""
        d = self.data
        lines = [
            f"# Twitter Analytics Report — @{d.account_username}",
            f"**Period:** {d.period.value} ({d.start_date} → {d.end_date})",
            f"**Generated:** {d.generated_at}",
            "",
            "## 📊 Overview",
            f"- Total Tweets: **{d.total_tweets}**",
        ]

        for m in d.metrics:
            delta_str = f" ({'+' if m.delta >= 0 else ''}{m.delta:.0f}, {'+' if m.delta_pct >= 0 else ''}{m.delta_pct:.1f}%)" if m.delta else ""
            lines.append(f"- {m.metric.value}: **{m.value:,.0f}**{delta_str}")

        lines.extend(["", "## 🏆 Top Performing Tweets"])
        for i, t in enumerate(d.top_tweets[:5], 1):
            lines.append(
                f"{i}. **{t.text[:80]}...**\n"
                f"   ❤️ {t.likes} | 🔁 {t.retweets} | 💬 {t.replies} | "
                f"📊 {t.engagement_rate:.2%} engagement"
            )

        if d.hashtag_stats:
            lines.extend(["", "## #️⃣ Hashtag Performance", "| Hashtag | Uses | Avg Engagement | Score |",
                         "|---------|------|----------------|-------|"])
            for h in sorted(d.hashtag_stats, key=lambda x: x.effectiveness_score, reverse=True)[:10]:
                lines.append(f"| #{h.hashtag} | {h.usage_count} | {h.avg_engagement:.1f} | {h.effectiveness_score:.2f} |")

        if d.posting_heatmap:
            best = d.posting_heatmap.get_best_times(5)
            if best:
                days = ["Monday", "Tuesday", "Wednesday", "Thursday",
                       "Friday", "Saturday", "Sunday"]
                lines.extend(["", "## ⏰ Best Posting Times"])
                for day, hour, eng in best:
                    lines.append(f"- {days[day]} {hour:02d}:00 — {eng:.2%} engagement")

        if d.competitors:
            lines.extend(["", "## 👥 Competitor Comparison",
                         "| Account | Followers | Eng Rate | Posts/Day |",
                         "|---------|-----------|----------|-----------|"])
            for c in d.competitors:
                lines.append(
                    f"| @{c.username} | {c.followers:,} | {c.avg_engagement_rate:.2%} | {c.posting_frequency:.1f} |"
                )

        if d.recommendations:
            lines.extend(["", "## 💡 Recommendations"])
            for r in d.recommendations:
                lines.append(f"- {r}")

        return "\n".join(lines)

    def _generate_html(self) -> str:
        """Generate full HTML report with inline CSS."""
        d = self.data
        c = self.COLORS

        # Build metric cards
        metric_cards = ""
        for m in d.metrics:
            delta_class = "success" if m.delta >= 0 else "danger"
            delta_str = f'{"+  " if m.delta >= 0 else ""}{m.delta_pct:.1f}%' if m.delta else "—"
            metric_cards += f"""
            <div class="metric-card">
                <div class="metric-label">{m.metric.value.replace("_", " ").title()}</div>
                <div class="metric-value">{m.value:,.0f}</div>
                <div class="metric-delta {delta_class}">{delta_str}</div>
            </div>"""

        # Top tweets table
        tweet_rows = ""
        for t in d.top_tweets[:10]:
            tweet_rows += f"""
            <tr>
                <td class="tweet-text">{_html_escape(t.text[:80])}...</td>
                <td>{t.impressions:,}</td>
                <td>{t.likes:,}</td>
                <td>{t.retweets:,}</td>
                <td>{t.replies:,}</td>
                <td>{t.engagement_rate:.2%}</td>
            </tr>"""

        # Hashtag table
        hashtag_rows = ""
        for h in sorted(d.hashtag_stats, key=lambda x: x.effectiveness_score, reverse=True)[:10]:
            bar_width = min(100, h.effectiveness_score * 50)
            hashtag_rows += f"""
            <tr>
                <td>#{_html_escape(h.hashtag)}</td>
                <td>{h.usage_count}</td>
                <td>{h.avg_engagement:.1f}</td>
                <td>
                    <div class="bar-container">
                        <div class="bar" style="width:{bar_width}%"></div>
                    </div>
                    {h.effectiveness_score:.2f}
                </td>
            </tr>"""

        # Best times
        best_times_html = ""
        if d.posting_heatmap:
            days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            for day, hour, eng in d.posting_heatmap.get_best_times(5):
                best_times_html += f"""
                <div class="time-slot">
                    <span class="time-day">{days[day]}</span>
                    <span class="time-hour">{hour:02d}:00</span>
                    <span class="time-eng">{eng:.2%}</span>
                </div>"""

        # Recommendations
        recs_html = ""
        for r in d.recommendations:
            recs_html += f'<div class="rec-item">{_html_escape(r)}</div>'

        # Competitor table
        competitor_rows = ""
        for comp in d.competitors:
            competitor_rows += f"""
            <tr>
                <td>@{_html_escape(comp.username)}</td>
                <td>{comp.followers:,}</td>
                <td>{comp.avg_engagement_rate:.2%}</td>
                <td>{comp.posting_frequency:.1f}/day</td>
            </tr>"""

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Twitter Analytics — @{_html_escape(d.account_username)}</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       background: {c['bg']}; color: {c['secondary']}; line-height: 1.6; }}
.container {{ max-width: 1200px; margin: 0 auto; padding: 24px; }}
.header {{ background: linear-gradient(135deg, {c['primary']}, #0D8BD9);
           color: white; padding: 40px; border-radius: 16px; margin-bottom: 24px; }}
.header h1 {{ font-size: 28px; margin-bottom: 8px; }}
.header .subtitle {{ opacity: 0.9; font-size: 14px; }}
.section {{ background: {c['card_bg']}; border-radius: 12px; padding: 24px;
           margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
.section h2 {{ font-size: 20px; margin-bottom: 16px; padding-bottom: 8px;
              border-bottom: 2px solid {c['bg']}; }}
.metrics-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                gap: 16px; margin-bottom: 24px; }}
.metric-card {{ background: {c['card_bg']}; border-radius: 12px; padding: 20px;
               box-shadow: 0 1px 3px rgba(0,0,0,0.1); text-align: center; }}
.metric-label {{ font-size: 12px; text-transform: uppercase; color: {c['light']}; margin-bottom: 4px; }}
.metric-value {{ font-size: 28px; font-weight: 700; color: {c['secondary']}; }}
.metric-delta {{ font-size: 13px; margin-top: 4px; }}
.metric-delta.success {{ color: {c['success']}; }}
.metric-delta.danger {{ color: {c['danger']}; }}
table {{ width: 100%; border-collapse: collapse; }}
th {{ text-align: left; padding: 10px; background: {c['bg']}; font-size: 12px;
     text-transform: uppercase; color: {c['light']}; }}
td {{ padding: 10px; border-bottom: 1px solid {c['bg']}; }}
.tweet-text {{ max-width: 300px; font-size: 13px; }}
.bar-container {{ display: inline-block; width: 60px; height: 8px; background: {c['bg']};
                 border-radius: 4px; margin-right: 8px; vertical-align: middle; }}
.bar {{ height: 100%; background: {c['primary']}; border-radius: 4px; }}
.time-slot {{ display: inline-block; background: {c['bg']}; padding: 8px 16px;
             border-radius: 8px; margin: 4px; }}
.time-day {{ font-weight: 600; }}
.time-eng {{ color: {c['success']}; font-weight: 600; }}
.rec-item {{ padding: 12px 16px; background: {c['bg']}; border-radius: 8px;
            margin-bottom: 8px; font-size: 14px; }}
.footer {{ text-align: center; padding: 20px; color: {c['light']}; font-size: 12px; }}
</style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>📊 @{_html_escape(d.account_username)} Analytics</h1>
        <div class="subtitle">{d.period.value.title()} Report — {d.start_date} to {d.end_date}</div>
    </div>

    <div class="metrics-grid">{metric_cards}</div>

    <div class="section">
        <h2>🏆 Top Performing Tweets</h2>
        <table>
            <tr><th>Tweet</th><th>Impressions</th><th>Likes</th><th>Retweets</th><th>Replies</th><th>Eng. Rate</th></tr>
            {tweet_rows}
        </table>
    </div>

    <div class="section">
        <h2>#️⃣ Hashtag Performance</h2>
        <table>
            <tr><th>Hashtag</th><th>Uses</th><th>Avg Engagement</th><th>Effectiveness</th></tr>
            {hashtag_rows}
        </table>
    </div>

    <div class="section">
        <h2>⏰ Best Posting Times</h2>
        {best_times_html if best_times_html else '<p style="color:#AAB8C2">No timing data available.</p>'}
    </div>

    {"<div class='section'><h2>👥 Competitor Comparison</h2><table><tr><th>Account</th><th>Followers</th><th>Eng Rate</th><th>Frequency</th></tr>" + competitor_rows + "</table></div>" if competitor_rows else ""}

    <div class="section">
        <h2>💡 Recommendations</h2>
        {recs_html if recs_html else '<p style="color:#AAB8C2">Generate recommendations with generate_recommendations().</p>'}
    </div>

    <div class="footer">
        Generated by TwitterBot Framework v7.0.0 — {d.generated_at}
    </div>
</div>
</body>
</html>"""
        return html


def _html_escape(text: str) -> str:
    """Basic HTML escaping."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )
