"""
Campaign Manager - 推广活动管理引擎 v3.0
多推文活动 + 目标追踪 + 预算管理 + 进度报告 + 自动化执行
"""

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Dict, List, Optional, Callable

from bot.database import Database
from bot.twitter_api import TwitterAPI
from bot.webhook import TelegramWebhook
from bot.content_generator import ContentGenerator

logger = logging.getLogger(__name__)


class CampaignStatus(str, Enum):
    DRAFT = "draft"
    SCHEDULED = "scheduled"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class CampaignGoalType(str, Enum):
    IMPRESSIONS = "impressions"
    ENGAGEMENTS = "engagements"
    FOLLOWERS = "followers"
    CLICKS = "clicks"
    CONVERSIONS = "conversions"


@dataclass
class CampaignGoal:
    """活动目标"""
    goal_type: CampaignGoalType
    target_value: int
    current_value: int = 0

    @property
    def progress_pct(self) -> float:
        if self.target_value <= 0:
            return 0
        return min(round(self.current_value / self.target_value * 100, 1), 100)

    @property
    def is_achieved(self) -> bool:
        return self.current_value >= self.target_value

    def to_dict(self) -> Dict:
        return {
            "type": self.goal_type.value,
            "target": self.target_value,
            "current": self.current_value,
            "progress_pct": self.progress_pct,
            "achieved": self.is_achieved,
        }


@dataclass
class CampaignTweet:
    """活动内推文"""
    content: str
    tweet_id: Optional[str] = None
    scheduled_at: Optional[str] = None
    sent_at: Optional[str] = None
    status: str = "pending"
    metrics: Dict = field(default_factory=dict)
    variant: Optional[str] = None  # A/B testing

    def to_dict(self) -> Dict:
        return {
            "content": self.content,
            "tweet_id": self.tweet_id,
            "scheduled_at": self.scheduled_at,
            "sent_at": self.sent_at,
            "status": self.status,
            "metrics": self.metrics,
            "variant": self.variant,
        }


@dataclass
class Campaign:
    """推广活动"""
    id: str = ""
    name: str = ""
    description: str = ""
    status: CampaignStatus = CampaignStatus.DRAFT
    goals: List[CampaignGoal] = field(default_factory=list)
    tweets: List[CampaignTweet] = field(default_factory=list)
    hashtags: List[str] = field(default_factory=list)
    target_audience: str = ""
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    budget_usd: float = 0
    spent_usd: float = 0
    tags: List[str] = field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""
    metadata: Dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())[:8]
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()

    @property
    def total_tweets(self) -> int:
        return len(self.tweets)

    @property
    def sent_tweets(self) -> int:
        return sum(1 for t in self.tweets if t.status == "sent")

    @property
    def total_engagement(self) -> int:
        return sum(
            t.metrics.get("like_count", 0) + t.metrics.get("retweet_count", 0)
            + t.metrics.get("reply_count", 0)
            for t in self.tweets
        )

    @property
    def total_impressions(self) -> int:
        return sum(t.metrics.get("impression_count", 0) for t in self.tweets)

    @property
    def avg_engagement_rate(self) -> float:
        impressions = self.total_impressions
        if impressions == 0:
            return 0
        return round(self.total_engagement / impressions * 100, 2)

    @property
    def roi(self) -> Optional[float]:
        if self.spent_usd <= 0:
            return None
        # Simple ROI = engagement / dollar spent
        return round(self.total_engagement / self.spent_usd, 2)

    @property
    def is_active(self) -> bool:
        return self.status == CampaignStatus.ACTIVE

    @property
    def overall_progress(self) -> float:
        if not self.goals:
            if self.total_tweets == 0:
                return 0
            return round(self.sent_tweets / self.total_tweets * 100, 1)
        return round(sum(g.progress_pct for g in self.goals) / len(self.goals), 1)

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "status": self.status.value,
            "goals": [g.to_dict() for g in self.goals],
            "tweets": [t.to_dict() for t in self.tweets],
            "hashtags": self.hashtags,
            "target_audience": self.target_audience,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "budget_usd": self.budget_usd,
            "spent_usd": self.spent_usd,
            "tags": self.tags,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "metadata": self.metadata,
            "stats": {
                "total_tweets": self.total_tweets,
                "sent_tweets": self.sent_tweets,
                "total_engagement": self.total_engagement,
                "total_impressions": self.total_impressions,
                "avg_engagement_rate": self.avg_engagement_rate,
                "roi": self.roi,
                "progress": self.overall_progress,
            },
        }


@dataclass
class CampaignComparison:
    """两个活动的对比分析结果"""
    campaign_a: Campaign
    campaign_b: Campaign

    @property
    def impressions_diff(self) -> int:
        return self.campaign_a.total_impressions - self.campaign_b.total_impressions

    @property
    def engagement_diff(self) -> int:
        return self.campaign_a.total_engagement - self.campaign_b.total_engagement

    @property
    def engagement_rate_diff(self) -> float:
        return round(self.campaign_a.avg_engagement_rate - self.campaign_b.avg_engagement_rate, 2)

    @property
    def winner(self) -> str:
        """基于综合评分判定胜者"""
        score_a = self._score(self.campaign_a)
        score_b = self._score(self.campaign_b)
        if score_a > score_b:
            return self.campaign_a.name
        elif score_b > score_a:
            return self.campaign_b.name
        return "tie"

    @property
    def winner_campaign(self) -> Optional[Campaign]:
        score_a = self._score(self.campaign_a)
        score_b = self._score(self.campaign_b)
        if score_a > score_b:
            return self.campaign_a
        elif score_b > score_a:
            return self.campaign_b
        return None

    def _score(self, c: Campaign) -> float:
        """综合评分 = 互动率权重50% + 总互动量权重30% + 进度权重20%"""
        return (
            c.avg_engagement_rate * 50
            + min(c.total_engagement / max(c.total_impressions, 1) * 100, 100) * 30
            + c.overall_progress * 20
        ) / 100

    @property
    def cost_efficiency(self) -> Optional[Dict]:
        """成本效率对比"""
        if self.campaign_a.spent_usd <= 0 and self.campaign_b.spent_usd <= 0:
            return None
        return {
            "a_cost_per_engagement": round(
                self.campaign_a.spent_usd / max(self.campaign_a.total_engagement, 1), 4
            ),
            "b_cost_per_engagement": round(
                self.campaign_b.spent_usd / max(self.campaign_b.total_engagement, 1), 4
            ),
        }

    def to_dict(self) -> Dict:
        return {
            "campaign_a": {"id": self.campaign_a.id, "name": self.campaign_a.name},
            "campaign_b": {"id": self.campaign_b.id, "name": self.campaign_b.name},
            "impressions_diff": self.impressions_diff,
            "engagement_diff": self.engagement_diff,
            "engagement_rate_diff": self.engagement_rate_diff,
            "winner": self.winner,
            "cost_efficiency": self.cost_efficiency,
        }

    def format_report(self) -> str:
        lines = [
            "📊 *Campaign Comparison*\n",
            f"🅰️ {self.campaign_a.name}",
            f"  Impressions: {self.campaign_a.total_impressions:,}",
            f"  Engagement: {self.campaign_a.total_engagement:,}",
            f"  Eng Rate: {self.campaign_a.avg_engagement_rate}%\n",
            f"🅱️ {self.campaign_b.name}",
            f"  Impressions: {self.campaign_b.total_impressions:,}",
            f"  Engagement: {self.campaign_b.total_engagement:,}",
            f"  Eng Rate: {self.campaign_b.avg_engagement_rate}%\n",
            f"🏆 Winner: {self.winner}",
        ]
        if self.cost_efficiency:
            ce = self.cost_efficiency
            lines.append(f"\n💰 Cost/Engagement: A=${ce['a_cost_per_engagement']} | B=${ce['b_cost_per_engagement']}")
        return "\n".join(lines)


class CampaignManager:
    """推广活动管理器"""

    def __init__(self, api: TwitterAPI, db: Database,
                 webhook: TelegramWebhook = None,
                 content_generator: ContentGenerator = None):
        self.api = api
        self.db = db
        self.webhook = webhook
        self.generator = content_generator or ContentGenerator()
        self._campaigns: Dict[str, Campaign] = {}
        self._load_campaigns()

    # ── 持久化 ──

    def _campaigns_table_exists(self) -> bool:
        """检查campaigns表是否存在"""
        conn = self.db._get_conn()
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='campaigns'"
        )
        return cursor.fetchone() is not None

    def _ensure_table(self):
        """确保campaigns表存在"""
        conn = self.db._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS campaigns (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                data TEXT NOT NULL,
                status TEXT DEFAULT 'draft',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_campaign_status ON campaigns(status);
        """)
        conn.commit()

    def _load_campaigns(self):
        """从数据库加载活动"""
        self._ensure_table()
        conn = self.db._get_conn()
        rows = conn.execute("SELECT id, data FROM campaigns").fetchall()
        for row in rows:
            try:
                data = json.loads(row["data"])
                campaign = self._dict_to_campaign(data)
                self._campaigns[campaign.id] = campaign
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Failed to load campaign {row['id']}: {e}")

    def _save_campaign(self, campaign: Campaign):
        """保存活动到数据库"""
        self._ensure_table()
        campaign.updated_at = datetime.now(timezone.utc).isoformat()
        conn = self.db._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO campaigns (id, name, data, status, updated_at)
            VALUES (?, ?, ?, ?, datetime('now'))
        """, (campaign.id, campaign.name, json.dumps(campaign.to_dict()), campaign.status.value))
        conn.commit()
        self._campaigns[campaign.id] = campaign

    def _dict_to_campaign(self, data: Dict) -> Campaign:
        """字典转Campaign对象"""
        campaign = Campaign(
            id=data.get("id", ""),
            name=data.get("name", ""),
            description=data.get("description", ""),
            status=CampaignStatus(data.get("status", "draft")),
            hashtags=data.get("hashtags", []),
            target_audience=data.get("target_audience", ""),
            start_date=data.get("start_date"),
            end_date=data.get("end_date"),
            budget_usd=data.get("budget_usd", 0),
            spent_usd=data.get("spent_usd", 0),
            tags=data.get("tags", []),
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
            metadata=data.get("metadata", {}),
        )

        for g in data.get("goals", []):
            campaign.goals.append(CampaignGoal(
                goal_type=CampaignGoalType(g["type"]),
                target_value=g["target"],
                current_value=g.get("current", 0),
            ))

        for t in data.get("tweets", []):
            campaign.tweets.append(CampaignTweet(
                content=t["content"],
                tweet_id=t.get("tweet_id"),
                scheduled_at=t.get("scheduled_at"),
                sent_at=t.get("sent_at"),
                status=t.get("status", "pending"),
                metrics=t.get("metrics", {}),
                variant=t.get("variant"),
            ))

        return campaign

    # ── CRUD ──

    def create(self, name: str, description: str = "",
               goals: List[Dict] = None,
               hashtags: List[str] = None,
               target_audience: str = "",
               budget_usd: float = 0,
               start_date: str = None,
               end_date: str = None,
               tags: List[str] = None) -> Campaign:
        """创建新活动"""
        campaign = Campaign(
            name=name,
            description=description,
            hashtags=hashtags or [],
            target_audience=target_audience,
            budget_usd=budget_usd,
            start_date=start_date,
            end_date=end_date,
            tags=tags or [],
        )

        if goals:
            for g in goals:
                campaign.goals.append(CampaignGoal(
                    goal_type=CampaignGoalType(g["type"]),
                    target_value=g["target"],
                ))

        self._save_campaign(campaign)
        logger.info(f"Campaign created: {campaign.id} - {campaign.name}")
        return campaign

    def get(self, campaign_id: str) -> Optional[Campaign]:
        return self._campaigns.get(campaign_id)

    def list_campaigns(self, status: CampaignStatus = None) -> List[Campaign]:
        campaigns = list(self._campaigns.values())
        if status:
            campaigns = [c for c in campaigns if c.status == status]
        campaigns.sort(key=lambda c: c.created_at, reverse=True)
        return campaigns

    def update_status(self, campaign_id: str, status: CampaignStatus) -> Optional[Campaign]:
        campaign = self.get(campaign_id)
        if not campaign:
            return None
        campaign.status = status
        self._save_campaign(campaign)
        return campaign

    def delete(self, campaign_id: str) -> bool:
        if campaign_id not in self._campaigns:
            return False
        del self._campaigns[campaign_id]
        conn = self.db._get_conn()
        conn.execute("DELETE FROM campaigns WHERE id = ?", (campaign_id,))
        conn.commit()
        return True

    # ── 推文管理 ──

    def add_tweet(self, campaign_id: str, content: str,
                   scheduled_at: str = None,
                   variant: str = None) -> Optional[Campaign]:
        """向活动添加推文"""
        campaign = self.get(campaign_id)
        if not campaign:
            return None

        tweet = CampaignTweet(
            content=content,
            scheduled_at=scheduled_at,
            variant=variant,
        )
        campaign.tweets.append(tweet)
        self._save_campaign(campaign)
        return campaign

    def add_generated_tweets(self, campaign_id: str,
                              category: str,
                              variables: Dict[str, str],
                              count: int = 5,
                              interval_hours: int = 4) -> Optional[Campaign]:
        """自动生成并添加多条推文"""
        campaign = self.get(campaign_id)
        if not campaign:
            return None

        base_time = datetime.now(timezone.utc)
        if campaign.start_date:
            try:
                base_time = datetime.fromisoformat(campaign.start_date)
            except ValueError:
                pass

        for i in range(count):
            content = self.generator.generate(category, variables)
            if not content:
                continue

            # 添加活动标签
            if campaign.hashtags:
                tags = " ".join(f"#{h}" for h in campaign.hashtags[:3])
                if len(content) + len(tags) + 2 <= 280:
                    content = f"{content}\n\n{tags}"

            sched_time = base_time + timedelta(hours=i * interval_hours)

            tweet = CampaignTweet(
                content=content,
                scheduled_at=sched_time.isoformat(),
            )
            campaign.tweets.append(tweet)

        self._save_campaign(campaign)
        return campaign

    def send_next(self, campaign_id: str) -> Optional[Dict]:
        """发送活动中下一条待发推文"""
        campaign = self.get(campaign_id)
        if not campaign or not campaign.is_active:
            return None

        pending = [t for t in campaign.tweets if t.status == "pending"]
        if not pending:
            return None

        tweet = pending[0]
        result = self.api.post_tweet(tweet.content)

        if result and "data" in result:
            tweet.tweet_id = result["data"].get("id", "")
            tweet.status = "sent"
            tweet.sent_at = datetime.now(timezone.utc).isoformat()

            if self.webhook:
                self.webhook.notify_scheduled_tweet(
                    tweet.content, "sent", tweet.tweet_id
                )
        else:
            tweet.status = "failed"

        self._save_campaign(campaign)
        return {"status": tweet.status, "tweet_id": tweet.tweet_id}

    # ── 指标收集 ──

    def refresh_metrics(self, campaign_id: str) -> Optional[Campaign]:
        """刷新活动中所有已发推文的指标"""
        campaign = self.get(campaign_id)
        if not campaign:
            return None

        for tweet in campaign.tweets:
            if tweet.tweet_id and tweet.status == "sent":
                data = self.api.get_tweet(tweet.tweet_id)
                if data and "data" in data:
                    metrics = data["data"].get("public_metrics", {})
                    tweet.metrics = {
                        "like_count": metrics.get("like_count", 0),
                        "retweet_count": metrics.get("retweet_count", 0),
                        "reply_count": metrics.get("reply_count", 0),
                        "quote_count": metrics.get("quote_count", 0),
                        "impression_count": metrics.get("impression_count", 0),
                    }

        # 更新目标进度
        for goal in campaign.goals:
            if goal.goal_type == CampaignGoalType.IMPRESSIONS:
                goal.current_value = campaign.total_impressions
            elif goal.goal_type == CampaignGoalType.ENGAGEMENTS:
                goal.current_value = campaign.total_engagement

        self._save_campaign(campaign)
        return campaign

    # ── A/B测试 ──

    def setup_ab_test(self, campaign_id: str,
                       content_a: str, content_b: str,
                       scheduled_at: str = None,
                       delay_minutes: int = 60) -> Optional[Dict]:
        """在活动中设置A/B测试"""
        campaign = self.get(campaign_id)
        if not campaign:
            return None

        dt_a = datetime.fromisoformat(scheduled_at) if scheduled_at else datetime.now(timezone.utc)
        dt_b = dt_a + timedelta(minutes=delay_minutes)

        tweet_a = CampaignTweet(
            content=content_a, scheduled_at=dt_a.isoformat(), variant="A"
        )
        tweet_b = CampaignTweet(
            content=content_b, scheduled_at=dt_b.isoformat(), variant="B"
        )

        campaign.tweets.extend([tweet_a, tweet_b])
        self._save_campaign(campaign)

        return {
            "variant_a": content_a,
            "variant_b": content_b,
            "schedule_a": dt_a.isoformat(),
            "schedule_b": dt_b.isoformat(),
        }

    def evaluate_ab_test(self, campaign_id: str) -> Optional[Dict]:
        """评估A/B测试结果"""
        campaign = self.get(campaign_id)
        if not campaign:
            return None

        variants = {"A": [], "B": []}
        for tweet in campaign.tweets:
            if tweet.variant in variants:
                variants[tweet.variant].append(tweet)

        if not variants["A"] or not variants["B"]:
            return None

        def _calc_score(tweets: List[CampaignTweet]) -> Dict:
            total_likes = sum(t.metrics.get("like_count", 0) for t in tweets)
            total_rts = sum(t.metrics.get("retweet_count", 0) for t in tweets)
            total_impressions = sum(t.metrics.get("impression_count", 0) for t in tweets)
            total_eng = total_likes + total_rts
            eng_rate = total_eng / max(total_impressions, 1) * 100
            return {
                "tweets": len(tweets),
                "total_likes": total_likes,
                "total_retweets": total_rts,
                "total_impressions": total_impressions,
                "engagement_rate": round(eng_rate, 2),
                "score": total_eng,
            }

        score_a = _calc_score(variants["A"])
        score_b = _calc_score(variants["B"])

        winner = "A" if score_a["score"] >= score_b["score"] else "B"
        margin = abs(score_a["score"] - score_b["score"])

        return {
            "variant_a": score_a,
            "variant_b": score_b,
            "winner": winner,
            "margin": margin,
            "significant": margin > (score_a["score"] + score_b["score"]) * 0.1,
        }

    # ── 活动对比 ──

    def compare(self, campaign_id_a: str, campaign_id_b: str) -> Optional[CampaignComparison]:
        """对比两个活动"""
        a = self.get(campaign_id_a)
        b = self.get(campaign_id_b)
        if not a or not b:
            return None
        return CampaignComparison(campaign_a=a, campaign_b=b)

    def clone(self, campaign_id: str, new_name: str = None) -> Optional[Campaign]:
        """克隆活动（不含推文的发送状态）"""
        source = self.get(campaign_id)
        if not source:
            return None
        clone = Campaign(
            name=new_name or f"{source.name} (copy)",
            description=source.description,
            hashtags=list(source.hashtags),
            target_audience=source.target_audience,
            budget_usd=source.budget_usd,
            tags=list(source.tags),
            start_date=source.start_date,
            end_date=source.end_date,
        )
        for g in source.goals:
            clone.goals.append(CampaignGoal(
                goal_type=g.goal_type,
                target_value=g.target_value,
            ))
        for t in source.tweets:
            clone.tweets.append(CampaignTweet(
                content=t.content,
                variant=t.variant,
            ))
        self._save_campaign(clone)
        return clone

    # ── 报告 ──

    def format_summary(self, campaign: Campaign) -> str:
        """格式化活动摘要"""
        status_emoji = {
            "draft": "📝", "scheduled": "📅", "active": "🚀",
            "paused": "⏸️", "completed": "✅", "cancelled": "❌",
        }

        lines = [
            f"{status_emoji.get(campaign.status.value, '❓')} *{campaign.name}* [{campaign.id}]",
            f"Status: {campaign.status.value} | Progress: {campaign.overall_progress}%\n",
        ]

        if campaign.goals:
            lines.append("🎯 *Goals*")
            for goal in campaign.goals:
                bar = "█" * int(goal.progress_pct / 5) + "░" * (20 - int(goal.progress_pct / 5))
                lines.append(f"  {goal.goal_type.value}: {bar} {goal.progress_pct}%")
                lines.append(f"    {goal.current_value:,} / {goal.target_value:,}")
            lines.append("")

        lines.append(f"📊 *Performance*")
        lines.append(f"  Tweets: {campaign.sent_tweets}/{campaign.total_tweets}")
        lines.append(f"  Impressions: {campaign.total_impressions:,}")
        lines.append(f"  Engagement: {campaign.total_engagement:,}")
        lines.append(f"  Eng Rate: {campaign.avg_engagement_rate}%")

        if campaign.budget_usd > 0:
            lines.append(f"\n💰 Budget: ${campaign.spent_usd:.2f} / ${campaign.budget_usd:.2f}")
            if campaign.roi is not None:
                lines.append(f"  ROI: {campaign.roi} eng/$")

        return "\n".join(lines)

    def format_list(self, campaigns: List[Campaign] = None) -> str:
        """格式化活动列表"""
        if campaigns is None:
            campaigns = self.list_campaigns()
        if not campaigns:
            return "📋 暂无推广活动"

        lines = [f"📋 *推广活动列表* ({len(campaigns)})\n"]
        for c in campaigns:
            emoji = {"draft": "📝", "active": "🚀", "completed": "✅"}.get(c.status.value, "❓")
            lines.append(f"{emoji} `{c.id}` {c.name}")
            lines.append(f"   {c.sent_tweets}/{c.total_tweets} tweets | {c.overall_progress}%")

        return "\n".join(lines)
