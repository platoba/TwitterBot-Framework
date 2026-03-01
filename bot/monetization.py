"""
Monetization Tracker - 推文变现追踪引擎 v1.0
联盟链接追踪 + 收入归因 + ROI分析 + 多渠道变现 + 收益报告
"""

import json
import logging
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger(__name__)


class RevenueSource(str, Enum):
    AFFILIATE = "affiliate"
    SPONSORED = "sponsored"
    TIP_JAR = "tip_jar"
    SUBSCRIPTION = "subscription"
    PRODUCT_SALE = "product_sale"
    REFERRAL = "referral"


class Currency(str, Enum):
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    CNY = "CNY"
    JPY = "JPY"


@dataclass
class AffiliateLink:
    """联盟链接"""
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    original_url: str = ""
    tracked_url: str = ""
    platform: str = ""  # amazon, shopify, etc
    tag: str = ""  # affiliate tag
    commission_rate: float = 0.0
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "original_url": self.original_url,
            "tracked_url": self.tracked_url,
            "platform": self.platform,
            "tag": self.tag,
            "commission_rate": self.commission_rate,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "AffiliateLink":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class RevenueEvent:
    """收入事件"""
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    source: RevenueSource = RevenueSource.AFFILIATE
    amount: float = 0.0
    currency: Currency = Currency.USD
    tweet_id: Optional[str] = None
    campaign_id: Optional[str] = None
    link_id: Optional[str] = None
    description: str = ""
    metadata: Dict = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "source": self.source.value if isinstance(self.source, RevenueSource) else self.source,
            "amount": self.amount,
            "currency": self.currency.value if isinstance(self.currency, Currency) else self.currency,
            "tweet_id": self.tweet_id,
            "campaign_id": self.campaign_id,
            "link_id": self.link_id,
            "description": self.description,
            "metadata": self.metadata,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "RevenueEvent":
        d = dict(data)
        if "source" in d and isinstance(d["source"], str):
            try:
                d["source"] = RevenueSource(d["source"])
            except ValueError:
                d["source"] = RevenueSource.AFFILIATE
        if "currency" in d and isinstance(d["currency"], str):
            try:
                d["currency"] = Currency(d["currency"])
            except ValueError:
                d["currency"] = Currency.USD
        if "metadata" in d and isinstance(d["metadata"], str):
            d["metadata"] = json.loads(d["metadata"])
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class ClickEvent:
    """点击追踪事件"""
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    link_id: str = ""
    tweet_id: Optional[str] = None
    referrer: str = ""
    user_agent: str = ""
    ip_hash: str = ""  # 隐私保护，只存hash
    country: str = ""
    clicked_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "link_id": self.link_id,
            "tweet_id": self.tweet_id,
            "referrer": self.referrer,
            "user_agent": self.user_agent,
            "ip_hash": self.ip_hash,
            "country": self.country,
            "clicked_at": self.clicked_at,
        }


# Affiliate platform detection patterns
AFFILIATE_PATTERNS = {
    "amazon": [
        re.compile(r"(?:amazon\.\w+|amzn\.to)/.*(?:tag=|ref=)(\w[\w-]+)", re.I),
        re.compile(r"amzn\.to/\w+", re.I),
    ],
    "shopify": [
        re.compile(r"[\w-]+\.myshopify\.com", re.I),
    ],
    "aliexpress": [
        re.compile(r"(?:s\.click\.aliexpress\.com|aliexpress\.com).*(?:aff_id|tp)=(\w+)", re.I),
    ],
    "ebay": [
        re.compile(r"ebay\.\w+.*(?:campid|mkcid)=(\w+)", re.I),
    ],
    "clickbank": [
        re.compile(r"(\w+)\.hop\.clickbank\.net", re.I),
    ],
    "cj": [
        re.compile(r"(?:www\.)?(?:anrdoezrs|jdoqocy|tkqlhce|dpbolvw|kqzyfj)\.com", re.I),
    ],
    "shareasale": [
        re.compile(r"shareasale\.com/r\.cfm", re.I),
    ],
    "impact": [
        re.compile(r"(?:\w+)\.sjv\.io/", re.I),
    ],
}

# Default commission rates by platform
DEFAULT_COMMISSION_RATES = {
    "amazon": 0.03,
    "shopify": 0.05,
    "aliexpress": 0.05,
    "ebay": 0.04,
    "clickbank": 0.50,
    "cj": 0.08,
    "shareasale": 0.08,
    "impact": 0.07,
}


class LinkDetector:
    """从推文中检测联盟链接"""

    @staticmethod
    def detect_platform(url: str) -> Tuple[str, str]:
        """检测URL属于哪个联盟平台，返回 (platform, tag)"""
        for platform, patterns in AFFILIATE_PATTERNS.items():
            for pattern in patterns:
                match = pattern.search(url)
                if match:
                    tag = match.group(1) if match.lastindex else ""
                    return platform, tag
        return "", ""

    @staticmethod
    def extract_links(text: str) -> List[str]:
        """从文本中提取所有URL"""
        url_pattern = re.compile(
            r'https?://[^\s<>"{}|\\^`\[\]]+',
            re.I
        )
        return url_pattern.findall(text)

    @classmethod
    def scan_tweet(cls, text: str) -> List[AffiliateLink]:
        """扫描推文，返回检测到的联盟链接"""
        links = []
        urls = cls.extract_links(text)
        for url in urls:
            platform, tag = cls.detect_platform(url)
            if platform:
                link = AffiliateLink(
                    original_url=url,
                    tracked_url=url,
                    platform=platform,
                    tag=tag,
                    commission_rate=DEFAULT_COMMISSION_RATES.get(platform, 0.05),
                )
                links.append(link)
        return links


class MonetizationStore:
    """变现数据存储（内存+可选持久化）"""

    def __init__(self):
        self._links: Dict[str, AffiliateLink] = {}
        self._revenue: List[RevenueEvent] = []
        self._clicks: List[ClickEvent] = []
        self._tweet_links: Dict[str, List[str]] = {}  # tweet_id -> [link_ids]

    def add_link(self, link: AffiliateLink) -> str:
        self._links[link.id] = link
        return link.id

    def get_link(self, link_id: str) -> Optional[AffiliateLink]:
        return self._links.get(link_id)

    def list_links(self, platform: Optional[str] = None) -> List[AffiliateLink]:
        links = list(self._links.values())
        if platform:
            links = [l for l in links if l.platform == platform]
        return links

    def remove_link(self, link_id: str) -> bool:
        if link_id in self._links:
            del self._links[link_id]
            return True
        return False

    def associate_tweet(self, tweet_id: str, link_ids: List[str]):
        self._tweet_links[tweet_id] = link_ids

    def get_tweet_links(self, tweet_id: str) -> List[str]:
        return self._tweet_links.get(tweet_id, [])

    def record_revenue(self, event: RevenueEvent) -> str:
        self._revenue.append(event)
        return event.id

    def record_click(self, click: ClickEvent) -> str:
        self._clicks.append(click)
        return click.id

    def get_revenue(
        self,
        source: Optional[RevenueSource] = None,
        tweet_id: Optional[str] = None,
        campaign_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[RevenueEvent]:
        events = list(self._revenue)
        if source:
            events = [e for e in events if e.source == source]
        if tweet_id:
            events = [e for e in events if e.tweet_id == tweet_id]
        if campaign_id:
            events = [e for e in events if e.campaign_id == campaign_id]
        if start_date:
            events = [e for e in events if e.created_at >= start_date]
        if end_date:
            events = [e for e in events if e.created_at <= end_date]
        return events

    def get_clicks(
        self,
        link_id: Optional[str] = None,
        tweet_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[ClickEvent]:
        clicks = list(self._clicks)
        if link_id:
            clicks = [c for c in clicks if c.link_id == link_id]
        if tweet_id:
            clicks = [c for c in clicks if c.tweet_id == tweet_id]
        if start_date:
            clicks = [c for c in clicks if c.clicked_at >= start_date]
        if end_date:
            clicks = [c for c in clicks if c.clicked_at <= end_date]
        return clicks

    def total_revenue(self, currency: Currency = Currency.USD) -> float:
        return sum(e.amount for e in self._revenue if e.currency == currency)

    def clear(self):
        self._links.clear()
        self._revenue.clear()
        self._clicks.clear()
        self._tweet_links.clear()


class ROICalculator:
    """投资回报率计算器"""

    def __init__(self, store: MonetizationStore):
        self.store = store

    def tweet_roi(self, tweet_id: str, cost: float = 0.0) -> Dict[str, Any]:
        """计算单条推文的ROI"""
        revenue = self.store.get_revenue(tweet_id=tweet_id)
        clicks = self.store.get_clicks(tweet_id=tweet_id)
        total_rev = sum(e.amount for e in revenue)
        total_clicks = len(clicks)

        roi_pct = ((total_rev - cost) / cost * 100) if cost > 0 else 0.0
        cpc = cost / total_clicks if total_clicks > 0 else 0.0
        rpc = total_rev / total_clicks if total_clicks > 0 else 0.0

        return {
            "tweet_id": tweet_id,
            "revenue": round(total_rev, 2),
            "cost": round(cost, 2),
            "profit": round(total_rev - cost, 2),
            "roi_percent": round(roi_pct, 2),
            "clicks": total_clicks,
            "conversions": len(revenue),
            "cpc": round(cpc, 4),
            "rpc": round(rpc, 4),
            "conversion_rate": round(len(revenue) / total_clicks * 100, 2) if total_clicks > 0 else 0.0,
        }

    def campaign_roi(self, campaign_id: str, budget: float = 0.0) -> Dict[str, Any]:
        """计算活动ROI"""
        revenue = self.store.get_revenue(campaign_id=campaign_id)
        total_rev = sum(e.amount for e in revenue)

        roi_pct = ((total_rev - budget) / budget * 100) if budget > 0 else 0.0

        by_source: Dict[str, float] = {}
        for e in revenue:
            src = e.source.value if isinstance(e.source, RevenueSource) else e.source
            by_source[src] = by_source.get(src, 0) + e.amount

        return {
            "campaign_id": campaign_id,
            "revenue": round(total_rev, 2),
            "budget": round(budget, 2),
            "profit": round(total_rev - budget, 2),
            "roi_percent": round(roi_pct, 2),
            "events": len(revenue),
            "by_source": by_source,
        }

    def platform_breakdown(self) -> Dict[str, Dict[str, Any]]:
        """按联盟平台统计收益"""
        breakdown: Dict[str, Dict[str, Any]] = {}
        for link in self.store.list_links():
            platform = link.platform
            if platform not in breakdown:
                breakdown[platform] = {
                    "links": 0,
                    "clicks": 0,
                    "revenue": 0.0,
                    "commission_rate": link.commission_rate,
                }
            breakdown[platform]["links"] += 1
            clicks = self.store.get_clicks(link_id=link.id)
            breakdown[platform]["clicks"] += len(clicks)

        for event in self.store._revenue:
            if event.link_id:
                link = self.store.get_link(event.link_id)
                if link:
                    breakdown.setdefault(link.platform, {"links": 0, "clicks": 0, "revenue": 0.0})
                    breakdown[link.platform]["revenue"] += event.amount

        for p in breakdown:
            breakdown[p]["revenue"] = round(breakdown[p]["revenue"], 2)

        return breakdown

    def daily_summary(self, date: Optional[str] = None) -> Dict[str, Any]:
        """每日收益汇总"""
        target = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        start = f"{target}T00:00:00"
        end = f"{target}T23:59:59"

        revenue = self.store.get_revenue(start_date=start, end_date=end)
        clicks = self.store.get_clicks(start_date=start, end_date=end)
        total = sum(e.amount for e in revenue)

        by_source: Dict[str, float] = {}
        for e in revenue:
            src = e.source.value if isinstance(e.source, RevenueSource) else e.source
            by_source[src] = by_source.get(src, 0) + e.amount

        return {
            "date": target,
            "total_revenue": round(total, 2),
            "transactions": len(revenue),
            "clicks": len(clicks),
            "by_source": by_source,
            "avg_per_transaction": round(total / len(revenue), 2) if revenue else 0.0,
        }


class MonetizationReport:
    """变现报告生成器"""

    def __init__(self, store: MonetizationStore):
        self.store = store
        self.calculator = ROICalculator(store)

    def full_report(self) -> Dict[str, Any]:
        """生成完整变现报告"""
        all_revenue = self.store.get_revenue()
        all_clicks = self.store.get_clicks()
        all_links = self.store.list_links()

        total_rev = sum(e.amount for e in all_revenue)

        # Top performing links by revenue
        link_revenue: Dict[str, float] = {}
        for e in all_revenue:
            if e.link_id:
                link_revenue[e.link_id] = link_revenue.get(e.link_id, 0) + e.amount
        top_links = sorted(link_revenue.items(), key=lambda x: x[1], reverse=True)[:10]

        # Top performing tweets
        tweet_revenue: Dict[str, float] = {}
        for e in all_revenue:
            if e.tweet_id:
                tweet_revenue[e.tweet_id] = tweet_revenue.get(e.tweet_id, 0) + e.amount
        top_tweets = sorted(tweet_revenue.items(), key=lambda x: x[1], reverse=True)[:10]

        return {
            "summary": {
                "total_revenue": round(total_rev, 2),
                "total_transactions": len(all_revenue),
                "total_clicks": len(all_clicks),
                "total_links": len(all_links),
                "avg_revenue_per_click": round(total_rev / len(all_clicks), 4) if all_clicks else 0.0,
            },
            "platform_breakdown": self.calculator.platform_breakdown(),
            "top_links": [{"link_id": lid, "revenue": round(rev, 2)} for lid, rev in top_links],
            "top_tweets": [{"tweet_id": tid, "revenue": round(rev, 2)} for tid, rev in top_tweets],
        }

    def export_csv(self) -> str:
        """导出收入CSV"""
        lines = ["id,source,amount,currency,tweet_id,campaign_id,link_id,created_at"]
        for e in self.store.get_revenue():
            src = e.source.value if isinstance(e.source, RevenueSource) else e.source
            cur = e.currency.value if isinstance(e.currency, Currency) else e.currency
            lines.append(
                f"{e.id},{src},{e.amount},{cur},"
                f"{e.tweet_id or ''},{e.campaign_id or ''},{e.link_id or ''},{e.created_at}"
            )
        return "\n".join(lines)

    def export_json(self) -> str:
        """导出收入JSON"""
        return json.dumps(
            [e.to_dict() for e in self.store.get_revenue()],
            indent=2,
            ensure_ascii=False,
        )

    def text_summary(self) -> str:
        """文本摘要报告"""
        report = self.full_report()
        s = report["summary"]
        lines = [
            "💰 Monetization Report",
            f"Total Revenue: ${s['total_revenue']:.2f}",
            f"Transactions: {s['total_transactions']}",
            f"Clicks: {s['total_clicks']}",
            f"Links Tracked: {s['total_links']}",
            f"Rev/Click: ${s['avg_revenue_per_click']:.4f}",
            "",
            "📊 Platform Breakdown:",
        ]
        for platform, stats in report["platform_breakdown"].items():
            lines.append(f"  {platform}: ${stats['revenue']:.2f} ({stats['clicks']} clicks, {stats['links']} links)")
        if report["top_tweets"]:
            lines.append("")
            lines.append("🏆 Top Tweets:")
            for item in report["top_tweets"][:5]:
                lines.append(f"  {item['tweet_id']}: ${item['revenue']:.2f}")
        return "\n".join(lines)


class MonetizationEngine:
    """变现引擎 - 统一入口"""

    def __init__(self):
        self.store = MonetizationStore()
        self.detector = LinkDetector()
        self.calculator = ROICalculator(self.store)
        self.reporter = MonetizationReport(self.store)

    def process_tweet(self, tweet_id: str, text: str) -> List[AffiliateLink]:
        """处理推文，自动检测和追踪联盟链接"""
        links = self.detector.scan_tweet(text)
        link_ids = []
        for link in links:
            self.store.add_link(link)
            link_ids.append(link.id)
        if link_ids:
            self.store.associate_tweet(tweet_id, link_ids)
        return links

    def record_sale(
        self,
        amount: float,
        source: RevenueSource = RevenueSource.AFFILIATE,
        tweet_id: Optional[str] = None,
        campaign_id: Optional[str] = None,
        link_id: Optional[str] = None,
        currency: Currency = Currency.USD,
        description: str = "",
    ) -> str:
        """记录一笔收入"""
        event = RevenueEvent(
            source=source,
            amount=amount,
            currency=currency,
            tweet_id=tweet_id,
            campaign_id=campaign_id,
            link_id=link_id,
            description=description,
        )
        return self.store.record_revenue(event)

    def record_click(
        self,
        link_id: str,
        tweet_id: Optional[str] = None,
        referrer: str = "",
        country: str = "",
    ) -> str:
        """记录一次点击"""
        click = ClickEvent(
            link_id=link_id,
            tweet_id=tweet_id,
            referrer=referrer,
            country=country,
        )
        return self.store.record_click(click)

    def get_report(self) -> Dict[str, Any]:
        return self.reporter.full_report()

    def get_text_report(self) -> str:
        return self.reporter.text_summary()

    def get_tweet_roi(self, tweet_id: str, cost: float = 0.0) -> Dict[str, Any]:
        return self.calculator.tweet_roi(tweet_id, cost)

    def get_campaign_roi(self, campaign_id: str, budget: float = 0.0) -> Dict[str, Any]:
        return self.calculator.campaign_roi(campaign_id, budget)
