"""
ROI Calculator v1.0
社交媒体ROI计算器 — 活动成本追踪 + 转化归因 + CPE/CPC/CPM指标 + 收入关联 + ROI报告

Features:
- CampaignCost: track all cost categories (ad spend, tools, labor, content)
- ConversionTracker: multi-touch attribution (first/last/linear/time-decay)
- MetricsCalculator: CPE, CPC, CPM, CTR, ROAS + benchmark comparison
- RevenueCorrelation: correlate social activity with revenue data
- ROIForecaster: predict future ROI based on historical trends
- ROIReport: comprehensive report with recommendations
"""

import json
import math
import sqlite3
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import List, Dict, Any, Tuple


class CostCategory(Enum):
    """费用类别"""
    AD_SPEND = "ad_spend"           # 广告支出
    CONTENT_CREATION = "content"    # 内容制作
    TOOLS_SOFTWARE = "tools"        # 工具/软件
    LABOR = "labor"                 # 人工
    INFLUENCER = "influencer"       # 网红合作
    PROMOTION = "promotion"         # 推广
    OTHER = "other"


class AttributionModel(Enum):
    """归因模型"""
    FIRST_TOUCH = "first_touch"     # 首次触点
    LAST_TOUCH = "last_touch"       # 最后触点
    LINEAR = "linear"               # 线性均分
    TIME_DECAY = "time_decay"       # 时间衰减
    POSITION_BASED = "position"     # U型归因


class ConversionType(Enum):
    """转化类型"""
    CLICK = "click"
    SIGNUP = "signup"
    PURCHASE = "purchase"
    LEAD = "lead"
    DOWNLOAD = "download"
    SUBSCRIBE = "subscribe"
    CUSTOM = "custom"


class ROIRating(Enum):
    """ROI评级"""
    EXCELLENT = "excellent"     # > 300%
    GOOD = "good"               # 100-300%
    MODERATE = "moderate"       # 50-100%
    POOR = "poor"               # 0-50%
    NEGATIVE = "negative"       # < 0%


@dataclass
class CostEntry:
    """费用条目"""
    entry_id: str
    campaign_id: str
    category: CostCategory
    amount: float
    currency: str = "USD"
    description: str = ""
    date: str = ""
    recurring: bool = False
    recurring_interval_days: int = 0

    def __post_init__(self):
        if not self.date:
            self.date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["category"] = self.category.value
        return d


@dataclass
class Conversion:
    """转化事件"""
    conversion_id: str
    campaign_id: str
    conversion_type: ConversionType
    value: float = 0.0           # 货币价值
    currency: str = "USD"
    source_tweet_id: str = ""
    touchpoints: List[str] = field(default_factory=list)  # tweet_ids in order
    occurred_at: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.occurred_at:
            self.occurred_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["conversion_type"] = self.conversion_type.value
        return d


@dataclass
class CampaignMetrics:
    """活动指标"""
    campaign_id: str
    campaign_name: str = ""
    period_start: str = ""
    period_end: str = ""
    # Reach
    impressions: int = 0
    reach: int = 0
    # Engagement
    engagements: int = 0
    clicks: int = 0
    likes: int = 0
    retweets: int = 0
    replies: int = 0
    # Cost
    total_cost: float = 0.0
    cost_breakdown: Dict[str, float] = field(default_factory=dict)
    # Conversions
    conversions: int = 0
    conversion_value: float = 0.0
    # Computed
    cpe: float = 0.0              # Cost Per Engagement
    cpc: float = 0.0              # Cost Per Click
    cpm: float = 0.0              # Cost Per Mille (1000 impressions)
    ctr: float = 0.0              # Click-Through Rate
    engagement_rate: float = 0.0
    roas: float = 0.0             # Return On Ad Spend
    roi_percentage: float = 0.0
    roi_rating: ROIRating = ROIRating.MODERATE

    def compute(self):
        """计算衍生指标"""
        if self.engagements > 0 and self.total_cost > 0:
            self.cpe = self.total_cost / self.engagements
        if self.clicks > 0 and self.total_cost > 0:
            self.cpc = self.total_cost / self.clicks
        if self.impressions > 0 and self.total_cost > 0:
            self.cpm = self.total_cost / self.impressions * 1000
        if self.impressions > 0:
            self.ctr = self.clicks / self.impressions * 100
            self.engagement_rate = self.engagements / self.impressions * 100
        if self.total_cost > 0:
            self.roas = self.conversion_value / self.total_cost
            self.roi_percentage = (self.conversion_value - self.total_cost) / self.total_cost * 100
        self.roi_rating = self._rate_roi(self.roi_percentage)

    @staticmethod
    def _rate_roi(roi_pct: float) -> ROIRating:
        if roi_pct > 300:
            return ROIRating.EXCELLENT
        elif roi_pct > 100:
            return ROIRating.GOOD
        elif roi_pct > 50:
            return ROIRating.MODERATE
        elif roi_pct > 0:
            return ROIRating.POOR
        return ROIRating.NEGATIVE

    def to_dict(self) -> Dict[str, Any]:
        self.compute()
        d = asdict(self)
        d["roi_rating"] = self.roi_rating.value
        # Round floats
        for key in ["cpe", "cpc", "cpm", "ctr", "engagement_rate", "roas", "roi_percentage"]:
            if key in d:
                d[key] = round(d[key], 4)
        return d


# Industry benchmark data (Twitter/X averages)
BENCHMARKS = {
    "cpe": {"low": 0.10, "avg": 0.50, "high": 2.00, "unit": "USD"},
    "cpc": {"low": 0.25, "avg": 0.80, "high": 3.00, "unit": "USD"},
    "cpm": {"low": 2.00, "avg": 6.50, "high": 15.00, "unit": "USD"},
    "ctr": {"low": 0.5, "avg": 1.5, "high": 3.0, "unit": "%"},
    "engagement_rate": {"low": 0.5, "avg": 1.8, "high": 5.0, "unit": "%"},
    "roas": {"low": 1.0, "avg": 3.0, "high": 8.0, "unit": "x"},
}


class ROIDB:
    """ROI数据库"""

    def __init__(self, db_path: str = "roi_calculator.db"):
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path)
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
        return self._local.conn

    def _init_db(self):
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS campaigns (
                campaign_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT DEFAULT '',
                start_date TEXT,
                end_date TEXT,
                budget REAL DEFAULT 0,
                currency TEXT DEFAULT 'USD',
                status TEXT DEFAULT 'active',
                metadata TEXT DEFAULT '{}',
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS costs (
                entry_id TEXT PRIMARY KEY,
                campaign_id TEXT NOT NULL,
                category TEXT NOT NULL,
                amount REAL NOT NULL,
                currency TEXT DEFAULT 'USD',
                description TEXT DEFAULT '',
                date TEXT,
                recurring INTEGER DEFAULT 0,
                recurring_interval_days INTEGER DEFAULT 0,
                FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id)
            );

            CREATE TABLE IF NOT EXISTS conversions (
                conversion_id TEXT PRIMARY KEY,
                campaign_id TEXT NOT NULL,
                conversion_type TEXT NOT NULL,
                value REAL DEFAULT 0,
                currency TEXT DEFAULT 'USD',
                source_tweet_id TEXT DEFAULT '',
                touchpoints TEXT DEFAULT '[]',
                occurred_at TEXT,
                metadata TEXT DEFAULT '{}',
                FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id)
            );

            CREATE TABLE IF NOT EXISTS engagement_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                campaign_id TEXT NOT NULL,
                tweet_id TEXT,
                impressions INTEGER DEFAULT 0,
                engagements INTEGER DEFAULT 0,
                clicks INTEGER DEFAULT 0,
                likes INTEGER DEFAULT 0,
                retweets INTEGER DEFAULT 0,
                replies INTEGER DEFAULT 0,
                recorded_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id)
            );

            CREATE TABLE IF NOT EXISTS revenue_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                revenue REAL NOT NULL,
                source TEXT DEFAULT 'total',
                currency TEXT DEFAULT 'USD',
                metadata TEXT DEFAULT '{}'
            );

            CREATE INDEX IF NOT EXISTS idx_costs_campaign
                ON costs(campaign_id);
            CREATE INDEX IF NOT EXISTS idx_conversions_campaign
                ON conversions(campaign_id);
            CREATE INDEX IF NOT EXISTS idx_engagement_campaign
                ON engagement_data(campaign_id);
            CREATE INDEX IF NOT EXISTS idx_revenue_date
                ON revenue_data(date);
        """)
        conn.commit()

    def create_campaign(self, campaign_id: str, name: str,
                        budget: float = 0, start_date: str = "",
                        end_date: str = "", **kwargs):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO campaigns
            (campaign_id, name, budget, start_date, end_date, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (campaign_id, name, budget, start_date, end_date,
              json.dumps(kwargs)))
        conn.commit()

    def add_cost(self, cost: CostEntry):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO costs
            (entry_id, campaign_id, category, amount, currency,
             description, date, recurring, recurring_interval_days)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            cost.entry_id, cost.campaign_id, cost.category.value,
            cost.amount, cost.currency, cost.description, cost.date,
            int(cost.recurring), cost.recurring_interval_days,
        ))
        conn.commit()

    def add_conversion(self, conversion: Conversion):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO conversions
            (conversion_id, campaign_id, conversion_type, value, currency,
             source_tweet_id, touchpoints, occurred_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            conversion.conversion_id, conversion.campaign_id,
            conversion.conversion_type.value, conversion.value,
            conversion.currency, conversion.source_tweet_id,
            json.dumps(conversion.touchpoints), conversion.occurred_at,
            json.dumps(conversion.metadata),
        ))
        conn.commit()

    def add_engagement(self, campaign_id: str, tweet_id: str = "",
                       impressions: int = 0, engagements: int = 0,
                       clicks: int = 0, likes: int = 0,
                       retweets: int = 0, replies: int = 0):
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO engagement_data
            (campaign_id, tweet_id, impressions, engagements, clicks,
             likes, retweets, replies)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (campaign_id, tweet_id, impressions, engagements, clicks,
              likes, retweets, replies))
        conn.commit()

    def add_revenue(self, date: str, revenue: float,
                    source: str = "total", currency: str = "USD"):
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO revenue_data (date, revenue, source, currency)
            VALUES (?, ?, ?, ?)
        """, (date, revenue, source, currency))
        conn.commit()

    def get_campaign_costs(self, campaign_id: str) -> float:
        conn = self._get_conn()
        row = conn.execute("""
            SELECT COALESCE(SUM(amount), 0) FROM costs
            WHERE campaign_id = ?
        """, (campaign_id,)).fetchone()
        return row[0]

    def get_cost_breakdown(self, campaign_id: str) -> Dict[str, float]:
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT category, SUM(amount) as total FROM costs
            WHERE campaign_id = ? GROUP BY category
        """, (campaign_id,)).fetchall()
        return {r["category"]: r["total"] for r in rows}

    def get_campaign_conversions(self, campaign_id: str) -> Tuple[int, float]:
        conn = self._get_conn()
        row = conn.execute("""
            SELECT COUNT(*), COALESCE(SUM(value), 0)
            FROM conversions WHERE campaign_id = ?
        """, (campaign_id,)).fetchone()
        return row[0], row[1]

    def get_campaign_engagement(self, campaign_id: str) -> Dict[str, int]:
        conn = self._get_conn()
        row = conn.execute("""
            SELECT
                COALESCE(SUM(impressions), 0) as impressions,
                COALESCE(SUM(engagements), 0) as engagements,
                COALESCE(SUM(clicks), 0) as clicks,
                COALESCE(SUM(likes), 0) as likes,
                COALESCE(SUM(retweets), 0) as retweets,
                COALESCE(SUM(replies), 0) as replies
            FROM engagement_data WHERE campaign_id = ?
        """, (campaign_id,)).fetchone()
        return dict(row)

    def get_all_campaigns(self) -> List[Dict]:
        conn = self._get_conn()
        rows = conn.execute("SELECT * FROM campaigns ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]

    def get_revenue_series(self, days: int = 30) -> List[Tuple[str, float]]:
        conn = self._get_conn()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = conn.execute("""
            SELECT date, SUM(revenue) as total FROM revenue_data
            WHERE date >= ? GROUP BY date ORDER BY date
        """, (cutoff,)).fetchall()
        return [(r["date"], r["total"]) for r in rows]

    def close(self):
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None


class AttributionEngine:
    """转化归因引擎"""

    @classmethod
    def attribute(cls, conversion: Conversion,
                  model: AttributionModel = AttributionModel.LAST_TOUCH
                  ) -> Dict[str, float]:
        """计算各触点的归因价值"""
        touchpoints = conversion.touchpoints
        if not touchpoints:
            if conversion.source_tweet_id:
                return {conversion.source_tweet_id: conversion.value}
            return {}

        total_value = conversion.value
        n = len(touchpoints)

        if model == AttributionModel.FIRST_TOUCH:
            return {touchpoints[0]: total_value}

        elif model == AttributionModel.LAST_TOUCH:
            return {touchpoints[-1]: total_value}

        elif model == AttributionModel.LINEAR:
            share = total_value / n
            return {tp: round(share, 4) for tp in touchpoints}

        elif model == AttributionModel.TIME_DECAY:
            # Exponential decay: more recent = more credit
            decay_factor = 0.7
            weights = [decay_factor ** (n - 1 - i) for i in range(n)]
            total_weight = sum(weights)
            return {
                tp: round(total_value * w / total_weight, 4)
                for tp, w in zip(touchpoints, weights)
            }

        elif model == AttributionModel.POSITION_BASED:
            # 40% first, 40% last, 20% split among middle
            result = {}
            if n == 1:
                result[touchpoints[0]] = total_value
            elif n == 2:
                result[touchpoints[0]] = total_value * 0.5
                result[touchpoints[1]] = total_value * 0.5
            else:
                result[touchpoints[0]] = total_value * 0.4
                result[touchpoints[-1]] = total_value * 0.4
                middle_share = total_value * 0.2 / (n - 2)
                for tp in touchpoints[1:-1]:
                    result[tp] = round(middle_share, 4)
            return result

        return {}

    @classmethod
    def compare_models(cls, conversion: Conversion) -> Dict[str, Dict[str, float]]:
        """对比所有归因模型"""
        return {
            model.value: cls.attribute(conversion, model)
            for model in AttributionModel
        }


class BenchmarkComparator:
    """基准对比"""

    @classmethod
    def compare(cls, metrics: CampaignMetrics) -> Dict[str, Dict[str, Any]]:
        """与行业基准对比"""
        metrics.compute()
        comparisons = {}

        metric_map = {
            "cpe": metrics.cpe,
            "cpc": metrics.cpc,
            "cpm": metrics.cpm,
            "ctr": metrics.ctr,
            "engagement_rate": metrics.engagement_rate,
            "roas": metrics.roas,
        }

        for name, value in metric_map.items():
            bench = BENCHMARKS.get(name)
            if not bench:
                continue

            if value == 0:
                rating = "no_data"
                percentile = 0
            elif name in ("cpe", "cpc", "cpm"):
                # Lower is better for cost metrics
                if value <= bench["low"]:
                    rating = "excellent"
                    percentile = 90
                elif value <= bench["avg"]:
                    rating = "good"
                    percentile = 60
                elif value <= bench["high"]:
                    rating = "average"
                    percentile = 30
                else:
                    rating = "poor"
                    percentile = 10
            else:
                # Higher is better for rate/return metrics
                if value >= bench["high"]:
                    rating = "excellent"
                    percentile = 90
                elif value >= bench["avg"]:
                    rating = "good"
                    percentile = 60
                elif value >= bench["low"]:
                    rating = "average"
                    percentile = 30
                else:
                    rating = "poor"
                    percentile = 10

            comparisons[name] = {
                "your_value": round(value, 4),
                "benchmark_avg": bench["avg"],
                "benchmark_low": bench["low"],
                "benchmark_high": bench["high"],
                "unit": bench["unit"],
                "rating": rating,
                "percentile": percentile,
            }

        return comparisons


class ROIForecaster:
    """ROI预测"""

    @classmethod
    def forecast(cls, historical_metrics: List[CampaignMetrics],
                 future_budget: float,
                 periods: int = 3) -> List[Dict[str, Any]]:
        """基于历史数据预测未来ROI"""
        if not historical_metrics:
            return []

        # Calculate average metrics
        avg_roi = sum(m.roi_percentage for m in historical_metrics) / len(historical_metrics)
        avg_cpe = sum(m.cpe for m in historical_metrics if m.cpe > 0) or [0]
        if isinstance(avg_cpe, list):
            avg_cpe = 0
        else:
            avg_cpe = avg_cpe / sum(1 for m in historical_metrics if m.cpe > 0)

        avg_roas = sum(m.roas for m in historical_metrics if m.roas > 0)
        count_roas = sum(1 for m in historical_metrics if m.roas > 0)
        avg_roas = avg_roas / count_roas if count_roas > 0 else 1.0

        # Trend direction
        if len(historical_metrics) >= 2:
            recent = historical_metrics[-1].roi_percentage
            previous = historical_metrics[-2].roi_percentage
            trend = (recent - previous) / abs(previous) if previous != 0 else 0
        else:
            trend = 0

        forecasts = []
        for i in range(1, periods + 1):
            projected_roi = avg_roi * (1 + trend * i * 0.5)  # Dampened trend
            projected_revenue = future_budget * (1 + projected_roi / 100)
            projected_roas = avg_roas * (1 + trend * i * 0.3)

            forecasts.append({
                "period": i,
                "budget": future_budget,
                "projected_roi": round(projected_roi, 2),
                "projected_revenue": round(projected_revenue, 2),
                "projected_roas": round(projected_roas, 2),
                "confidence": round(max(0.3, 1.0 - i * 0.15), 2),
                "trend_direction": "up" if trend > 0 else "down" if trend < 0 else "flat",
            })

        return forecasts


class RevenueCorrelator:
    """收入关联分析"""

    @classmethod
    def correlate(cls, engagement_series: List[Tuple[str, int]],
                  revenue_series: List[Tuple[str, float]],
                  lag_days: int = 0) -> Dict[str, Any]:
        """计算互动量与收入的相关性"""
        if not engagement_series or not revenue_series:
            return {"correlation": 0, "significance": "insufficient_data"}

        # Align series by date
        rev_by_date = {d: v for d, v in revenue_series}
        aligned_eng = []
        aligned_rev = []

        for date_str, eng_count in engagement_series:
            # Apply lag
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                lagged = (dt + timedelta(days=lag_days)).strftime("%Y-%m-%d")
            except ValueError:
                continue

            if lagged in rev_by_date:
                aligned_eng.append(eng_count)
                aligned_rev.append(rev_by_date[lagged])

        if len(aligned_eng) < 3:
            return {"correlation": 0, "significance": "insufficient_data", "n": len(aligned_eng)}

        # Pearson correlation
        correlation = cls._pearson(aligned_eng, aligned_rev)

        significance = "none"
        if abs(correlation) > 0.7:
            significance = "strong"
        elif abs(correlation) > 0.4:
            significance = "moderate"
        elif abs(correlation) > 0.2:
            significance = "weak"

        return {
            "correlation": round(correlation, 4),
            "significance": significance,
            "lag_days": lag_days,
            "data_points": len(aligned_eng),
            "direction": "positive" if correlation > 0 else "negative",
        }

    @staticmethod
    def _pearson(x: List[float], y: List[float]) -> float:
        """Pearson相关系数"""
        n = len(x)
        if n == 0:
            return 0.0
        mean_x = sum(x) / n
        mean_y = sum(y) / n
        num = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
        den_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
        den_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))
        if den_x == 0 or den_y == 0:
            return 0.0
        return num / (den_x * den_y)

    @classmethod
    def find_optimal_lag(cls, engagement_series: List[Tuple[str, int]],
                         revenue_series: List[Tuple[str, float]],
                         max_lag: int = 14) -> Dict[str, Any]:
        """找到最佳滞后天数"""
        best_lag = 0
        best_corr = 0

        results = []
        for lag in range(max_lag + 1):
            result = cls.correlate(engagement_series, revenue_series, lag)
            corr = abs(result.get("correlation", 0))
            results.append({"lag": lag, "correlation": result.get("correlation", 0)})
            if corr > best_corr:
                best_corr = corr
                best_lag = lag

        return {
            "optimal_lag_days": best_lag,
            "correlation_at_optimal": round(best_corr, 4),
            "all_lags": results,
        }


class ROIReportGenerator:
    """ROI报告生成器"""

    @classmethod
    def generate(cls, db: ROIDB, campaign_id: str,
                 attribution_model: AttributionModel = AttributionModel.LAST_TOUCH
                 ) -> Dict[str, Any]:
        """生成完整ROI报告"""
        # Gather data
        total_cost = db.get_campaign_costs(campaign_id)
        cost_breakdown = db.get_cost_breakdown(campaign_id)
        conv_count, conv_value = db.get_campaign_conversions(campaign_id)
        engagement = db.get_campaign_engagement(campaign_id)

        # Build metrics
        metrics = CampaignMetrics(
            campaign_id=campaign_id,
            impressions=engagement.get("impressions", 0),
            engagements=engagement.get("engagements", 0),
            clicks=engagement.get("clicks", 0),
            likes=engagement.get("likes", 0),
            retweets=engagement.get("retweets", 0),
            replies=engagement.get("replies", 0),
            total_cost=total_cost,
            cost_breakdown=cost_breakdown,
            conversions=conv_count,
            conversion_value=conv_value,
        )
        metrics.compute()

        # Benchmark comparison
        benchmarks = BenchmarkComparator.compare(metrics)

        report = {
            "campaign_id": campaign_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "metrics": metrics.to_dict(),
            "benchmarks": benchmarks,
            "attribution_model": attribution_model.value,
            "recommendations": cls._generate_recommendations(metrics, benchmarks),
        }

        return report

    @classmethod
    def generate_comparison(cls, db: ROIDB,
                            campaign_ids: List[str]) -> Dict[str, Any]:
        """多活动对比报告"""
        campaigns = []
        for cid in campaign_ids:
            total_cost = db.get_campaign_costs(cid)
            conv_count, conv_value = db.get_campaign_conversions(cid)
            engagement = db.get_campaign_engagement(cid)

            metrics = CampaignMetrics(
                campaign_id=cid,
                impressions=engagement.get("impressions", 0),
                engagements=engagement.get("engagements", 0),
                clicks=engagement.get("clicks", 0),
                total_cost=total_cost,
                conversions=conv_count,
                conversion_value=conv_value,
            )
            metrics.compute()
            campaigns.append(metrics)

        # Rank by ROI
        campaigns.sort(key=lambda m: m.roi_percentage, reverse=True)

        return {
            "campaigns": [m.to_dict() for m in campaigns],
            "best_roi": campaigns[0].campaign_id if campaigns else None,
            "best_cpe": min(campaigns, key=lambda m: m.cpe if m.cpe > 0 else float('inf')).campaign_id if campaigns else None,
            "highest_engagement_rate": max(campaigns, key=lambda m: m.engagement_rate).campaign_id if campaigns else None,
            "total_spend": sum(m.total_cost for m in campaigns),
            "total_revenue": sum(m.conversion_value for m in campaigns),
            "overall_roi": round(
                (sum(m.conversion_value for m in campaigns) - sum(m.total_cost for m in campaigns))
                / sum(m.total_cost for m in campaigns) * 100, 2
            ) if sum(m.total_cost for m in campaigns) > 0 else 0,
        }

    @classmethod
    def generate_text(cls, report: Dict[str, Any]) -> str:
        """生成文本格式报告"""
        lines = []
        lines.append(f"💰 ROI Report — Campaign: {report.get('campaign_id', 'N/A')}")
        lines.append(f"Generated: {report.get('generated_at', 'N/A')}")
        lines.append("")

        metrics = report.get("metrics", {})
        lines.append("📊 Performance Metrics:")
        lines.append(f"  Impressions: {metrics.get('impressions', 0):,}")
        lines.append(f"  Engagements: {metrics.get('engagements', 0):,}")
        lines.append(f"  Clicks: {metrics.get('clicks', 0):,}")
        lines.append(f"  Conversions: {metrics.get('conversions', 0):,}")
        lines.append("")

        lines.append("💵 Financial:")
        lines.append(f"  Total Cost: ${metrics.get('total_cost', 0):,.2f}")
        lines.append(f"  Revenue: ${metrics.get('conversion_value', 0):,.2f}")
        lines.append(f"  ROI: {metrics.get('roi_percentage', 0):.1f}% ({metrics.get('roi_rating', 'N/A')})")
        lines.append(f"  ROAS: {metrics.get('roas', 0):.2f}x")
        lines.append("")

        lines.append("📈 Efficiency:")
        lines.append(f"  CPE: ${metrics.get('cpe', 0):.4f}")
        lines.append(f"  CPC: ${metrics.get('cpc', 0):.4f}")
        lines.append(f"  CPM: ${metrics.get('cpm', 0):.4f}")
        lines.append(f"  CTR: {metrics.get('ctr', 0):.2f}%")
        lines.append(f"  Engagement Rate: {metrics.get('engagement_rate', 0):.2f}%")
        lines.append("")

        benchmarks = report.get("benchmarks", {})
        if benchmarks:
            lines.append("🏆 vs Industry Benchmark:")
            for name, data in benchmarks.items():
                lines.append(f"  {name}: {data['your_value']} (avg: {data['benchmark_avg']}) — {data['rating']}")
            lines.append("")

        recs = report.get("recommendations", [])
        if recs:
            lines.append("💡 Recommendations:")
            for r in recs:
                lines.append(f"  • {r}")

        return "\n".join(lines)

    @classmethod
    def export_json(cls, report: Dict[str, Any], filepath: str):
        """导出JSON报告"""
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

    @classmethod
    def export_csv(cls, campaigns: List[CampaignMetrics], filepath: str):
        """导出CSV"""
        import csv
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "campaign_id", "impressions", "engagements", "clicks",
                "conversions", "total_cost", "conversion_value",
                "cpe", "cpc", "cpm", "ctr", "engagement_rate",
                "roas", "roi_percentage", "roi_rating",
            ])
            for m in campaigns:
                m.compute()
                writer.writerow([
                    m.campaign_id, m.impressions, m.engagements, m.clicks,
                    m.conversions, round(m.total_cost, 2), round(m.conversion_value, 2),
                    round(m.cpe, 4), round(m.cpc, 4), round(m.cpm, 4),
                    round(m.ctr, 2), round(m.engagement_rate, 2),
                    round(m.roas, 2), round(m.roi_percentage, 2), m.roi_rating.value,
                ])

    @classmethod
    def _generate_recommendations(cls, metrics: CampaignMetrics,
                                   benchmarks: Dict) -> List[str]:
        """生成优化建议"""
        recs = []

        # ROI check
        if metrics.roi_percentage < 0:
            recs.append("Campaign is ROI-negative — review targeting and creative assets")
        elif metrics.roi_percentage < 50:
            recs.append("Low ROI — consider A/B testing ad creative and reducing underperforming segments")

        # CPE check
        cpe_bench = benchmarks.get("cpe", {})
        if cpe_bench.get("rating") == "poor":
            recs.append(f"CPE (${metrics.cpe:.2f}) is above industry average — optimize content for engagement")

        # CTR check
        ctr_bench = benchmarks.get("ctr", {})
        if ctr_bench.get("rating") == "poor":
            recs.append(f"CTR ({metrics.ctr:.2f}%) is below average — improve headlines and CTAs")
        elif ctr_bench.get("rating") == "excellent":
            recs.append("CTR is excellent — consider scaling budget for this campaign")

        # Engagement rate
        eng_bench = benchmarks.get("engagement_rate", {})
        if eng_bench.get("rating") == "excellent":
            recs.append("High engagement rate — leverage this for organic growth")

        # ROAS
        if metrics.roas > 5:
            recs.append(f"ROAS of {metrics.roas:.1f}x is outstanding — prioritize this campaign type")
        elif metrics.roas < 1:
            recs.append("ROAS below 1x — spending more than earning, needs immediate optimization")

        # Cost distribution
        if metrics.cost_breakdown:
            max_cat = max(metrics.cost_breakdown, key=metrics.cost_breakdown.get)
            max_pct = metrics.cost_breakdown[max_cat] / metrics.total_cost * 100 if metrics.total_cost > 0 else 0
            if max_pct > 70:
                recs.append(f"{max_cat} accounts for {max_pct:.0f}% of spend — diversify cost allocation")

        if not recs:
            recs.append("Campaign metrics are within healthy ranges — continue current strategy")

        return recs


class ROICalculator:
    """统一ROI计算器"""

    def __init__(self, db_path: str = "roi_calculator.db"):
        self.db = ROIDB(db_path)

    def create_campaign(self, campaign_id: str, name: str,
                        budget: float = 0, **kwargs):
        """创建活动"""
        self.db.create_campaign(campaign_id, name, budget, **kwargs)

    def add_cost(self, campaign_id: str, category: CostCategory,
                 amount: float, **kwargs) -> CostEntry:
        """添加费用"""
        entry_id = f"cost-{campaign_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        cost = CostEntry(
            entry_id=entry_id,
            campaign_id=campaign_id,
            category=category,
            amount=amount,
            **kwargs,
        )
        self.db.add_cost(cost)
        return cost

    def track_conversion(self, campaign_id: str,
                         conversion_type: ConversionType,
                         value: float = 0, **kwargs) -> Conversion:
        """记录转化"""
        conv_id = f"conv-{campaign_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        conversion = Conversion(
            conversion_id=conv_id,
            campaign_id=campaign_id,
            conversion_type=conversion_type,
            value=value,
            **kwargs,
        )
        self.db.add_conversion(conversion)
        return conversion

    def record_engagement(self, campaign_id: str, **kwargs):
        """记录互动数据"""
        self.db.add_engagement(campaign_id, **kwargs)

    def record_revenue(self, date: str, revenue: float, **kwargs):
        """记录收入"""
        self.db.add_revenue(date, revenue, **kwargs)

    def get_roi(self, campaign_id: str) -> Dict[str, Any]:
        """获取ROI"""
        return ROIReportGenerator.generate(self.db, campaign_id)

    def compare_campaigns(self, campaign_ids: List[str]) -> Dict[str, Any]:
        """对比活动"""
        return ROIReportGenerator.generate_comparison(self.db, campaign_ids)

    def get_text_report(self, campaign_id: str) -> str:
        """获取文本报告"""
        report = self.get_roi(campaign_id)
        return ROIReportGenerator.generate_text(report)

    def close(self):
        self.db.close()
