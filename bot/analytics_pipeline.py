"""
Analytics Pipeline v1.0
实时推文分析管道 — Engagement曲线 + 最佳发帖时间 + 趋势检测 + 表现预测

Features:
- TweetMetrics ingestion and storage
- EngagementCurve: track engagement over time after posting
- PostingTimeAnalyzer: optimal posting times by hour/day
- TrendDetector: moving average + anomaly detection
- Period comparison and report export
"""

import json
import math
import sqlite3
import threading
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple


class TrendDirection(Enum):
    RISING = "rising"
    FALLING = "falling"
    STABLE = "stable"


@dataclass
class TweetMetrics:
    """推文指标"""
    tweet_id: str
    impressions: int = 0
    engagements: int = 0
    likes: int = 0
    retweets: int = 0
    replies: int = 0
    quotes: int = 0
    bookmarks: int = 0
    timestamp: str = ""
    author_id: str = ""
    text_preview: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()

    @property
    def engagement_rate(self) -> float:
        if self.impressions == 0:
            return 0.0
        return self.engagements / self.impressions


@dataclass
class TimeSlot:
    """时间段统计"""
    hour: int
    day_of_week: int  # 0=Monday
    avg_engagement_rate: float
    avg_impressions: float
    sample_count: int


@dataclass
class TrendResult:
    """趋势检测结果"""
    direction: TrendDirection
    slope: float
    confidence: float
    anomalies: List[Dict[str, Any]] = field(default_factory=list)


class EngagementCurve:
    """Engagement曲线追踪器"""

    def __init__(self):
        self._curves: Dict[str, List[Tuple[float, int]]] = {}  # tweet_id → [(hours_after, engagements)]

    def add_point(self, tweet_id: str, hours_after_post: float, total_engagements: int):
        """添加数据点"""
        if tweet_id not in self._curves:
            self._curves[tweet_id] = []
        self._curves[tweet_id].append((hours_after_post, total_engagements))
        self._curves[tweet_id].sort(key=lambda x: x[0])

    def get_curve(self, tweet_id: str) -> List[Tuple[float, int]]:
        """获取engagement曲线"""
        return self._curves.get(tweet_id, [])

    def get_peak_hour(self, tweet_id: str) -> Optional[float]:
        """获取engagement峰值时间"""
        curve = self.get_curve(tweet_id)
        if len(curve) < 2:
            return None
        # 找最大增长率的时间点
        max_rate = 0
        peak_hour = 0
        for i in range(1, len(curve)):
            dt = curve[i][0] - curve[i - 1][0]
            if dt > 0:
                rate = (curve[i][1] - curve[i - 1][1]) / dt
                if rate > max_rate:
                    max_rate = rate
                    peak_hour = curve[i][0]
        return peak_hour

    def get_decay_rate(self, tweet_id: str) -> Optional[float]:
        """计算engagement衰减率"""
        curve = self.get_curve(tweet_id)
        if len(curve) < 3:
            return None
        # 用后半段计算衰减
        mid = len(curve) // 2
        late = curve[mid:]
        if len(late) < 2:
            return None
        rates = []
        for i in range(1, len(late)):
            dt = late[i][0] - late[i - 1][0]
            if dt > 0 and late[i - 1][1] > 0:
                rate = (late[i][1] - late[i - 1][1]) / (late[i - 1][1] * dt)
                rates.append(rate)
        return sum(rates) / len(rates) if rates else 0.0


class PostingTimeAnalyzer:
    """最佳发帖时间分析器"""

    def __init__(self):
        self._data: List[Dict[str, Any]] = []

    def add_data(self, hour: int, day_of_week: int, engagement_rate: float, impressions: int):
        """添加数据"""
        self._data.append({
            "hour": hour,
            "dow": day_of_week,
            "er": engagement_rate,
            "imp": impressions,
        })

    def get_best_times(self, top_n: int = 5) -> List[TimeSlot]:
        """获取最佳发帖时间"""
        slots: Dict[Tuple[int, int], List[Dict]] = defaultdict(list)
        for d in self._data:
            slots[(d["hour"], d["dow"])].append(d)

        results = []
        for (hour, dow), entries in slots.items():
            avg_er = sum(e["er"] for e in entries) / len(entries)
            avg_imp = sum(e["imp"] for e in entries) / len(entries)
            results.append(TimeSlot(
                hour=hour,
                day_of_week=dow,
                avg_engagement_rate=avg_er,
                avg_impressions=avg_imp,
                sample_count=len(entries),
            ))

        # 按engagement rate排序，要求至少3个样本
        results = [r for r in results if r.sample_count >= 2]
        results.sort(key=lambda x: -x.avg_engagement_rate)
        return results[:top_n]

    def get_hourly_heatmap(self) -> Dict[int, float]:
        """按小时的engagement热图"""
        hourly: Dict[int, List[float]] = defaultdict(list)
        for d in self._data:
            hourly[d["hour"]].append(d["er"])
        return {h: sum(v) / len(v) for h, v in hourly.items()}


class TrendDetector:
    """趋势检测器"""

    def detect(self, values: List[float], window: int = 7) -> TrendResult:
        """检测趋势"""
        if len(values) < 3:
            return TrendResult(direction=TrendDirection.STABLE, slope=0.0, confidence=0.0)

        # 移动平均
        ma = self._moving_average(values, min(window, len(values)))

        # 线性回归计算斜率
        n = len(ma)
        x_mean = (n - 1) / 2
        y_mean = sum(ma) / n
        numerator = sum((i - x_mean) * (ma[i] - y_mean) for i in range(n))
        denominator = sum((i - x_mean) ** 2 for i in range(n))
        slope = numerator / denominator if denominator else 0.0

        # 计算R²作为置信度
        ss_res = sum((ma[i] - (slope * i + (y_mean - slope * x_mean))) ** 2 for i in range(n))
        ss_tot = sum((ma[i] - y_mean) ** 2 for i in range(n))
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

        # 方向判定
        threshold = y_mean * 0.01 if y_mean else 0.01
        if slope > threshold:
            direction = TrendDirection.RISING
        elif slope < -threshold:
            direction = TrendDirection.FALLING
        else:
            direction = TrendDirection.STABLE

        # 异常检测 (超过2个标准差)
        anomalies = self._detect_anomalies(values)

        return TrendResult(
            direction=direction,
            slope=round(slope, 6),
            confidence=round(max(0, r_squared), 4),
            anomalies=anomalies,
        )

    def _moving_average(self, values: List[float], window: int) -> List[float]:
        """计算移动平均"""
        if window <= 0 or not values:
            return values
        result = []
        for i in range(len(values)):
            start = max(0, i - window + 1)
            window_vals = values[start:i + 1]
            result.append(sum(window_vals) / len(window_vals))
        return result

    def _detect_anomalies(self, values: List[float]) -> List[Dict[str, Any]]:
        """检测异常值"""
        if len(values) < 5:
            return []
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        std = math.sqrt(variance) if variance > 0 else 0

        if std == 0:
            return []

        anomalies = []
        for i, v in enumerate(values):
            z = abs(v - mean) / std
            if z > 2.0:
                anomalies.append({
                    "index": i,
                    "value": v,
                    "z_score": round(z, 2),
                    "direction": "above" if v > mean else "below",
                })
        return anomalies


class AnalyticsPipeline:
    """推文分析管道"""

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._curve = EngagementCurve()
        self._time_analyzer = PostingTimeAnalyzer()
        self._trend_detector = TrendDetector()
        self._init_db()

    def _init_db(self):
        with self._lock:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS tweet_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tweet_id TEXT,
                    impressions INTEGER DEFAULT 0,
                    engagements INTEGER DEFAULT 0,
                    likes INTEGER DEFAULT 0,
                    retweets INTEGER DEFAULT 0,
                    replies INTEGER DEFAULT 0,
                    quotes INTEGER DEFAULT 0,
                    bookmarks INTEGER DEFAULT 0,
                    author_id TEXT DEFAULT '',
                    text_preview TEXT DEFAULT '',
                    timestamp TEXT,
                    ingested_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_metrics_tweet ON tweet_metrics(tweet_id);
                CREATE INDEX IF NOT EXISTS idx_metrics_ts ON tweet_metrics(timestamp);
            """)
            self._conn.commit()

    def ingest(self, metrics: TweetMetrics):
        """摄入推文指标"""
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self._conn.execute(
                """INSERT INTO tweet_metrics
                   (tweet_id, impressions, engagements, likes, retweets, replies, quotes, bookmarks,
                    author_id, text_preview, timestamp, ingested_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (metrics.tweet_id, metrics.impressions, metrics.engagements,
                 metrics.likes, metrics.retweets, metrics.replies, metrics.quotes,
                 metrics.bookmarks, metrics.author_id, metrics.text_preview,
                 metrics.timestamp, now)
            )
            self._conn.commit()

        # 更新时间分析器
        try:
            ts = datetime.fromisoformat(metrics.timestamp)
            self._time_analyzer.add_data(
                hour=ts.hour,
                day_of_week=ts.weekday(),
                engagement_rate=metrics.engagement_rate,
                impressions=metrics.impressions,
            )
        except (ValueError, AttributeError):
            pass

    def get_best_posting_times(self, top_n: int = 5) -> List[Dict[str, Any]]:
        """获取最佳发帖时段"""
        slots = self._time_analyzer.get_best_times(top_n)
        days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        return [{
            "hour": s.hour,
            "day": days[s.day_of_week] if 0 <= s.day_of_week < 7 else "?",
            "avg_engagement_rate": round(s.avg_engagement_rate, 4),
            "avg_impressions": round(s.avg_impressions, 1),
            "samples": s.sample_count,
        } for s in slots]

    def get_engagement_curve(self, tweet_id: str) -> List[Tuple[float, int]]:
        """获取推文engagement曲线"""
        return self._curve.get_curve(tweet_id)

    def add_curve_point(self, tweet_id: str, hours_after: float, engagements: int):
        """添加曲线数据点"""
        self._curve.add_point(tweet_id, hours_after, engagements)

    def detect_trends(self, window_days: int = 30) -> Dict[str, TrendResult]:
        """检测趋势"""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
        rows = self._conn.execute(
            "SELECT * FROM tweet_metrics WHERE timestamp > ? ORDER BY timestamp", (cutoff,)
        ).fetchall()

        if not rows:
            return {}

        # 按日聚合
        daily: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
        for r in rows:
            try:
                day = r["timestamp"][:10]
            except (TypeError, IndexError):
                continue
            daily[day]["engagement_rate"].append(
                r["engagements"] / r["impressions"] if r["impressions"] > 0 else 0
            )
            daily[day]["impressions"].append(r["impressions"])
            daily[day]["likes"].append(r["likes"])

        sorted_days = sorted(daily.keys())

        trends = {}
        for metric in ["engagement_rate", "impressions", "likes"]:
            values = [
                sum(daily[d][metric]) / len(daily[d][metric]) if daily[d][metric] else 0
                for d in sorted_days
            ]
            if values:
                trends[metric] = self._trend_detector.detect(values)

        return trends

    def predict_performance(self, text_length: int, posting_hour: int, posting_dow: int) -> Dict[str, float]:
        """简单预测表现(基于历史数据)"""
        rows = self._conn.execute(
            "SELECT impressions, engagements, likes, retweets FROM tweet_metrics"
        ).fetchall()

        if not rows:
            return {"predicted_impressions": 0, "predicted_engagement_rate": 0, "confidence": 0}

        avg_imp = sum(r["impressions"] for r in rows) / len(rows)
        avg_er = sum(
            r["engagements"] / r["impressions"] if r["impressions"] > 0 else 0
            for r in rows
        ) / len(rows)

        # 时间调整
        heatmap = self._time_analyzer.get_hourly_heatmap()
        hour_factor = 1.0
        if heatmap:
            overall_avg = sum(heatmap.values()) / len(heatmap) if heatmap else avg_er
            if overall_avg > 0 and posting_hour in heatmap:
                hour_factor = heatmap[posting_hour] / overall_avg

        return {
            "predicted_impressions": round(avg_imp * hour_factor, 1),
            "predicted_engagement_rate": round(avg_er * hour_factor, 4),
            "confidence": min(len(rows) / 100, 1.0),
            "hour_factor": round(hour_factor, 3),
        }

    def compare_periods(
        self, start1: str, end1: str, start2: str, end2: str
    ) -> Dict[str, Any]:
        """周期对比"""
        def get_period_stats(start: str, end: str) -> Dict[str, float]:
            rows = self._conn.execute(
                "SELECT * FROM tweet_metrics WHERE timestamp BETWEEN ? AND ?",
                (start, end)
            ).fetchall()
            if not rows:
                return {"count": 0, "avg_impressions": 0, "avg_er": 0, "total_likes": 0}
            return {
                "count": len(rows),
                "avg_impressions": sum(r["impressions"] for r in rows) / len(rows),
                "avg_er": sum(
                    r["engagements"] / r["impressions"] if r["impressions"] > 0 else 0
                    for r in rows
                ) / len(rows),
                "total_likes": sum(r["likes"] for r in rows),
            }

        p1 = get_period_stats(start1, end1)
        p2 = get_period_stats(start2, end2)

        def pct_change(a: float, b: float) -> float:
            if b == 0:
                return 0.0
            return round((a - b) / b * 100, 2)

        return {
            "period1": {"range": f"{start1} → {end1}", **p1},
            "period2": {"range": f"{start2} → {end2}", **p2},
            "changes": {
                "impressions_pct": pct_change(p1["avg_impressions"], p2["avg_impressions"]),
                "engagement_rate_pct": pct_change(p1["avg_er"], p2["avg_er"]),
                "likes_pct": pct_change(p1["total_likes"], p2["total_likes"]),
            }
        }

    def export_report(self, format: str = "json", days: int = 30) -> str:
        """导出报告"""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = self._conn.execute(
            "SELECT * FROM tweet_metrics WHERE timestamp > ? ORDER BY timestamp", (cutoff,)
        ).fetchall()

        total = len(rows)
        if total == 0:
            data = {"total_tweets": 0, "period_days": days}
        else:
            data = {
                "total_tweets": total,
                "period_days": days,
                "avg_impressions": round(sum(r["impressions"] for r in rows) / total, 1),
                "avg_engagement_rate": round(sum(
                    r["engagements"] / r["impressions"] if r["impressions"] > 0 else 0
                    for r in rows) / total, 4),
                "total_likes": sum(r["likes"] for r in rows),
                "total_retweets": sum(r["retweets"] for r in rows),
                "best_times": self.get_best_posting_times(3),
            }

        if format == "json":
            return json.dumps(data, indent=2)
        else:
            lines = [f"═══ Analytics Report ({days}d) ═══"]
            for k, v in data.items():
                if k != "best_times":
                    lines.append(f"  {k}: {v}")
            return "\n".join(lines)

    def get_total_metrics(self) -> int:
        """获取总指标数"""
        return self._conn.execute("SELECT COUNT(*) as c FROM tweet_metrics").fetchone()["c"]

    def close(self):
        self._conn.close()
