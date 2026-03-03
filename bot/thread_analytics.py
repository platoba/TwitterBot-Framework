"""
Thread Analytics Engine
线程深度分析: 完读率追踪 + 互动衰减分析 + 最优长度建议 + 格式对比 + 位置级指标
"""

import json
import statistics
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any


class ThreadFormat(str, Enum):
    NUMBERED = "numbered"        # 1/ 2/ 3/ ...
    NARRATIVE = "narrative"      # 连续故事
    QA = "qa"                    # 问答式
    LISTICLE = "listicle"        # 列表式要点
    TUTORIAL = "tutorial"        # 教程步骤
    DEBATE = "debate"            # 正反论证
    UNKNOWN = "unknown"


class EngagementTrend(str, Enum):
    RISING = "rising"
    STABLE = "stable"
    DECLINING = "declining"
    CLIFF = "cliff"              # 断崖式下跌
    RESURGENT = "resurgent"      # 先降后升


@dataclass
class TweetMetrics:
    """单条推文的指标"""
    position: int                # 在线程中的位置 (1-based)
    tweet_id: str = ""
    text: str = ""
    impressions: int = 0
    likes: int = 0
    retweets: int = 0
    replies: int = 0
    quotes: int = 0
    bookmarks: int = 0
    clicks: int = 0
    posted_at: Optional[str] = None

    @property
    def engagement_total(self) -> int:
        return self.likes + self.retweets + self.replies + self.quotes + self.bookmarks

    @property
    def engagement_rate(self) -> float:
        if self.impressions == 0:
            return 0.0
        return self.engagement_total / self.impressions

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["engagement_total"] = self.engagement_total
        d["engagement_rate"] = round(self.engagement_rate, 6)
        return d


@dataclass
class ThreadRecord:
    """线程完整记录"""
    thread_id: str
    author: str = ""
    title: str = ""
    format: ThreadFormat = ThreadFormat.UNKNOWN
    tweets: List[TweetMetrics] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    created_at: Optional[str] = None
    topic: str = ""

    @property
    def length(self) -> int:
        return len(self.tweets)

    @property
    def total_impressions(self) -> int:
        return sum(t.impressions for t in self.tweets)

    @property
    def total_engagement(self) -> int:
        return sum(t.engagement_total for t in self.tweets)

    @property
    def avg_engagement_rate(self) -> float:
        rates = [t.engagement_rate for t in self.tweets if t.impressions > 0]
        return statistics.mean(rates) if rates else 0.0


class ThreadAnalytics:
    """线程分析引擎"""

    def __init__(self, db=None):
        self.db = db
        self._threads: Dict[str, ThreadRecord] = {}

    # ─── CRUD ──────────────────────────────────────────────────

    def add_thread(self, thread: ThreadRecord) -> None:
        """添加线程记录"""
        if not thread.thread_id:
            raise ValueError("thread_id is required")
        if thread.format == ThreadFormat.UNKNOWN and thread.tweets:
            thread.format = self._detect_format(thread.tweets)
        self._threads[thread.thread_id] = thread

    def get_thread(self, thread_id: str) -> Optional[ThreadRecord]:
        return self._threads.get(thread_id)

    def list_threads(
        self,
        author: Optional[str] = None,
        fmt: Optional[ThreadFormat] = None,
        min_length: int = 0,
        max_length: int = 999,
        topic: Optional[str] = None,
    ) -> List[ThreadRecord]:
        """过滤线程列表"""
        result = []
        for t in self._threads.values():
            if author and t.author != author:
                continue
            if fmt and t.format != fmt:
                continue
            if t.length < min_length or t.length > max_length:
                continue
            if topic and topic.lower() not in t.topic.lower():
                continue
            result.append(t)
        return result

    def remove_thread(self, thread_id: str) -> bool:
        return self._threads.pop(thread_id, None) is not None

    # ─── 完读率 ──────────────────────────────────────────────

    def completion_rate(self, thread_id: str) -> Dict[str, Any]:
        """
        计算线程完读率
        基于impression衰减: 最后一条impression / 第一条impression
        """
        thread = self._threads.get(thread_id)
        if not thread or not thread.tweets:
            return {"thread_id": thread_id, "completion_rate": 0.0, "error": "no data"}

        first_imp = thread.tweets[0].impressions
        if first_imp == 0:
            return {"thread_id": thread_id, "completion_rate": 0.0, "error": "no impressions"}

        last_imp = thread.tweets[-1].impressions
        rate = last_imp / first_imp

        # 逐位置完读率
        position_rates = []
        for t in thread.tweets:
            position_rates.append({
                "position": t.position,
                "impressions": t.impressions,
                "retention": round(t.impressions / first_imp, 4) if first_imp else 0,
            })

        # 最大流失点
        max_drop_pos = 0
        max_drop_val = 0
        for i in range(1, len(thread.tweets)):
            drop = thread.tweets[i - 1].impressions - thread.tweets[i].impressions
            if drop > max_drop_val:
                max_drop_val = drop
                max_drop_pos = i + 1  # 1-based

        return {
            "thread_id": thread_id,
            "length": thread.length,
            "completion_rate": round(rate, 4),
            "completion_pct": f"{rate * 100:.1f}%",
            "first_impressions": first_imp,
            "last_impressions": last_imp,
            "max_drop_position": max_drop_pos,
            "max_drop_amount": max_drop_val,
            "position_retention": position_rates,
        }

    def batch_completion_rates(self) -> List[Dict[str, Any]]:
        """所有线程完读率排名"""
        results = []
        for tid in self._threads:
            cr = self.completion_rate(tid)
            if "error" not in cr:
                results.append(cr)
        results.sort(key=lambda x: x["completion_rate"], reverse=True)
        return results

    # ─── 互动衰减分析 ──────────────────────────────────────

    def engagement_decay(self, thread_id: str) -> Dict[str, Any]:
        """
        分析互动在线程中如何衰减
        计算每个位置的engagement变化率 + 趋势分类
        """
        thread = self._threads.get(thread_id)
        if not thread or len(thread.tweets) < 2:
            return {"thread_id": thread_id, "trend": "insufficient_data"}

        engagements = [t.engagement_total for t in thread.tweets]
        rates = [t.engagement_rate for t in thread.tweets]

        # 逐位置变化率
        decay_points = []
        for i in range(1, len(engagements)):
            prev = engagements[i - 1]
            curr = engagements[i]
            if prev > 0:
                change = (curr - prev) / prev
            else:
                change = 0.0 if curr == 0 else 1.0
            decay_points.append({
                "from_pos": i,
                "to_pos": i + 1,
                "prev_engagement": prev,
                "curr_engagement": curr,
                "change_rate": round(change, 4),
            })

        # 趋势判定
        trend = self._classify_trend(engagements)

        # 半衰期: engagement降到首条50%的位置
        half_life = None
        if engagements[0] > 0:
            threshold = engagements[0] * 0.5
            for i, e in enumerate(engagements):
                if e <= threshold:
                    half_life = i + 1
                    break

        # 回弹点: 衰减后重新上升
        bounce_back = None
        if len(engagements) >= 3:
            for i in range(2, len(engagements)):
                if engagements[i] > engagements[i - 1] and engagements[i - 1] < engagements[i - 2]:
                    bounce_back = i + 1
                    break

        return {
            "thread_id": thread_id,
            "trend": trend.value,
            "half_life_position": half_life,
            "bounce_back_position": bounce_back,
            "avg_decay_rate": round(
                statistics.mean([d["change_rate"] for d in decay_points]), 4
            ),
            "decay_points": decay_points,
            "engagement_sequence": engagements,
        }

    # ─── 最优长度建议 ──────────────────────────────────────

    def optimal_length(
        self,
        author: Optional[str] = None,
        min_threads: int = 3,
    ) -> Dict[str, Any]:
        """
        基于历史数据推荐最优线程长度
        综合完读率 + 总互动量 + engagement rate
        """
        threads = self.list_threads(author=author)
        if len(threads) < min_threads:
            return {"recommendation": None, "reason": f"need at least {min_threads} threads"}

        # 按长度分桶
        buckets: Dict[str, List[ThreadRecord]] = {
            "short(2-4)": [],
            "medium(5-8)": [],
            "long(9-15)": [],
            "epic(16+)": [],
        }

        for t in threads:
            if t.length <= 4:
                buckets["short(2-4)"].append(t)
            elif t.length <= 8:
                buckets["medium(5-8)"].append(t)
            elif t.length <= 15:
                buckets["long(9-15)"].append(t)
            else:
                buckets["epic(16+)"].append(t)

        scores = {}
        for bucket_name, bucket_threads in buckets.items():
            if not bucket_threads:
                continue

            avg_eng_rate = statistics.mean([t.avg_engagement_rate for t in bucket_threads])
            avg_total_eng = statistics.mean([t.total_engagement for t in bucket_threads])

            # 完读率
            completion_rates = []
            for t in bucket_threads:
                cr = self.completion_rate(t.thread_id)
                if "error" not in cr:
                    completion_rates.append(cr["completion_rate"])

            avg_completion = statistics.mean(completion_rates) if completion_rates else 0

            # 综合得分: 40%完读 + 35%engagement rate + 25%总互动
            # 归一化: 用相对值
            scores[bucket_name] = {
                "count": len(bucket_threads),
                "avg_completion": round(avg_completion, 4),
                "avg_engagement_rate": round(avg_eng_rate, 6),
                "avg_total_engagement": round(avg_total_eng, 1),
                "composite_score": 0,  # filled below
            }

        if not scores:
            return {"recommendation": None, "reason": "no data in any bucket"}

        # 归一化计算composite
        max_cr = max(s["avg_completion"] for s in scores.values()) or 1
        max_er = max(s["avg_engagement_rate"] for s in scores.values()) or 1
        max_te = max(s["avg_total_engagement"] for s in scores.values()) or 1

        best_bucket = None
        best_score = -1

        for name, s in scores.items():
            norm_cr = s["avg_completion"] / max_cr
            norm_er = s["avg_engagement_rate"] / max_er
            norm_te = s["avg_total_engagement"] / max_te
            composite = 0.4 * norm_cr + 0.35 * norm_er + 0.25 * norm_te
            s["composite_score"] = round(composite, 4)
            if composite > best_score:
                best_score = composite
                best_bucket = name

        return {
            "recommendation": best_bucket,
            "best_score": round(best_score, 4),
            "buckets": scores,
            "total_threads_analyzed": len(threads),
        }

    # ─── 格式对比 ──────────────────────────────────────────

    def format_comparison(self) -> Dict[str, Any]:
        """对比不同线程格式的表现"""
        by_format: Dict[str, List[ThreadRecord]] = {}
        for t in self._threads.values():
            fmt = t.format.value
            by_format.setdefault(fmt, []).append(t)

        comparisons = {}
        for fmt, threads in by_format.items():
            eng_rates = [t.avg_engagement_rate for t in threads]
            total_engs = [t.total_engagement for t in threads]
            lengths = [t.length for t in threads]

            completion_rates = []
            for t in threads:
                cr = self.completion_rate(t.thread_id)
                if "error" not in cr:
                    completion_rates.append(cr["completion_rate"])

            comparisons[fmt] = {
                "count": len(threads),
                "avg_length": round(statistics.mean(lengths), 1),
                "avg_engagement_rate": round(statistics.mean(eng_rates), 6) if eng_rates else 0,
                "median_engagement_rate": round(statistics.median(eng_rates), 6) if eng_rates else 0,
                "avg_total_engagement": round(statistics.mean(total_engs), 1),
                "avg_completion_rate": round(statistics.mean(completion_rates), 4) if completion_rates else 0,
                "best_thread": max(threads, key=lambda x: x.total_engagement).thread_id if threads else None,
            }

        # 排名
        ranked = sorted(comparisons.items(), key=lambda x: x[1]["avg_engagement_rate"], reverse=True)

        return {
            "formats": comparisons,
            "ranking": [{"format": f, "avg_engagement_rate": d["avg_engagement_rate"]} for f, d in ranked],
            "best_format": ranked[0][0] if ranked else None,
        }

    # ─── 位置级热力图 ──────────────────────────────────────

    def position_heatmap(self, thread_id: str) -> Dict[str, Any]:
        """生成线程位置级互动热力图"""
        thread = self._threads.get(thread_id)
        if not thread or not thread.tweets:
            return {"thread_id": thread_id, "error": "no data"}

        max_eng = max(t.engagement_total for t in thread.tweets) or 1

        heatmap = []
        for t in thread.tweets:
            intensity = t.engagement_total / max_eng  # 0~1
            level = (
                "🔥🔥🔥" if intensity > 0.8
                else "🔥🔥" if intensity > 0.5
                else "🔥" if intensity > 0.2
                else "❄️"
            )
            heatmap.append({
                "position": t.position,
                "engagement": t.engagement_total,
                "intensity": round(intensity, 3),
                "level": level,
                "top_metric": self._top_metric(t),
            })

        # 找热点位置 (互动最高的)
        hotspots = sorted(heatmap, key=lambda x: x["engagement"], reverse=True)[:3]

        return {
            "thread_id": thread_id,
            "length": thread.length,
            "heatmap": heatmap,
            "hotspots": [{"position": h["position"], "engagement": h["engagement"]} for h in hotspots],
            "cold_spots": [
                {"position": h["position"], "engagement": h["engagement"]}
                for h in sorted(heatmap, key=lambda x: x["engagement"])[:2]
            ],
        }

    # ─── 跨线程聚合 ──────────────────────────────────────

    def aggregate_position_performance(self, max_position: int = 20) -> Dict[str, Any]:
        """
        聚合所有线程，按位置统计平均表现
        回答: "第N条推文通常表现如何?"
        """
        position_data: Dict[int, List[Dict]] = {}

        for thread in self._threads.values():
            for t in thread.tweets:
                if t.position > max_position:
                    break
                position_data.setdefault(t.position, []).append({
                    "impressions": t.impressions,
                    "engagement": t.engagement_total,
                    "eng_rate": t.engagement_rate,
                })

        positions = {}
        for pos in sorted(position_data.keys()):
            data = position_data[pos]
            positions[pos] = {
                "sample_count": len(data),
                "avg_impressions": round(statistics.mean([d["impressions"] for d in data]), 1),
                "avg_engagement": round(statistics.mean([d["engagement"] for d in data]), 1),
                "avg_engagement_rate": round(statistics.mean([d["eng_rate"] for d in data]), 6),
                "median_engagement": round(statistics.median([d["engagement"] for d in data]), 1),
            }

        # 最佳位置
        best_pos = max(positions.items(), key=lambda x: x[1]["avg_engagement_rate"])[0] if positions else None

        return {
            "positions": positions,
            "best_position": best_pos,
            "total_threads": len(self._threads),
        }

    # ─── 时间序列 ──────────────────────────────────────────

    def performance_over_time(
        self,
        author: Optional[str] = None,
        days: int = 30,
    ) -> Dict[str, Any]:
        """线程表现随时间的趋势"""
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=days)

        data_points = []
        for t in self._threads.values():
            if author and t.author != author:
                continue
            if t.created_at:
                try:
                    created = datetime.fromisoformat(t.created_at)
                    if created < cutoff:
                        continue
                except (ValueError, TypeError):
                    pass

            data_points.append({
                "thread_id": t.thread_id,
                "created_at": t.created_at or "unknown",
                "length": t.length,
                "total_engagement": t.total_engagement,
                "avg_engagement_rate": round(t.avg_engagement_rate, 6),
                "format": t.format.value,
            })

        data_points.sort(key=lambda x: x["created_at"])

        # 趋势: 前半 vs 后半
        if len(data_points) >= 4:
            mid = len(data_points) // 2
            first_half_avg = statistics.mean([d["avg_engagement_rate"] for d in data_points[:mid]])
            second_half_avg = statistics.mean([d["avg_engagement_rate"] for d in data_points[mid:]])
            if second_half_avg > first_half_avg * 1.1:
                trend = "improving"
            elif second_half_avg < first_half_avg * 0.9:
                trend = "declining"
            else:
                trend = "stable"
        else:
            trend = "insufficient_data"

        return {
            "period_days": days,
            "thread_count": len(data_points),
            "trend": trend,
            "data": data_points,
        }

    # ─── 推荐引擎 ──────────────────────────────────────────

    def recommendations(self, author: Optional[str] = None) -> List[Dict[str, str]]:
        """基于分析数据生成线程创作建议"""
        recs = []
        threads = self.list_threads(author=author)

        if not threads:
            return [{"type": "info", "message": "No threads to analyze. Start posting threads!"}]

        # 1. 长度建议
        opt = self.optimal_length(author=author, min_threads=2)
        if opt.get("recommendation"):
            recs.append({
                "type": "length",
                "message": f"Your best-performing length bucket is {opt['recommendation']}. "
                           f"Focus on this range for maximum engagement.",
                "score": str(opt["best_score"]),
            })

        # 2. 格式建议
        fmt_comp = self.format_comparison()
        if fmt_comp.get("best_format"):
            recs.append({
                "type": "format",
                "message": f"'{fmt_comp['best_format']}' format performs best. "
                           f"Consider using it more often.",
            })

        # 3. 完读率低的线程
        for t in threads:
            cr = self.completion_rate(t.thread_id)
            if "error" not in cr and cr["completion_rate"] < 0.3:
                recs.append({
                    "type": "retention",
                    "message": f"Thread '{t.thread_id}' has low completion ({cr['completion_pct']}). "
                               f"Biggest drop at position {cr['max_drop_position']}. "
                               f"Consider adding a hook or CTA at that point.",
                })

        # 4. 位置分析
        agg = self.aggregate_position_performance()
        if agg.get("best_position"):
            recs.append({
                "type": "position",
                "message": f"Position {agg['best_position']} typically gets the highest engagement rate. "
                           f"Place your strongest content there.",
            })

        return recs

    # ─── 报告导出 ──────────────────────────────────────────

    def generate_report(self, thread_id: str) -> Dict[str, Any]:
        """生成单条线程的完整分析报告"""
        thread = self._threads.get(thread_id)
        if not thread:
            return {"error": f"thread {thread_id} not found"}

        return {
            "thread_id": thread_id,
            "author": thread.author,
            "format": thread.format.value,
            "length": thread.length,
            "tags": thread.tags,
            "topic": thread.topic,
            "total_impressions": thread.total_impressions,
            "total_engagement": thread.total_engagement,
            "avg_engagement_rate": round(thread.avg_engagement_rate, 6),
            "completion": self.completion_rate(thread_id),
            "decay": self.engagement_decay(thread_id),
            "heatmap": self.position_heatmap(thread_id),
            "tweets": [t.to_dict() for t in thread.tweets],
        }

    def export_all(self, fmt: str = "json") -> str:
        """导出所有线程数据"""
        data = {
            "threads": [],
            "summary": {
                "total_threads": len(self._threads),
                "total_tweets": sum(t.length for t in self._threads.values()),
                "formats": {},
            },
        }

        for t in self._threads.values():
            data["threads"].append(self.generate_report(t.thread_id))
            fmt_name = t.format.value
            data["summary"]["formats"][fmt_name] = data["summary"]["formats"].get(fmt_name, 0) + 1

        if fmt == "json":
            return json.dumps(data, indent=2, ensure_ascii=False)
        else:
            # CSV-like text
            lines = ["thread_id,format,length,total_engagement,completion_rate"]
            for t in self._threads.values():
                cr = self.completion_rate(t.thread_id)
                cr_val = cr.get("completion_rate", 0)
                lines.append(f"{t.thread_id},{t.format.value},{t.length},{t.total_engagement},{cr_val}")
            return "\n".join(lines)

    # ─── Private Helpers ──────────────────────────────────

    def _detect_format(self, tweets: List[TweetMetrics]) -> ThreadFormat:
        """自动检测线程格式"""
        if not tweets:
            return ThreadFormat.UNKNOWN

        texts = [t.text for t in tweets]

        # 检查编号格式
        numbered_count = sum(1 for t in texts if any(
            t.strip().startswith(f"{i}/") or t.strip().startswith(f"{i}.")
            for i in range(1, 30)
        ))
        if numbered_count >= len(texts) * 0.6:
            return ThreadFormat.NUMBERED

        # 检查Q&A格式
        qa_markers = ["Q:", "A:", "问:", "答:", "❓", "💡"]
        qa_count = sum(1 for t in texts if any(m in t for m in qa_markers))
        if qa_count >= len(texts) * 0.4:
            return ThreadFormat.QA

        # 检查列表格式
        list_markers = ["•", "→", "✅", "🔹", "▪️", "- "]
        list_count = sum(1 for t in texts if any(t.strip().startswith(m) for m in list_markers))
        if list_count >= len(texts) * 0.5:
            return ThreadFormat.LISTICLE

        # 检查教程格式
        tutorial_markers = ["Step", "步骤", "How to", "First", "Next", "Then", "Finally"]
        tutorial_count = sum(1 for t in texts if any(m.lower() in t.lower() for m in tutorial_markers))
        if tutorial_count >= len(texts) * 0.3:
            return ThreadFormat.TUTORIAL

        return ThreadFormat.NARRATIVE

    def _classify_trend(self, values: List[int]) -> EngagementTrend:
        """分类互动趋势"""
        if len(values) < 2:
            return EngagementTrend.STABLE

        # 线性回归斜率
        n = len(values)
        x_mean = (n - 1) / 2
        y_mean = statistics.mean(values)

        numerator = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
        denominator = sum((i - x_mean) ** 2 for i in range(n))

        if denominator == 0:
            return EngagementTrend.STABLE

        slope = numerator / denominator
        relative_slope = slope / y_mean if y_mean else 0

        # 检查断崖
        for i in range(1, len(values)):
            if values[i - 1] > 0:
                drop = (values[i - 1] - values[i]) / values[i - 1]
                if drop > 0.5:
                    # 检查是否回弹
                    if i < len(values) - 1 and values[i + 1] > values[i]:
                        return EngagementTrend.RESURGENT
                    return EngagementTrend.CLIFF

        if relative_slope > 0.05:
            return EngagementTrend.RISING
        elif relative_slope < -0.05:
            return EngagementTrend.DECLINING
        else:
            return EngagementTrend.STABLE

    def _top_metric(self, tweet: TweetMetrics) -> str:
        """找出推文最突出的指标"""
        metrics = {
            "likes": tweet.likes,
            "retweets": tweet.retweets,
            "replies": tweet.replies,
            "quotes": tweet.quotes,
            "bookmarks": tweet.bookmarks,
        }
        if not any(metrics.values()):
            return "none"
        return max(metrics, key=metrics.get)
