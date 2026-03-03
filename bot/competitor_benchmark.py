"""
Competitive Benchmarking Engine
竞品对标: 指标对比 + 内容策略检测 + engagement rate基准 + 增长轨迹 + 内容缺口分析 + 发帖频率
"""

import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, List, Dict, Any


class ContentStrategy(str, Enum):
    THREAD_HEAVY = "thread_heavy"       # 主打长线程
    MEDIA_FIRST = "media_first"         # 图片/视频为主
    ENGAGEMENT_BAIT = "engagement_bait" # 互动钓鱼（投票/问答）
    NEWS_COMMENTARY = "news_commentary" # 新闻点评
    EDUCATIONAL = "educational"         # 教育科普
    PROMOTIONAL = "promotional"         # 推广营销
    MIXED = "mixed"                     # 混合策略
    CURATED = "curated"                 # 内容策展/转发为主
    COMMUNITY = "community"            # 社区互动为主


class GrowthPhase(str, Enum):
    EXPLOSIVE = "explosive"     # 爆发增长 (>50%/月)
    RAPID = "rapid"             # 快速增长 (20-50%/月)
    STEADY = "steady"           # 稳定增长 (5-20%/月)
    STAGNANT = "stagnant"       # 停滞 (0-5%/月)
    DECLINING = "declining"     # 下降 (<0%/月)


@dataclass
class CompetitorProfile:
    """竞品画像"""
    handle: str
    display_name: str = ""
    bio: str = ""
    followers: int = 0
    following: int = 0
    tweet_count: int = 0
    listed_count: int = 0
    created_at: str = ""
    verified: bool = False
    category: str = ""
    tags: List[str] = field(default_factory=list)

    @property
    def follower_ratio(self) -> float:
        """关注比 (followers / following)"""
        return self.followers / max(1, self.following)

    @property
    def tweets_per_follower(self) -> float:
        return self.tweet_count / max(1, self.followers)


@dataclass
class CompetitorMetrics:
    """竞品指标快照"""
    handle: str
    snapshot_date: str
    followers: int = 0
    avg_likes: float = 0
    avg_retweets: float = 0
    avg_replies: float = 0
    avg_impressions: float = 0
    engagement_rate: float = 0
    posts_per_day: float = 0
    thread_ratio: float = 0     # 线程占比
    media_ratio: float = 0      # 媒体帖占比
    reply_ratio: float = 0      # 回复占比
    top_hashtags: List[str] = field(default_factory=list)
    active_hours: List[int] = field(default_factory=list)
    content_types: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ContentPiece:
    """竞品内容样本"""
    handle: str
    tweet_id: str = ""
    text: str = ""
    content_type: str = "tweet"
    likes: int = 0
    retweets: int = 0
    replies: int = 0
    impressions: int = 0
    posted_at: str = ""
    hashtags: List[str] = field(default_factory=list)
    has_media: bool = False
    is_thread: bool = False
    is_reply: bool = False

    @property
    def engagement(self) -> int:
        return self.likes + self.retweets + self.replies

    @property
    def engagement_rate(self) -> float:
        return self.engagement / max(1, self.impressions)


class CompetitorBenchmark:
    """竞品对标引擎"""

    def __init__(self, my_handle: str = ""):
        self.my_handle = my_handle
        self._profiles: Dict[str, CompetitorProfile] = {}
        self._metrics: Dict[str, List[CompetitorMetrics]] = defaultdict(list)
        self._content: Dict[str, List[ContentPiece]] = defaultdict(list)
        self._my_metrics: List[CompetitorMetrics] = []
        self._my_content: List[ContentPiece] = []

    # ─── 数据管理 ──────────────────────────────────────────

    def add_competitor(self, profile: CompetitorProfile) -> None:
        """添加竞品"""
        self._profiles[profile.handle] = profile

    def remove_competitor(self, handle: str) -> bool:
        if handle in self._profiles:
            del self._profiles[handle]
            self._metrics.pop(handle, None)
            self._content.pop(handle, None)
            return True
        return False

    def list_competitors(self) -> List[Dict[str, Any]]:
        return [
            {
                "handle": p.handle,
                "display_name": p.display_name,
                "followers": p.followers,
                "follower_ratio": round(p.follower_ratio, 2),
                "category": p.category,
            }
            for p in self._profiles.values()
        ]

    def add_metrics(self, metrics: CompetitorMetrics) -> None:
        """添加指标快照"""
        self._metrics[metrics.handle].append(metrics)

    def add_my_metrics(self, metrics: CompetitorMetrics) -> None:
        """添加自己的指标"""
        self._my_metrics.append(metrics)

    def add_content(self, piece: ContentPiece) -> None:
        """添加竞品内容样本"""
        self._content[piece.handle].append(piece)

    def add_my_content(self, piece: ContentPiece) -> None:
        self._my_content.append(piece)

    # ─── 指标对比 ──────────────────────────────────────────

    def compare_metrics(
        self,
        handles: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        指标横向对比
        包含自己 vs 所有竞品
        """
        targets = handles or list(self._profiles.keys())

        comparisons = {}
        for handle in targets:
            metrics_list = self._metrics.get(handle, [])
            if not metrics_list:
                continue
            latest = metrics_list[-1]
            comparisons[handle] = {
                "followers": latest.followers,
                "engagement_rate": round(latest.engagement_rate, 6),
                "avg_likes": round(latest.avg_likes, 1),
                "avg_retweets": round(latest.avg_retweets, 1),
                "avg_replies": round(latest.avg_replies, 1),
                "posts_per_day": round(latest.posts_per_day, 1),
                "thread_ratio": round(latest.thread_ratio, 3),
                "media_ratio": round(latest.media_ratio, 3),
            }

        # 加入自己的数据
        if self._my_metrics:
            my_latest = self._my_metrics[-1]
            comparisons[f"📍{self.my_handle}(me)"] = {
                "followers": my_latest.followers,
                "engagement_rate": round(my_latest.engagement_rate, 6),
                "avg_likes": round(my_latest.avg_likes, 1),
                "avg_retweets": round(my_latest.avg_retweets, 1),
                "avg_replies": round(my_latest.avg_replies, 1),
                "posts_per_day": round(my_latest.posts_per_day, 1),
                "thread_ratio": round(my_latest.thread_ratio, 3),
                "media_ratio": round(my_latest.media_ratio, 3),
            }

        # 排名
        rankings = {}
        if comparisons:
            for metric in ["followers", "engagement_rate", "avg_likes", "posts_per_day"]:
                sorted_handles = sorted(
                    comparisons.items(),
                    key=lambda x: x[1].get(metric, 0),
                    reverse=True,
                )
                rankings[metric] = [
                    {"rank": i + 1, "handle": h, "value": d[metric]}
                    for i, (h, d) in enumerate(sorted_handles)
                ]

        return {
            "competitors": comparisons,
            "rankings": rankings,
            "total_compared": len(comparisons),
        }

    # ─── 内容策略检测 ──────────────────────────────────────

    def detect_strategy(self, handle: str) -> Dict[str, Any]:
        """检测竞品的内容策略"""
        content = self._content.get(handle, [])
        profile = self._profiles.get(handle)

        if not content:
            return {"handle": handle, "strategy": "unknown", "reason": "no content data"}

        total = len(content)

        # 内容类型分布
        type_counts = Counter()
        for c in content:
            if c.is_thread:
                type_counts["thread"] += 1
            elif c.has_media:
                type_counts["media"] += 1
            elif c.is_reply:
                type_counts["reply"] += 1
            else:
                type_counts["text"] += 1

        # 话题标签分析
        all_hashtags = []
        for c in content:
            all_hashtags.extend(c.hashtags)
        top_hashtags = Counter(all_hashtags).most_common(10)

        # 策略判定
        thread_pct = type_counts.get("thread", 0) / total
        media_pct = type_counts.get("media", 0) / total
        reply_pct = type_counts.get("reply", 0) / total

        # 检测互动钓鱼
        bait_keywords = ["what do you think", "agree?", "hot take", "unpopular opinion",
                         "drop your", "reply with", "quote tweet", "poll"]
        bait_count = sum(
            1 for c in content
            if any(k in c.text.lower() for k in bait_keywords)
        )
        bait_pct = bait_count / total

        if thread_pct > 0.4:
            strategy = ContentStrategy.THREAD_HEAVY
        elif media_pct > 0.5:
            strategy = ContentStrategy.MEDIA_FIRST
        elif bait_pct > 0.3:
            strategy = ContentStrategy.ENGAGEMENT_BAIT
        elif reply_pct > 0.5:
            strategy = ContentStrategy.COMMUNITY
        else:
            strategy = ContentStrategy.MIXED

        # 内容表现分析
        type_performance = {}
        for ct_name, _ in type_counts.items():
            ct_content = [c for c in content if (
                (ct_name == "thread" and c.is_thread) or
                (ct_name == "media" and c.has_media and not c.is_thread) or
                (ct_name == "reply" and c.is_reply) or
                (ct_name == "text" and not c.is_thread and not c.has_media and not c.is_reply)
            )]
            if ct_content:
                type_performance[ct_name] = {
                    "count": len(ct_content),
                    "pct": round(len(ct_content) / total, 3),
                    "avg_engagement": round(statistics.mean([c.engagement for c in ct_content]), 1),
                    "avg_engagement_rate": round(
                        statistics.mean([c.engagement_rate for c in ct_content]), 6
                    ),
                }

        return {
            "handle": handle,
            "strategy": strategy.value,
            "content_mix": {k: round(v / total, 3) for k, v in type_counts.items()},
            "type_performance": type_performance,
            "top_hashtags": [{"tag": t, "count": c} for t, c in top_hashtags],
            "engagement_bait_ratio": round(bait_pct, 3),
            "total_analyzed": total,
            "best_performing_type": max(
                type_performance.items(),
                key=lambda x: x[1]["avg_engagement"],
            )[0] if type_performance else None,
        }

    # ─── Engagement Rate基准 ──────────────────────────────

    def engagement_benchmark(self) -> Dict[str, Any]:
        """
        Engagement rate基准线
        按follower量级分层对标
        """
        all_rates = []
        by_tier: Dict[str, List[float]] = defaultdict(list)

        for handle, metrics_list in self._metrics.items():
            if not metrics_list:
                continue
            latest = metrics_list[-1]
            profile = self._profiles.get(handle)
            followers = profile.followers if profile else latest.followers

            all_rates.append(latest.engagement_rate)

            tier = self._follower_tier(followers)
            by_tier[tier].append(latest.engagement_rate)

        if not all_rates:
            return {"benchmark": 0, "note": "no data"}

        # 我的位置
        my_rate = self._my_metrics[-1].engagement_rate if self._my_metrics else None
        my_percentile = None
        if my_rate is not None and all_rates:
            below = sum(1 for r in all_rates if r < my_rate)
            my_percentile = round(below / len(all_rates) * 100, 1)

        # 分层基准
        tier_benchmarks = {}
        for tier, rates in by_tier.items():
            tier_benchmarks[tier] = {
                "count": len(rates),
                "mean": round(statistics.mean(rates), 6),
                "median": round(statistics.median(rates), 6),
                "p25": round(sorted(rates)[len(rates) // 4], 6) if len(rates) >= 4 else None,
                "p75": round(sorted(rates)[len(rates) * 3 // 4], 6) if len(rates) >= 4 else None,
            }

        return {
            "overall_mean": round(statistics.mean(all_rates), 6),
            "overall_median": round(statistics.median(all_rates), 6),
            "my_engagement_rate": round(my_rate, 6) if my_rate else None,
            "my_percentile": my_percentile,
            "tier_benchmarks": tier_benchmarks,
            "competitors_count": len(all_rates),
        }

    # ─── 增长轨迹对比 ──────────────────────────────────────

    def growth_comparison(self) -> Dict[str, Any]:
        """对比所有竞品的增长轨迹"""
        growth_data = {}

        for handle, metrics_list in self._metrics.items():
            if len(metrics_list) < 2:
                continue

            first = metrics_list[0]
            last = metrics_list[-1]

            follower_growth = last.followers - first.followers
            if first.followers > 0:
                growth_pct = follower_growth / first.followers
            else:
                growth_pct = 0

            # 计算时间跨度
            try:
                t0 = datetime.fromisoformat(first.snapshot_date)
                t1 = datetime.fromisoformat(last.snapshot_date)
                days = (t1 - t0).days or 1
                monthly_growth_rate = growth_pct / days * 30
            except (ValueError, TypeError):
                days = 1
                monthly_growth_rate = 0

            phase = self._growth_phase(monthly_growth_rate)

            # 互动趋势
            eng_first = first.engagement_rate
            eng_last = last.engagement_rate
            eng_change = eng_last - eng_first

            growth_data[handle] = {
                "follower_start": first.followers,
                "follower_end": last.followers,
                "follower_growth": follower_growth,
                "growth_pct": round(growth_pct * 100, 2),
                "monthly_growth_rate": round(monthly_growth_rate * 100, 2),
                "phase": phase.value,
                "engagement_change": round(eng_change, 6),
                "period_days": days,
                "snapshots": len(metrics_list),
            }

        # 加入自己
        if len(self._my_metrics) >= 2:
            first = self._my_metrics[0]
            last = self._my_metrics[-1]
            fg = last.followers - first.followers
            gp = fg / max(1, first.followers)
            try:
                t0 = datetime.fromisoformat(first.snapshot_date)
                t1 = datetime.fromisoformat(last.snapshot_date)
                days = (t1 - t0).days or 1
                mgr = gp / days * 30
            except (ValueError, TypeError):
                days = 1
                mgr = 0

            growth_data[f"📍{self.my_handle}(me)"] = {
                "follower_start": first.followers,
                "follower_end": last.followers,
                "follower_growth": fg,
                "growth_pct": round(gp * 100, 2),
                "monthly_growth_rate": round(mgr * 100, 2),
                "phase": self._growth_phase(mgr).value,
                "engagement_change": round(last.engagement_rate - first.engagement_rate, 6),
                "period_days": days,
                "snapshots": len(self._my_metrics),
            }

        # 排名
        ranked = sorted(
            growth_data.items(),
            key=lambda x: x[1]["monthly_growth_rate"],
            reverse=True,
        )

        return {
            "growth_data": growth_data,
            "fastest_grower": ranked[0][0] if ranked else None,
            "ranking": [
                {"rank": i + 1, "handle": h, "monthly_growth": d["monthly_growth_rate"]}
                for i, (h, d) in enumerate(ranked)
            ],
        }

    # ─── 内容缺口分析 ──────────────────────────────────────

    def content_gap_analysis(self) -> Dict[str, Any]:
        """
        分析竞品在做而你没在做的内容类型/话题
        """
        # 竞品话题
        competitor_topics: Counter = Counter()
        competitor_types: Counter = Counter()
        competitor_hashtags: Counter = Counter()

        for handle, pieces in self._content.items():
            for c in pieces:
                if c.is_thread:
                    competitor_types["thread"] += 1
                elif c.has_media:
                    competitor_types["media"] += 1
                elif c.is_reply:
                    competitor_types["reply"] += 1
                else:
                    competitor_types["text"] += 1

                for tag in c.hashtags:
                    competitor_hashtags[tag.lower()] += 1

        # 我的话题
        my_types: Counter = Counter()
        my_hashtags: Counter = Counter()

        for c in self._my_content:
            if c.is_thread:
                my_types["thread"] += 1
            elif c.has_media:
                my_types["media"] += 1
            elif c.is_reply:
                my_types["reply"] += 1
            else:
                my_types["text"] += 1

            for tag in c.hashtags:
                my_hashtags[tag.lower()] += 1

        # 内容类型缺口
        type_gaps = []
        total_comp = sum(competitor_types.values()) or 1
        total_my = sum(my_types.values()) or 1

        for ct, count in competitor_types.items():
            comp_pct = count / total_comp
            my_pct = my_types.get(ct, 0) / total_my
            gap = comp_pct - my_pct
            if gap > 0.05:  # 5%以上差距
                type_gaps.append({
                    "content_type": ct,
                    "competitor_pct": round(comp_pct, 3),
                    "my_pct": round(my_pct, 3),
                    "gap": round(gap, 3),
                    "action": f"Increase {ct} content by {gap * 100:.1f}%",
                })

        type_gaps.sort(key=lambda x: x["gap"], reverse=True)

        # 话题标签缺口
        tag_gaps = []
        top_competitor_tags = competitor_hashtags.most_common(30)
        for tag, count in top_competitor_tags:
            if tag not in my_hashtags or my_hashtags[tag] < count * 0.3:
                tag_gaps.append({
                    "hashtag": tag,
                    "competitor_usage": count,
                    "my_usage": my_hashtags.get(tag, 0),
                    "opportunity": "high" if count >= 10 else "medium" if count >= 5 else "low",
                })

        # 高互动但我没用的格式
        high_eng_gaps = []
        for handle, pieces in self._content.items():
            for c in pieces:
                if c.engagement > 0 and c.engagement_rate > 0.05:
                    ct = "thread" if c.is_thread else "media" if c.has_media else "text"
                    if my_types.get(ct, 0) == 0:
                        high_eng_gaps.append({
                            "handle": handle,
                            "content_type": ct,
                            "engagement": c.engagement,
                            "engagement_rate": round(c.engagement_rate, 4),
                        })

        return {
            "content_type_gaps": type_gaps,
            "hashtag_gaps": tag_gaps[:15],
            "high_engagement_gaps": high_eng_gaps[:10],
            "recommendation": self._gap_recommendation(type_gaps, tag_gaps),
        }

    # ─── 发帖频率对比 ──────────────────────────────────────

    def posting_frequency_comparison(self) -> Dict[str, Any]:
        """对比发帖频率"""
        freq_data = {}

        for handle, metrics_list in self._metrics.items():
            if metrics_list:
                latest = metrics_list[-1]
                freq_data[handle] = {
                    "posts_per_day": round(latest.posts_per_day, 1),
                    "active_hours": latest.active_hours,
                    "thread_ratio": round(latest.thread_ratio, 3),
                }

        if self._my_metrics:
            my_latest = self._my_metrics[-1]
            freq_data[f"📍{self.my_handle}(me)"] = {
                "posts_per_day": round(my_latest.posts_per_day, 1),
                "active_hours": my_latest.active_hours,
                "thread_ratio": round(my_latest.thread_ratio, 3),
            }

        avg_freq = statistics.mean([d["posts_per_day"] for d in freq_data.values()]) if freq_data else 0

        return {
            "frequency_data": freq_data,
            "average_posts_per_day": round(avg_freq, 1),
            "most_active": max(freq_data.items(), key=lambda x: x[1]["posts_per_day"])[0] if freq_data else None,
            "least_active": min(freq_data.items(), key=lambda x: x[1]["posts_per_day"])[0] if freq_data else None,
        }

    # ─── Top内容分析 ──────────────────────────────────────

    def top_content_analysis(
        self,
        handle: Optional[str] = None,
        top_n: int = 10,
    ) -> Dict[str, Any]:
        """分析竞品表现最好的内容"""
        if handle:
            content = self._content.get(handle, [])
        else:
            content = [c for pieces in self._content.values() for c in pieces]

        if not content:
            return {"top_content": [], "note": "no data"}

        # 按engagement排序
        sorted_content = sorted(content, key=lambda c: c.engagement, reverse=True)
        top = sorted_content[:top_n]

        # 分析共性
        top_types = Counter(
            "thread" if c.is_thread else "media" if c.has_media else "text"
            for c in top
        )
        top_tags = Counter(
            tag for c in top for tag in c.hashtags
        )

        return {
            "top_content": [
                {
                    "handle": c.handle,
                    "text_preview": c.text[:120] + "..." if len(c.text) > 120 else c.text,
                    "engagement": c.engagement,
                    "engagement_rate": round(c.engagement_rate, 4),
                    "likes": c.likes,
                    "retweets": c.retweets,
                    "type": "thread" if c.is_thread else "media" if c.has_media else "text",
                    "hashtags": c.hashtags,
                }
                for c in top
            ],
            "common_types": dict(top_types),
            "common_hashtags": [{"tag": t, "count": c} for t, c in top_tags.most_common(5)],
            "pattern": self._identify_pattern(top),
        }

    # ─── 综合报告 ──────────────────────────────────────────

    def full_benchmark_report(self) -> Dict[str, Any]:
        """生成完整竞品对标报告"""
        return {
            "my_handle": self.my_handle,
            "competitors": self.list_competitors(),
            "metrics_comparison": self.compare_metrics(),
            "engagement_benchmark": self.engagement_benchmark(),
            "growth_comparison": self.growth_comparison(),
            "content_gap": self.content_gap_analysis(),
            "posting_frequency": self.posting_frequency_comparison(),
            "strategies": {
                handle: self.detect_strategy(handle)
                for handle in self._profiles
            },
            "top_competitor_content": self.top_content_analysis(top_n=5),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    # ─── SWOT分析 ──────────────────────────────────────────

    def swot_analysis(self) -> Dict[str, List[str]]:
        """基于对标数据生成SWOT分析"""
        strengths = []
        weaknesses = []
        opportunities = []
        threats = []

        benchmark = self.engagement_benchmark()
        growth = self.growth_comparison()
        gap = self.content_gap_analysis()

        # Strengths
        if benchmark.get("my_percentile") and benchmark["my_percentile"] > 75:
            strengths.append(f"Top {100 - benchmark['my_percentile']:.0f}% engagement rate among competitors")
        if self._my_metrics:
            my_ppd = self._my_metrics[-1].posts_per_day
            avg_ppd = statistics.mean(
                [m[-1].posts_per_day for m in self._metrics.values() if m]
            ) if self._metrics else 0
            if my_ppd > avg_ppd:
                strengths.append(f"Higher posting frequency ({my_ppd:.1f}/day vs avg {avg_ppd:.1f})")

        # Weaknesses
        if benchmark.get("my_percentile") and benchmark["my_percentile"] < 25:
            weaknesses.append(f"Bottom {benchmark['my_percentile']:.0f}% engagement rate")
        if gap.get("content_type_gaps"):
            for g in gap["content_type_gaps"][:2]:
                weaknesses.append(f"Under-utilizing {g['content_type']} content ({g['my_pct']*100:.0f}% vs {g['competitor_pct']*100:.0f}%)")

        # Opportunities
        if gap.get("hashtag_gaps"):
            top_tags = [g["hashtag"] for g in gap["hashtag_gaps"][:3]]
            opportunities.append(f"Untapped hashtags: {', '.join(top_tags)}")
        if gap.get("high_engagement_gaps"):
            types = set(g["content_type"] for g in gap["high_engagement_gaps"])
            opportunities.append(f"High-engagement formats not yet used: {', '.join(types)}")

        # Threats
        growth_data = growth.get("growth_data", {})
        fast_growers = [
            h for h, d in growth_data.items()
            if d.get("phase") in ("explosive", "rapid") and "(me)" not in h
        ]
        if fast_growers:
            threats.append(f"Fast-growing competitors: {', '.join(fast_growers[:3])}")

        if not strengths:
            strengths.append("Need more data to identify strengths")
        if not weaknesses:
            weaknesses.append("No significant weaknesses detected")
        if not opportunities:
            opportunities.append("Analyze more competitor content for opportunities")
        if not threats:
            threats.append("No immediate threats detected")

        return {
            "strengths": strengths,
            "weaknesses": weaknesses,
            "opportunities": opportunities,
            "threats": threats,
        }

    # ─── Private Helpers ──────────────────────────────────

    def _follower_tier(self, followers: int) -> str:
        """粉丝量级分层"""
        if followers >= 1_000_000:
            return "mega(1M+)"
        elif followers >= 100_000:
            return "macro(100K-1M)"
        elif followers >= 10_000:
            return "mid(10K-100K)"
        elif followers >= 1_000:
            return "micro(1K-10K)"
        else:
            return "nano(<1K)"

    def _growth_phase(self, monthly_rate: float) -> GrowthPhase:
        if monthly_rate > 0.5:
            return GrowthPhase.EXPLOSIVE
        elif monthly_rate > 0.2:
            return GrowthPhase.RAPID
        elif monthly_rate > 0.05:
            return GrowthPhase.STEADY
        elif monthly_rate >= 0:
            return GrowthPhase.STAGNANT
        else:
            return GrowthPhase.DECLINING

    def _gap_recommendation(
        self,
        type_gaps: List[Dict],
        tag_gaps: List[Dict],
    ) -> str:
        """生成内容缺口建议"""
        parts = []
        if type_gaps:
            top_gap = type_gaps[0]
            parts.append(
                f"Priority: Increase {top_gap['content_type']} content "
                f"(competitors use {top_gap['competitor_pct']*100:.0f}%, you use {top_gap['my_pct']*100:.0f}%)"
            )
        if tag_gaps:
            high_opp = [g for g in tag_gaps if g["opportunity"] == "high"]
            if high_opp:
                parts.append(
                    f"High-opportunity hashtags to adopt: {', '.join(g['hashtag'] for g in high_opp[:3])}"
                )
        return "; ".join(parts) if parts else "Keep monitoring competitors for emerging opportunities."

    def _identify_pattern(self, top_content: List[ContentPiece]) -> str:
        """识别高表现内容的共性"""
        if not top_content:
            return "insufficient data"

        has_media_pct = sum(1 for c in top_content if c.has_media) / len(top_content)
        is_thread_pct = sum(1 for c in top_content if c.is_thread) / len(top_content)
        avg_len = statistics.mean([len(c.text) for c in top_content]) if top_content else 0

        patterns = []
        if has_media_pct > 0.6:
            patterns.append("media-heavy")
        if is_thread_pct > 0.4:
            patterns.append("thread-dominant")
        if avg_len > 200:
            patterns.append("long-form")
        elif avg_len < 100:
            patterns.append("concise")

        return ", ".join(patterns) if patterns else "mixed format"
