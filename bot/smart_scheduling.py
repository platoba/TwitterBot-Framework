"""
Smart Scheduling Engine
智能发帖调度: 时区感知最优窗口 + 互动预测 + 内容类型专属调度 + 受众在线检测 + 节假日感知
"""

import json
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple


class ContentType(str, Enum):
    TWEET = "tweet"
    THREAD = "thread"
    POLL = "poll"
    MEDIA = "media"          # 图片/视频
    REPLY = "reply"
    QUOTE = "quote"
    SPACE = "space"


class DayOfWeek(int, Enum):
    MONDAY = 0
    TUESDAY = 1
    WEDNESDAY = 2
    THURSDAY = 3
    FRIDAY = 4
    SATURDAY = 5
    SUNDAY = 6


@dataclass
class PostRecord:
    """历史发帖记录"""
    post_id: str
    content_type: ContentType
    posted_at: str  # ISO format
    timezone_offset: int = 0  # UTC offset in hours
    impressions: int = 0
    likes: int = 0
    retweets: int = 0
    replies: int = 0
    clicks: int = 0
    engagement_rate: float = 0.0
    tags: List[str] = field(default_factory=list)

    @property
    def total_engagement(self) -> int:
        return self.likes + self.retweets + self.replies + self.clicks

    @property
    def posted_datetime(self) -> Optional[datetime]:
        try:
            return datetime.fromisoformat(self.posted_at)
        except (ValueError, TypeError):
            return None

    @property
    def hour(self) -> Optional[int]:
        dt = self.posted_datetime
        return dt.hour if dt else None

    @property
    def day_of_week(self) -> Optional[int]:
        dt = self.posted_datetime
        return dt.weekday() if dt else None


@dataclass
class TimeSlot:
    """时间槽推荐"""
    day: int  # 0=Monday
    hour: int  # 0-23
    score: float = 0.0
    avg_engagement: float = 0.0
    avg_impressions: float = 0.0
    sample_count: int = 0
    confidence: str = "low"  # low/medium/high
    best_content_type: Optional[str] = None


@dataclass
class ScheduleEntry:
    """调度条目"""
    entry_id: str
    content_type: ContentType
    scheduled_at: str
    timezone_offset: int = 0
    priority: int = 5  # 1-10
    tags: List[str] = field(default_factory=list)
    notes: str = ""
    status: str = "scheduled"  # scheduled/posted/cancelled/skipped

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SmartScheduler:
    """智能调度引擎"""

    # 通用最佳发帖时段 (UTC, baseline)
    DEFAULT_PEAK_HOURS = [13, 14, 15, 16, 17, 18]  # 1-6 PM UTC
    DEFAULT_GOOD_HOURS = [9, 10, 11, 12, 19, 20]
    DEFAULT_AVOID_HOURS = [0, 1, 2, 3, 4, 5]

    # 内容类型最佳时段 (offset from audience timezone)
    CONTENT_OPTIMAL_HOURS = {
        ContentType.THREAD: [8, 9, 10],       # 早晨阅读
        ContentType.POLL: [12, 13, 14, 15],    # 午间互动
        ContentType.MEDIA: [17, 18, 19],       # 傍晚浏览
        ContentType.TWEET: [10, 11, 14, 15, 18, 19],
        ContentType.SPACE: [20, 21, 22],       # 晚间直播
        ContentType.REPLY: [9, 10, 11, 14, 15, 16],
        ContentType.QUOTE: [11, 12, 15, 16],
    }

    # 主要节假日 (月-日)
    HOLIDAYS = {
        (1, 1): "New Year's Day",
        (2, 14): "Valentine's Day",
        (3, 8): "Women's Day",
        (5, 1): "Labor Day",
        (7, 4): "Independence Day (US)",
        (10, 1): "National Day (CN)",
        (10, 31): "Halloween",
        (11, 11): "Singles' Day",
        (12, 25): "Christmas",
        (12, 31): "New Year's Eve",
    }

    def __init__(self, audience_timezone: int = 0):
        """
        Args:
            audience_timezone: 受众主要时区 UTC offset (e.g., -5 for EST, 8 for CST)
        """
        self.audience_tz = audience_timezone
        self._history: List[PostRecord] = []
        self._schedule: Dict[str, ScheduleEntry] = {}
        self._blocked_slots: List[Tuple[int, int]] = []  # (day, hour)

    # ─── 历史数据 ──────────────────────────────────────────

    def add_record(self, record: PostRecord) -> None:
        """添加历史发帖记录"""
        self._history.append(record)

    def add_records(self, records: List[PostRecord]) -> None:
        for r in records:
            self._history.append(r)

    def clear_history(self) -> None:
        self._history.clear()

    # ─── 最优时段分析 ──────────────────────────────────────

    def analyze_best_times(
        self,
        content_type: Optional[ContentType] = None,
        top_n: int = 10,
    ) -> List[TimeSlot]:
        """
        分析历史数据找出最佳发帖时段
        返回按得分排序的时间槽
        """
        records = self._history
        if content_type:
            records = [r for r in records if r.content_type == content_type]

        if not records:
            # 返回默认推荐
            return self._default_recommendations(content_type, top_n)

        # 按 (day, hour) 分桶
        slots: Dict[Tuple[int, int], List[PostRecord]] = defaultdict(list)
        for r in records:
            if r.day_of_week is not None and r.hour is not None:
                slots[(r.day_of_week, r.hour)].append(r)

        time_slots = []
        for (day, hour), slot_records in slots.items():
            eng_rates = [r.engagement_rate for r in slot_records if r.engagement_rate > 0]
            if not eng_rates:
                eng_rates = [
                    r.total_engagement / r.impressions if r.impressions > 0 else 0
                    for r in slot_records
                ]

            avg_eng = statistics.mean([r.total_engagement for r in slot_records])
            avg_imp = statistics.mean([r.impressions for r in slot_records])
            avg_rate = statistics.mean(eng_rates) if eng_rates else 0

            # 置信度
            n = len(slot_records)
            confidence = "high" if n >= 10 else "medium" if n >= 5 else "low"

            # 综合得分: engagement_rate为主 + impressions加权
            score = avg_rate * 0.6 + (avg_imp / max(1, max(r.impressions for r in self._history))) * 0.4

            # 找该时段最佳内容类型
            type_perf: Dict[str, float] = defaultdict(list)
            for r in slot_records:
                type_perf[r.content_type.value].append(r.total_engagement)
            best_type = max(type_perf, key=lambda k: statistics.mean(type_perf[k])) if type_perf else None

            time_slots.append(TimeSlot(
                day=day,
                hour=hour,
                score=round(score, 6),
                avg_engagement=round(avg_eng, 1),
                avg_impressions=round(avg_imp, 1),
                sample_count=n,
                confidence=confidence,
                best_content_type=best_type,
            ))

        time_slots.sort(key=lambda x: x.score, reverse=True)
        return time_slots[:top_n]

    def _default_recommendations(
        self,
        content_type: Optional[ContentType],
        top_n: int,
    ) -> List[TimeSlot]:
        """无历史数据时的默认推荐"""
        slots = []

        if content_type and content_type in self.CONTENT_OPTIMAL_HOURS:
            hours = self.CONTENT_OPTIMAL_HOURS[content_type]
        else:
            hours = self.DEFAULT_PEAK_HOURS

        # 调整到受众时区
        adjusted_hours = [(h - self.audience_tz) % 24 for h in hours]

        for day in range(7):
            for hour in adjusted_hours:
                # 周末稍微降分
                weekend_penalty = 0.9 if day >= 5 else 1.0
                base_score = 0.5 * weekend_penalty
                slots.append(TimeSlot(
                    day=day,
                    hour=hour,
                    score=round(base_score, 3),
                    confidence="baseline",
                    best_content_type=content_type.value if content_type else None,
                ))

        slots.sort(key=lambda x: x.score, reverse=True)
        return slots[:top_n]

    # ─── 互动预测 ──────────────────────────────────────────

    def predict_engagement(
        self,
        content_type: ContentType,
        target_day: int,
        target_hour: int,
    ) -> Dict[str, Any]:
        """预测在特定时间发帖的预期互动量"""
        # 历史同时段数据
        similar = [
            r for r in self._history
            if r.day_of_week == target_day
            and r.hour == target_hour
            and r.content_type == content_type
        ]

        # 扩展到相邻时段 (±1h, 同天)
        nearby = [
            r for r in self._history
            if r.day_of_week == target_day
            and r.hour is not None
            and abs(r.hour - target_hour) <= 1
            and r.content_type == content_type
        ]

        # 同内容类型所有数据
        all_type = [r for r in self._history if r.content_type == content_type]

        if similar:
            dataset = similar
            method = "exact_match"
        elif nearby:
            dataset = nearby
            method = "nearby_slots"
        elif all_type:
            dataset = all_type
            method = "content_type_avg"
        else:
            return {
                "predicted_engagement": 0,
                "confidence": "none",
                "method": "no_data",
            }

        engs = [r.total_engagement for r in dataset]
        imps = [r.impressions for r in dataset]

        predicted_eng = statistics.mean(engs)
        predicted_imp = statistics.mean(imps) if imps else 0

        # 置信区间 (简单标准误)
        if len(engs) >= 2:
            se = statistics.stdev(engs) / math.sqrt(len(engs))
            ci_low = max(0, predicted_eng - 1.96 * se)
            ci_high = predicted_eng + 1.96 * se
        else:
            ci_low = predicted_eng * 0.5
            ci_high = predicted_eng * 1.5

        # 时段加成/减分
        day_name = DayOfWeek(target_day).name.lower()
        hour_label = f"{target_hour:02d}:00"

        is_peak = target_hour in self.DEFAULT_PEAK_HOURS
        is_avoid = target_hour in self.DEFAULT_AVOID_HOURS

        return {
            "day": day_name,
            "hour": hour_label,
            "content_type": content_type.value,
            "predicted_engagement": round(predicted_eng, 1),
            "predicted_impressions": round(predicted_imp, 1),
            "confidence_interval": [round(ci_low, 1), round(ci_high, 1)],
            "confidence": "high" if len(dataset) >= 10 else "medium" if len(dataset) >= 3 else "low",
            "method": method,
            "sample_size": len(dataset),
            "is_peak_hour": is_peak,
            "is_avoid_hour": is_avoid,
        }

    # ─── 调度管理 ──────────────────────────────────────────

    def schedule(self, entry: ScheduleEntry) -> Dict[str, Any]:
        """添加调度条目"""
        # 检查冲突
        conflicts = self._check_conflicts(entry)

        # 检查节假日
        holiday = self._check_holiday(entry.scheduled_at)

        self._schedule[entry.entry_id] = entry

        result = {
            "entry_id": entry.entry_id,
            "scheduled_at": entry.scheduled_at,
            "status": "scheduled",
        }

        if conflicts:
            result["conflicts"] = conflicts
        if holiday:
            result["holiday_warning"] = holiday

        return result

    def unschedule(self, entry_id: str) -> bool:
        entry = self._schedule.get(entry_id)
        if entry:
            entry.status = "cancelled"
            return True
        return False

    def get_schedule(
        self,
        days_ahead: int = 7,
        content_type: Optional[ContentType] = None,
    ) -> List[Dict[str, Any]]:
        """获取未来调度列表"""
        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(days=days_ahead)

        entries = []
        for entry in self._schedule.values():
            if entry.status != "scheduled":
                continue
            if content_type and entry.content_type != content_type:
                continue

            try:
                dt = datetime.fromisoformat(entry.scheduled_at)
                if dt > cutoff:
                    continue
            except (ValueError, TypeError):
                pass

            entries.append(entry.to_dict())

        entries.sort(key=lambda x: x.get("scheduled_at", ""))
        return entries

    def reschedule(self, entry_id: str, new_time: str) -> Dict[str, Any]:
        """重新调度"""
        entry = self._schedule.get(entry_id)
        if not entry:
            return {"error": f"entry {entry_id} not found"}

        old_time = entry.scheduled_at
        entry.scheduled_at = new_time

        return {
            "entry_id": entry_id,
            "old_time": old_time,
            "new_time": new_time,
            "status": "rescheduled",
        }

    # ─── 自动排程 ──────────────────────────────────────────

    def auto_schedule(
        self,
        content_types: List[ContentType],
        days_ahead: int = 7,
        posts_per_day: int = 3,
        min_gap_hours: int = 2,
    ) -> List[Dict[str, Any]]:
        """
        自动生成发帖计划
        基于历史最佳时段 + 内容类型偏好 + 间隔约束
        """
        best_times = self.analyze_best_times(top_n=50)

        if not best_times:
            # 使用默认
            best_times = []
            for day in range(7):
                for hour in [9, 13, 18]:
                    best_times.append(TimeSlot(day=day, hour=hour, score=0.5))

        now = datetime.now(timezone.utc)
        plan = []
        type_index = 0

        for day_offset in range(days_ahead):
            target_date = now + timedelta(days=day_offset)
            target_day = target_date.weekday()

            # 该天的最佳时段
            day_slots = [s for s in best_times if s.day == target_day]
            day_slots.sort(key=lambda x: x.score, reverse=True)

            # 如果没有历史数据对应该天
            if not day_slots:
                day_slots = [TimeSlot(day=target_day, hour=h, score=0.3) for h in [9, 13, 18]]

            posted_hours = []
            count = 0

            for slot in day_slots:
                if count >= posts_per_day:
                    break

                # 间隔检查
                if any(abs(slot.hour - h) < min_gap_hours for h in posted_hours):
                    continue

                # 阻塞检查
                if (target_day, slot.hour) in self._blocked_slots:
                    continue

                ct = content_types[type_index % len(content_types)]
                type_index += 1

                scheduled_dt = target_date.replace(
                    hour=slot.hour, minute=0, second=0, microsecond=0
                )

                entry = {
                    "day": target_date.strftime("%Y-%m-%d"),
                    "day_name": DayOfWeek(target_day).name,
                    "hour": slot.hour,
                    "content_type": ct.value,
                    "predicted_score": round(slot.score, 4),
                    "scheduled_at": scheduled_dt.isoformat(),
                }

                # 节假日检查
                holiday = self._check_holiday(scheduled_dt.isoformat())
                if holiday:
                    entry["holiday"] = holiday

                plan.append(entry)
                posted_hours.append(slot.hour)
                count += 1

        return plan

    # ─── 受众在线检测 ──────────────────────────────────────

    def audience_online_windows(self) -> Dict[str, Any]:
        """
        基于历史impression数据推断受众在线窗口
        impression高 = 受众在线
        """
        if not self._history:
            return {"windows": [], "note": "no data"}

        hour_impressions: Dict[int, List[int]] = defaultdict(list)
        day_impressions: Dict[int, List[int]] = defaultdict(list)

        for r in self._history:
            if r.hour is not None:
                hour_impressions[r.hour].append(r.impressions)
            if r.day_of_week is not None:
                day_impressions[r.day_of_week].append(r.impressions)

        # 每小时平均impression
        hourly = {}
        for h in range(24):
            imps = hour_impressions.get(h, [])
            hourly[h] = round(statistics.mean(imps), 1) if imps else 0

        # 找峰值窗口
        if not any(hourly.values()):
            return {"windows": [], "note": "no impression data"}

        max_imp = max(hourly.values())
        threshold = max_imp * 0.6

        windows = []
        in_window = False
        start = 0

        for h in range(24):
            if hourly[h] >= threshold and not in_window:
                in_window = True
                start = h
            elif hourly[h] < threshold and in_window:
                in_window = False
                windows.append({
                    "start": f"{start:02d}:00",
                    "end": f"{h:02d}:00",
                    "peak_hour": max(range(start, h), key=lambda x: hourly[x]),
                    "avg_impressions": round(statistics.mean([hourly[x] for x in range(start, h)]), 1),
                })

        if in_window:
            windows.append({
                "start": f"{start:02d}:00",
                "end": "24:00",
                "peak_hour": max(range(start, 24), key=lambda x: hourly[x]),
                "avg_impressions": round(
                    statistics.mean([hourly[x] for x in range(start, 24)]), 1
                ),
            })

        # 每天排名
        daily = {}
        for d in range(7):
            imps = day_impressions.get(d, [])
            daily[DayOfWeek(d).name] = round(statistics.mean(imps), 1) if imps else 0

        best_day = max(daily, key=daily.get) if daily else None

        return {
            "online_windows": windows,
            "hourly_impressions": hourly,
            "daily_impressions": daily,
            "best_day": best_day,
            "audience_timezone_offset": self.audience_tz,
        }

    # ─── 节假日感知 ──────────────────────────────────────

    def _check_holiday(self, date_str: str) -> Optional[str]:
        """检查日期是否是节假日"""
        try:
            dt = datetime.fromisoformat(date_str)
            key = (dt.month, dt.day)
            return self.HOLIDAYS.get(key)
        except (ValueError, TypeError):
            return None

    def upcoming_holidays(self, days_ahead: int = 30) -> List[Dict[str, str]]:
        """列出未来的节假日"""
        now = datetime.now(timezone.utc)
        result = []

        for day_offset in range(days_ahead):
            dt = now + timedelta(days=day_offset)
            key = (dt.month, dt.day)
            if key in self.HOLIDAYS:
                result.append({
                    "date": dt.strftime("%Y-%m-%d"),
                    "name": self.HOLIDAYS[key],
                    "days_away": day_offset,
                })

        return result

    # ─── 阻塞管理 ──────────────────────────────────────────

    def block_slot(self, day: int, hour: int) -> None:
        """阻塞特定时间槽"""
        self._blocked_slots.append((day, hour))

    def unblock_slot(self, day: int, hour: int) -> bool:
        try:
            self._blocked_slots.remove((day, hour))
            return True
        except ValueError:
            return False

    def blocked_slots(self) -> List[Dict[str, int]]:
        return [{"day": d, "hour": h} for d, h in self._blocked_slots]

    # ─── 冲突检测 ──────────────────────────────────────────

    def _check_conflicts(self, entry: ScheduleEntry) -> List[Dict[str, str]]:
        """检查调度冲突"""
        conflicts = []
        try:
            new_dt = datetime.fromisoformat(entry.scheduled_at)
        except (ValueError, TypeError):
            return []

        for existing in self._schedule.values():
            if existing.status != "scheduled":
                continue
            try:
                existing_dt = datetime.fromisoformat(existing.scheduled_at)
            except (ValueError, TypeError):
                continue

            diff = abs((new_dt - existing_dt).total_seconds()) / 3600
            if diff < 1:
                conflicts.append({
                    "entry_id": existing.entry_id,
                    "scheduled_at": existing.scheduled_at,
                    "gap_hours": round(diff, 2),
                })

        return conflicts

    # ─── A/B调度测试 ──────────────────────────────────────

    def ab_schedule_test(
        self,
        content_type: ContentType,
        slot_a: Tuple[int, int],  # (day, hour)
        slot_b: Tuple[int, int],
    ) -> Dict[str, Any]:
        """对比两个时段的历史表现"""
        records_a = [
            r for r in self._history
            if r.content_type == content_type
            and r.day_of_week == slot_a[0]
            and r.hour == slot_a[1]
        ]
        records_b = [
            r for r in self._history
            if r.content_type == content_type
            and r.day_of_week == slot_b[0]
            and r.hour == slot_b[1]
        ]

        def stats(recs):
            if not recs:
                return {"count": 0, "avg_engagement": 0, "avg_impressions": 0}
            return {
                "count": len(recs),
                "avg_engagement": round(statistics.mean([r.total_engagement for r in recs]), 1),
                "avg_impressions": round(statistics.mean([r.impressions for r in recs]), 1),
                "avg_engagement_rate": round(
                    statistics.mean([r.engagement_rate for r in recs if r.engagement_rate > 0] or [0]),
                    6,
                ),
            }

        day_a = DayOfWeek(slot_a[0]).name
        day_b = DayOfWeek(slot_b[0]).name

        stats_a = stats(records_a)
        stats_b = stats(records_b)

        winner = None
        if stats_a["avg_engagement"] > stats_b["avg_engagement"]:
            winner = "A"
        elif stats_b["avg_engagement"] > stats_a["avg_engagement"]:
            winner = "B"

        return {
            "content_type": content_type.value,
            "slot_a": {"day": day_a, "hour": slot_a[1], **stats_a},
            "slot_b": {"day": day_b, "hour": slot_b[1], **stats_b},
            "winner": winner,
            "recommendation": f"Slot {'A' if winner == 'A' else 'B'} ({day_a if winner == 'A' else day_b} {slot_a[1] if winner == 'A' else slot_b[1]}:00) performs better"
            if winner else "No clear winner - need more data",
        }

    # ─── 频率分析 ──────────────────────────────────────────

    def posting_frequency_analysis(self) -> Dict[str, Any]:
        """分析发帖频率与表现的关系"""
        if not self._history:
            return {"note": "no data"}

        # 按天分组
        daily_counts: Dict[str, int] = defaultdict(int)
        daily_engagement: Dict[str, float] = defaultdict(float)

        for r in self._history:
            dt = r.posted_datetime
            if dt:
                day_key = dt.strftime("%Y-%m-%d")
                daily_counts[day_key] += 1
                daily_engagement[day_key] += r.total_engagement

        if not daily_counts:
            return {"note": "no dated records"}

        counts = list(daily_counts.values())
        avg_per_day = statistics.mean(counts)

        # 高频天 vs 低频天的平均互动
        high_freq_days = {d for d, c in daily_counts.items() if c >= avg_per_day}
        low_freq_days = {d for d, c in daily_counts.items() if c < avg_per_day}

        high_avg_eng = statistics.mean(
            [daily_engagement[d] / daily_counts[d] for d in high_freq_days]
        ) if high_freq_days else 0

        low_avg_eng = statistics.mean(
            [daily_engagement[d] / daily_counts[d] for d in low_freq_days]
        ) if low_freq_days else 0

        return {
            "avg_posts_per_day": round(avg_per_day, 1),
            "max_posts_day": max(counts),
            "min_posts_day": min(counts),
            "total_days": len(daily_counts),
            "high_frequency_avg_engagement_per_post": round(high_avg_eng, 1),
            "low_frequency_avg_engagement_per_post": round(low_avg_eng, 1),
            "recommendation": (
                "Post more! Higher frequency correlates with better per-post engagement."
                if high_avg_eng >= low_avg_eng
                else "Consider reducing frequency. Quality over quantity seems to work better."
            ),
        }

    # ─── 报告 ──────────────────────────────────────────────

    def generate_schedule_report(self) -> Dict[str, Any]:
        """生成调度分析报告"""
        best_times = self.analyze_best_times(top_n=5)
        windows = self.audience_online_windows()
        freq = self.posting_frequency_analysis()
        holidays = self.upcoming_holidays(14)

        return {
            "top_5_time_slots": [
                {
                    "day": DayOfWeek(s.day).name,
                    "hour": f"{s.hour:02d}:00",
                    "score": s.score,
                    "samples": s.sample_count,
                }
                for s in best_times
            ],
            "audience_windows": windows.get("online_windows", []),
            "best_day": windows.get("best_day"),
            "posting_frequency": freq,
            "upcoming_holidays": holidays,
            "scheduled_count": sum(
                1 for e in self._schedule.values() if e.status == "scheduled"
            ),
        }
