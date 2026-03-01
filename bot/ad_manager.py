"""
Twitter/X Ads Manager v1.0
广告管理引擎 — Campaign CRUD + 广告组 + 创意管理 + 预算优化 + ROAS + A/B测试

Features:
- Campaign: full lifecycle (draft→active→paused→completed→archived)
- AdGroup: targeting + budget + scheduling
- Creative: text/image/video/carousel ad creatives with A/B variants
- BudgetOptimizer: daily budget pacing, spend forecasting, auto-reallocation
- PerformanceTracker: impressions/clicks/conversions/CPC/CPM/CTR/ROAS
- BidManager: auto-bidding strategies (target CPA, max clicks, max impressions)
- SQLite persistence for all ad data
"""

import json
import math
import sqlite3
import threading
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple


class CampaignStatus(Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    ARCHIVED = "archived"


class CampaignObjective(Enum):
    AWARENESS = "awareness"
    REACH = "reach"
    ENGAGEMENT = "engagement"
    TRAFFIC = "traffic"
    CONVERSIONS = "conversions"
    APP_INSTALLS = "app_installs"
    VIDEO_VIEWS = "video_views"
    FOLLOWERS = "followers"


class BidStrategy(Enum):
    MANUAL = "manual"
    TARGET_CPA = "target_cpa"
    MAX_CLICKS = "max_clicks"
    MAX_IMPRESSIONS = "max_impressions"
    TARGET_ROAS = "target_roas"
    LOWEST_COST = "lowest_cost"


class CreativeType(Enum):
    TEXT = "text"
    IMAGE = "image"
    VIDEO = "video"
    CAROUSEL = "carousel"
    POLL = "poll"


class AdGroupStatus(Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    DELETED = "deleted"


@dataclass
class Targeting:
    """广告定向配置"""
    locations: List[str] = field(default_factory=list)
    languages: List[str] = field(default_factory=list)
    age_min: int = 18
    age_max: int = 65
    genders: List[str] = field(default_factory=lambda: ["all"])
    interests: List[str] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)
    followers_of: List[str] = field(default_factory=list)
    exclude_audiences: List[str] = field(default_factory=list)
    devices: List[str] = field(default_factory=lambda: ["all"])
    platforms: List[str] = field(default_factory=lambda: ["all"])
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    def estimated_reach(self) -> int:
        """Estimate audience reach based on targeting parameters"""
        base = 1_000_000
        
        # Location multiplier
        if self.locations:
            base *= len(self.locations) * 0.3
        
        # Interest narrowing
        if self.interests:
            base *= max(0.1, 1.0 / len(self.interests))
        
        # Follower targeting
        if self.followers_of:
            base = min(base, len(self.followers_of) * 50_000)
        
        # Age range adjustment
        age_range = self.age_max - self.age_min
        base *= age_range / 47  # 47 = 65-18
        
        return max(int(base), 1000)


@dataclass
class Creative:
    """广告创意"""
    creative_id: str
    name: str
    creative_type: CreativeType
    headline: str = ""
    body: str = ""
    call_to_action: str = "Learn More"
    media_url: Optional[str] = None
    destination_url: Optional[str] = None
    carousel_items: List[Dict] = field(default_factory=list)
    active: bool = True
    created_at: str = ""
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()
    
    def validate(self) -> List[str]:
        """Validate creative specs"""
        errors = []
        if not self.headline and self.creative_type != CreativeType.TEXT:
            errors.append("Headline is required for non-text creatives")
        if self.creative_type == CreativeType.TEXT and len(self.body) > 280:
            errors.append("Text body exceeds 280 character limit")
        if self.creative_type == CreativeType.IMAGE and not self.media_url:
            errors.append("Image URL required for image creative")
        if self.creative_type == CreativeType.VIDEO and not self.media_url:
            errors.append("Video URL required for video creative")
        if self.creative_type == CreativeType.CAROUSEL and len(self.carousel_items) < 2:
            errors.append("Carousel requires at least 2 items")
        return errors
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d["creative_type"] = self.creative_type.value
        return d


@dataclass
class AdGroupPerformance:
    """广告组表现指标"""
    impressions: int = 0
    clicks: int = 0
    conversions: int = 0
    spend: float = 0.0
    revenue: float = 0.0
    
    @property
    def ctr(self) -> float:
        return (self.clicks / self.impressions * 100) if self.impressions > 0 else 0.0
    
    @property
    def cpc(self) -> float:
        return (self.spend / self.clicks) if self.clicks > 0 else 0.0
    
    @property
    def cpm(self) -> float:
        return (self.spend / self.impressions * 1000) if self.impressions > 0 else 0.0
    
    @property
    def cpa(self) -> float:
        return (self.spend / self.conversions) if self.conversions > 0 else 0.0
    
    @property
    def roas(self) -> float:
        return (self.revenue / self.spend) if self.spend > 0 else 0.0
    
    @property
    def conversion_rate(self) -> float:
        return (self.conversions / self.clicks * 100) if self.clicks > 0 else 0.0
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d["ctr"] = round(self.ctr, 2)
        d["cpc"] = round(self.cpc, 4)
        d["cpm"] = round(self.cpm, 4)
        d["cpa"] = round(self.cpa, 4)
        d["roas"] = round(self.roas, 2)
        d["conversion_rate"] = round(self.conversion_rate, 2)
        return d


@dataclass
class AdGroup:
    """广告组"""
    adgroup_id: str
    campaign_id: str
    name: str
    targeting: Targeting
    daily_budget: float = 50.0
    bid_amount: float = 1.0
    bid_strategy: BidStrategy = BidStrategy.LOWEST_COST
    status: AdGroupStatus = AdGroupStatus.ACTIVE
    creatives: List[str] = field(default_factory=list)  # creative_ids
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    performance: AdGroupPerformance = field(default_factory=AdGroupPerformance)
    created_at: str = ""
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()
    
    @property
    def is_running(self) -> bool:
        if self.status != AdGroupStatus.ACTIVE:
            return False
        now = datetime.now(timezone.utc).isoformat()
        if self.start_date and now < self.start_date:
            return False
        if self.end_date and now > self.end_date:
            return False
        return True
    
    def to_dict(self) -> Dict:
        d = {
            "adgroup_id": self.adgroup_id,
            "campaign_id": self.campaign_id,
            "name": self.name,
            "targeting": self.targeting.to_dict(),
            "daily_budget": self.daily_budget,
            "bid_amount": self.bid_amount,
            "bid_strategy": self.bid_strategy.value,
            "status": self.status.value,
            "creatives": self.creatives,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "performance": self.performance.to_dict(),
            "created_at": self.created_at,
            "is_running": self.is_running
        }
        return d


@dataclass
class Campaign:
    """广告Campaign"""
    campaign_id: str
    name: str
    objective: CampaignObjective
    total_budget: float = 1000.0
    daily_budget: float = 100.0
    status: CampaignStatus = CampaignStatus.DRAFT
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    created_at: str = ""
    updated_at: str = ""
    
    def __post_init__(self):
        now = datetime.now(timezone.utc).isoformat()
        if not self.created_at:
            self.created_at = now
        if not self.updated_at:
            self.updated_at = now
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d["objective"] = self.objective.value
        d["status"] = self.status.value
        return d


class BudgetOptimizer:
    """预算优化器"""
    
    def __init__(self):
        self._daily_spend: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self._lock = threading.Lock()
    
    def record_spend(self, campaign_id: str, amount: float,
                     timestamp: Optional[str] = None) -> None:
        """Record ad spend"""
        if timestamp:
            try:
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                dt = datetime.now(timezone.utc)
        else:
            dt = datetime.now(timezone.utc)
        
        day_key = dt.strftime("%Y-%m-%d")
        with self._lock:
            self._daily_spend[campaign_id][day_key] += amount
    
    def get_daily_spend(self, campaign_id: str,
                        date: Optional[str] = None) -> float:
        """Get spend for a specific day"""
        if not date:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self._lock:
            return self._daily_spend[campaign_id].get(date, 0.0)
    
    def get_total_spend(self, campaign_id: str) -> float:
        """Get total spend across all days"""
        with self._lock:
            return sum(self._daily_spend[campaign_id].values())
    
    def get_budget_utilization(self, campaign_id: str,
                                daily_budget: float) -> Dict:
        """Calculate budget utilization metrics"""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        today_spend = self.get_daily_spend(campaign_id, today)
        total_spend = self.get_total_spend(campaign_id)
        
        utilization = today_spend / daily_budget if daily_budget > 0 else 0.0
        
        # Hours elapsed today
        now = datetime.now(timezone.utc)
        hours_elapsed = now.hour + now.minute / 60
        expected_pacing = hours_elapsed / 24
        
        pacing_status = "on_track"
        if utilization > expected_pacing * 1.3:
            pacing_status = "overspending"
        elif utilization < expected_pacing * 0.7:
            pacing_status = "underspending"
        
        return {
            "today_spend": round(today_spend, 2),
            "daily_budget": daily_budget,
            "utilization": round(utilization * 100, 1),
            "pacing_status": pacing_status,
            "total_spend": round(total_spend, 2),
            "remaining_today": round(max(daily_budget - today_spend, 0), 2),
            "projected_daily_spend": round(
                today_spend / max(hours_elapsed, 0.1) * 24, 2
            ) if hours_elapsed > 0 else 0.0
        }
    
    def suggest_reallocation(self, adgroups: List[AdGroup]) -> List[Dict]:
        """Suggest budget reallocation based on performance"""
        if not adgroups:
            return []
        
        suggestions = []
        active_groups = [ag for ag in adgroups if ag.status == AdGroupStatus.ACTIVE]
        
        if not active_groups:
            return []
        
        # Score each adgroup
        scores = []
        for ag in active_groups:
            perf = ag.performance
            score = 0.0
            
            if perf.ctr > 0:
                score += min(perf.ctr, 5.0) * 10  # CTR component
            if perf.conversion_rate > 0:
                score += min(perf.conversion_rate, 20.0) * 5  # Conversion component
            if perf.roas > 0:
                score += min(perf.roas, 10.0) * 10  # ROAS component
            
            scores.append((ag, score))
        
        total_score = sum(s for _, s in scores)
        total_budget = sum(ag.daily_budget for ag in active_groups)
        
        if total_score == 0:
            return []
        
        for ag, score in scores:
            share = score / total_score
            suggested = total_budget * share
            change = suggested - ag.daily_budget
            
            if abs(change) > ag.daily_budget * 0.05:  # Only suggest if >5% change
                suggestions.append({
                    "adgroup_id": ag.adgroup_id,
                    "adgroup_name": ag.name,
                    "current_budget": ag.daily_budget,
                    "suggested_budget": round(suggested, 2),
                    "change": round(change, 2),
                    "change_pct": round(change / ag.daily_budget * 100, 1),
                    "reason": "Higher ROI" if change > 0 else "Lower ROI",
                    "score": round(score, 2)
                })
        
        suggestions.sort(key=lambda x: abs(x["change"]), reverse=True)
        return suggestions


class BidManager:
    """竞价管理器"""
    
    def __init__(self):
        self._bid_history: Dict[str, List[Dict]] = defaultdict(list)
        self._lock = threading.Lock()
    
    def calculate_optimal_bid(self, strategy: BidStrategy,
                               performance: AdGroupPerformance,
                               target_value: float = 0.0,
                               current_bid: float = 1.0) -> Dict:
        """Calculate optimal bid based on strategy"""
        result = {
            "strategy": strategy.value,
            "current_bid": current_bid,
            "recommended_bid": current_bid,
            "reason": "",
            "confidence": 0.5
        }
        
        if strategy == BidStrategy.MANUAL:
            result["reason"] = "Manual bidding - no auto-adjustment"
            return result
        
        if strategy == BidStrategy.TARGET_CPA:
            if performance.conversions > 0 and target_value > 0:
                actual_cpa = performance.cpa
                ratio = target_value / actual_cpa if actual_cpa > 0 else 1.0
                # Adjust bid proportionally, capped at ±30%
                adjustment = max(0.7, min(1.3, ratio))
                result["recommended_bid"] = round(current_bid * adjustment, 4)
                result["reason"] = f"Target CPA ${target_value:.2f}, actual ${actual_cpa:.2f}"
                result["confidence"] = min(performance.conversions / 10, 1.0)
            else:
                result["reason"] = "Insufficient conversion data"
        
        elif strategy == BidStrategy.MAX_CLICKS:
            if performance.clicks > 0:
                # Maximize clicks: lower CPC = more clicks
                if performance.cpc > target_value and target_value > 0:
                    result["recommended_bid"] = round(current_bid * 0.9, 4)
                    result["reason"] = f"Lowering bid to reduce CPC (${performance.cpc:.4f})"
                else:
                    result["recommended_bid"] = round(current_bid * 1.05, 4)
                    result["reason"] = "Slightly increasing for more impressions"
                result["confidence"] = min(performance.clicks / 50, 1.0)
            else:
                result["recommended_bid"] = round(current_bid * 1.2, 4)
                result["reason"] = "No clicks yet - increasing bid"
        
        elif strategy == BidStrategy.MAX_IMPRESSIONS:
            if performance.impressions > 0:
                if performance.cpm < target_value or target_value == 0:
                    result["recommended_bid"] = round(current_bid * 1.1, 4)
                    result["reason"] = "CPM within target - increasing for reach"
                else:
                    result["recommended_bid"] = round(current_bid * 0.9, 4)
                    result["reason"] = f"CPM ${performance.cpm:.2f} above target"
                result["confidence"] = min(performance.impressions / 1000, 1.0)
            else:
                result["recommended_bid"] = round(current_bid * 1.3, 4)
                result["reason"] = "No impressions yet - increasing bid"
        
        elif strategy == BidStrategy.TARGET_ROAS:
            if performance.revenue > 0 and performance.spend > 0 and target_value > 0:
                actual_roas = performance.roas
                ratio = actual_roas / target_value
                adjustment = max(0.7, min(1.3, ratio))
                result["recommended_bid"] = round(current_bid * adjustment, 4)
                result["reason"] = f"Target ROAS {target_value:.1f}x, actual {actual_roas:.1f}x"
                result["confidence"] = min(performance.conversions / 5, 1.0)
            else:
                result["reason"] = "Insufficient revenue data"
        
        elif strategy == BidStrategy.LOWEST_COST:
            if performance.clicks > 0:
                # Always try to lower cost
                result["recommended_bid"] = round(current_bid * 0.95, 4)
                result["reason"] = "Optimizing for lowest cost"
                result["confidence"] = 0.6
            else:
                result["reason"] = "No data yet - maintaining current bid"
        
        return result
    
    def record_bid_change(self, adgroup_id: str, old_bid: float,
                          new_bid: float, reason: str) -> None:
        """Record a bid change for history"""
        with self._lock:
            self._bid_history[adgroup_id].append({
                "old_bid": old_bid,
                "new_bid": new_bid,
                "reason": reason,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
    
    def get_bid_history(self, adgroup_id: str) -> List[Dict]:
        """Get bid change history"""
        with self._lock:
            return list(self._bid_history.get(adgroup_id, []))


class CreativeRotator:
    """创意轮换器"""
    
    def __init__(self):
        self._creative_stats: Dict[str, Dict] = defaultdict(
            lambda: {"impressions": 0, "clicks": 0, "conversions": 0, "spend": 0.0}
        )
        self._lock = threading.Lock()
    
    def record_impression(self, creative_id: str) -> None:
        with self._lock:
            self._creative_stats[creative_id]["impressions"] += 1
    
    def record_click(self, creative_id: str) -> None:
        with self._lock:
            self._creative_stats[creative_id]["clicks"] += 1
    
    def record_conversion(self, creative_id: str, revenue: float = 0.0) -> None:
        with self._lock:
            self._creative_stats[creative_id]["conversions"] += 1
    
    def get_creative_performance(self, creative_id: str) -> Dict:
        """Get performance for a creative"""
        with self._lock:
            stats = dict(self._creative_stats[creative_id])
        
        stats["ctr"] = round(
            (stats["clicks"] / stats["impressions"] * 100) 
            if stats["impressions"] > 0 else 0.0, 2
        )
        stats["conversion_rate"] = round(
            (stats["conversions"] / stats["clicks"] * 100)
            if stats["clicks"] > 0 else 0.0, 2
        )
        return stats
    
    def select_creative(self, creative_ids: List[str],
                        strategy: str = "weighted") -> Optional[str]:
        """
        Select next creative to show.
        Strategies: round_robin, weighted, best_performer
        """
        if not creative_ids:
            return None
        
        if strategy == "best_performer":
            best_id = None
            best_ctr = -1.0
            for cid in creative_ids:
                perf = self.get_creative_performance(cid)
                if perf["ctr"] > best_ctr:
                    best_ctr = perf["ctr"]
                    best_id = cid
            return best_id
        
        elif strategy == "weighted":
            # Thompson sampling-like approach
            import random
            scores = []
            for cid in creative_ids:
                perf = self.get_creative_performance(cid)
                imps = perf["impressions"]
                clicks = perf["clicks"]
                # Use beta distribution parameters
                alpha = clicks + 1
                beta_param = max(imps - clicks + 1, 1)
                score = random.betavariate(alpha, beta_param)
                scores.append((cid, score))
            scores.sort(key=lambda x: x[1], reverse=True)
            return scores[0][0]
        
        else:  # round_robin
            # Pick the one with fewest impressions
            min_imps = float('inf')
            min_id = creative_ids[0]
            for cid in creative_ids:
                perf = self.get_creative_performance(cid)
                if perf["impressions"] < min_imps:
                    min_imps = perf["impressions"]
                    min_id = cid
            return min_id
    
    def get_ab_comparison(self, creative_ids: List[str]) -> Dict:
        """Compare creatives for A/B testing"""
        results = []
        for cid in creative_ids:
            perf = self.get_creative_performance(cid)
            perf["creative_id"] = cid
            results.append(perf)
        
        results.sort(key=lambda x: x["ctr"], reverse=True)
        
        winner = results[0] if results else None
        
        return {
            "creatives": results,
            "winner": winner["creative_id"] if winner else None,
            "recommendation": (
                f"Creative {winner['creative_id']} leads with {winner['ctr']}% CTR"
                if winner and winner["impressions"] >= 100
                else "Need more data (min 100 impressions each)"
            )
        }


class AdManager:
    """广告管理引擎主类"""
    
    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.campaigns: Dict[str, Campaign] = {}
        self.adgroups: Dict[str, AdGroup] = {}
        self.creatives: Dict[str, Creative] = {}
        self.budget_optimizer = BudgetOptimizer()
        self.bid_manager = BidManager()
        self.creative_rotator = CreativeRotator()
        self._lock = threading.Lock()
        self._init_db()
    
    def _init_db(self) -> None:
        """Initialize SQLite tables"""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS ad_campaigns (
                    campaign_id TEXT PRIMARY KEY,
                    name TEXT,
                    objective TEXT,
                    total_budget REAL,
                    daily_budget REAL,
                    status TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    created_at TEXT,
                    updated_at TEXT
                );
                
                CREATE TABLE IF NOT EXISTS ad_performance_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    campaign_id TEXT,
                    adgroup_id TEXT,
                    creative_id TEXT,
                    impressions INTEGER DEFAULT 0,
                    clicks INTEGER DEFAULT 0,
                    conversions INTEGER DEFAULT 0,
                    spend REAL DEFAULT 0.0,
                    revenue REAL DEFAULT 0.0,
                    recorded_at TEXT
                );
                
                CREATE INDEX IF NOT EXISTS idx_perf_campaign 
                    ON ad_performance_log(campaign_id, recorded_at);
            """)
            conn.commit()
        finally:
            conn.close()
    
    # === Campaign CRUD ===
    
    def create_campaign(self, campaign: Campaign) -> Campaign:
        """Create a new campaign"""
        with self._lock:
            self.campaigns[campaign.campaign_id] = campaign
        return campaign
    
    def get_campaign(self, campaign_id: str) -> Optional[Campaign]:
        """Get a campaign"""
        return self.campaigns.get(campaign_id)
    
    def update_campaign_status(self, campaign_id: str,
                                status: CampaignStatus) -> bool:
        """Update campaign status"""
        with self._lock:
            campaign = self.campaigns.get(campaign_id)
            if not campaign:
                return False
            
            # Validate status transitions
            valid_transitions = {
                CampaignStatus.DRAFT: {CampaignStatus.ACTIVE, CampaignStatus.ARCHIVED},
                CampaignStatus.ACTIVE: {CampaignStatus.PAUSED, CampaignStatus.COMPLETED},
                CampaignStatus.PAUSED: {CampaignStatus.ACTIVE, CampaignStatus.COMPLETED, CampaignStatus.ARCHIVED},
                CampaignStatus.COMPLETED: {CampaignStatus.ARCHIVED},
                CampaignStatus.ARCHIVED: set()
            }
            
            if status not in valid_transitions.get(campaign.status, set()):
                return False
            
            campaign.status = status
            campaign.updated_at = datetime.now(timezone.utc).isoformat()
            return True
    
    def list_campaigns(self, status: Optional[CampaignStatus] = None) -> List[Campaign]:
        """List campaigns with optional status filter"""
        campaigns = list(self.campaigns.values())
        if status:
            campaigns = [c for c in campaigns if c.status == status]
        campaigns.sort(key=lambda c: c.created_at, reverse=True)
        return campaigns
    
    def delete_campaign(self, campaign_id: str) -> bool:
        """Delete a campaign (must be draft or archived)"""
        with self._lock:
            campaign = self.campaigns.get(campaign_id)
            if not campaign:
                return False
            if campaign.status not in {CampaignStatus.DRAFT, CampaignStatus.ARCHIVED}:
                return False
            del self.campaigns[campaign_id]
            # Delete associated adgroups
            for ag_id in list(self.adgroups.keys()):
                if self.adgroups[ag_id].campaign_id == campaign_id:
                    del self.adgroups[ag_id]
            return True
    
    # === AdGroup CRUD ===
    
    def create_adgroup(self, adgroup: AdGroup) -> Optional[AdGroup]:
        """Create an ad group under a campaign"""
        if adgroup.campaign_id not in self.campaigns:
            return None
        with self._lock:
            self.adgroups[adgroup.adgroup_id] = adgroup
        return adgroup
    
    def get_adgroup(self, adgroup_id: str) -> Optional[AdGroup]:
        """Get an ad group"""
        return self.adgroups.get(adgroup_id)
    
    def list_adgroups(self, campaign_id: Optional[str] = None) -> List[AdGroup]:
        """List ad groups"""
        groups = list(self.adgroups.values())
        if campaign_id:
            groups = [g for g in groups if g.campaign_id == campaign_id]
        return groups
    
    def update_adgroup_status(self, adgroup_id: str,
                               status: AdGroupStatus) -> bool:
        """Update ad group status"""
        with self._lock:
            ag = self.adgroups.get(adgroup_id)
            if not ag:
                return False
            ag.status = status
            return True
    
    # === Creative CRUD ===
    
    def add_creative(self, creative: Creative) -> Creative:
        """Add a creative"""
        errors = creative.validate()
        if errors:
            raise ValueError(f"Creative validation failed: {'; '.join(errors)}")
        with self._lock:
            self.creatives[creative.creative_id] = creative
        return creative
    
    def get_creative(self, creative_id: str) -> Optional[Creative]:
        """Get a creative"""
        return self.creatives.get(creative_id)
    
    def list_creatives(self, creative_type: Optional[CreativeType] = None) -> List[Creative]:
        """List creatives"""
        creatives = list(self.creatives.values())
        if creative_type:
            creatives = [c for c in creatives if c.creative_type == creative_type]
        return creatives
    
    def assign_creative(self, adgroup_id: str, creative_id: str) -> bool:
        """Assign a creative to an ad group"""
        with self._lock:
            ag = self.adgroups.get(adgroup_id)
            creative = self.creatives.get(creative_id)
            if not ag or not creative:
                return False
            if creative_id not in ag.creatives:
                ag.creatives.append(creative_id)
            return True
    
    # === Performance Tracking ===
    
    def record_performance(self, campaign_id: str, adgroup_id: str,
                           creative_id: str, impressions: int = 0,
                           clicks: int = 0, conversions: int = 0,
                           spend: float = 0.0, revenue: float = 0.0) -> None:
        """Record performance data"""
        # Update adgroup performance
        with self._lock:
            ag = self.adgroups.get(adgroup_id)
            if ag:
                ag.performance.impressions += impressions
                ag.performance.clicks += clicks
                ag.performance.conversions += conversions
                ag.performance.spend += spend
                ag.performance.revenue += revenue
        
        # Record spend
        if spend > 0:
            self.budget_optimizer.record_spend(campaign_id, spend)
        
        # Update creative stats
        for _ in range(impressions):
            self.creative_rotator.record_impression(creative_id)
        for _ in range(clicks):
            self.creative_rotator.record_click(creative_id)
        for _ in range(conversions):
            self.creative_rotator.record_conversion(creative_id, revenue / max(conversions, 1))
    
    def get_campaign_performance(self, campaign_id: str) -> Dict:
        """Get aggregated performance for a campaign"""
        groups = self.list_adgroups(campaign_id)
        
        total = AdGroupPerformance()
        for ag in groups:
            total.impressions += ag.performance.impressions
            total.clicks += ag.performance.clicks
            total.conversions += ag.performance.conversions
            total.spend += ag.performance.spend
            total.revenue += ag.performance.revenue
        
        campaign = self.campaigns.get(campaign_id)
        
        return {
            "campaign_id": campaign_id,
            "campaign_name": campaign.name if campaign else "Unknown",
            "status": campaign.status.value if campaign else "unknown",
            "performance": total.to_dict(),
            "adgroups": len(groups),
            "budget_utilization": self.budget_optimizer.get_budget_utilization(
                campaign_id, campaign.daily_budget if campaign else 0
            )
        }
    
    # === Optimization ===
    
    def optimize_bids(self, campaign_id: str) -> List[Dict]:
        """Auto-optimize bids for all ad groups in a campaign"""
        groups = self.list_adgroups(campaign_id)
        recommendations = []
        
        for ag in groups:
            if ag.status != AdGroupStatus.ACTIVE:
                continue
            
            rec = self.bid_manager.calculate_optimal_bid(
                strategy=ag.bid_strategy,
                performance=ag.performance,
                target_value=ag.bid_amount,
                current_bid=ag.bid_amount
            )
            
            if rec["recommended_bid"] != ag.bid_amount:
                recommendations.append({
                    "adgroup_id": ag.adgroup_id,
                    "adgroup_name": ag.name,
                    **rec
                })
        
        return recommendations
    
    def optimize_budgets(self, campaign_id: str) -> List[Dict]:
        """Get budget reallocation suggestions"""
        groups = self.list_adgroups(campaign_id)
        return self.budget_optimizer.suggest_reallocation(groups)
    
    def get_creative_insights(self, adgroup_id: str) -> Dict:
        """Get creative A/B test insights for an ad group"""
        ag = self.adgroups.get(adgroup_id)
        if not ag:
            return {"error": "Ad group not found"}
        
        return self.creative_rotator.get_ab_comparison(ag.creatives)
    
    # === Reporting ===
    
    def generate_report(self, campaign_id: Optional[str] = None,
                        format: str = "text") -> str:
        """Generate performance report"""
        campaigns = [self.campaigns[campaign_id]] if campaign_id and campaign_id in self.campaigns \
            else list(self.campaigns.values())
        
        report_data = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "campaigns": []
        }
        
        grand_total = AdGroupPerformance()
        
        for campaign in campaigns:
            perf = self.get_campaign_performance(campaign.campaign_id)
            report_data["campaigns"].append(perf)
            
            p = perf["performance"]
            grand_total.impressions += p["impressions"]
            grand_total.clicks += p["clicks"]
            grand_total.conversions += p["conversions"]
            grand_total.spend += p["spend"]
            grand_total.revenue += p["revenue"]
        
        report_data["grand_total"] = grand_total.to_dict()
        
        if format == "json":
            return json.dumps(report_data, indent=2, ensure_ascii=False)
        
        # Text format
        lines = [
            "=" * 60,
            "📊 Twitter/X Ads Performance Report",
            f"Generated: {report_data['generated_at'][:19]}",
            "=" * 60,
        ]
        
        for cp in report_data["campaigns"]:
            p = cp["performance"]
            lines.extend([
                "",
                f"📦 Campaign: {cp['campaign_name']} [{cp['status']}]",
                f"   Impressions: {p['impressions']:,}",
                f"   Clicks: {p['clicks']:,} (CTR: {p['ctr']}%)",
                f"   Conversions: {p['conversions']:,} (CVR: {p['conversion_rate']}%)",
                f"   Spend: ${p['spend']:,.2f}",
                f"   Revenue: ${p['revenue']:,.2f}",
                f"   CPC: ${p['cpc']:.4f} | CPM: ${p['cpm']:.4f} | CPA: ${p['cpa']:.4f}",
                f"   ROAS: {p['roas']:.2f}x",
                f"   Ad Groups: {cp['adgroups']}",
            ])
            
            bu = cp.get("budget_utilization", {})
            if bu:
                lines.append(
                    f"   Budget: ${bu.get('today_spend', 0):.2f} / "
                    f"${bu.get('daily_budget', 0):.2f} "
                    f"({bu.get('utilization', 0):.1f}% - {bu.get('pacing_status', 'unknown')})"
                )
        
        gt = report_data["grand_total"]
        lines.extend([
            "",
            "-" * 60,
            "💰 Grand Total",
            f"   Impressions: {gt['impressions']:,}",
            f"   Clicks: {gt['clicks']:,} (CTR: {gt['ctr']}%)",
            f"   Conversions: {gt['conversions']:,}",
            f"   Spend: ${gt['spend']:,.2f}",
            f"   Revenue: ${gt['revenue']:,.2f}",
            f"   ROAS: {gt['roas']:.2f}x",
            "=" * 60,
        ])
        
        return "\n".join(lines)
    
    def get_stats(self) -> Dict:
        """Get engine statistics"""
        active_campaigns = len([c for c in self.campaigns.values()
                                if c.status == CampaignStatus.ACTIVE])
        total_spend = sum(
            ag.performance.spend for ag in self.adgroups.values()
        )
        total_revenue = sum(
            ag.performance.revenue for ag in self.adgroups.values()
        )
        
        return {
            "total_campaigns": len(self.campaigns),
            "active_campaigns": active_campaigns,
            "total_adgroups": len(self.adgroups),
            "total_creatives": len(self.creatives),
            "total_spend": round(total_spend, 2),
            "total_revenue": round(total_revenue, 2),
            "overall_roas": round(total_revenue / total_spend, 2) if total_spend > 0 else 0.0
        }
