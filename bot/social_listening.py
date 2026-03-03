"""
Social Listening Engine v1.0
实时社交聆听引擎 — 关键词监控 + 话题追踪 + 竞品提及 + 告警规则 + 趋势分析

Features:
- ListeningQuery: keyword/hashtag/mention monitoring rules
- VolumeTracker: real-time volume tracking with moving averages
- AlertRule: configurable alert triggers (volume spike, sentiment shift, keyword co-occurrence)
- CompetitorMentionTracker: competitive brand mention monitoring
- ListeningReport: comprehensive listening reports (text/json/csv)
- SQLite persistence for all listening data
"""

import re
import json
import sqlite3
import threading
import csv
import io
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict, Tuple, Callable


class QueryType(Enum):
    KEYWORD = "keyword"
    HASHTAG = "hashtag"
    MENTION = "mention"
    PHRASE = "phrase"
    BOOLEAN = "boolean"


class AlertType(Enum):
    VOLUME_SPIKE = "volume_spike"
    SENTIMENT_SHIFT = "sentiment_shift"
    KEYWORD_COOCCURRENCE = "keyword_cooccurrence"
    NEW_INFLUENCER = "new_influencer"
    CRISIS_SIGNAL = "crisis_signal"
    COMPETITOR_SURGE = "competitor_surge"


class AlertSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class SentimentLabel(Enum):
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"
    MIXED = "mixed"


@dataclass
class ListeningQuery:
    """监听查询规则"""
    query_id: str
    name: str
    query_type: QueryType
    keywords: List[str]
    exclude_keywords: List[str] = field(default_factory=list)
    languages: List[str] = field(default_factory=lambda: ["en"])
    active: bool = True
    created_at: str = ""
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()
    
    def matches(self, text: str) -> bool:
        """Check if text matches this query"""
        text_lower = text.lower()
        # Check exclude first
        for excl in self.exclude_keywords:
            if excl.lower() in text_lower:
                return False
        # Check inclusion
        if self.query_type == QueryType.BOOLEAN:
            return self._boolean_match(text_lower)
        for kw in self.keywords:
            if kw.lower() in text_lower:
                return True
        return False
    
    def _boolean_match(self, text_lower: str) -> bool:
        """Simple boolean matching: AND/OR/NOT"""
        # All keywords must be present (AND logic for boolean)
        for kw in self.keywords:
            parts = kw.lower().split(" or ")
            if not any(p.strip() in text_lower for p in parts):
                return False
        return True
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d["query_type"] = self.query_type.value
        return d


@dataclass
class ListeningMatch:
    """监听匹配结果"""
    match_id: str
    query_id: str
    tweet_id: str
    author: str
    author_followers: int
    text: str
    sentiment: SentimentLabel
    sentiment_score: float
    matched_keywords: List[str]
    timestamp: str = ""
    reach: int = 0
    engagement: int = 0
    
    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()
    
    @property
    def is_influencer(self) -> bool:
        return self.author_followers >= 10000
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d["sentiment"] = self.sentiment.value
        d["is_influencer"] = self.is_influencer
        return d


@dataclass
class AlertRule:
    """告警规则"""
    rule_id: str
    name: str
    alert_type: AlertType
    query_id: str
    threshold: float  # e.g., 2.0 = 2x normal volume
    window_minutes: int = 60
    cooldown_minutes: int = 30
    severity: AlertSeverity = AlertSeverity.WARNING
    active: bool = True
    last_triggered: Optional[str] = None
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d["alert_type"] = self.alert_type.value
        d["severity"] = self.severity.value
        return d


@dataclass
class Alert:
    """触发的告警"""
    alert_id: str
    rule_id: str
    alert_type: AlertType
    severity: AlertSeverity
    message: str
    data: Dict = field(default_factory=dict)
    triggered_at: str = ""
    acknowledged: bool = False
    
    def __post_init__(self):
        if not self.triggered_at:
            self.triggered_at = datetime.now(timezone.utc).isoformat()
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d["alert_type"] = self.alert_type.value
        d["severity"] = self.severity.value
        return d


class VolumeTracker:
    """实时话题量追踪器"""
    
    def __init__(self, window_size: int = 24):
        """
        Args:
            window_size: Number of hourly buckets to track
        """
        self.window_size = window_size
        self._hourly_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._lock = threading.Lock()
    
    def record(self, query_id: str, timestamp: Optional[str] = None) -> None:
        """Record a match event"""
        if timestamp:
            try:
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                dt = datetime.now(timezone.utc)
        else:
            dt = datetime.now(timezone.utc)
        
        hour_key = dt.strftime("%Y-%m-%d-%H")
        with self._lock:
            self._hourly_counts[query_id][hour_key] += 1
    
    def get_current_volume(self, query_id: str, hours: int = 1) -> int:
        """Get volume for the last N hours"""
        now = datetime.now(timezone.utc)
        total = 0
        with self._lock:
            for i in range(hours):
                dt = now - timedelta(hours=i)
                hour_key = dt.strftime("%Y-%m-%d-%H")
                total += self._hourly_counts[query_id].get(hour_key, 0)
        return total
    
    def get_moving_average(self, query_id: str, window_hours: int = 24) -> float:
        """Calculate moving average over window"""
        now = datetime.now(timezone.utc)
        total = 0
        count = 0
        with self._lock:
            for i in range(window_hours):
                dt = now - timedelta(hours=i)
                hour_key = dt.strftime("%Y-%m-%d-%H")
                val = self._hourly_counts[query_id].get(hour_key, 0)
                total += val
                count += 1
        return total / max(count, 1)
    
    def detect_spike(self, query_id: str, threshold: float = 2.0,
                     recent_hours: int = 1, baseline_hours: int = 24) -> Tuple[bool, float]:
        """
        Detect volume spike compared to baseline.
        Returns (is_spike, ratio)
        """
        recent = self.get_current_volume(query_id, recent_hours)
        baseline = self.get_moving_average(query_id, baseline_hours)
        
        if baseline == 0:
            return (recent > 0, float('inf') if recent > 0 else 0.0)
        
        ratio = recent / baseline
        return (ratio >= threshold, round(ratio, 2))
    
    def get_hourly_breakdown(self, query_id: str, hours: int = 24) -> List[Dict]:
        """Get hourly breakdown for charting"""
        now = datetime.now(timezone.utc)
        breakdown = []
        with self._lock:
            for i in range(hours - 1, -1, -1):
                dt = now - timedelta(hours=i)
                hour_key = dt.strftime("%Y-%m-%d-%H")
                count = self._hourly_counts[query_id].get(hour_key, 0)
                breakdown.append({
                    "hour": hour_key,
                    "count": count,
                    "datetime": dt.isoformat()
                })
        return breakdown
    
    def clear(self, query_id: Optional[str] = None) -> None:
        """Clear tracking data"""
        with self._lock:
            if query_id:
                self._hourly_counts.pop(query_id, None)
            else:
                self._hourly_counts.clear()


class CompetitorTracker:
    """竞品提及追踪器"""
    
    def __init__(self):
        self._competitors: Dict[str, List[str]] = {}  # name -> keywords
        self._mentions: Dict[str, List[ListeningMatch]] = defaultdict(list)
        self._lock = threading.Lock()
    
    def add_competitor(self, name: str, keywords: List[str]) -> None:
        """Register a competitor to track"""
        with self._lock:
            self._competitors[name] = [k.lower() for k in keywords]
    
    def remove_competitor(self, name: str) -> bool:
        """Remove a competitor"""
        with self._lock:
            if name in self._competitors:
                del self._competitors[name]
                self._mentions.pop(name, None)
                return True
            return False
    
    def list_competitors(self) -> Dict[str, List[str]]:
        """List all tracked competitors"""
        with self._lock:
            return dict(self._competitors)
    
    def check_mention(self, text: str, match: ListeningMatch) -> List[str]:
        """Check if text mentions any competitors, return matched names"""
        text_lower = text.lower()
        mentioned = []
        with self._lock:
            for name, keywords in self._competitors.items():
                for kw in keywords:
                    if kw in text_lower:
                        self._mentions[name].append(match)
                        mentioned.append(name)
                        break
        return mentioned
    
    def get_mention_count(self, competitor: str, hours: int = 24) -> int:
        """Get mention count for a competitor in the last N hours"""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        with self._lock:
            return sum(
                1 for m in self._mentions.get(competitor, [])
                if m.timestamp >= cutoff
            )
    
    def get_sentiment_breakdown(self, competitor: str) -> Dict[str, int]:
        """Get sentiment breakdown for competitor mentions"""
        breakdown = {"positive": 0, "negative": 0, "neutral": 0, "mixed": 0}
        with self._lock:
            for m in self._mentions.get(competitor, []):
                breakdown[m.sentiment.value] = breakdown.get(m.sentiment.value, 0) + 1
        return breakdown
    
    def get_share_of_voice(self) -> Dict[str, Dict]:
        """Calculate share of voice across competitors"""
        with self._lock:
            total = sum(len(mentions) for mentions in self._mentions.values())
            if total == 0:
                return {}
            
            sov = {}
            for name, mentions in self._mentions.items():
                count = len(mentions)
                sov[name] = {
                    "mentions": count,
                    "share": round(count / total * 100, 1),
                    "avg_sentiment": round(
                        sum(m.sentiment_score for m in mentions) / max(len(mentions), 1), 2
                    ),
                    "influencer_mentions": sum(1 for m in mentions if m.is_influencer)
                }
            return sov
    
    def compare_competitors(self) -> Dict:
        """Full competitor comparison report"""
        sov = self.get_share_of_voice()
        comparison = {
            "share_of_voice": sov,
            "total_mentions": sum(v["mentions"] for v in sov.values()),
            "leader": max(sov.items(), key=lambda x: x[1]["mentions"])[0] if sov else None,
            "most_positive": max(
                sov.items(), key=lambda x: x[1]["avg_sentiment"]
            )[0] if sov else None,
            "generated_at": datetime.now(timezone.utc).isoformat()
        }
        return comparison


class SocialListeningEngine:
    """社交聆听引擎主类"""
    
    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.queries: Dict[str, ListeningQuery] = {}
        self.alert_rules: Dict[str, AlertRule] = {}
        self.volume_tracker = VolumeTracker()
        self.competitor_tracker = CompetitorTracker()
        self._alerts: List[Alert] = []
        self._matches: List[ListeningMatch] = []
        self._callbacks: Dict[AlertType, List[Callable]] = defaultdict(list)
        self._lock = threading.Lock()
        self._init_db()
    
    def _init_db(self) -> None:
        """Initialize SQLite tables"""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS listening_matches (
                    match_id TEXT PRIMARY KEY,
                    query_id TEXT NOT NULL,
                    tweet_id TEXT,
                    author TEXT,
                    author_followers INTEGER DEFAULT 0,
                    text TEXT,
                    sentiment TEXT,
                    sentiment_score REAL DEFAULT 0.0,
                    matched_keywords TEXT,
                    reach INTEGER DEFAULT 0,
                    engagement INTEGER DEFAULT 0,
                    timestamp TEXT
                );
                
                CREATE TABLE IF NOT EXISTS listening_alerts (
                    alert_id TEXT PRIMARY KEY,
                    rule_id TEXT,
                    alert_type TEXT,
                    severity TEXT,
                    message TEXT,
                    data TEXT,
                    triggered_at TEXT,
                    acknowledged INTEGER DEFAULT 0
                );
                
                CREATE INDEX IF NOT EXISTS idx_matches_query 
                    ON listening_matches(query_id, timestamp);
                CREATE INDEX IF NOT EXISTS idx_matches_sentiment 
                    ON listening_matches(sentiment);
                CREATE INDEX IF NOT EXISTS idx_alerts_severity 
                    ON listening_alerts(severity, acknowledged);
            """)
            conn.commit()
        finally:
            conn.close()
    
    # === Query Management ===
    
    def add_query(self, query: ListeningQuery) -> None:
        """Add a listening query"""
        with self._lock:
            self.queries[query.query_id] = query
    
    def remove_query(self, query_id: str) -> bool:
        """Remove a listening query"""
        with self._lock:
            if query_id in self.queries:
                del self.queries[query_id]
                return True
            return False
    
    def get_query(self, query_id: str) -> Optional[ListeningQuery]:
        """Get a query by ID"""
        return self.queries.get(query_id)
    
    def list_queries(self, active_only: bool = True) -> List[ListeningQuery]:
        """List all queries"""
        queries = list(self.queries.values())
        if active_only:
            queries = [q for q in queries if q.active]
        return queries
    
    # === Alert Rule Management ===
    
    def add_alert_rule(self, rule: AlertRule) -> None:
        """Add an alert rule"""
        with self._lock:
            self.alert_rules[rule.rule_id] = rule
    
    def remove_alert_rule(self, rule_id: str) -> bool:
        """Remove an alert rule"""
        with self._lock:
            if rule_id in self.alert_rules:
                del self.alert_rules[rule_id]
                return True
            return False
    
    def register_callback(self, alert_type: AlertType, callback: Callable) -> None:
        """Register a callback for alert type"""
        self._callbacks[alert_type].append(callback)
    
    # === Core Processing ===
    
    def process_tweet(self, tweet_id: str, author: str, text: str,
                      author_followers: int = 0, reach: int = 0,
                      engagement: int = 0) -> List[ListeningMatch]:
        """
        Process a tweet against all active queries.
        Returns list of matches.
        """
        matches = []
        
        for query in self.list_queries(active_only=True):
            if query.matches(text):
                # Simple keyword-based sentiment
                sentiment, score = self._analyze_sentiment(text)
                matched_kws = [kw for kw in query.keywords if kw.lower() in text.lower()]
                
                match = ListeningMatch(
                    match_id=f"m_{tweet_id}_{query.query_id}",
                    query_id=query.query_id,
                    tweet_id=tweet_id,
                    author=author,
                    author_followers=author_followers,
                    text=text,
                    sentiment=sentiment,
                    sentiment_score=score,
                    matched_keywords=matched_kws,
                    reach=reach,
                    engagement=engagement
                )
                
                matches.append(match)
                self._store_match(match)
                self.volume_tracker.record(query.query_id, match.timestamp)
                
                # Check competitor mentions
                self.competitor_tracker.check_mention(text, match)
        
        # Check alert rules after processing
        if matches:
            self._check_alerts()
        
        return matches
    
    def _analyze_sentiment(self, text: str) -> Tuple[SentimentLabel, float]:
        """Simple rule-based sentiment analysis"""
        text_lower = text.lower()
        
        positive_words = {
            "love", "great", "amazing", "awesome", "excellent", "fantastic",
            "wonderful", "perfect", "best", "happy", "good", "thanks",
            "beautiful", "brilliant", "outstanding", "superb", "impressive"
        }
        negative_words = {
            "hate", "terrible", "awful", "worst", "bad", "horrible",
            "disgusting", "pathetic", "useless", "broken", "scam", "fraud",
            "disappointed", "angry", "annoyed", "frustrating", "poor", "fail"
        }
        
        words = set(re.findall(r'\b\w+\b', text_lower))
        pos_count = len(words & positive_words)
        neg_count = len(words & negative_words)
        
        total = pos_count + neg_count
        if total == 0:
            return SentimentLabel.NEUTRAL, 0.0
        
        score = (pos_count - neg_count) / total
        
        if pos_count > 0 and neg_count > 0:
            return SentimentLabel.MIXED, round(score, 2)
        elif pos_count > neg_count:
            return SentimentLabel.POSITIVE, round(min(score, 1.0), 2)
        elif neg_count > pos_count:
            return SentimentLabel.NEGATIVE, round(max(score, -1.0), 2)
        else:
            return SentimentLabel.NEUTRAL, 0.0
    
    def _store_match(self, match: ListeningMatch) -> None:
        """Store match in SQLite and in-memory list"""
        with self._lock:
            self._matches.append(match)
        
        if self.db_path != ":memory:":
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute(
                    """INSERT OR REPLACE INTO listening_matches 
                    (match_id, query_id, tweet_id, author, author_followers,
                     text, sentiment, sentiment_score, matched_keywords,
                     reach, engagement, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (match.match_id, match.query_id, match.tweet_id,
                     match.author, match.author_followers, match.text,
                     match.sentiment.value, match.sentiment_score,
                     json.dumps(match.matched_keywords),
                     match.reach, match.engagement, match.timestamp)
                )
                conn.commit()
            finally:
                conn.close()
    
    def _check_alerts(self) -> None:
        """Check all alert rules"""
        now = datetime.now(timezone.utc)
        
        for rule in list(self.alert_rules.values()):
            if not rule.active:
                continue
            
            # Check cooldown
            if rule.last_triggered:
                try:
                    last = datetime.fromisoformat(rule.last_triggered.replace("Z", "+00:00"))
                    if (now - last).total_seconds() < rule.cooldown_minutes * 60:
                        continue
                except (ValueError, TypeError):
                    pass
            
            triggered = False
            message = ""
            data = {}
            
            if rule.alert_type == AlertType.VOLUME_SPIKE:
                is_spike, ratio = self.volume_tracker.detect_spike(
                    rule.query_id, rule.threshold
                )
                if is_spike:
                    triggered = True
                    message = f"Volume spike detected for query '{rule.query_id}': {ratio}x normal"
                    data = {"ratio": ratio, "threshold": rule.threshold}
            
            elif rule.alert_type == AlertType.SENTIMENT_SHIFT:
                recent_sentiment = self._get_recent_sentiment(rule.query_id, rule.window_minutes)
                if recent_sentiment is not None and recent_sentiment < -rule.threshold:
                    triggered = True
                    message = f"Negative sentiment shift for '{rule.query_id}': {recent_sentiment:.2f}"
                    data = {"sentiment": recent_sentiment}
            
            elif rule.alert_type == AlertType.CRISIS_SIGNAL:
                neg_ratio = self._get_negative_ratio(rule.query_id, rule.window_minutes)
                if neg_ratio >= rule.threshold:
                    triggered = True
                    message = f"Crisis signal: {neg_ratio:.0%} negative mentions for '{rule.query_id}'"
                    data = {"negative_ratio": neg_ratio}
            
            if triggered:
                alert = Alert(
                    alert_id=f"alert_{rule.rule_id}_{now.strftime('%Y%m%d%H%M%S')}",
                    rule_id=rule.rule_id,
                    alert_type=rule.alert_type,
                    severity=rule.severity,
                    message=message,
                    data=data
                )
                self._alerts.append(alert)
                rule.last_triggered = now.isoformat()
                
                # Fire callbacks
                for cb in self._callbacks.get(rule.alert_type, []):
                    try:
                        cb(alert)
                    except Exception:
                        pass
    
    def _get_recent_sentiment(self, query_id: str, minutes: int) -> Optional[float]:
        """Get average sentiment for recent matches"""
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        with self._lock:
            recent = [
                m.sentiment_score for m in self._matches
                if m.query_id == query_id and m.timestamp >= cutoff
            ]
        if not recent:
            return None
        return sum(recent) / len(recent)
    
    def _get_negative_ratio(self, query_id: str, minutes: int) -> float:
        """Get ratio of negative mentions"""
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        with self._lock:
            recent = [
                m for m in self._matches
                if m.query_id == query_id and m.timestamp >= cutoff
            ]
        if not recent:
            return 0.0
        neg = sum(1 for m in recent if m.sentiment == SentimentLabel.NEGATIVE)
        return neg / len(recent)
    
    # === Query & Reporting ===
    
    def get_matches(self, query_id: Optional[str] = None,
                    sentiment: Optional[SentimentLabel] = None,
                    limit: int = 100) -> List[ListeningMatch]:
        """Get matches with optional filters"""
        with self._lock:
            results = list(self._matches)
        
        if query_id:
            results = [m for m in results if m.query_id == query_id]
        if sentiment:
            results = [m for m in results if m.sentiment == sentiment]
        
        results.sort(key=lambda m: m.timestamp, reverse=True)
        return results[:limit]
    
    def get_alerts(self, unacknowledged_only: bool = False,
                   severity: Optional[AlertSeverity] = None) -> List[Alert]:
        """Get alerts with optional filters"""
        alerts = list(self._alerts)
        if unacknowledged_only:
            alerts = [a for a in alerts if not a.acknowledged]
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        alerts.sort(key=lambda a: a.triggered_at, reverse=True)
        return alerts
    
    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert"""
        for alert in self._alerts:
            if alert.alert_id == alert_id:
                alert.acknowledged = True
                return True
        return False
    
    def get_top_authors(self, query_id: Optional[str] = None,
                        limit: int = 10) -> List[Dict]:
        """Get top authors by mention frequency"""
        with self._lock:
            matches = list(self._matches)
        
        if query_id:
            matches = [m for m in matches if m.query_id == query_id]
        
        author_stats: Dict[str, Dict] = defaultdict(
            lambda: {"count": 0, "followers": 0, "total_sentiment": 0.0,
                      "total_reach": 0, "total_engagement": 0}
        )
        
        for m in matches:
            stats = author_stats[m.author]
            stats["count"] += 1
            stats["followers"] = max(stats["followers"], m.author_followers)
            stats["total_sentiment"] += m.sentiment_score
            stats["total_reach"] += m.reach
            stats["total_engagement"] += m.engagement
        
        result = []
        for author, stats in author_stats.items():
            result.append({
                "author": author,
                "mentions": stats["count"],
                "followers": stats["followers"],
                "avg_sentiment": round(stats["total_sentiment"] / max(stats["count"], 1), 2),
                "total_reach": stats["total_reach"],
                "total_engagement": stats["total_engagement"],
                "is_influencer": stats["followers"] >= 10000
            })
        
        result.sort(key=lambda x: x["mentions"], reverse=True)
        return result[:limit]
    
    def get_keyword_frequency(self, query_id: Optional[str] = None) -> Dict[str, int]:
        """Get keyword frequency across matches"""
        with self._lock:
            matches = list(self._matches)
        
        if query_id:
            matches = [m for m in matches if m.query_id == query_id]
        
        freq: Counter = Counter()
        for m in matches:
            for kw in m.matched_keywords:
                freq[kw] += 1
        
        return dict(freq.most_common())
    
    def get_sentiment_over_time(self, query_id: str,
                                 hours: int = 24) -> List[Dict]:
        """Get sentiment trend over time buckets"""
        now = datetime.now(timezone.utc)
        buckets = []
        
        with self._lock:
            for i in range(hours - 1, -1, -1):
                start = now - timedelta(hours=i + 1)
                end = now - timedelta(hours=i)
                start_iso = start.isoformat()
                end_iso = end.isoformat()
                
                hour_matches = [
                    m for m in self._matches
                    if m.query_id == query_id and start_iso <= m.timestamp < end_iso
                ]
                
                if hour_matches:
                    avg_sent = sum(m.sentiment_score for m in hour_matches) / len(hour_matches)
                    pos = sum(1 for m in hour_matches if m.sentiment == SentimentLabel.POSITIVE)
                    neg = sum(1 for m in hour_matches if m.sentiment == SentimentLabel.NEGATIVE)
                else:
                    avg_sent = 0.0
                    pos = neg = 0
                
                buckets.append({
                    "hour": end.strftime("%Y-%m-%d %H:00"),
                    "count": len(hour_matches),
                    "avg_sentiment": round(avg_sent, 2),
                    "positive": pos,
                    "negative": neg
                })
        
        return buckets
    
    def generate_report(self, query_id: Optional[str] = None,
                        format: str = "text") -> str:
        """
        Generate a comprehensive listening report.
        Formats: text, json, csv
        """
        with self._lock:
            matches = list(self._matches)
        
        if query_id:
            matches = [m for m in matches if m.query_id == query_id]
        
        total = len(matches)
        sentiments = Counter(m.sentiment.value for m in matches)
        avg_score = sum(m.sentiment_score for m in matches) / max(total, 1)
        influencer_mentions = sum(1 for m in matches if m.is_influencer)
        total_reach = sum(m.reach for m in matches)
        total_engagement = sum(m.engagement for m in matches)
        
        top_authors = self.get_top_authors(query_id, limit=5)
        keyword_freq = self.get_keyword_frequency(query_id)
        
        alerts = [a for a in self._alerts if not query_id or 
                  any(r.query_id == query_id for r in self.alert_rules.values() 
                      if r.rule_id == a.rule_id)]
        
        report_data = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "query_id": query_id or "all",
            "total_matches": total,
            "sentiment": {
                "positive": sentiments.get("positive", 0),
                "negative": sentiments.get("negative", 0),
                "neutral": sentiments.get("neutral", 0),
                "mixed": sentiments.get("mixed", 0),
                "average_score": round(avg_score, 2)
            },
            "influencer_mentions": influencer_mentions,
            "total_reach": total_reach,
            "total_engagement": total_engagement,
            "top_authors": top_authors[:5],
            "top_keywords": dict(list(keyword_freq.items())[:10]),
            "active_alerts": len([a for a in alerts if not a.acknowledged]),
            "competitor_sov": self.competitor_tracker.get_share_of_voice()
        }
        
        if format == "json":
            return json.dumps(report_data, indent=2, ensure_ascii=False)
        
        elif format == "csv":
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(["Metric", "Value"])
            writer.writerow(["Total Matches", total])
            writer.writerow(["Positive", sentiments.get("positive", 0)])
            writer.writerow(["Negative", sentiments.get("negative", 0)])
            writer.writerow(["Neutral", sentiments.get("neutral", 0)])
            writer.writerow(["Avg Sentiment", round(avg_score, 2)])
            writer.writerow(["Influencer Mentions", influencer_mentions])
            writer.writerow(["Total Reach", total_reach])
            writer.writerow(["Total Engagement", total_engagement])
            writer.writerow(["Active Alerts", report_data["active_alerts"]])
            return output.getvalue()
        
        else:  # text
            lines = [
                "=" * 50,
                "📡 Social Listening Report",
                f"Query: {query_id or 'All Queries'}",
                f"Generated: {report_data['generated_at'][:19]}",
                "=" * 50,
                "",
                f"📊 Total Matches: {total}",
                f"   ✅ Positive: {sentiments.get('positive', 0)}",
                f"   ❌ Negative: {sentiments.get('negative', 0)}",
                f"   ⚪ Neutral: {sentiments.get('neutral', 0)}",
                f"   🔀 Mixed: {sentiments.get('mixed', 0)}",
                f"   📈 Avg Sentiment: {avg_score:.2f}",
                "",
                f"👥 Influencer Mentions: {influencer_mentions}",
                f"📣 Total Reach: {total_reach:,}",
                f"💬 Total Engagement: {total_engagement:,}",
                "",
                "🏆 Top Authors:",
            ]
            
            for i, author in enumerate(top_authors[:5], 1):
                lines.append(
                    f"   {i}. @{author['author']} "
                    f"({author['mentions']} mentions, "
                    f"{author['followers']:,} followers)"
                )
            
            if keyword_freq:
                lines.append("")
                lines.append("🔑 Top Keywords:")
                for kw, count in list(keyword_freq.items())[:5]:
                    lines.append(f"   • {kw}: {count}")
            
            sov = report_data["competitor_sov"]
            if sov:
                lines.append("")
                lines.append("🏢 Competitor Share of Voice:")
                for name, data in sorted(sov.items(), key=lambda x: x[1]["share"], reverse=True):
                    lines.append(
                        f"   • {name}: {data['share']}% "
                        f"({data['mentions']} mentions, "
                        f"sentiment: {data['avg_sentiment']:.2f})"
                    )
            
            if report_data["active_alerts"] > 0:
                lines.append("")
                lines.append(f"⚠️ Active Alerts: {report_data['active_alerts']}")
            
            lines.append("")
            lines.append("=" * 50)
            
            return "\n".join(lines)
    
    def get_stats(self) -> Dict:
        """Get engine statistics"""
        with self._lock:
            return {
                "active_queries": len([q for q in self.queries.values() if q.active]),
                "total_queries": len(self.queries),
                "alert_rules": len(self.alert_rules),
                "total_matches": len(self._matches),
                "total_alerts": len(self._alerts),
                "unacknowledged_alerts": sum(1 for a in self._alerts if not a.acknowledged),
                "competitors_tracked": len(self.competitor_tracker.list_competitors())
            }
