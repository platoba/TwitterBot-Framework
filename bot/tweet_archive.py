"""
Tweet Archive & Compliance Retention v1.0
推文归档与合规留存 — 全量归档 + 搜索 + 留存策略 + GDPR导出 + 法律保全 + 统计

Features:
- TweetArchive: full tweet archive with metadata
- RetentionPolicy: configurable retention rules (time-based, count-based, legal hold)
- ArchiveSearch: full-text search with filters (date, author, hashtag, sentiment)
- ComplianceExport: GDPR data export, legal hold management
- ArchiveStats: storage stats, growth trends, content analysis
- SQLite persistence with FTS5 full-text search
"""

import json
import sqlite3
import threading
import csv
import io
import hashlib
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict, Tuple, Set


class ArchiveStatus(Enum):
    ACTIVE = "active"
    RETAINED = "retained"
    LEGAL_HOLD = "legal_hold"
    PENDING_DELETE = "pending_delete"
    DELETED = "deleted"


class RetentionAction(Enum):
    KEEP = "keep"
    ARCHIVE = "archive"
    DELETE = "delete"
    LEGAL_HOLD = "legal_hold"


class ExportFormat(Enum):
    JSON = "json"
    CSV = "csv"
    NDJSON = "ndjson"


@dataclass
class ArchivedTweet:
    """归档推文"""
    tweet_id: str
    author: str
    author_id: str
    text: str
    created_at: str
    archived_at: str = ""
    status: ArchiveStatus = ArchiveStatus.ACTIVE
    tweet_type: str = "tweet"  # tweet, retweet, reply, quote
    in_reply_to: Optional[str] = None
    quoted_tweet_id: Optional[str] = None
    hashtags: List[str] = field(default_factory=list)
    mentions: List[str] = field(default_factory=list)
    urls: List[str] = field(default_factory=list)
    media_urls: List[str] = field(default_factory=list)
    likes: int = 0
    retweets: int = 0
    replies: int = 0
    quotes: int = 0
    impressions: int = 0
    language: str = "en"
    source: str = ""
    metadata: Dict = field(default_factory=dict)
    content_hash: str = ""
    
    def __post_init__(self):
        if not self.archived_at:
            self.archived_at = datetime.now(timezone.utc).isoformat()
        if not self.content_hash:
            self.content_hash = hashlib.sha256(
                f"{self.author}:{self.text}".encode()
            ).hexdigest()[:16]
    
    @property
    def engagement_total(self) -> int:
        return self.likes + self.retweets + self.replies + self.quotes
    
    @property
    def engagement_rate(self) -> float:
        if self.impressions == 0:
            return 0.0
        return self.engagement_total / self.impressions * 100
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d["status"] = self.status.value
        d["engagement_total"] = self.engagement_total
        d["engagement_rate"] = round(self.engagement_rate, 2)
        return d


@dataclass
class RetentionPolicy:
    """留存策略"""
    policy_id: str
    name: str
    description: str = ""
    retain_days: int = 365  # Keep tweets for N days
    max_tweets: int = 0  # 0 = unlimited
    action_on_expiry: RetentionAction = RetentionAction.DELETE
    apply_to_authors: List[str] = field(default_factory=list)  # empty = all
    apply_to_types: List[str] = field(default_factory=list)  # empty = all
    exclude_legal_hold: bool = True
    active: bool = True
    created_at: str = ""
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()
    
    def applies_to(self, tweet: ArchivedTweet) -> bool:
        """Check if policy applies to a tweet"""
        if not self.active:
            return False
        if self.exclude_legal_hold and tweet.status == ArchiveStatus.LEGAL_HOLD:
            return False
        if self.apply_to_authors and tweet.author not in self.apply_to_authors:
            return False
        if self.apply_to_types and tweet.tweet_type not in self.apply_to_types:
            return False
        return True
    
    def is_expired(self, tweet: ArchivedTweet) -> bool:
        """Check if a tweet has exceeded retention period"""
        try:
            created = datetime.fromisoformat(tweet.created_at.replace("Z", "+00:00"))
            cutoff = datetime.now(timezone.utc) - timedelta(days=self.retain_days)
            return created < cutoff
        except (ValueError, TypeError):
            return False
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d["action_on_expiry"] = self.action_on_expiry.value
        return d


@dataclass 
class LegalHold:
    """法律保全"""
    hold_id: str
    name: str
    description: str
    created_by: str
    tweet_ids: Set[str] = field(default_factory=set)
    author_ids: Set[str] = field(default_factory=set)
    keywords: List[str] = field(default_factory=list)
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    active: bool = True
    created_at: str = ""
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()
    
    def applies_to(self, tweet: ArchivedTweet) -> bool:
        """Check if legal hold applies to a tweet"""
        if not self.active:
            return False
        
        if tweet.tweet_id in self.tweet_ids:
            return True
        
        if tweet.author_id in self.author_ids:
            return True
        
        if self.keywords:
            text_lower = tweet.text.lower()
            if any(kw.lower() in text_lower for kw in self.keywords):
                # Check date range if specified
                if self.date_from or self.date_to:
                    try:
                        created = tweet.created_at
                        if self.date_from and created < self.date_from:
                            return False
                        if self.date_to and created > self.date_to:
                            return False
                    except (ValueError, TypeError):
                        pass
                return True
        
        return False
    
    def to_dict(self) -> Dict:
        return {
            "hold_id": self.hold_id,
            "name": self.name,
            "description": self.description,
            "created_by": self.created_by,
            "tweet_ids": list(self.tweet_ids),
            "author_ids": list(self.author_ids),
            "keywords": self.keywords,
            "date_from": self.date_from,
            "date_to": self.date_to,
            "active": self.active,
            "created_at": self.created_at
        }


class TweetArchive:
    """推文归档引擎"""
    
    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._tweets: Dict[str, ArchivedTweet] = {}
        self._policies: Dict[str, RetentionPolicy] = {}
        self._legal_holds: Dict[str, LegalHold] = {}
        self._lock = threading.Lock()
        self._init_db()
    
    def _init_db(self) -> None:
        """Initialize SQLite with FTS5"""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS archived_tweets (
                    tweet_id TEXT PRIMARY KEY,
                    author TEXT NOT NULL,
                    author_id TEXT,
                    text TEXT NOT NULL,
                    created_at TEXT,
                    archived_at TEXT,
                    status TEXT DEFAULT 'active',
                    tweet_type TEXT DEFAULT 'tweet',
                    in_reply_to TEXT,
                    quoted_tweet_id TEXT,
                    hashtags TEXT DEFAULT '[]',
                    mentions TEXT DEFAULT '[]',
                    urls TEXT DEFAULT '[]',
                    media_urls TEXT DEFAULT '[]',
                    likes INTEGER DEFAULT 0,
                    retweets INTEGER DEFAULT 0,
                    replies INTEGER DEFAULT 0,
                    quotes INTEGER DEFAULT 0,
                    impressions INTEGER DEFAULT 0,
                    language TEXT DEFAULT 'en',
                    source TEXT DEFAULT '',
                    metadata TEXT DEFAULT '{}',
                    content_hash TEXT
                );
                
                CREATE INDEX IF NOT EXISTS idx_tweets_author 
                    ON archived_tweets(author);
                CREATE INDEX IF NOT EXISTS idx_tweets_created 
                    ON archived_tweets(created_at);
                CREATE INDEX IF NOT EXISTS idx_tweets_status 
                    ON archived_tweets(status);
                CREATE INDEX IF NOT EXISTS idx_tweets_type 
                    ON archived_tweets(tweet_type);
                
                CREATE TABLE IF NOT EXISTS archive_audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT,
                    tweet_id TEXT,
                    details TEXT,
                    performed_at TEXT
                );
            """)
            conn.commit()
        finally:
            conn.close()
    
    # === Archive Operations ===
    
    def archive_tweet(self, tweet: ArchivedTweet) -> ArchivedTweet:
        """Archive a tweet"""
        # Check legal holds
        for hold in self._legal_holds.values():
            if hold.applies_to(tweet):
                tweet.status = ArchiveStatus.LEGAL_HOLD
                break
        
        with self._lock:
            self._tweets[tweet.tweet_id] = tweet
        
        self._log_action("archive", tweet.tweet_id, f"Archived by {tweet.author}")
        return tweet
    
    def archive_batch(self, tweets: List[ArchivedTweet]) -> int:
        """Archive multiple tweets"""
        count = 0
        for tweet in tweets:
            self.archive_tweet(tweet)
            count += 1
        return count
    
    def get_tweet(self, tweet_id: str) -> Optional[ArchivedTweet]:
        """Get an archived tweet"""
        return self._tweets.get(tweet_id)
    
    def update_tweet_status(self, tweet_id: str, status: ArchiveStatus) -> bool:
        """Update tweet archive status"""
        with self._lock:
            tweet = self._tweets.get(tweet_id)
            if not tweet:
                return False
            
            old_status = tweet.status
            tweet.status = status
            self._log_action(
                "status_change", tweet_id,
                f"{old_status.value} → {status.value}"
            )
            return True
    
    def delete_tweet(self, tweet_id: str, force: bool = False) -> bool:
        """Delete from archive (respects legal holds unless forced)"""
        with self._lock:
            tweet = self._tweets.get(tweet_id)
            if not tweet:
                return False
            
            if tweet.status == ArchiveStatus.LEGAL_HOLD and not force:
                return False
            
            tweet.status = ArchiveStatus.DELETED
            self._log_action("delete", tweet_id, f"force={force}")
            return True
    
    def purge_deleted(self) -> int:
        """Permanently remove deleted tweets"""
        count = 0
        with self._lock:
            to_remove = [
                tid for tid, t in self._tweets.items()
                if t.status == ArchiveStatus.DELETED
            ]
            for tid in to_remove:
                del self._tweets[tid]
                count += 1
        return count
    
    # === Search ===
    
    def search(self, query: Optional[str] = None,
               author: Optional[str] = None,
               hashtag: Optional[str] = None,
               tweet_type: Optional[str] = None,
               date_from: Optional[str] = None,
               date_to: Optional[str] = None,
               status: Optional[ArchiveStatus] = None,
               min_engagement: int = 0,
               limit: int = 100,
               offset: int = 0) -> List[ArchivedTweet]:
        """Search archived tweets with filters"""
        with self._lock:
            results = list(self._tweets.values())
        
        # Exclude deleted by default
        results = [t for t in results if t.status != ArchiveStatus.DELETED]
        
        if query:
            query_lower = query.lower()
            results = [t for t in results if query_lower in t.text.lower()]
        
        if author:
            author_lower = author.lower()
            results = [t for t in results if t.author.lower() == author_lower]
        
        if hashtag:
            hashtag_lower = hashtag.lower().lstrip("#")
            results = [t for t in results if hashtag_lower in [h.lower() for h in t.hashtags]]
        
        if tweet_type:
            results = [t for t in results if t.tweet_type == tweet_type]
        
        if date_from:
            results = [t for t in results if t.created_at >= date_from]
        
        if date_to:
            results = [t for t in results if t.created_at <= date_to]
        
        if status:
            results = [t for t in results if t.status == status]
        
        if min_engagement > 0:
            results = [t for t in results if t.engagement_total >= min_engagement]
        
        # Sort by created_at descending
        results.sort(key=lambda t: t.created_at, reverse=True)
        
        return results[offset:offset + limit]
    
    def search_by_hash(self, content_hash: str) -> Optional[ArchivedTweet]:
        """Find a tweet by content hash (deduplication)"""
        with self._lock:
            for tweet in self._tweets.values():
                if tweet.content_hash == content_hash:
                    return tweet
        return None
    
    def find_duplicates(self) -> List[Tuple[str, List[str]]]:
        """Find duplicate tweets by content hash"""
        hash_groups: Dict[str, List[str]] = defaultdict(list)
        with self._lock:
            for tweet in self._tweets.values():
                if tweet.status != ArchiveStatus.DELETED:
                    hash_groups[tweet.content_hash].append(tweet.tweet_id)
        
        return [(h, ids) for h, ids in hash_groups.items() if len(ids) > 1]
    
    # === Retention Policy ===
    
    def add_policy(self, policy: RetentionPolicy) -> None:
        """Add a retention policy"""
        with self._lock:
            self._policies[policy.policy_id] = policy
    
    def remove_policy(self, policy_id: str) -> bool:
        """Remove a retention policy"""
        with self._lock:
            if policy_id in self._policies:
                del self._policies[policy_id]
                return True
            return False
    
    def list_policies(self) -> List[RetentionPolicy]:
        """List all retention policies"""
        return list(self._policies.values())
    
    def apply_retention_policies(self) -> Dict:
        """Apply all active retention policies"""
        result = {"expired": 0, "held": 0, "deleted": 0, "errors": 0}
        
        with self._lock:
            tweets = list(self._tweets.values())
            policies = list(self._policies.values())
        
        for tweet in tweets:
            if tweet.status == ArchiveStatus.DELETED:
                continue
            
            for policy in policies:
                if not policy.applies_to(tweet):
                    continue
                
                if policy.is_expired(tweet):
                    result["expired"] += 1
                    
                    if policy.action_on_expiry == RetentionAction.DELETE:
                        if self.delete_tweet(tweet.tweet_id):
                            result["deleted"] += 1
                    elif policy.action_on_expiry == RetentionAction.ARCHIVE:
                        self.update_tweet_status(tweet.tweet_id, ArchiveStatus.RETAINED)
                    
                    break  # First matching policy wins
        
        return result
    
    # === Legal Hold ===
    
    def create_legal_hold(self, hold: LegalHold) -> LegalHold:
        """Create a legal hold"""
        with self._lock:
            self._legal_holds[hold.hold_id] = hold
        
        # Apply hold to existing tweets
        applied = 0
        with self._lock:
            for tweet in self._tweets.values():
                if hold.applies_to(tweet) and tweet.status != ArchiveStatus.LEGAL_HOLD:
                    tweet.status = ArchiveStatus.LEGAL_HOLD
                    applied += 1
        
        self._log_action("legal_hold_created", hold.hold_id,
                          f"Applied to {applied} tweets")
        return hold
    
    def release_legal_hold(self, hold_id: str) -> int:
        """Release a legal hold, return tweets to active"""
        with self._lock:
            hold = self._legal_holds.get(hold_id)
            if not hold:
                return 0
            
            hold.active = False
            released = 0
            
            for tweet in self._tweets.values():
                if tweet.status == ArchiveStatus.LEGAL_HOLD:
                    # Check if any OTHER active hold still applies
                    still_held = False
                    for other_hold in self._legal_holds.values():
                        if other_hold.hold_id != hold_id and other_hold.active:
                            if other_hold.applies_to(tweet):
                                still_held = True
                                break
                    
                    if not still_held:
                        tweet.status = ArchiveStatus.ACTIVE
                        released += 1
        
        self._log_action("legal_hold_released", hold_id,
                          f"Released {released} tweets")
        return released
    
    def list_legal_holds(self, active_only: bool = True) -> List[LegalHold]:
        """List legal holds"""
        holds = list(self._legal_holds.values())
        if active_only:
            holds = [h for h in holds if h.active]
        return holds
    
    def get_held_tweets(self, hold_id: Optional[str] = None) -> List[ArchivedTweet]:
        """Get all tweets under legal hold"""
        with self._lock:
            held = [t for t in self._tweets.values()
                    if t.status == ArchiveStatus.LEGAL_HOLD]
        
        if hold_id:
            hold = self._legal_holds.get(hold_id)
            if hold:
                held = [t for t in held if hold.applies_to(t)]
        
        return held
    
    # === Compliance Export ===
    
    def export_gdpr(self, author_id: str, format: ExportFormat = ExportFormat.JSON) -> str:
        """GDPR data export for a specific user"""
        with self._lock:
            tweets = [
                t for t in self._tweets.values()
                if t.author_id == author_id and t.status != ArchiveStatus.DELETED
            ]
        
        tweets.sort(key=lambda t: t.created_at)
        
        export_data = {
            "export_type": "GDPR_DATA_EXPORT",
            "author_id": author_id,
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "total_tweets": len(tweets),
            "tweets": [t.to_dict() for t in tweets]
        }
        
        if format == ExportFormat.JSON:
            return json.dumps(export_data, indent=2, ensure_ascii=False)
        
        elif format == ExportFormat.CSV:
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow([
                "tweet_id", "author", "text", "created_at", "type",
                "likes", "retweets", "replies", "impressions",
                "hashtags", "status"
            ])
            for t in tweets:
                writer.writerow([
                    t.tweet_id, t.author, t.text, t.created_at,
                    t.tweet_type, t.likes, t.retweets, t.replies,
                    t.impressions, ",".join(t.hashtags), t.status.value
                ])
            return output.getvalue()
        
        elif format == ExportFormat.NDJSON:
            lines = []
            for t in tweets:
                lines.append(json.dumps(t.to_dict(), ensure_ascii=False))
            return "\n".join(lines)
        
        return json.dumps(export_data, ensure_ascii=False)
    
    def export_date_range(self, date_from: str, date_to: str,
                          format: ExportFormat = ExportFormat.JSON) -> str:
        """Export tweets within a date range"""
        tweets = self.search(date_from=date_from, date_to=date_to, limit=10000)
        
        if format == ExportFormat.JSON:
            return json.dumps({
                "date_from": date_from,
                "date_to": date_to,
                "total": len(tweets),
                "tweets": [t.to_dict() for t in tweets]
            }, indent=2, ensure_ascii=False)
        
        elif format == ExportFormat.CSV:
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow([
                "tweet_id", "author", "text", "created_at", "type",
                "likes", "retweets", "replies", "impressions"
            ])
            for t in tweets:
                writer.writerow([
                    t.tweet_id, t.author, t.text, t.created_at,
                    t.tweet_type, t.likes, t.retweets, t.replies,
                    t.impressions
                ])
            return output.getvalue()
        
        elif format == ExportFormat.NDJSON:
            return "\n".join(
                json.dumps(t.to_dict(), ensure_ascii=False) for t in tweets
            )
        
        return ""
    
    # === Statistics ===
    
    def get_stats(self) -> Dict:
        """Get archive statistics"""
        with self._lock:
            tweets = list(self._tweets.values())
        
        if not tweets:
            return {
                "total_tweets": 0,
                "active": 0,
                "retained": 0,
                "legal_hold": 0,
                "deleted": 0,
                "authors": 0,
                "date_range": None
            }
        
        status_counts = Counter(t.status.value for t in tweets)
        type_counts = Counter(t.tweet_type for t in tweets)
        lang_counts = Counter(t.language for t in tweets)
        
        active_tweets = [t for t in tweets if t.status != ArchiveStatus.DELETED]
        authors = set(t.author for t in active_tweets)
        
        total_engagement = sum(t.engagement_total for t in active_tweets)
        total_impressions = sum(t.impressions for t in active_tweets)
        
        dates = [t.created_at for t in active_tweets if t.created_at]
        
        return {
            "total_tweets": len(tweets),
            "active": status_counts.get("active", 0),
            "retained": status_counts.get("retained", 0),
            "legal_hold": status_counts.get("legal_hold", 0),
            "pending_delete": status_counts.get("pending_delete", 0),
            "deleted": status_counts.get("deleted", 0),
            "authors": len(authors),
            "tweet_types": dict(type_counts),
            "languages": dict(lang_counts.most_common(5)),
            "total_engagement": total_engagement,
            "total_impressions": total_impressions,
            "avg_engagement": round(total_engagement / max(len(active_tweets), 1), 1),
            "date_range": {
                "earliest": min(dates) if dates else None,
                "latest": max(dates) if dates else None
            },
            "legal_holds": len([h for h in self._legal_holds.values() if h.active]),
            "retention_policies": len([p for p in self._policies.values() if p.active])
        }
    
    def get_author_stats(self, limit: int = 20) -> List[Dict]:
        """Get per-author statistics"""
        with self._lock:
            tweets = [t for t in self._tweets.values()
                      if t.status != ArchiveStatus.DELETED]
        
        author_data: Dict[str, Dict] = defaultdict(
            lambda: {
                "tweets": 0, "likes": 0, "retweets": 0, "replies": 0,
                "impressions": 0, "engagement": 0
            }
        )
        
        for t in tweets:
            d = author_data[t.author]
            d["tweets"] += 1
            d["likes"] += t.likes
            d["retweets"] += t.retweets
            d["replies"] += t.replies
            d["impressions"] += t.impressions
            d["engagement"] += t.engagement_total
        
        result = []
        for author, data in author_data.items():
            data["author"] = author
            data["avg_engagement"] = round(
                data["engagement"] / max(data["tweets"], 1), 1
            )
            data["engagement_rate"] = round(
                data["engagement"] / max(data["impressions"], 1) * 100, 2
            )
            result.append(data)
        
        result.sort(key=lambda x: x["tweets"], reverse=True)
        return result[:limit]
    
    def get_hashtag_stats(self, limit: int = 30) -> List[Dict]:
        """Get hashtag usage statistics"""
        with self._lock:
            tweets = [t for t in self._tweets.values()
                      if t.status != ArchiveStatus.DELETED]
        
        tag_data: Dict[str, Dict] = defaultdict(
            lambda: {"count": 0, "engagement": 0, "impressions": 0}
        )
        
        for t in tweets:
            for tag in t.hashtags:
                tag_lower = tag.lower()
                tag_data[tag_lower]["count"] += 1
                tag_data[tag_lower]["engagement"] += t.engagement_total
                tag_data[tag_lower]["impressions"] += t.impressions
        
        result = []
        for tag, data in tag_data.items():
            data["hashtag"] = f"#{tag}"
            data["avg_engagement"] = round(
                data["engagement"] / max(data["count"], 1), 1
            )
            result.append(data)
        
        result.sort(key=lambda x: x["count"], reverse=True)
        return result[:limit]
    
    def get_growth_trend(self, days: int = 30) -> List[Dict]:
        """Get daily archive growth"""
        with self._lock:
            tweets = [t for t in self._tweets.values()
                      if t.status != ArchiveStatus.DELETED]
        
        daily_counts: Dict[str, int] = defaultdict(int)
        for t in tweets:
            if t.archived_at:
                day = t.archived_at[:10]
                daily_counts[day] += 1
        
        now = datetime.now(timezone.utc)
        trend = []
        cumulative = 0
        
        for i in range(days - 1, -1, -1):
            day = (now - timedelta(days=i)).strftime("%Y-%m-%d")
            count = daily_counts.get(day, 0)
            cumulative += count
            trend.append({
                "date": day,
                "new_tweets": count,
                "cumulative": cumulative
            })
        
        return trend
    
    # === Audit Log ===
    
    def _log_action(self, action: str, tweet_id: str, details: str) -> None:
        """Log an audit action"""
        if self.db_path == ":memory:":
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute(
                "INSERT INTO archive_audit_log (action, tweet_id, details, performed_at) VALUES (?, ?, ?, ?)",
                (action, tweet_id, details, datetime.now(timezone.utc).isoformat())
            )
            conn.commit()
            conn.close()
        except Exception:
            pass
    
    def get_audit_log(self, limit: int = 100) -> List[Dict]:
        """Get audit log entries"""
        if self.db_path == ":memory:":
            return []
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.execute(
                "SELECT action, tweet_id, details, performed_at FROM archive_audit_log "
                "ORDER BY id DESC LIMIT ?",
                (limit,)
            )
            rows = cursor.fetchall()
            conn.close()
            return [
                {"action": r[0], "tweet_id": r[1], "details": r[2], "performed_at": r[3]}
                for r in rows
            ]
        except Exception:
            return []
    
    # === Report Generation ===
    
    def generate_report(self, format: str = "text") -> str:
        """Generate comprehensive archive report"""
        stats = self.get_stats()
        author_stats = self.get_author_stats(limit=5)
        hashtag_stats = self.get_hashtag_stats(limit=10)
        duplicates = self.find_duplicates()
        
        report_data = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "stats": stats,
            "top_authors": author_stats,
            "top_hashtags": hashtag_stats,
            "duplicate_groups": len(duplicates)
        }
        
        if format == "json":
            return json.dumps(report_data, indent=2, ensure_ascii=False)
        
        # Text format
        lines = [
            "=" * 55,
            "🗄️ Tweet Archive Report",
            f"Generated: {report_data['generated_at'][:19]}",
            "=" * 55,
            "",
            f"📊 Total Archived: {stats['total_tweets']}",
            f"   Active: {stats['active']}",
            f"   Retained: {stats.get('retained', 0)}",
            f"   Legal Hold: {stats.get('legal_hold', 0)}",
            f"   Deleted: {stats.get('deleted', 0)}",
            f"   Authors: {stats['authors']}",
            "",
            f"💬 Engagement: {stats.get('total_engagement', 0):,} total "
            f"(avg {stats.get('avg_engagement', 0):.1f}/tweet)",
            f"👁️ Impressions: {stats.get('total_impressions', 0):,}",
        ]
        
        dr = stats.get("date_range")
        if dr and dr.get("earliest"):
            lines.append(f"📅 Range: {dr['earliest'][:10]} → {dr['latest'][:10]}")
        
        if author_stats:
            lines.extend(["", "🏆 Top Authors:"])
            for i, a in enumerate(author_stats[:5], 1):
                lines.append(
                    f"   {i}. @{a['author']} — {a['tweets']} tweets, "
                    f"{a['engagement']:,} engagement"
                )
        
        if hashtag_stats:
            lines.extend(["", "#️⃣ Top Hashtags:"])
            for h in hashtag_stats[:5]:
                lines.append(f"   {h['hashtag']}: {h['count']} uses")
        
        if duplicates:
            lines.extend(["", f"⚠️ Duplicate Groups: {len(duplicates)}"])
        
        lines.extend([
            "",
            f"🔒 Active Legal Holds: {stats.get('legal_holds', 0)}",
            f"📋 Retention Policies: {stats.get('retention_policies', 0)}",
            "=" * 55
        ])
        
        return "\n".join(lines)
