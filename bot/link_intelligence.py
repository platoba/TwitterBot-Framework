"""
链接智能管理
UTM参数管理 + 短链生成 + 点击追踪 + 链接健康检测 + 批量管理
"""

import hashlib
import json
import re
import sqlite3
import string
import threading
import random
from collections import Counter
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse


class LinkStatus(Enum):
    """链接状态"""
    ACTIVE = "active"
    EXPIRED = "expired"
    BROKEN = "broken"
    REDIRECTED = "redirected"
    BLOCKED = "blocked"


class UTMSource(Enum):
    """UTM来源"""
    TWITTER = "twitter"
    FACEBOOK = "facebook"
    INSTAGRAM = "instagram"
    LINKEDIN = "linkedin"
    EMAIL = "email"
    NEWSLETTER = "newsletter"
    ORGANIC = "organic"
    PAID = "paid"


class UTMMedium(Enum):
    """UTM媒介"""
    SOCIAL = "social"
    CPC = "cpc"
    EMAIL = "email"
    BANNER = "banner"
    REFERRAL = "referral"
    ORGANIC = "organic"


@dataclass
class UTMParams:
    """UTM参数集"""
    source: str = ""
    medium: str = ""
    campaign: str = ""
    term: str = ""
    content: str = ""

    def to_dict(self) -> Dict[str, str]:
        params = {}
        if self.source:
            params["utm_source"] = self.source
        if self.medium:
            params["utm_medium"] = self.medium
        if self.campaign:
            params["utm_campaign"] = self.campaign
        if self.term:
            params["utm_term"] = self.term
        if self.content:
            params["utm_content"] = self.content
        return params

    @classmethod
    def from_url(cls, url: str) -> "UTMParams":
        """从URL提取UTM参数"""
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        return cls(
            source=qs.get("utm_source", [""])[0],
            medium=qs.get("utm_medium", [""])[0],
            campaign=qs.get("utm_campaign", [""])[0],
            term=qs.get("utm_term", [""])[0],
            content=qs.get("utm_content", [""])[0],
        )

    @classmethod
    def twitter_default(cls, campaign: str = "",
                        content: str = "") -> "UTMParams":
        """Twitter默认UTM"""
        return cls(
            source=UTMSource.TWITTER.value,
            medium=UTMMedium.SOCIAL.value,
            campaign=campaign,
            content=content,
        )


@dataclass
class TrackedLink:
    """追踪链接"""
    link_id: str
    original_url: str
    short_code: str
    short_url: str
    utm: UTMParams
    clicks: int = 0
    unique_clicks: int = 0
    status: LinkStatus = LinkStatus.ACTIVE
    created_at: str = ""
    expires_at: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        d = asdict(self)
        d["status"] = self.status.value
        return d


@dataclass
class ClickEvent:
    """点击事件"""
    link_id: str
    clicked_at: str
    referrer: str = ""
    user_agent: str = ""
    ip_hash: str = ""   # 哈希后的IP (隐私)
    country: str = ""
    device: str = ""     # mobile/desktop/tablet
    platform: str = ""   # twitter/direct/other


@dataclass
class LinkAnalytics:
    """链接分析报告"""
    link_id: str
    total_clicks: int
    unique_clicks: int
    ctr: float = 0.0
    top_referrers: List[Tuple[str, int]] = field(default_factory=list)
    top_countries: List[Tuple[str, int]] = field(default_factory=list)
    device_breakdown: Dict[str, int] = field(default_factory=dict)
    hourly_clicks: Dict[int, int] = field(default_factory=dict)
    daily_clicks: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return asdict(self)


class ShortCodeGenerator:
    """短链码生成器"""

    ALPHABET = string.ascii_letters + string.digits  # 62字符
    DEFAULT_LENGTH = 6

    @classmethod
    def generate(cls, length: int = 6, seed: Optional[str] = None) -> str:
        """生成随机短码"""
        if seed:
            rng = random.Random(seed)
            return "".join(rng.choices(cls.ALPHABET, k=length))
        return "".join(random.choices(cls.ALPHABET, k=length))

    @classmethod
    def from_url(cls, url: str, length: int = 6) -> str:
        """基于URL哈希生成确定性短码"""
        h = hashlib.sha256(url.encode()).hexdigest()
        result = []
        for i in range(length):
            idx = int(h[i * 2:i * 2 + 2], 16) % len(cls.ALPHABET)
            result.append(cls.ALPHABET[idx])
        return "".join(result)

    @classmethod
    def is_valid(cls, code: str) -> bool:
        """检查短码是否有效"""
        return bool(code) and all(c in cls.ALPHABET for c in code)


class UTMBuilder:
    """UTM参数构建器"""

    @staticmethod
    def build_url(base_url: str, utm: UTMParams) -> str:
        """给URL添加UTM参数"""
        parsed = urlparse(base_url)
        existing_params = parse_qs(parsed.query)
        # 合并UTM参数
        utm_dict = utm.to_dict()
        for k, v in utm_dict.items():
            existing_params[k] = [v]
        # 重建query string
        flat_params = {k: v[0] if isinstance(v, list) else v
                       for k, v in existing_params.items()}
        new_query = urlencode(flat_params)
        new_parsed = parsed._replace(query=new_query)
        return urlunparse(new_parsed)

    @staticmethod
    def strip_utm(url: str) -> str:
        """移除URL中的UTM参数"""
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        cleaned = {k: v for k, v in params.items()
                   if not k.startswith("utm_")}
        flat = {k: v[0] if isinstance(v, list) else v
                for k, v in cleaned.items()}
        new_query = urlencode(flat) if flat else ""
        return urlunparse(parsed._replace(query=new_query))

    @staticmethod
    def validate_utm(utm: UTMParams) -> List[str]:
        """验证UTM参数完整性"""
        issues = []
        if not utm.source:
            issues.append("Missing utm_source (required)")
        if not utm.medium:
            issues.append("Missing utm_medium (recommended)")
        if not utm.campaign:
            issues.append("Missing utm_campaign (recommended)")
        if utm.source and " " in utm.source:
            issues.append("utm_source should not contain spaces")
        if utm.campaign and " " in utm.campaign:
            issues.append("utm_campaign should not contain spaces (use hyphens)")
        return issues


class LinkStore:
    """链接数据持久化"""

    def __init__(self, db_path: str = "links.db"):
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
            CREATE TABLE IF NOT EXISTS links (
                link_id TEXT PRIMARY KEY,
                original_url TEXT NOT NULL,
                short_code TEXT UNIQUE NOT NULL,
                short_url TEXT,
                utm_source TEXT DEFAULT '',
                utm_medium TEXT DEFAULT '',
                utm_campaign TEXT DEFAULT '',
                utm_term TEXT DEFAULT '',
                utm_content TEXT DEFAULT '',
                clicks INTEGER DEFAULT 0,
                unique_clicks INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active',
                tags TEXT DEFAULT '[]',
                metadata TEXT DEFAULT '{}',
                created_at TEXT DEFAULT (datetime('now')),
                expires_at TEXT
            );
            CREATE TABLE IF NOT EXISTS click_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                link_id TEXT NOT NULL,
                clicked_at TEXT DEFAULT (datetime('now')),
                referrer TEXT DEFAULT '',
                user_agent TEXT DEFAULT '',
                ip_hash TEXT DEFAULT '',
                country TEXT DEFAULT '',
                device TEXT DEFAULT '',
                platform TEXT DEFAULT '',
                FOREIGN KEY (link_id) REFERENCES links(link_id)
            );
            CREATE TABLE IF NOT EXISTS link_groups (
                group_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT DEFAULT '',
                link_ids TEXT DEFAULT '[]',
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_links_code ON links(short_code);
            CREATE INDEX IF NOT EXISTS idx_clicks_link ON click_events(link_id);
            CREATE INDEX IF NOT EXISTS idx_clicks_date ON click_events(clicked_at);
        """)
        conn.commit()

    def save_link(self, link: TrackedLink):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO links
            (link_id, original_url, short_code, short_url,
             utm_source, utm_medium, utm_campaign, utm_term, utm_content,
             clicks, unique_clicks, status, tags, metadata, created_at, expires_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            link.link_id, link.original_url, link.short_code, link.short_url,
            link.utm.source, link.utm.medium, link.utm.campaign,
            link.utm.term, link.utm.content,
            link.clicks, link.unique_clicks, link.status.value,
            json.dumps(link.tags), json.dumps(link.metadata),
            link.created_at, link.expires_at,
        ))
        conn.commit()

    def get_link(self, link_id: str) -> Optional[Dict]:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM links WHERE link_id=?", (link_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_by_code(self, short_code: str) -> Optional[Dict]:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM links WHERE short_code=?", (short_code,)
        ).fetchone()
        return dict(row) if row else None

    def record_click(self, event: ClickEvent):
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO click_events
            (link_id, clicked_at, referrer, user_agent, ip_hash, country, device, platform)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            event.link_id, event.clicked_at, event.referrer,
            event.user_agent, event.ip_hash, event.country,
            event.device, event.platform,
        ))
        # 更新clicks计数
        conn.execute(
            "UPDATE links SET clicks = clicks + 1 WHERE link_id=?",
            (event.link_id,)
        )
        conn.commit()

    def get_clicks(self, link_id: str, limit: int = 100) -> List[Dict]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM click_events WHERE link_id=? "
            "ORDER BY clicked_at DESC LIMIT ?",
            (link_id, limit)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_all_links(self, status: Optional[str] = None,
                      limit: int = 100) -> List[Dict]:
        conn = self._get_conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM links WHERE status=? "
                "ORDER BY created_at DESC LIMIT ?",
                (status, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM links ORDER BY created_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def update_status(self, link_id: str, status: LinkStatus):
        conn = self._get_conn()
        conn.execute(
            "UPDATE links SET status=? WHERE link_id=?",
            (status.value, link_id)
        )
        conn.commit()

    def get_analytics(self, link_id: str) -> LinkAnalytics:
        conn = self._get_conn()
        clicks = self.get_clicks(link_id, limit=10000)
        if not clicks:
            return LinkAnalytics(link_id=link_id, total_clicks=0, unique_clicks=0)

        # 聚合分析
        referrers = Counter(c.get("referrer", "") for c in clicks if c.get("referrer"))
        countries = Counter(c.get("country", "") for c in clicks if c.get("country"))
        devices = Counter(c.get("device", "") for c in clicks if c.get("device"))
        ip_set = {c.get("ip_hash") for c in clicks if c.get("ip_hash")}
        hourly = Counter()
        daily = Counter()
        for c in clicks:
            ts = c.get("clicked_at", "")
            if ts and len(ts) >= 13:
                try:
                    hour = int(ts[11:13])
                    hourly[hour] += 1
                except (ValueError, IndexError):
                    pass
            if ts and len(ts) >= 10:
                daily[ts[:10]] += 1

        return LinkAnalytics(
            link_id=link_id,
            total_clicks=len(clicks),
            unique_clicks=len(ip_set),
            top_referrers=referrers.most_common(10),
            top_countries=countries.most_common(10),
            device_breakdown=dict(devices),
            hourly_clicks=dict(hourly),
            daily_clicks=dict(daily),
        )

    def close(self):
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None


class LinkIntelligence:
    """
    链接智能管理引擎

    功能:
    - UTM参数自动管理
    - 短链生成与追踪
    - 点击分析
    - 批量链接管理
    - 链接健康检测
    """

    def __init__(self, base_domain: str = "link.example.com",
                 store: Optional[LinkStore] = None):
        self.base_domain = base_domain
        self.store = store or LinkStore()

    def create_tracked_link(self, url: str,
                            campaign: str = "",
                            content: str = "",
                            tags: Optional[List[str]] = None,
                            custom_code: Optional[str] = None,
                            expires_at: Optional[str] = None,
                            utm: Optional[UTMParams] = None) -> TrackedLink:
        """创建追踪链接"""
        # 生成UTM
        if utm is None:
            utm = UTMParams.twitter_default(campaign=campaign, content=content)

        # 构建完整URL
        full_url = UTMBuilder.build_url(url, utm)

        # 生成短码
        code = custom_code or ShortCodeGenerator.generate()
        short_url = f"https://{self.base_domain}/{code}"

        # 生成link_id
        link_id = hashlib.sha256(
            f"{url}:{code}:{datetime.now(timezone.utc).isoformat()}".encode()
        ).hexdigest()[:12]

        link = TrackedLink(
            link_id=link_id,
            original_url=full_url,
            short_code=code,
            short_url=short_url,
            utm=utm,
            clicks=0,
            unique_clicks=0,
            status=LinkStatus.ACTIVE,
            created_at=datetime.now(timezone.utc).isoformat(),
            expires_at=expires_at,
            tags=tags or [],
        )
        self.store.save_link(link)
        return link

    def create_campaign_links(self, urls: List[str],
                               campaign: str,
                               tags: Optional[List[str]] = None) -> List[TrackedLink]:
        """批量创建campaign链接"""
        links = []
        for i, url in enumerate(urls):
            link = self.create_tracked_link(
                url=url,
                campaign=campaign,
                content=f"link_{i + 1}",
                tags=tags,
            )
            links.append(link)
        return links

    def record_click(self, link_id: str, referrer: str = "",
                     user_agent: str = "", ip: str = "",
                     country: str = "") -> bool:
        """记录点击事件"""
        link = self.store.get_link(link_id)
        if not link:
            return False
        if link["status"] != LinkStatus.ACTIVE.value:
            return False

        # 检测设备类型
        device = self._detect_device(user_agent)
        platform = self._detect_platform(referrer)

        event = ClickEvent(
            link_id=link_id,
            clicked_at=datetime.now(timezone.utc).isoformat(),
            referrer=referrer,
            user_agent=user_agent,
            ip_hash=hashlib.sha256(ip.encode()).hexdigest()[:16] if ip else "",
            country=country,
            device=device,
            platform=platform,
        )
        self.store.record_click(event)
        return True

    def get_analytics(self, link_id: str) -> LinkAnalytics:
        """获取链接分析"""
        return self.store.get_analytics(link_id)

    def resolve_short_link(self, short_code: str) -> Optional[str]:
        """解析短链 → 原始URL"""
        link = self.store.get_by_code(short_code)
        if link and link["status"] == LinkStatus.ACTIVE.value:
            return link["original_url"]
        return None

    def check_expired_links(self) -> List[str]:
        """检查并标记过期链接"""
        expired = []
        now = datetime.now(timezone.utc).isoformat()
        links = self.store.get_all_links(status=LinkStatus.ACTIVE.value)
        for link in links:
            if link.get("expires_at") and link["expires_at"] < now:
                self.store.update_status(link["link_id"], LinkStatus.EXPIRED)
                expired.append(link["link_id"])
        return expired

    def get_campaign_report(self, campaign: str) -> Dict[str, Any]:
        """生成campaign级别报告"""
        links = self.store.get_all_links()
        campaign_links = [l for l in links if l.get("utm_campaign") == campaign]
        if not campaign_links:
            return {"campaign": campaign, "links": 0, "total_clicks": 0}

        total_clicks = sum(l.get("clicks", 0) for l in campaign_links)
        return {
            "campaign": campaign,
            "links": len(campaign_links),
            "total_clicks": total_clicks,
            "avg_clicks": round(total_clicks / len(campaign_links), 1),
            "top_links": sorted(
                campaign_links, key=lambda l: l.get("clicks", 0), reverse=True
            )[:5],
        }

    def export_links(self, format: str = "json",
                     status: Optional[str] = None) -> str:
        """导出链接数据"""
        links = self.store.get_all_links(status=status)
        if format == "csv":
            lines = ["link_id,short_url,original_url,clicks,status,campaign,created_at"]
            for l in links:
                lines.append(
                    f"{l['link_id']},{l.get('short_url','')},{l['original_url']},"
                    f"{l.get('clicks',0)},{l['status']},{l.get('utm_campaign','')},{l['created_at']}"
                )
            return "\n".join(lines)
        return json.dumps(links, indent=2, ensure_ascii=False, default=str)

    @staticmethod
    def _detect_device(user_agent: str) -> str:
        ua = user_agent.lower()
        if any(k in ua for k in ["mobile", "iphone", "android", "phone"]):
            return "mobile"
        if any(k in ua for k in ["ipad", "tablet"]):
            return "tablet"
        return "desktop"

    @staticmethod
    def _detect_platform(referrer: str) -> str:
        ref = referrer.lower()
        platforms = {
            "twitter": ["twitter.com", "t.co", "x.com"],
            "facebook": ["facebook.com", "fb.com"],
            "instagram": ["instagram.com"],
            "linkedin": ["linkedin.com"],
            "reddit": ["reddit.com"],
        }
        for platform, domains in platforms.items():
            if any(d in ref for d in domains):
                return platform
        return "direct" if not referrer else "other"

    def find_links_in_text(self, text: str) -> List[str]:
        """从文本中提取所有URL"""
        return re.findall(r"https?://[^\s<>\"']+", text)

    def replace_links_in_text(self, text: str, campaign: str = "") -> Tuple[str, List[TrackedLink]]:
        """替换文本中的URL为追踪短链"""
        urls = self.find_links_in_text(text)
        created = []
        result = text
        for url in urls:
            link = self.create_tracked_link(url, campaign=campaign)
            result = result.replace(url, link.short_url)
            created.append(link)
        return result, created
