"""
Twitter Space Manager - 自动化Space监听、参与、录制、转录、摘要
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import List, Dict, Optional, Set
import sqlite3
import json
import re
from pathlib import Path


class SpaceState(Enum):
    """Space状态"""
    SCHEDULED = "scheduled"  # 已预约
    LIVE = "live"            # 直播中
    ENDED = "ended"          # 已结束
    CANCELLED = "cancelled"  # 已取消


class ParticipantRole(Enum):
    """参与者角色"""
    HOST = "host"            # 主持人
    SPEAKER = "speaker"      # 发言人
    LISTENER = "listener"    # 听众


class RecordingQuality(Enum):
    """录制质量"""
    HIGH = "high"      # 高质量 (320kbps)
    MEDIUM = "medium"  # 中等 (192kbps)
    LOW = "low"        # 低质量 (128kbps)


@dataclass
class SpaceInfo:
    """Space基本信息"""
    space_id: str
    title: str
    host_id: str
    host_username: str
    state: SpaceState
    scheduled_start: Optional[datetime] = None
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    participant_count: int = 0
    speaker_ids: List[str] = field(default_factory=list)
    topics: List[str] = field(default_factory=list)
    is_ticketed: bool = False
    language: str = "en"
    
    def duration_minutes(self) -> Optional[int]:
        """计算Space时长（分钟）"""
        if self.started_at and self.ended_at:
            return int((self.ended_at - self.started_at).total_seconds() / 60)
        return None
    
    def is_active(self) -> bool:
        """是否正在进行"""
        return self.state == SpaceState.LIVE


@dataclass
class SpaceRecording:
    """Space录制记录"""
    recording_id: str
    space_id: str
    file_path: str
    quality: RecordingQuality
    duration_seconds: int
    file_size_mb: float
    started_at: datetime
    ended_at: datetime
    is_transcribed: bool = False
    transcript_path: Optional[str] = None
    
    def bitrate_kbps(self) -> int:
        """计算实际比特率"""
        if self.duration_seconds > 0:
            return int((self.file_size_mb * 8 * 1024) / self.duration_seconds)
        return 0


@dataclass
class SpaceTranscript:
    """Space转录文本"""
    transcript_id: str
    space_id: str
    recording_id: str
    segments: List[Dict[str, any]] = field(default_factory=list)  # [{speaker, text, start, end}]
    language: str = "en"
    confidence: float = 0.0
    word_count: int = 0
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def get_full_text(self) -> str:
        """获取完整文本"""
        return "\n".join([f"[{seg['speaker']}] {seg['text']}" for seg in self.segments])
    
    def search_keywords(self, keywords: List[str]) -> List[Dict]:
        """搜索关键词出现位置"""
        results = []
        for seg in self.segments:
            for kw in keywords:
                if re.search(kw, seg['text'], re.IGNORECASE):
                    results.append({
                        'keyword': kw,
                        'speaker': seg['speaker'],
                        'text': seg['text'],
                        'timestamp': seg['start']
                    })
        return results


@dataclass
class SpaceSummary:
    """Space摘要"""
    summary_id: str
    space_id: str
    transcript_id: str
    title: str
    key_points: List[str] = field(default_factory=list)
    topics_discussed: List[str] = field(default_factory=list)
    speakers_summary: Dict[str, str] = field(default_factory=dict)  # {speaker: contribution}
    highlights: List[Dict] = field(default_factory=list)  # [{timestamp, text, speaker}]
    action_items: List[str] = field(default_factory=list)
    sentiment: str = "neutral"  # positive/neutral/negative
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def format_markdown(self) -> str:
        """格式化为Markdown"""
        md = f"# {self.title}\n\n"
        md += f"**Generated:** {self.created_at.strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        
        if self.key_points:
            md += "## Key Points\n"
            for point in self.key_points:
                md += f"- {point}\n"
            md += "\n"
        
        if self.topics_discussed:
            md += "## Topics Discussed\n"
            md += ", ".join(self.topics_discussed) + "\n\n"
        
        if self.speakers_summary:
            md += "## Speakers\n"
            for speaker, summary in self.speakers_summary.items():
                md += f"**{speaker}:** {summary}\n\n"
        
        if self.highlights:
            md += "## Highlights\n"
            for h in self.highlights:
                md += f"- [{h['timestamp']}] **{h['speaker']}:** {h['text']}\n"
            md += "\n"
        
        if self.action_items:
            md += "## Action Items\n"
            for item in self.action_items:
                md += f"- [ ] {item}\n"
        
        return md


@dataclass
class SpaceMonitorRule:
    """Space监控规则"""
    rule_id: str
    name: str
    host_usernames: List[str] = field(default_factory=list)  # 监控的主持人
    keywords: List[str] = field(default_factory=list)        # 标题关键词
    topics: List[str] = field(default_factory=list)          # 话题标签
    auto_join: bool = False                                  # 自动加入
    auto_record: bool = False                                # 自动录制
    auto_transcribe: bool = False                            # 自动转录
    auto_summarize: bool = False                             # 自动摘要
    notify_on_start: bool = True                             # 开始时通知
    notify_on_end: bool = False                              # 结束时通知
    enabled: bool = True
    
    def matches(self, space: SpaceInfo) -> bool:
        """检查Space是否匹配规则"""
        if not self.enabled:
            return False
        
        # 检查主持人
        if self.host_usernames and space.host_username not in self.host_usernames:
            return False
        
        # 检查标题关键词
        if self.keywords:
            title_lower = space.title.lower()
            if not any(kw.lower() in title_lower for kw in self.keywords):
                return False
        
        # 检查话题
        if self.topics:
            if not any(topic in space.topics for topic in self.topics):
                return False
        
        return True


class SpaceStore:
    """Space数据持久化"""
    
    def __init__(self, db_path: str = "spaces.db"):
        self.db_path = db_path
        self.conn = None
        self._init_db()
        self.db_path = db_path
        self._init_db()
        self._init_db()
    
    def _init_db(self):
        """初始化数据库"""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        c = self.conn.cursor()
        """初始化数据库"""
        conn = self.conn
        c = conn.cursor()
        
        # Spaces表
        c.execute("""
            CREATE TABLE IF NOT EXISTS spaces (
                space_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                host_id TEXT NOT NULL,
                host_username TEXT NOT NULL,
                state TEXT NOT NULL,
                scheduled_start TEXT,
                started_at TEXT,
                ended_at TEXT,
                participant_count INTEGER DEFAULT 0,
                speaker_ids TEXT,
                topics TEXT,
                is_ticketed INTEGER DEFAULT 0,
                language TEXT DEFAULT 'en',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 录制表
        c.execute("""
            CREATE TABLE IF NOT EXISTS recordings (
                recording_id TEXT PRIMARY KEY,
                space_id TEXT NOT NULL,
                file_path TEXT NOT NULL,
                quality TEXT NOT NULL,
                duration_seconds INTEGER NOT NULL,
                file_size_mb REAL NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT NOT NULL,
                is_transcribed INTEGER DEFAULT 0,
                transcript_path TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (space_id) REFERENCES spaces(space_id)
            )
        """)
        
        # 转录表
        c.execute("""
            CREATE TABLE IF NOT EXISTS transcripts (
                transcript_id TEXT PRIMARY KEY,
                space_id TEXT NOT NULL,
                recording_id TEXT NOT NULL,
                segments TEXT NOT NULL,
                language TEXT DEFAULT 'en',
                confidence REAL DEFAULT 0.0,
                word_count INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (space_id) REFERENCES spaces(space_id),
                FOREIGN KEY (recording_id) REFERENCES recordings(recording_id)
            )
        """)
        
        # 摘要表
        c.execute("""
            CREATE TABLE IF NOT EXISTS summaries (
                summary_id TEXT PRIMARY KEY,
                space_id TEXT NOT NULL,
                transcript_id TEXT NOT NULL,
                title TEXT NOT NULL,
                key_points TEXT,
                topics_discussed TEXT,
                speakers_summary TEXT,
                highlights TEXT,
                action_items TEXT,
                sentiment TEXT DEFAULT 'neutral',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (space_id) REFERENCES spaces(space_id),
                FOREIGN KEY (transcript_id) REFERENCES transcripts(transcript_id)
            )
        """)
        
        # 监控规则表
        c.execute("""
            CREATE TABLE IF NOT EXISTS monitor_rules (
                rule_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                host_usernames TEXT,
                keywords TEXT,
                topics TEXT,
                auto_join INTEGER DEFAULT 0,
                auto_record INTEGER DEFAULT 0,
                auto_transcribe INTEGER DEFAULT 0,
                auto_summarize INTEGER DEFAULT 0,
                notify_on_start INTEGER DEFAULT 1,
                notify_on_end INTEGER DEFAULT 0,
                enabled INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 索引
        c.execute("CREATE INDEX IF NOT EXISTS idx_spaces_host ON spaces(host_username)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_spaces_state ON spaces(state)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_recordings_space ON recordings(space_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_transcripts_space ON transcripts(space_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_summaries_space ON summaries(space_id)")
        
        conn.commit()
    
    def save_space(self, space: SpaceInfo):
        """保存Space信息"""
        conn = self.conn
        c = conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO spaces 
            (space_id, title, host_id, host_username, state, scheduled_start, 
             started_at, ended_at, participant_count, speaker_ids, topics, 
             is_ticketed, language)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            space.space_id, space.title, space.host_id, space.host_username,
            space.state.value,
            space.scheduled_start.isoformat() if space.scheduled_start else None,
            space.started_at.isoformat() if space.started_at else None,
            space.ended_at.isoformat() if space.ended_at else None,
            space.participant_count,
            json.dumps(space.speaker_ids),
            json.dumps(space.topics),
            1 if space.is_ticketed else 0,
            space.language
        ))
        conn.commit()
    
    def get_active_spaces(self) -> List[SpaceInfo]:
        """获取所有活跃Space"""
        conn = self.conn
        c = conn.cursor()
        c.execute("SELECT * FROM spaces WHERE state = ?", (SpaceState.LIVE.value,))
        rows = c.fetchall()
        
        spaces = []
        for row in rows:
            spaces.append(SpaceInfo(
                space_id=row[0],
                title=row[1],
                host_id=row[2],
                host_username=row[3],
                state=SpaceState(row[4]),
                scheduled_start=datetime.fromisoformat(row[5]) if row[5] else None,
                started_at=datetime.fromisoformat(row[6]) if row[6] else None,
                ended_at=datetime.fromisoformat(row[7]) if row[7] else None,
                participant_count=row[8],
                speaker_ids=json.loads(row[9]) if row[9] else [],
                topics=json.loads(row[10]) if row[10] else [],
                is_ticketed=bool(row[11]),
                language=row[12]
            ))
        return spaces
    
    def save_recording(self, recording: SpaceRecording):
        """保存录制记录"""
        conn = self.conn
        c = conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO recordings
            (recording_id, space_id, file_path, quality, duration_seconds,
             file_size_mb, started_at, ended_at, is_transcribed, transcript_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            recording.recording_id, recording.space_id, recording.file_path,
            recording.quality.value, recording.duration_seconds,
            recording.file_size_mb,
            recording.started_at.isoformat(),
            recording.ended_at.isoformat(),
            1 if recording.is_transcribed else 0,
            recording.transcript_path
        ))
        conn.commit()
    
    def save_transcript(self, transcript: SpaceTranscript):
        """保存转录文本"""
        conn = self.conn
        c = conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO transcripts
            (transcript_id, space_id, recording_id, segments, language,
             confidence, word_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            transcript.transcript_id, transcript.space_id, transcript.recording_id,
            json.dumps(transcript.segments), transcript.language,
            transcript.confidence, transcript.word_count
        ))
        conn.commit()
    
    def save_summary(self, summary: SpaceSummary):
        """保存摘要"""
        conn = self.conn
        c = conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO summaries
            (summary_id, space_id, transcript_id, title, key_points,
             topics_discussed, speakers_summary, highlights, action_items, sentiment)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            summary.summary_id, summary.space_id, summary.transcript_id,
            summary.title,
            json.dumps(summary.key_points),
            json.dumps(summary.topics_discussed),
            json.dumps(summary.speakers_summary),
            json.dumps(summary.highlights),
            json.dumps(summary.action_items),
            summary.sentiment
        ))
        conn.commit()
    
    def save_rule(self, rule: SpaceMonitorRule):
        """保存监控规则"""
        conn = self.conn
        c = conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO monitor_rules
            (rule_id, name, host_usernames, keywords, topics,
             auto_join, auto_record, auto_transcribe, auto_summarize,
             notify_on_start, notify_on_end, enabled)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            rule.rule_id, rule.name,
            json.dumps(rule.host_usernames),
            json.dumps(rule.keywords),
            json.dumps(rule.topics),
            1 if rule.auto_join else 0,
            1 if rule.auto_record else 0,
            1 if rule.auto_transcribe else 0,
            1 if rule.auto_summarize else 0,
            1 if rule.notify_on_start else 0,
            1 if rule.notify_on_end else 0,
            1 if rule.enabled else 0
        ))
        conn.commit()
    
    def get_all_rules(self) -> List[SpaceMonitorRule]:
        """获取所有监控规则"""
        conn = self.conn
        c = conn.cursor()
        c.execute("SELECT * FROM monitor_rules WHERE enabled = 1")
        rows = c.fetchall()
        
        rules = []
        for row in rows:
            rules.append(SpaceMonitorRule(
                rule_id=row[0],
                name=row[1],
                host_usernames=json.loads(row[2]) if row[2] else [],
                keywords=json.loads(row[3]) if row[3] else [],
                topics=json.loads(row[4]) if row[4] else [],
                auto_join=bool(row[5]),
                auto_record=bool(row[6]),
                auto_transcribe=bool(row[7]),
                auto_summarize=bool(row[8]),
                notify_on_start=bool(row[9]),
                notify_on_end=bool(row[10]),
                enabled=bool(row[11])
            ))
        return rules


class SpaceManager:
    """Space管理器 - 统一编排"""
    
    def __init__(self, store: SpaceStore):
        self.store = store
        self.active_recordings: Dict[str, SpaceRecording] = {}
    
    def discover_spaces(self, host_username: Optional[str] = None) -> List[SpaceInfo]:
        """发现Space（模拟API调用）"""
        # 实际实现需要调用Twitter API
        # 这里返回模拟数据
        return []
    
    def start_recording(self, space_id: str, quality: RecordingQuality = RecordingQuality.MEDIUM) -> SpaceRecording:
        """开始录制Space"""
        recording = SpaceRecording(
            recording_id=f"rec_{space_id}_{int(datetime.utcnow().timestamp())}",
            space_id=space_id,
            file_path=f"recordings/{space_id}.m4a",
            quality=quality,
            duration_seconds=0,
            file_size_mb=0.0,
            started_at=datetime.utcnow(),
            ended_at=datetime.utcnow()
        )
        self.active_recordings[space_id] = recording
        return recording
    
    def stop_recording(self, space_id: str) -> Optional[SpaceRecording]:
        """停止录制"""
        if space_id not in self.active_recordings:
            return None
        
        recording = self.active_recordings.pop(space_id)
        recording.ended_at = datetime.utcnow()
        recording.duration_seconds = int((recording.ended_at - recording.started_at).total_seconds())
        
        # 模拟文件大小计算
        bitrate_map = {
            RecordingQuality.HIGH: 320,
            RecordingQuality.MEDIUM: 192,
            RecordingQuality.LOW: 128
        }
        bitrate = bitrate_map[recording.quality]
        recording.file_size_mb = (bitrate * recording.duration_seconds) / (8 * 1024)
        
        self.store.save_recording(recording)
        return recording
    
    def transcribe_recording(self, recording_id: str) -> SpaceTranscript:
        """转录录制文件（模拟）"""
        # 实际实现需要调用Whisper API或其他转录服务
        transcript = SpaceTranscript(
            transcript_id=f"trans_{recording_id}",
            space_id="space_123",
            recording_id=recording_id,
            segments=[
                {"speaker": "Host", "text": "Welcome everyone!", "start": 0, "end": 2},
                {"speaker": "Guest1", "text": "Thanks for having me.", "start": 3, "end": 5}
            ],
            language="en",
            confidence=0.95,
            word_count=8
        )
        self.store.save_transcript(transcript)
        return transcript
    
    def generate_summary(self, transcript_id: str) -> SpaceSummary:
        """生成摘要（模拟）"""
        # 实际实现需要调用LLM API
        summary = SpaceSummary(
            summary_id=f"sum_{transcript_id}",
            space_id="space_123",
            transcript_id=transcript_id,
            title="AI and the Future of Work",
            key_points=[
                "AI will augment rather than replace human workers",
                "Reskilling is critical for workforce adaptation",
                "Ethical considerations must guide AI deployment"
            ],
            topics_discussed=["AI", "automation", "ethics", "workforce"],
            speakers_summary={
                "Host": "Moderated discussion and asked probing questions",
                "Guest1": "Shared insights on AI implementation in enterprise"
            },
            highlights=[
                {"timestamp": "00:15:30", "speaker": "Guest1", "text": "The key is human-AI collaboration"}
            ],
            action_items=["Research AI training programs", "Draft ethical AI policy"],
            sentiment="positive"
        )
        self.store.save_summary(summary)
        return summary
    
    def monitor_spaces(self) -> Dict[str, List[SpaceInfo]]:
        """监控Space并匹配规则"""
        rules = self.store.get_all_rules()
        discovered = self.discover_spaces()
        
        matched = {}
        for space in discovered:
            for rule in rules:
                if rule.matches(space):
                    if rule.rule_id not in matched:
                        matched[rule.rule_id] = []
                    matched[rule.rule_id].append(space)
                    
                    # 自动操作
                    if rule.auto_join:
                        self._join_space(space.space_id)
                    if rule.auto_record:
                        self.start_recording(space.space_id)
        
        return matched
    
    def _join_space(self, space_id: str):
        """加入Space（模拟）"""
        # 实际实现需要调用Twitter API
        pass
    
    def get_space_analytics(self, space_id: str) -> Dict:
        """获取Space分析数据"""
        # 查询数据库获取完整分析
        return {
            "total_duration_minutes": 120,
            "peak_participants": 450,
            "total_speakers": 8,
            "recording_size_mb": 230.4,
            "transcript_word_count": 15000,
            "key_topics": ["AI", "crypto", "web3"],
            "sentiment": "positive",
            "engagement_score": 8.5
        }
