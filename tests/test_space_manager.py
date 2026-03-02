"""
Tests for Space Manager
"""
import pytest
from datetime import datetime, timedelta, UTC
from bot.space_manager import (
    SpaceInfo, SpaceState, SpaceRecording, RecordingQuality,
    SpaceTranscript, SpaceSummary, SpaceMonitorRule,
    SpaceStore, SpaceManager, ParticipantRole
)


@pytest.fixture
def store():
    """测试用Store"""
    return SpaceStore(":memory:")


@pytest.fixture
def manager(store):
    """测试用Manager"""
    return SpaceManager(store)


class TestSpaceInfo:
    """测试SpaceInfo"""
    
    def test_create_space(self):
        space = SpaceInfo(
            space_id="sp_123",
            title="AI Discussion",
            host_id="user_1",
            host_username="ai_expert",
            state=SpaceState.LIVE
        )
        assert space.space_id == "sp_123"
        assert space.is_active()
    
    def test_duration_calculation(self):
        space = SpaceInfo(
            space_id="sp_123",
            title="Test",
            host_id="user_1",
            host_username="host",
            state=SpaceState.ENDED,
            started_at=datetime(2026, 3, 1, 10, 0),
            ended_at=datetime(2026, 3, 1, 11, 30)
        )
        assert space.duration_minutes() == 90
    
    def test_no_duration_when_not_ended(self):
        space = SpaceInfo(
            space_id="sp_123",
            title="Test",
            host_id="user_1",
            host_username="host",
            state=SpaceState.LIVE,
            started_at=datetime.now(UTC)
        )
        assert space.duration_minutes() is None


class TestSpaceRecording:
    """测试SpaceRecording"""
    
    def test_bitrate_calculation(self):
        recording = SpaceRecording(
            recording_id="rec_1",
            space_id="sp_1",
            file_path="/tmp/rec.m4a",
            quality=RecordingQuality.MEDIUM,
            duration_seconds=3600,  # 1 hour
            file_size_mb=82.0,      # ~192kbps
            started_at=datetime.now(UTC),
            ended_at=datetime.now(UTC)
        )
        # 82MB * 8 * 1024 / 3600s ≈ 187kbps
        assert 180 <= recording.bitrate_kbps() <= 195


class TestSpaceTranscript:
    """测试SpaceTranscript"""
    
    def test_full_text_generation(self):
        transcript = SpaceTranscript(
            transcript_id="trans_1",
            space_id="sp_1",
            recording_id="rec_1",
            segments=[
                {"speaker": "Host", "text": "Hello", "start": 0, "end": 1},
                {"speaker": "Guest", "text": "Hi there", "start": 2, "end": 3}
            ]
        )
        text = transcript.get_full_text()
        assert "[Host] Hello" in text
        assert "[Guest] Hi there" in text
    
    def test_keyword_search(self):
        transcript = SpaceTranscript(
            transcript_id="trans_1",
            space_id="sp_1",
            recording_id="rec_1",
            segments=[
                {"speaker": "Host", "text": "Let's talk about AI", "start": 0, "end": 2},
                {"speaker": "Guest", "text": "Machine learning is fascinating", "start": 3, "end": 5}
            ]
        )
        results = transcript.search_keywords(["AI", "learning"])
        assert len(results) == 2
        assert results[0]['keyword'] == "AI"
        assert results[1]['keyword'] == "learning"


class TestSpaceSummary:
    """测试SpaceSummary"""
    
    def test_markdown_formatting(self):
        summary = SpaceSummary(
            summary_id="sum_1",
            space_id="sp_1",
            transcript_id="trans_1",
            title="AI Discussion",
            key_points=["Point 1", "Point 2"],
            topics_discussed=["AI", "ML"],
            action_items=["Research AI", "Write blog post"]
        )
        md = summary.format_markdown()
        assert "# AI Discussion" in md
        assert "## Key Points" in md
        assert "- Point 1" in md
        assert "## Action Items" in md
        assert "- [ ] Research AI" in md


class TestSpaceMonitorRule:
    """测试SpaceMonitorRule"""
    
    def test_host_matching(self):
        rule = SpaceMonitorRule(
            rule_id="rule_1",
            name="Monitor AI Experts",
            host_usernames=["ai_expert", "ml_guru"]
        )
        
        space1 = SpaceInfo(
            space_id="sp_1",
            title="AI Talk",
            host_id="user_1",
            host_username="ai_expert",
            state=SpaceState.LIVE
        )
        space2 = SpaceInfo(
            space_id="sp_2",
            title="Crypto Talk",
            host_id="user_2",
            host_username="crypto_bro",
            state=SpaceState.LIVE
        )
        
        assert rule.matches(space1)
        assert not rule.matches(space2)
    
    def test_keyword_matching(self):
        rule = SpaceMonitorRule(
            rule_id="rule_1",
            name="AI Spaces",
            keywords=["AI", "machine learning"]
        )
        
        space1 = SpaceInfo(
            space_id="sp_1",
            title="Deep Dive into AI",
            host_id="user_1",
            host_username="host",
            state=SpaceState.LIVE
        )
        space2 = SpaceInfo(
            space_id="sp_2",
            title="Cooking Tips",
            host_id="user_2",
            host_username="chef",
            state=SpaceState.LIVE
        )
        
        assert rule.matches(space1)
        assert not rule.matches(space2)
    
    def test_disabled_rule_no_match(self):
        rule = SpaceMonitorRule(
            rule_id="rule_1",
            name="Test",
            host_usernames=["anyone"],
            enabled=False
        )
        
        space = SpaceInfo(
            space_id="sp_1",
            title="Test",
            host_id="user_1",
            host_username="anyone",
            state=SpaceState.LIVE
        )
        
        assert not rule.matches(space)


class TestSpaceStore:
    """测试SpaceStore"""
    
    def test_save_and_retrieve_space(self, store):
        space = SpaceInfo(
            space_id="sp_123",
            title="Test Space",
            host_id="user_1",
            host_username="host",
            state=SpaceState.LIVE,
            started_at=datetime.now(UTC)
        )
        
        store.save_space(space)
        active = store.get_active_spaces()
        
        assert len(active) == 1
        assert active[0].space_id == "sp_123"
        assert active[0].title == "Test Space"
    
    def test_save_recording(self, store):
        recording = SpaceRecording(
            recording_id="rec_1",
            space_id="sp_1",
            file_path="/tmp/rec.m4a",
            quality=RecordingQuality.HIGH,
            duration_seconds=3600,
            file_size_mb=150.0,
            started_at=datetime.now(UTC),
            ended_at=datetime.now(UTC)
        )
        
        store.save_recording(recording)
        # 验证保存成功（实际需要查询验证）
    
    def test_save_transcript(self, store):
        transcript = SpaceTranscript(
            transcript_id="trans_1",
            space_id="sp_1",
            recording_id="rec_1",
            segments=[{"speaker": "Host", "text": "Hello", "start": 0, "end": 1}],
            word_count=1
        )
        
        store.save_transcript(transcript)
    
    def test_save_summary(self, store):
        summary = SpaceSummary(
            summary_id="sum_1",
            space_id="sp_1",
            transcript_id="trans_1",
            title="Test Summary",
            key_points=["Point 1"]
        )
        
        store.save_summary(summary)
    
    def test_save_and_retrieve_rules(self, store):
        rule = SpaceMonitorRule(
            rule_id="rule_1",
            name="Test Rule",
            host_usernames=["host1", "host2"],
            auto_record=True
        )
        
        store.save_rule(rule)
        rules = store.get_all_rules()
        
        assert len(rules) == 1
        assert rules[0].rule_id == "rule_1"
        assert rules[0].auto_record


class TestSpaceManager:
    """测试SpaceManager"""
    
    def test_start_recording(self, manager):
        recording = manager.start_recording("sp_123", RecordingQuality.HIGH)
        
        assert recording.space_id == "sp_123"
        assert recording.quality == RecordingQuality.HIGH
        assert "sp_123" in manager.active_recordings
    
    def test_stop_recording(self, manager):
        manager.start_recording("sp_123")
        recording = manager.stop_recording("sp_123")
        
        assert recording is not None
        assert recording.duration_seconds >= 0
        assert "sp_123" not in manager.active_recordings
    
    def test_transcribe_recording(self, manager):
        transcript = manager.transcribe_recording("rec_123")
        
        assert transcript.recording_id == "rec_123"
        assert len(transcript.segments) > 0
    
    def test_generate_summary(self, manager):
        summary = manager.generate_summary("trans_123")
        
        assert summary.transcript_id == "trans_123"
        assert len(summary.key_points) > 0
    
    def test_get_space_analytics(self, manager):
        analytics = manager.get_space_analytics("sp_123")
        
        assert "total_duration_minutes" in analytics
        assert "engagement_score" in analytics


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
