import pytest
from utils.models import UploaderInfo, VideoInfo
from pydantic import ValidationError
import tempfile
import shutil
import os
from utils.channel_map_manager import ChannelMapManager


def test_uploader_info_valid():
    info = UploaderInfo(
        uploader_mid=123,
        uploader_name="testuser",
        uploader_follower=1000,
        uploader_total_videos=50,
    )
    assert info.uploader_mid == 123
    assert info.uploader_name == "testuser"
    assert info.uploader_follower == 1000
    assert info.uploader_total_videos == 50


def test_uploader_info_missing_optional():
    info = UploaderInfo(
        uploader_mid=123,
        uploader_name=None,
        uploader_follower=None,
        uploader_total_videos=None,
    )
    assert info.uploader_mid == 123
    assert info.uploader_name is None
    assert info.uploader_follower is None
    assert info.uploader_total_videos is None


def test_video_info_valid():
    info = VideoInfo(
        bvid="BV1xx411C7Yd",
        title="Test Video",
        desc="desc",
        pubdate=1234567890,
        duration=100,
        tname="Category",
        stat={"like": 10},
        owner={"mid": 123},
        pages=[{"cid": 1}],
        rights={"download": 1},
        tags=[{"tag_id": 1}],
        related_videos=[{"bvid": "BV2xx411C7Yd"}],
        danmaku_count=5,
        num_pages=1,
        days_since_upload=1.5,
        uploader_mid=123,
        uploader_name="testuser",
        uploader_follower=1000,
        uploader_total_videos=50,
    )
    assert info.bvid == "BV1xx411C7Yd"
    assert info.title == "Test Video"
    assert info.tags[0]["tag_id"] == 1
    assert info.related_videos[0]["bvid"] == "BV2xx411C7Yd"


def test_video_info_missing_required():
    with pytest.raises(ValidationError):
        VideoInfo(title="Test Video")


def test_video_info_wrong_type():
    with pytest.raises(ValueError):
        VideoInfo(
            bvid="BV1xx411C7Yd",
            title="Test Video",
            desc="desc",
            pubdate=1234567890,
            duration=100,
            tname="Category",
            stat={"like": 10},
            owner={"mid": 123},
            pages=[{"cid": 1}],
            rights={"download": 1},
            tags=[{"tag_id": 1}],
            related_videos=[{"bvid": "BV2xx411C7Yd"}],
            danmaku_count=5,
            num_pages=1,
            days_since_upload=1.5,
            uploader_mid="not_an_int",
            uploader_name="testuser",
            uploader_follower=1000,
            uploader_total_videos=50,
        )


def test_channel_map_manager_basic(tmp_path):
    # Setup temp files
    urls_path = tmp_path / "urls.txt"
    map_path = tmp_path / "map.json"
    urls = [
        "https://space.bilibili.com/12345",
        "https://space.bilibili.com/67890",
        "https://space.bilibili.com/12345",  # duplicate
    ]
    with open(urls_path, "w", encoding="utf-8") as f:
        for url in urls:
            f.write(url + "\n")
    mgr = ChannelMapManager(str(map_path), str(urls_path))
    mgr.update_map_from_urls()
    # Should have two entries
    assert len(mgr.channel_map) == 2
    assert "12345" in mgr.channel_map
    assert "67890" in mgr.channel_map
    # Test id extraction
    assert mgr.extract_channel_id(urls[0]) == 12345
    assert mgr.extract_channel_id(urls[1]) == 67890
    # Test no update if run again
    mgr.update_map_from_urls()
    assert len(mgr.channel_map) == 2
    # Test save/load
    mgr2 = ChannelMapManager(str(map_path), str(urls_path))
    assert mgr2.channel_map == mgr.channel_map


def test_channel_map_manager_related_channels(tmp_path):
    import json
    from utils.channel_map_manager import ChannelMapManager

    # Create a mock channel info file
    info_dir = tmp_path / "info"
    info_dir.mkdir()
    related_mids = [111, 222, 333]
    videos = [
        {
            "bvid": "BV1xx411C7Yd",
            "title": "Test Video",
            "desc": "desc",
            "pubdate": 1234567890,
            "duration": 100,
            "tname": "Category",
            "stat": {"like": 10},
            "owner": {"mid": 999},
            "pages": [{"cid": 1}],
            "rights": {"download": 1},
            "tags": [{"tag_id": 1}],
            "related_videos": [
                {"bvid": "BV2xx411C7Yd", "owner": {"mid": mid, "name": f"user{mid}"}}
                for mid in related_mids
            ],
            "danmaku_count": 5,
            "num_pages": 1,
            "days_since_upload": 1.5,
            "uploader_mid": 999,
            "uploader_name": "testuser",
            "uploader_follower": 1000,
            "uploader_total_videos": 50,
        }
    ]
    info_file = info_dir / "mock_channel.json"
    with open(info_file, "w", encoding="utf-8") as f:
        json.dump(videos, f)
    # Setup ChannelMapManager
    map_path = tmp_path / "map.json"
    urls_path = tmp_path / "urls.txt"
    mgr = ChannelMapManager(str(map_path), str(urls_path))
    # Should start empty
    assert mgr.channel_map == {}
    # Update from related channels
    mgr.update_map_from_related_channels(str(info_dir))
    # Should have all related mids
    for mid in related_mids:
        assert str(mid) in mgr.channel_map or mid in mgr.channel_map.values()
    # Rerun should not add duplicates
    before = dict(mgr.channel_map)
    mgr.update_map_from_related_channels(str(info_dir))
    assert mgr.channel_map == before
