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
