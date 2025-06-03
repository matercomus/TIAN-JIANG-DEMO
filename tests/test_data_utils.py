import pytest
import polars as pl
from utils.data_utils import load_channel_data, load_all_channel_videos
from pathlib import Path
import json
import logging


def make_channel_json(path, records):
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")


def test_load_channel_data(tmp_path):
    data = [
        {"bvid": "BV1", "title": "A"},
        {"bvid": "BV2", "title": "B"},
    ]
    file = tmp_path / "test_channel.json"
    make_channel_json(file, data)
    df = load_channel_data(file)
    assert df.shape == (2, 2)
    assert set(df["bvid"]) == {"BV1", "BV2"}


def test_load_channel_data_missing(tmp_path):
    file = tmp_path / "missing.json"
    with pytest.raises(FileNotFoundError):
        load_channel_data(file)


def test_load_all_channel_videos(tmp_path):
    # Create two channel files
    data1 = [{"bvid": "BV1", "title": "A"}]
    data2 = [{"bvid": "BV2", "title": "B"}]
    make_channel_json(tmp_path / "chan1.json", data1)
    make_channel_json(tmp_path / "chan2.json", data2)
    # Add a channel_map.json (should be ignored)
    (tmp_path / "channel_map.json").write_text("{}", encoding="utf-8")
    # Add a .txt file (should be ignored)
    (tmp_path / "channel_URLs.txt").write_text("url", encoding="utf-8")
    lf = load_all_channel_videos(tmp_path)
    df = lf.collect()
    assert df.shape[0] == 2
    assert set(df["bvid"]) == {"BV1", "BV2"}


def test_load_all_channel_videos_empty(tmp_path):
    # Only channel_map.json present
    (tmp_path / "channel_map.json").write_text("{}", encoding="utf-8")
    with pytest.raises(FileNotFoundError):
        load_all_channel_videos(tmp_path) 