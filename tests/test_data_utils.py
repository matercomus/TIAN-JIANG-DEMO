import pytest
import polars as pl
from utils.data_utils import load_channel_data, load_all_channel_videos, fix_channel_dtypes, extract_struct_fields, extract_stat_fields, extract_time_features, handle_nulls_and_types, convert_categorical_columns, add_engagement_metrics, add_text_features, process_channel_data
import json


def make_channel_json(path, records):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False)


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
    df = load_all_channel_videos(tmp_path)
    assert df.shape[0] == 2
    assert set(df["bvid"]) == {"BV1", "BV2"}


def test_load_all_channel_videos_empty(tmp_path):
    # Only channel_map.json present
    (tmp_path / "channel_map.json").write_text("{}", encoding="utf-8")
    with pytest.raises(FileNotFoundError):
        load_all_channel_videos(tmp_path)


# Helper: minimal DataFrame for stat struct
@pytest.fixture
def minimal_stat_df():
    return pl.DataFrame({
        "stat": [
            {"view": 10, "like": 2, "coin": 1, "share": 0, "favorite": 1, "reply": 0},
            {"view": 20, "like": 4, "coin": 2, "share": 1, "favorite": 2, "reply": 1},
        ]
    })


def test_fix_channel_dtypes_basic():
    df = pl.DataFrame({"pubdate": [1_600_000_000, 1_700_000_000], "duration": [60, 120], "days_since_upload": [1.5, 2.5]})
    out = fix_channel_dtypes(df)
    assert "pubdate_dt" in out.columns
    assert "duration_td" in out.columns
    assert "days_since_upload_td" in out.columns
    assert out["pubdate_dt"].dtype == pl.Datetime("ms")


def test_fix_channel_dtypes_missing_cols():
    df = pl.DataFrame({"foo": [1, 2]})
    out = fix_channel_dtypes(df)
    assert "foo" in out.columns


def test_extract_struct_fields(minimal_stat_df):
    out = extract_struct_fields(minimal_stat_df, "stat", ["view", "like"])
    assert "view" in out.columns and "like" in out.columns
    assert out["view"].to_list() == [10, 20]


def test_extract_struct_fields_missing_struct():
    df = pl.DataFrame({"foo": [1, 2]})
    out = extract_struct_fields(df, "stat", ["view"])
    assert "foo" in out.columns
    assert "stat" not in out.columns or "view" not in out.columns or out["foo"].to_list() == [1, 2]


def test_extract_stat_fields(minimal_stat_df):
    out = extract_stat_fields(minimal_stat_df)
    for field in ["view", "like", "coin", "share", "favorite", "reply"]:
        assert field in out.columns


def test_extract_time_features():
    # Use integer timestamps and cast to pl.Datetime("ms")
    timestamps = [1641081600000, 1675382400000]  # 2022-01-02, 2023-02-03 in ms
    df = pl.DataFrame({"pubdate_dt": pl.Series(timestamps, dtype=pl.Datetime("ms"))})
    out = extract_time_features(df)
    for col in ["year", "month", "day", "weekday", "hour"]:
        assert col in out.columns
    # Edge: missing datetime_col
    df2 = pl.DataFrame({"foo": [1, 2]})
    out2 = extract_time_features(df2)
    assert "year" not in out2.columns


def test_handle_nulls_and_types():
    df = pl.DataFrame({"view": [None, None], "title": [None, None]})
    out = handle_nulls_and_types(df)
    assert out["view"].to_list() == [0, 0]
    assert out["title"].to_list() == ["", ""]
    # Edge: already correct types
    df2 = pl.DataFrame({"view": [1, 2], "title": ["a", "b"]})
    out2 = handle_nulls_and_types(df2)
    assert out2["view"].to_list() == [1, 2]
    assert out2["title"].to_list() == ["a", "b"]


def test_convert_categorical_columns():
    df = pl.DataFrame({"tname": ["a", "b"], "uploader_name": ["x", "y"]})
    out = convert_categorical_columns(df)
    assert out["tname"].dtype == pl.Categorical
    assert out["uploader_name"].dtype == pl.Categorical
    # Edge: non-string columns
    df2 = pl.DataFrame({"tname": [1, 2]})
    out2 = convert_categorical_columns(df2)
    assert out2["tname"].dtype != pl.Categorical


def test_add_engagement_metrics():
    df = pl.DataFrame({"view": [10, 20], "like": [2, 4], "coin": [1, 2], "share": [0, 1], "favorite": [1, 2], "reply": [0, 1]})
    out = add_engagement_metrics(df)
    for rate in ["like_rate", "coin_rate", "share_rate", "favorite_rate"]:
        assert rate in out.columns
    assert "total_engagement" in out.columns
    # Edge: missing columns
    df2 = pl.DataFrame({"view": [10, 20]})
    out2 = add_engagement_metrics(df2)
    assert "like_rate" not in out2.columns


def test_add_text_features():
    df = pl.DataFrame({"title": ["abc", "defg"], "desc": ["x", "yz"]})
    out = add_text_features(df)
    assert out["title_len"].to_list() == [3, 4]
    assert out["desc_len"].to_list() == [1, 2]
    assert all(isinstance(s, str) for s in out["title_short"])
    # Edge: missing columns
    df2 = pl.DataFrame({"foo": [1, 2]})
    out2 = add_text_features(df2)
    assert "title_len" not in out2.columns


def test_process_channel_data():
    # Minimal input with all needed columns
    df = pl.DataFrame({
        "pubdate": [1_600_000_000],
        "duration": [60],
        "days_since_upload": [1.5],
        "stat": [{"view": 10, "like": 2, "coin": 1, "share": 0, "favorite": 1, "reply": 0}],
        "title": ["abc"],
        "desc": ["desc"],
        "tname": ["cat"],
        "uploader_name": ["up"],
    })
    out = process_channel_data(df, log=False)
    # Check for derived columns
    for col in ["pubdate_dt", "duration_td", "days_since_upload_td", "view", "like", "coin", "share", "favorite", "reply", "year", "month", "day", "weekday", "hour", "like_rate", "coin_rate", "share_rate", "favorite_rate", "total_engagement", "title_len", "desc_len", "title_short", "desc_short"]:
        assert col in out.columns
    # Edge: missing columns
    df2 = pl.DataFrame({"foo": [1]})
    out2 = process_channel_data(df2, log=False)
    assert "foo" in out2.columns 