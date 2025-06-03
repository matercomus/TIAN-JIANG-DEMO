'''Utility functions for data processing'''

# EDA Enhancement Checklist:
# [x] 1. Flatten/extract useful fields from Structs and Lists (stat, owner, rights, tags, ...)
# [x] 2. Convert/format time columns (year, month, etc.)
# [x] 3. Handle nulls and types
# [x] 4. Categorical columns
# [x] 5. Add derived columns (engagement, etc.)
# [x] 6. Text columns (length, truncation, ...)

import polars as pl
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import logging
from IPython.display import display, Markdown

def load_channel_data(file_path):
    """
    Loads a single channel's video info JSON file as a polars DataFrame (eager).
    Assumes the file is a JSON array of dicts (one channel's videos).
    """
    logger = logging.getLogger(__name__)
    path = Path(file_path)
    logger.info(f"Loading channel data from {file_path}")
    if not path.exists():
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")
    df = pl.read_json(str(path))
    logger.info(f"Loaded {df.height} records from {file_path}")
    return df


def load_all_channel_videos(channels_info_dir="channels_info"):
    """
    Loads all channel video JSON files in the given directory as a single polars DataFrame (eager).
    Only files ending with .json are loaded, except channel_map.json and channel_URLs.txt.
    Assumes each file is a JSON array of dicts (one channel's videos).
    """
    logger = logging.getLogger(__name__)
    channels_info_dir = Path(channels_info_dir)
    logger.info(f"Scanning for channel video JSON files in {channels_info_dir}")
    json_files = [
        f for f in channels_info_dir.glob("*.json")
        if f.name != "channel_map.json"
    ]
    logger.info(f"Found {len(json_files)} channel video JSON files in {channels_info_dir}")
    if not json_files:
        logger.error(f"No channel video .json files found in {channels_info_dir}")
        raise FileNotFoundError(f"No channel video .json files found in {channels_info_dir}")
    dfs = [pl.read_json(str(f)) for f in json_files]
    logger.info(f"Returning DataFrame for {len(dfs)} files.")
    return pl.concat(dfs)

def display_polars_summary(df, name=None):
    """
    Display a readable summary of a polars DataFrame or LazyFrame in a Jupyter notebook.
    Shows shape, columns, dtypes, schema, and describe.
    """
    title = f"**{name}**\n" if name else ""
    if hasattr(df, "collect"):
        # LazyFrame: collect to DataFrame for summary
        df = df.collect()
    display(Markdown(f"{title}**Shape:** `{df.shape}`"))
    display(Markdown(f"**Columns:** `{list(df.columns)}`"))
    display(Markdown("**Dtypes:**"))
    display(df.dtypes)
    display(Markdown("**Schema:**"))
    display(df.schema)
    display(Markdown("**Describe:**"))
    display(df.describe())

def fix_channel_dtypes(df: pl.DataFrame) -> pl.DataFrame:
    """
    Convert columns in the channel data DataFrame to more usable types using polars.
    - Converts Null columns to Int64 or String if possible.
    - Ensures numeric columns are correct.
    - Adds pubdate_dt as Datetime (milliseconds since epoch; multiplies by 1000 if needed).
    - Adds duration_td as Duration (seconds).
    - Adds days_since_upload_td as Duration (days).
    Returns a new DataFrame with improved dtypes and extra time columns.
    """
    out = df.clone()
    # Convert uploader_follower and uploader_total_videos to Int64 if possible
    for col in ["uploader_follower", "uploader_total_videos"]:
        if col in out.columns and out[col].dtype == pl.Null:
            out = out.with_columns(
                pl.col(col).cast(pl.Int64, strict=False)
            )
    # pubdate_dt: milliseconds since epoch
    if "pubdate" in out.columns and out["pubdate"].dtype in [pl.Int64, pl.Float64]:
        # Heuristic: if pubdate < 10**12, treat as seconds and multiply by 1000
        if out["pubdate"].max() < 10**12:
            out = out.with_columns(
                (pl.col("pubdate") * 1000).cast(pl.Datetime("ms")).alias("pubdate_dt")
            )
        else:
            out = out.with_columns(
                pl.col("pubdate").cast(pl.Datetime("ms")).alias("pubdate_dt")
            )
    # duration_td: Duration in seconds
    if "duration" in out.columns and out["duration"].dtype in [pl.Int64, pl.Float64]:
        out = out.with_columns(
            (pl.col("duration") * 1_000_000_000).cast(pl.Duration("ns")).alias("duration_td")
        )
    # days_since_upload_td: Duration in days
    if "days_since_upload" in out.columns and out["days_since_upload"].dtype in [pl.Int64, pl.Float64]:
        out = out.with_columns(
            (pl.col("days_since_upload") * 86_400_000_000_000).cast(pl.Duration("ns")).alias("days_since_upload_td")
        )
    return out

def extract_struct_fields(df: pl.DataFrame, struct_col: str, fields: list[str]) -> pl.DataFrame:
    """
    Extract specified fields from a struct column into new columns.
    E.g., extract_struct_fields(df, 'stat', ['view', 'like'])
    """
    out = df.clone()
    for field in fields:
        if struct_col in out.columns:
            out = out.with_columns(
                pl.col(struct_col).struct.field(field).alias(field)
            )
    return out

# Example usage: extract stat fields
STAT_FIELDS = ["view", "like", "coin", "share", "favorite", "reply"]

def extract_stat_fields(df: pl.DataFrame) -> pl.DataFrame:
    """
    Extracts common stat fields from the 'stat' struct column into top-level columns.
    """
    return extract_struct_fields(df, "stat", STAT_FIELDS)

def extract_time_features(df: pl.DataFrame, datetime_col: str = "pubdate_dt") -> pl.DataFrame:
    """
    Add year, month, day, weekday, and hour columns from a datetime column.
    """
    out = df.clone()
    if datetime_col in out.columns:
        out = out.with_columns([
            pl.col(datetime_col).dt.year().alias("year"),
            pl.col(datetime_col).dt.month().alias("month"),
            pl.col(datetime_col).dt.day().alias("day"),
            pl.col(datetime_col).dt.weekday().alias("weekday"),
            pl.col(datetime_col).dt.hour().alias("hour"),
        ])
    return out

def handle_nulls_and_types(df: pl.DataFrame) -> pl.DataFrame:
    """
    Fill or cast common null columns to appropriate types and fill with 0 or empty string where useful.
    - Fills numeric columns with 0 if all null or missing.
    - Fills string columns with empty string if all null or missing.
    - Casts columns to Int64/Float64/String as appropriate.
    """
    out = df.clone()
    # Numeric columns to fill with 0 if all null
    numeric_cols = [
        "uploader_follower", "uploader_total_videos", "danmaku_count", "num_pages", "view", "like", "coin", "share", "favorite", "reply"
    ]
    for col in numeric_cols:
        if col in out.columns:
            if out[col].null_count() == out.height:
                out = out.with_columns(pl.lit(0, dtype=pl.Int64).alias(col))
            else:
                # Cast to Int64 if not already
                if out[col].dtype != pl.Int64:
                    out = out.with_columns(pl.col(col).cast(pl.Int64, strict=False))
    # String columns to fill with empty string if all null
    string_cols = ["desc", "title", "tname", "uploader_name"]
    for col in string_cols:
        if col in out.columns:
            if out[col].null_count() == out.height:
                out = out.with_columns(pl.lit("", dtype=pl.String).alias(col))
            else:
                if out[col].dtype != pl.String:
                    out = out.with_columns(pl.col(col).cast(pl.String, strict=False))
    return out

def convert_categorical_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Convert common columns to polars Categorical type for efficient grouping and plotting.
    Only string columns can be cast to Categorical in polars.
    E.g., 'tname', 'uploader_name'.
    """
    out = df.clone()
    # Only cast string columns to categorical (polars does not allow numeric to categorical)
    cat_cols = ["tname", "uploader_name"]  # 'month' is usually numeric, so not included
    for col in cat_cols:
        if col in out.columns and out[col].dtype == pl.String:
            out = out.with_columns(pl.col(col).cast(pl.Categorical))
    return out

def add_engagement_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add derived engagement columns: like_rate, coin_rate, share_rate, favorite_rate (e.g., like/view),
    and total_engagement (sum of like, coin, share, favorite, reply).
    Handles missing columns gracefully.
    """
    out = df.clone()
    # Engagement rates
    for metric in ["like", "coin", "share", "favorite"]:
        if metric in out.columns and "view" in out.columns:
            out = out.with_columns(
                (pl.col(metric) / pl.col("view")).alias(f"{metric}_rate")
            )
    # Total engagement
    engagement_cols = [c for c in ["like", "coin", "share", "favorite", "reply"] if c in out.columns]
    if engagement_cols:
        out = out.with_columns(
            sum([pl.col(c) for c in engagement_cols]).alias("total_engagement")
        )
    return out

def add_text_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add text length columns for 'title' and 'desc', and truncated versions for display.
    - Adds 'title_len', 'desc_len', 'title_short', 'desc_short'.
    """
    out = df.clone()
    for col in ["title", "desc"]:
        if col in out.columns:
            out = out.with_columns([
                pl.col(col).str.len_chars().alias(f"{col}_len"),
                pl.col(col).str.slice(0, 40).alias(f"{col}_short"),
            ])
    return out

def process_channel_data(df: pl.DataFrame, log: bool = True) -> pl.DataFrame:
    """
    Apply all EDA enhancement steps to the channel data DataFrame in order.
    Steps: fix dtypes, extract stat fields, extract time features, handle nulls/types,
    convert categoricals, add engagement metrics, add text features.
    Logs each step if log=True.
    Returns the processed DataFrame.
    """
    logger = logging.getLogger(__name__)
    steps = [
        ("Fix dtypes", fix_channel_dtypes),
        ("Extract stat fields", extract_stat_fields),
        ("Extract time features", extract_time_features),
        ("Handle nulls and types", handle_nulls_and_types),
        ("Convert categoricals", convert_categorical_columns),
        ("Add engagement metrics", add_engagement_metrics),
        ("Add text features", add_text_features),
    ]
    out = df
    for name, func in steps:
        if log:
            logger.info(f"Processing step: {name}")
        out = func(out)
    if log:
        logger.info("All EDA enhancement steps complete.")
    return out
