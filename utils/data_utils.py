'''Utility functions for data processing'''

import polars as pl
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import logging

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
