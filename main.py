import asyncio
from bilibili_api import Credential
from bilibili_api.user import User
from dotenv import load_dotenv
import os
import json
import logging
from utils.bili_video import (
    get_all_bvids,
    fetch_video_info,
    fetch_uploader_info,
    ensure_channel_map_exists,
    load_channel_map,
    get_channel_id,
)
import argparse
import sys


load_dotenv()

SESSDATA = os.getenv("SESSDATA")
BILI_JCT = os.getenv("BILI_JCT")
BUVID3 = os.getenv("BUVID3")


def setup_logging(verbosity):
    log_levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    log_level = log_levels[min(verbosity, 2)]
    logging.basicConfig(
        level=log_level, format="%(asctime)s %(levelname)s: %(message)s"
    )
    logging.getLogger().setLevel(log_level)


def ensure_channel_map():
    if not os.path.exists(CHANNEL_MAP_PATH):
        os.makedirs(os.path.dirname(CHANNEL_MAP_PATH), exist_ok=True)
        example = {"tianjiang": 21143599}
        with open(CHANNEL_MAP_PATH, "w", encoding="utf-8") as f:
            json.dump(example, f, ensure_ascii=False, indent=4)
        print(
            f"Created example channel map at {CHANNEL_MAP_PATH}. Please edit it and rerun."
        )
        exit(1)
    with open(CHANNEL_MAP_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


async def main():
    parser = argparse.ArgumentParser(
        description="Fetch Bilibili channel videos and info."
    )
    parser.add_argument(
        "--channel",
        type=str,
        required=False,
        help="Channel name as in the channel map file. If omitted, process all channels.",
    )
    parser.add_argument(
        "--channel-map",
        type=str,
        default="channels_info/channel_map.json",
        help="Path to channel map file (default: channels_info/channel_map.json)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output file path (default: {uploader_name}_{uploader_mid}.json)",
    )
    parser.add_argument(
        "--overwrite", action="store_true", help="Overwrite existing output file"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase log verbosity (-v, -vv, -vvv)",
    )
    args = parser.parse_args()

    setup_logging(args.verbose)

    channel_map_path = args.channel_map
    if not ensure_channel_map_exists(channel_map_path):
        exit(1)
    channel_map = load_channel_map(channel_map_path)
    channels_to_process = []
    if args.channel:
        channel_id = get_channel_id(channel_map, args.channel, channel_map_path)
        if channel_id is None:
            logging.warning(f"Skipping unknown channel: {args.channel}")
        else:
            channels_to_process = [(args.channel, channel_id)]
    else:
        channels_to_process = [(name, cid) for name, cid in channel_map.items()]
        if not channels_to_process:
            logging.error(f"No channels found in channel map {channel_map_path}")
            exit(1)

    for channel_name, channel_id in channels_to_process:
        logging.info(f"Processing channel: {channel_name} (id: {channel_id})")
        try:
            credential = Credential(sessdata=SESSDATA, bili_jct=BILI_JCT, buvid3=BUVID3)
            user = User(int(channel_id), credential=credential)
            uploader_info = await fetch_uploader_info(user)
            uploader_name = uploader_info.uploader_name or "unknown"
            uploader_mid = uploader_info.uploader_mid
            default_filename = f"{uploader_name}_{uploader_mid}.json"
            channels_info_dir = "channels_info"
            os.makedirs(channels_info_dir, exist_ok=True)
            default_path = os.path.join(channels_info_dir, default_filename)
            output_file = args.output if args.output else default_path

            # Load existing data if file exists and not overwriting
            existing_data = []
            existing_bvids = set()
            if os.path.exists(output_file) and not args.overwrite:
                with open(output_file, "r", encoding="utf-8") as f:
                    try:
                        existing_data = json.load(f)
                        existing_bvids = {
                            item["bvid"] for item in existing_data if "bvid" in item
                        }
                    except Exception as e:
                        logging.warning(
                            f"Could not read existing file {output_file}: {e}"
                        )
                        existing_data = []
                        existing_bvids = set()

            all_bvids = await get_all_bvids(user)
            all_video_info = list(existing_data)  # Start with existing data
            new_bvids = [bvid for bvid in all_bvids if bvid not in existing_bvids]
            skipped_bvids = [bvid for bvid in all_bvids if bvid in existing_bvids]
            for bvid in skipped_bvids:
                logging.debug(f"Skipping already present video: {bvid}")
            logging.info(
                f"{len(new_bvids)} new videos to fetch (out of {len(all_bvids)})"
            )
            for idx, bvid in enumerate(new_bvids, 1):
                info = await fetch_video_info(
                    bvid, credential, idx, len(new_bvids), uploader_info
                )
                if info is not None:
                    all_video_info.append(info.model_dump())

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(
                    all_video_info,
                    f,
                    ensure_ascii=False,
                    indent=4,
                )
            logging.info(f"Wrote {len(all_video_info)} videos to {output_file}")
        except Exception as e:
            logging.error(
                f"Failed to process channel {channel_name} (id: {channel_id}): {e}"
            )
    logging.info("All channel processing complete.")


if __name__ == "__main__":
    asyncio.run(main())
