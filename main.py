import asyncio
from bilibili_api import Credential
from bilibili_api.user import User
from dotenv import load_dotenv
import os
import json
import logging
from utils.bili_video import get_all_bvids, fetch_video_info, fetch_uploader_info
import argparse
import sys


load_dotenv()

SESSDATA = os.getenv("SESSDATA")
BILI_JCT = os.getenv("BILI_JCT")
BUVID3 = os.getenv("BUVID3")
TIAN_JIANG_CHANNEL_ID = os.getenv("TIAN_JIANG_CHANNEL_ID")


def setup_logging(verbosity):
    log_levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    log_level = log_levels[min(verbosity, 2)]
    logging.basicConfig(
        level=log_level, format="%(asctime)s %(levelname)s: %(message)s"
    )
    logging.getLogger().setLevel(log_level)


async def main():
    parser = argparse.ArgumentParser(
        description="Fetch Bilibili channel videos and info."
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

    credential = Credential(sessdata=SESSDATA, bili_jct=BILI_JCT, buvid3=BUVID3)
    user = User(int(TIAN_JIANG_CHANNEL_ID), credential=credential)
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
                logging.warning(f"Could not read existing file {output_file}: {e}")
                existing_data = []
                existing_bvids = set()

    all_bvids = await get_all_bvids(user)
    all_video_info = list(existing_data)  # Start with existing data
    new_bvids = [bvid for bvid in all_bvids if bvid not in existing_bvids]
    skipped_bvids = [bvid for bvid in all_bvids if bvid in existing_bvids]
    for bvid in skipped_bvids:
        logging.debug(f"Skipping already present video: {bvid}")
    logging.info(f"{len(new_bvids)} new videos to fetch (out of {len(all_bvids)})")
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


if __name__ == "__main__":
    asyncio.run(main())
