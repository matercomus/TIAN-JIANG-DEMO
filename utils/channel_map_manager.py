import os
import json
import logging
import re
from typing import Dict, List
from pydantic import BaseModel, Field
from bilibili_api.user import User
from bilibili_api import Credential

logger = logging.getLogger(__name__)


class ChannelEntry(BaseModel):
    name: str
    id: int


class ChannelMapManager:
    def __init__(self, map_path: str, urls_path: str):
        self.map_path = map_path
        self.urls_path = urls_path
        self.channel_map: Dict[str, int] = {}
        self.load_map()

    def load_map(self):
        if os.path.exists(self.map_path):
            try:
                with open(self.map_path, "r", encoding="utf-8") as f:
                    self.channel_map = json.load(f)
                logger.info(f"Loaded channel map from {self.map_path}")
            except Exception as e:
                logger.error(f"Failed to load channel map: {e}")
                self.channel_map = {}
        else:
            self.channel_map = {}
            logger.info(f"No existing channel map at {self.map_path}, starting fresh.")

    def save_map(self):
        os.makedirs(os.path.dirname(self.map_path), exist_ok=True)
        with open(self.map_path, "w", encoding="utf-8") as f:
            json.dump(self.channel_map, f, ensure_ascii=False, indent=4)
        logger.info(f"Saved channel map to {self.map_path}")

    def read_urls(self) -> List[str]:
        if not os.path.exists(self.urls_path):
            logger.warning(f"URLs file not found: {self.urls_path}")
            return []
        with open(self.urls_path, "r", encoding="utf-8") as f:
            urls = [line.strip() for line in f if line.strip()]
        logger.info(f"Read {len(urls)} URLs from {self.urls_path}")
        return urls

    @staticmethod
    def extract_channel_id(url: str) -> int:
        match = re.search(r"/space.bilibili.com/(\d+)", url)
        if match:
            return int(match.group(1))
        raise ValueError(f"Could not extract channel id from URL: {url}")

    def update_map_from_urls(self, name_map: Dict[str, str] = None):
        urls = self.read_urls()
        updated = False
        for url in urls:
            try:
                channel_id = self.extract_channel_id(url)
                # Use provided name map or fallback to id as name
                name = (
                    name_map.get(str(channel_id), str(channel_id))
                    if name_map
                    else str(channel_id)
                )
                if name not in self.channel_map:
                    self.channel_map[name] = channel_id
                    logger.info(f"Added channel: {name} -> {channel_id}")
                    updated = True
                else:
                    logger.debug(f"Channel already present: {name} -> {channel_id}")
            except Exception as e:
                logger.warning(f"Skipping URL '{url}': {e}")
        if updated:
            self.save_map()
        else:
            logger.info("No new channels to add.")

    def fetch_and_update_names(self, sessdata=None, bili_jct=None, buvid3=None):
        """Fetch channel names for all ids in the map and update keys."""
        updated = False
        new_map = {}
        for old_key, channel_id in self.channel_map.items():
            try:
                if sessdata and bili_jct and buvid3:
                    cred = Credential(
                        sessdata=sessdata, bili_jct=bili_jct, buvid3=buvid3
                    )
                    user = User(int(channel_id), credential=cred)
                else:
                    user = User(int(channel_id))
                info = user.get_user_info()
                if hasattr(info, "__await__"):
                    import asyncio

                    info = asyncio.get_event_loop().run_until_complete(info)
                name = info.get("name") or str(channel_id)
                if name != old_key:
                    logger.info(f"Updating channel key: {old_key} -> {name}")
                    updated = True
                new_map[name] = channel_id
            except Exception as e:
                logger.warning(f"Could not fetch name for channel id {channel_id}: {e}")
                new_map[old_key] = channel_id
        if updated:
            self.channel_map = new_map
            self.save_map()
            logger.info("Channel map keys updated with fetched names.")
        else:
            logger.info("No channel names updated.")


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s"
    )
    parser = argparse.ArgumentParser(
        description="Manage Bilibili channel map from URLs."
    )
    parser.add_argument(
        "--map",
        type=str,
        default="channels_info/channel_map.json",
        help="Path to channel map JSON (default: channels_info/channel_map.json)",
    )
    parser.add_argument(
        "--urls",
        type=str,
        default="channels_info/channel_URLs.txt",
        help="Path to channel URLs txt (default: channels_info/channel_URLs.txt)",
    )
    parser.add_argument(
        "--name-map",
        type=str,
        default=None,
        help="Optional JSON file mapping channel ids to names",
    )
    parser.add_argument(
        "--remove-extracted",
        action="store_true",
        help="Remove URLs from the URLs file after they are added to the map",
    )
    parser.add_argument(
        "--fetch-names",
        action="store_true",
        help="Fetch channel names for all ids in the map and update the map keys",
    )
    args = parser.parse_args()

    logger.info(f"Using channel map: {args.map}")
    logger.info(f"Using URLs file: {args.urls}")
    if args.name_map:
        logger.info(f"Using name map: {args.name_map}")

    name_map = None
    if args.name_map and os.path.exists(args.name_map):
        with open(args.name_map, "r", encoding="utf-8") as f:
            name_map = json.load(f)
        logger.info(f"Loaded name map from {args.name_map}")

    mgr = ChannelMapManager(args.map, args.urls)
    before_map = set(mgr.channel_map.values())
    mgr.update_map_from_urls(name_map)
    after_map = set(mgr.channel_map.values())
    if args.remove_extracted:
        # Remove URLs that have been extracted and added to the map
        urls = mgr.read_urls()
        remaining_urls = []
        for url in urls:
            try:
                channel_id = mgr.extract_channel_id(url)
                if channel_id not in after_map:
                    remaining_urls.append(url)
            except Exception:
                remaining_urls.append(url)  # keep malformed/unknown
        with open(args.urls, "w", encoding="utf-8") as f:
            for url in remaining_urls:
                f.write(url + "\n")
        logger.info(
            f"Removed extracted URLs. {len(remaining_urls)} URLs remain in {args.urls}"
        )
    if args.fetch_names:
        import os
        from dotenv import load_dotenv

        load_dotenv()
        sessdata = os.getenv("SESSDATA")
        bili_jct = os.getenv("BILI_JCT")
        buvid3 = os.getenv("BUVID3")
        mgr.fetch_and_update_names(sessdata, bili_jct, buvid3)
