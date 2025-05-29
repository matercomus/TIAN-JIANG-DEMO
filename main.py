import asyncio
from bilibili_api import video, Credential
from bilibili_api.user import User
from dotenv import load_dotenv
import os
import json
import logging
from bilibili_api.exceptions.ResponseCodeException import ResponseCodeException
import time
from utils.bili_video import get_all_bvids, fetch_video_info, fetch_uploader_info

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

SESSDATA = os.getenv("SESSDATA")
BILI_JCT = os.getenv("BILI_JCT")
BUVID3 = os.getenv("BUVID3")
TIAN_JIANG_CHANNEL_ID = os.getenv("TIAN_JIANG_CHANNEL_ID")


async def main() -> None:
    credential = Credential(sessdata=SESSDATA, bili_jct=BILI_JCT, buvid3=BUVID3)
    user = User(int(TIAN_JIANG_CHANNEL_ID), credential=credential)
    all_bvids = await get_all_bvids(user)
    uploader_info = await fetch_uploader_info(user)
    all_video_info = []
    for idx, bvid in enumerate(all_bvids, 1):
        info = await fetch_video_info(
            bvid, credential, idx, len(all_bvids), uploader_info
        )
        if info is not None:
            all_video_info.append(info)
    with open("video_info.json", "w", encoding="utf-8") as f:
        json.dump(
            [info.model_dump() for info in all_video_info],
            f,
            ensure_ascii=False,
            indent=4,
        )


if __name__ == "__main__":
    asyncio.run(main())
