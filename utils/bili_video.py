import logging
import time
from bilibili_api import video
from bilibili_api.exceptions.ResponseCodeException import ResponseCodeException
from utils.models import UploaderInfo, VideoInfo
from typing import List, Optional

logger = logging.getLogger(__name__)


async def get_all_bvids(user, page_size: int = 30) -> List[str]:
    """Fetch all BVIDs for a user asynchronously."""
    all_bvids = []
    page = 1
    logger.info("Starting to fetch all video BVIDs for user %s", user.get_uid())
    while True:
        logger.debug("Fetching page %d of videos", page)
        result = await user.get_videos(pn=page, ps=page_size)
        videos = result.get("list", {}).get("vlist", [])
        if not videos:
            logger.info("No more videos found on page %d.", page)
            break
        all_bvids.extend([v["bvid"] for v in videos if "bvid" in v])
        logger.info("Fetched %d videos from page %d", len(videos), page)
        if len(videos) < page_size:
            break
        page += 1
    logger.info("Total videos found: %d", len(all_bvids))
    return all_bvids


async def fetch_uploader_info(user) -> UploaderInfo:
    try:
        up_info = await user.get_user_info()
        return UploaderInfo(
            uploader_mid=user.get_uid(),
            uploader_name=up_info.get("name"),
            uploader_follower=up_info.get("follower"),
            uploader_total_videos=up_info.get("archive_count"),
        )
    except Exception as e:
        logger.warning("Could not fetch uploader info: %s", e)
        return UploaderInfo(
            uploader_mid=user.get_uid(),
            uploader_name=None,
            uploader_follower=None,
            uploader_total_videos=None,
        )


async def fetch_video_info(
    bvid: str,
    credential,
    idx: int,
    total: int,
    uploader_info: Optional[UploaderInfo] = None,
) -> Optional[VideoInfo]:
    v = video.Video(bvid, credential=credential)
    try:
        logger.info("[%d/%d] Fetching info for video: %s", idx, total, bvid)
        video_info = await v.get_info()
        # Add tags (full list)
        try:
            tags = await v.get_tags()
        except Exception as e:
            tags = []
            logger.warning("Could not fetch tags for %s: %s", bvid, e)
        # Add related videos (full list)
        try:
            related = await v.get_related()
            if isinstance(related, dict) and "data" in related:
                related_videos = related["data"]
            else:
                related_videos = related
        except Exception as e:
            related_videos = []
            logger.warning("Could not fetch related for %s: %s", bvid, e)
        # Add danmaku count
        try:
            danmaku_count = video_info["stat"]["danmaku"]
        except Exception:
            danmaku_count = None
        # Add number of pages
        try:
            num_pages = len(video_info.get("pages", []))
        except Exception:
            num_pages = None
        # Add time since upload (in days)
        try:
            days_since_upload = (time.time() - video_info["pubdate"]) / 86400
        except Exception:
            days_since_upload = None
        # Uploader info
        uploader = uploader_info or UploaderInfo(
            uploader_mid=video_info.get("owner", {}).get("mid"),
            uploader_name=video_info.get("owner", {}).get("name"),
            uploader_follower=None,
            uploader_total_videos=None,
        )
        # Build VideoInfo
        try:
            return VideoInfo(
                bvid=video_info["bvid"],
                title=video_info.get("title", ""),
                desc=video_info.get("desc"),
                pubdate=video_info.get("pubdate"),
                duration=video_info.get("duration"),
                tname=video_info.get("tname"),
                stat=video_info.get("stat", {}),
                owner=video_info.get("owner", {}),
                pages=video_info.get("pages"),
                rights=video_info.get("rights"),
                tags=tags,
                related_videos=related_videos,
                danmaku_count=danmaku_count,
                num_pages=num_pages,
                days_since_upload=days_since_upload,
                uploader_mid=uploader.uploader_mid,
                uploader_name=uploader.uploader_name,
                uploader_follower=uploader.uploader_follower,
                uploader_total_videos=uploader.uploader_total_videos,
            )
        except Exception as e:
            logger.error(f"Failed to build VideoInfo for {bvid}: {e}")
            return None
    except ResponseCodeException as e:
        logger.warning("Video %s could not be fetched: %s", bvid, e)
    except Exception as e:
        logger.error("Unexpected error for video %s: %s", bvid, e)
    return None
