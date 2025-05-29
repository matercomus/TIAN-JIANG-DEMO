import logging
import time
from bilibili_api import video
from bilibili_api.exceptions.ResponseCodeException import ResponseCodeException

async def get_all_bvids(user, page_size=30):
    """Fetch all BVIDs for a user asynchronously."""
    all_bvids = []
    page = 1
    logging.info("Starting to fetch all video BVIDs for user %s", user.get_uid())
    while True:
        logging.debug("Fetching page %d of videos", page)
        result = await user.get_videos(pn=page, ps=page_size)
        videos = result.get('list', {}).get('vlist', [])
        if not videos:
            logging.info("No more videos found on page %d.", page)
            break
        all_bvids.extend([v['bvid'] for v in videos if 'bvid' in v])
        logging.info("Fetched %d videos from page %d", len(videos), page)
        if len(videos) < page_size:
            break
        page += 1
    logging.info("Total videos found: %d", len(all_bvids))
    return all_bvids

async def fetch_video_info(bvid, credential, idx, total):
    v = video.Video(bvid, credential=credential)
    try:
        logging.info("[%d/%d] Fetching info for video: %s", idx, total, bvid)
        video_info = await v.get_info()
        # Add tags (full list)
        try:
            tags = await v.get_tags()
            video_info['tags'] = tags
        except Exception as e:
            video_info['tags'] = []
            logging.warning("Could not fetch tags for %s: %s", bvid, e)
        # Add related videos (full list)
        try:
            related = await v.get_related()
            # Some APIs return a dict with 'data' key, some return a list directly
            if isinstance(related, dict) and 'data' in related:
                video_info['related_videos'] = related['data']
            else:
                video_info['related_videos'] = related
        except Exception as e:
            video_info['related_videos'] = []
            logging.warning("Could not fetch related for %s: %s", bvid, e)
        # Add danmaku count
        try:
            video_info['danmaku_count'] = video_info['stat']['danmaku']
        except Exception:
            video_info['danmaku_count'] = None
        # Add number of pages
        try:
            video_info['num_pages'] = len(video_info.get('pages', []))
        except Exception:
            video_info['num_pages'] = None
        # Add time since upload (in days)
        try:
            video_info['days_since_upload'] = (time.time() - video_info['pubdate']) / 86400
        except Exception:
            video_info['days_since_upload'] = None
        # Add uploader info (mid, name, follower count, total uploads)
        try:
            owner = video_info.get('owner', {})
            video_info['uploader_mid'] = owner.get('mid')
            video_info['uploader_name'] = owner.get('name')
            from bilibili_api.user import User as BUser
            up_user = BUser(owner.get('mid'))
            up_info = await up_user.get_user_info()
            video_info['uploader_follower'] = up_info.get('follower')
            video_info['uploader_total_videos'] = up_info.get('archive_count')
        except Exception as e:
            video_info['uploader_follower'] = None
            video_info['uploader_total_videos'] = None
            logging.warning("Could not fetch uploader info for %s: %s", bvid, e)
        logging.info("Fetched info for video: %s", bvid)
        return video_info
    except ResponseCodeException as e:
        logging.warning("Video %s could not be fetched: %s", bvid, e)
    except Exception as e:
        logging.error("Unexpected error for video %s: %s", bvid, e)
    return None 