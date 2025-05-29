from pydantic import BaseModel, Field
from typing import Optional, List, Any

class UploaderInfo(BaseModel):
    uploader_mid: int
    uploader_name: Optional[str]
    uploader_follower: Optional[int]
    uploader_total_videos: Optional[int]

class VideoInfo(BaseModel):
    bvid: str
    title: str
    desc: Optional[str]
    pubdate: Optional[int]
    duration: Optional[int]
    tname: Optional[str]
    stat: dict
    owner: dict
    pages: Optional[List[dict]]
    rights: Optional[dict]
    tags: List[Any] = Field(default_factory=list)
    related_videos: List[Any] = Field(default_factory=list)
    danmaku_count: Optional[int]
    num_pages: Optional[int]
    days_since_upload: Optional[float]
    uploader_mid: int
    uploader_name: Optional[str]
    uploader_follower: Optional[int]
    uploader_total_videos: Optional[int]
    # Add more fields as needed for your use case 