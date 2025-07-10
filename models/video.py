import os
import isodate
import logging
from dataclasses import dataclass, \
    field as _field, fields as _fields, asdict as _asdict

from utils.env import LOG_LEVEL


__all__ = (
    'asdict', 'fields', 'Video',
)


logging.basicConfig(level=LOG_LEVEL)



def asdict(video, name_prefix=None):

    if isinstance(video, dict):
        return video

    return {f"{name_prefix or ''}{k}": v
            for k, v in _asdict(video).items()
            if not k.startswith('_')}  # and not k.startswith('video_id')}


def fields(cls):
    return [f.name for f in _fields(cls) if not f.name.startswith('_')]


@dataclass
class Video:
    """
  Video resource from YouTube Data API v3 with Analytics data in JFP field naming.
  https://developers.google.com/youtube/v3/docs/videos
  https://developers.google.com/youtube/analytics/
  """

    # Required metadata
    video_id: str
    title: str
    published_at: str
    upload_date: str
    language_code: str
    view_count: str

    # Optional metadata
    url: str = ''
    thumbnail_url: str = ''
    channel_id: str = ''
    channel_name: str = ''
    language_name: str = ''
    country: str = ''
    is_unlisted: bool = False

    # Engagement metrics (from Data API and Analytics API)
    likes: str = ''
    comments: str = ''
    shares: str = ''
    dislikes: str = ''
    subscribers_gained: str = ''
    subscribers_lost: str = ''

    # Analytics-specific metrics (YouTube Analytics API)
    estimated_minutes_watched: int = 0
    average_view_duration: float = 0.0
    average_view_percentage: float = 0.0
    subscriber_views: int = 0
    non_subscriber_views: int = 0
    
    # Revenue metrics (requires monetized channel)
    estimated_revenue: float = 0.0
    estimated_ad_revenue: float = 0.0
    estimated_red_revenue: float = 0.0
    cpm: float = 0.0
    playback_based_cpm: float = 0.0
    ad_impressions: int = 0
    monetized_playbacks: int = 0
    
    # Demographics and discovery (additional analytics fields)
    impression_click_through_rate: float = 0.0
    impressions: int = 0
    
    # Traffic sources (common analytics dimensions)
    views_from_youtube_search: int = 0
    views_from_suggested_videos: int = 0
    views_from_external_sources: int = 0
    views_from_direct_url: int = 0
    
    # Device and platform metrics
    views_from_mobile: int = 0
    views_from_desktop: int = 0
    views_from_tablet: int = 0
    views_from_tv: int = 0

    duration: str = ''
    _duration: str = _field(init=False, repr=False)

    def __str__(self):
        return f"{self.video_id} {self.title} ({self.duration}s)"

    @property
    def duration(self):
        return self._duration

    @duration.setter
    def duration(self, value):
        """ ISO 8601 date duration -> seconds """
        try:
            self._duration = isodate.parse_duration(str(value)).seconds
        except isodate.isoerror.ISO8601Error:
            self._duration = value
    
    def get_engagement_rate(self):
        """Calculate engagement rate as (likes + comments) / views"""
        try:
            views = int(self.view_count) if self.view_count else 0
            likes = int(self.likes) if self.likes else 0
            comments = int(self.comments) if self.comments else 0
            
            if views > 0:
                return (likes + comments) / views * 100
            return 0.0
        except (ValueError, TypeError):
            return 0.0
    
    def get_retention_rate(self):
        """Calculate average view percentage"""
        return self.average_view_percentage
    
    def get_subscriber_ratio(self):
        """Calculate ratio of subscriber views to total views"""
        try:
            total_views = self.subscriber_views + self.non_subscriber_views
            if total_views > 0:
                return self.subscriber_views / total_views * 100
            return 0.0
        except (ValueError, TypeError):
            return 0.0
