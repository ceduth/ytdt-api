import os
import isodate
import logging
from dataclasses import dataclass, \
    field as _field, fields as _fields, asdict as _asdict


__all__ = (
    'asdict', 'fields', 'Video',
)


logging.basicConfig(
    level=os.environ.get('LOGLEVEL', logging.INFO))


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
  Video resource from YouTube Data API v3 in JFP field naming.
  https://developers.google.com/youtube/v3/docs/videos
  """

    # Required metadata
    video_id: str
    title: str
    published_at: str
    upload_date: str
    language_code: str
    # duration: str
    view_count: str

    # Optional metadata
    url: str = ''
    thumbnail_url: str = ''
    channel_id: str = ''
    channel_name: str = ''
    language_name: str = ''
    country: str = ''

    # Engagement
    likes: str = ''
    comments: str = ''
    shares: str = ''
    dislikes: str = ''
    subscribers_gained: str = ''
    subscribers_lost: str = ''

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
