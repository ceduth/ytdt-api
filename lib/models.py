
import os
import csv
import isodate
import logging
import pathlib
from dataclasses import dataclass, field, \
  fields as _fields, asdict as _asdict


logging.basicConfig(level=logging.INFO)


__all__ = ('asdict', 'fields', 'Video', 'VideoDataPipeline') 


def asdict(video, name_prefix=None):
  return { f"{name_prefix or ''}{k}": v
          for k,v in _asdict(video).items() 
          if not k.startswith('_') } #and not k.startswith('video_id')}


def fields(cls):
  return [f for f in _fields(cls) if not f.name.startswith('_')]
    
    
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
  _duration: str = field(init=False, repr=False)

  def __str__(self):
    return f"{self.id} {self.title} ({self.duration}s)"

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
  


class VideoDataPipeline:
  """
  Batch-save videos to csv file.
  No dataframe here, be as fast as possible
  """

  def __init__(self, csv_output_path, header=None, data_queue_limit=50, dry_run=False):
    """Initialize the video data pipeline."""

    self.data_queue = []
    self.data_queue_limit = data_queue_limit
    self.csv_output_path = csv_output_path
    self.header = header
    self.dry_run = dry_run

  def __enter__(self):
     # TODO: backup existing output csv file
     pathlib.Path(self.csv_output_path).unlink(missing_ok=True)
     return self
  
  def __exit__(self, exc_type, exc_val, exc_tb):
    # close pipeline after saving remaining data
    if len(self.data_queue) > 0:
        self.save_to_csv()

  def enqueue_video(self, video: Video):
      """ Enqueue a video item to the pipeline 
      and save data if queue limit is reached. """

      self.data_queue.append(video)
      if len(self.data_queue) >= self.data_queue_limit:
          self.save_to_csv()
          self.data_queue.clear()

  def save_to_csv(self) -> None:
        
        data_batch = []
        data_batch.extend(self.data_queue)

        if not data_batch or self.dry_run:
            return
        
        if not self.header:
          header = [f.name for f in fields(Video)]

        file_exists = (
            os.path.isfile(self.csv_output_path) and os.path.getsize(
                self.csv_output_path) > 0
        )

        with open(self.csv_output_path, mode="a", newline="", encoding="utf-8-sig") as file:
            writer = csv.DictWriter(file, fieldnames=header)
            if not file_exists:
              writer.writeheader()
            for video in data_batch:
              video_dict = asdict(video)
              reordered_video_dict = {
                  field: video_dict[field] for field in header
              }
              writer.writerow(reordered_video_dict)

