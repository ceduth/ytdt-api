
from collections import Counter
import os
import csv
import time
import isodate
import logging
import pathlib
from dataclasses import dataclass, \
  field as _field, fields as _fields, asdict as _asdict

from .helpers import AsyncException


__all__ = (
  'asdict', 'fields', 'Video', 'DataPipeline'
) 


logging.basicConfig(level=logging.INFO)


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
  


class DataPipeline:
  """
  Batch-save data to csv file asynchronously.
  Nota: No pandas dataframe here, be as fast as possible
  """


  def __init__(self, csv_output_path, header=None, data_queue_limit=50, 
               dry_run=False, name=None):
    """Initialize the data pipeline."""

    self.data_queue = []
    self.data_queue_limit = data_queue_limit
    self.csv_output_path = csv_output_path
    self.header = header
    self.dry_run = dry_run
    self.name = name

    self.fields = set()
    self.stats = dict(
      counts=Counter(queued=0, saved=0, bytes=0), 
      started_at=None, ended_at=None
    )

  async def __aenter__(self):

    # TODO: backup existing output csv file
    pathlib.Path(self.csv_output_path).unlink(missing_ok=True)
    self.stats["started_at"] = time.time()
    return self
  
  async def __aexit__(self, exc_type, exc_val, exc_tb):
    """ Close pipeline after saving remaining data """

    if len(self.data_queue) > 0:
      
      await self.save_to_csv()     
      self.stats["ended_at"] = time.time()

      msg_kwargs = { 
        "name": self.name or 'Unnamed',
        "elapsed": self.stats["ended_at"] - self.stats["started_at"],
        "bytes": self.stats["counts"]["bytes"], 
        "saved": self.stats["counts"]["saved"], 
        "queued": self.stats["counts"]["queued"] }
      
      logging.info(
        f"""\n{"-"*10}\n"""
         """<DataPipeline> "{name}" processed jobs : 
          saved/queued {saved}/{queued} ({bytes} B) in {elapsed:.6f} seconds\n"""
            .format(**msg_kwargs))

  async def enqueue(self, item: dict, **kwargs):
    """ Enqueue a data item to the pipeline 
    and save data if queue limit is reached. """

    if not isinstance(item, dict):
      raise AsyncException(f"item for queue must be a dict, got {type(item)}")
    
    self.fields = self.fields | set([*list(item), *list(kwargs)])
    self.data_queue.append({**item, **kwargs})
    self.stats["counts"].update(queued=1)

    if len(self.data_queue) >= self.data_queue_limit:
      await self.save_to_csv()
      self.data_queue.clear()

  async def save_to_csv(self) -> None:

    data_batch = []
    data_batch.extend(self.data_queue)

    if not data_batch or self.dry_run:
      return
    
    if not self.header:
      header = self.fields

    file_exists = (
      os.path.isfile(self.csv_output_path) and os.path.getsize(
        self.csv_output_path) > 0
    )

    with open(self.csv_output_path, mode="a", newline="", encoding="utf-8-sig") as file:

      writer = csv.DictWriter(file, fieldnames=header)
      if not file_exists:
        bytes = writer.writeheader()
        self.stats["counts"].update(bytes=bytes)

      for data in data_batch:
        reordered_data_dict = {
            field: data[field] for field in header
        }
        bytes = writer.writerow(reordered_data_dict)
        self.stats["counts"].update(saved=1, bytes=bytes)

