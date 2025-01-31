
from collections import Counter
import os
import csv
import time
import isodate
import logging
import pathlib
from dataclasses import dataclass, \
  field as _field, fields as _fields, asdict as _asdict

from .helpers import AsyncException, save_to_csv


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
  Datatype-agnostic pipeline that batch-saves queued data (dict) 
  to a csv file asynchronously.
  Nota: Using no pandas dataframe here, be as fast as possible
  """


  def __init__(self, csv_output_path, header=None, data_queue_limit=50, 
               dry_run=False, name=None):
    """Initialize the data pipeline."""

    self.data_queue = []
    self.errors_queue = []

    self.data_queue_limit = data_queue_limit
    self.csv_output_path = csv_output_path
    self.err_output_path = "{}-errors.csv".format(csv_output_path)
  
    self.header = header
    self.dry_run = dry_run
    self.name = name

    self.stats = dict(
      data_queue=Counter(queued=0, saved=0, bytes=0), 
      errors_queue=Counter(queued=0, saved=0, bytes=0), 
      started_at=None, ended_at=None
    )

  async def __aenter__(self):

    # TODO: backup existing output files
    pathlib.Path(self.csv_output_path).unlink(missing_ok=True)
    pathlib.Path(self.err_output_path).unlink(missing_ok=True)

    self.stats["started_at"] = time.time()
    return self
  
  async def __aexit__(self, exc_type, exc_val, exc_tb):
    """ Close pipeline after saving remaining data """

    if len(self.data_queue) > 0:    

      for queue, output_path, counts in (
        self.switch_queue(), self.switch_queue(is_error=True)
      ):
        saved, written = await save_to_csv(queue, output_path)
        counts.update(saved=saved, bytes=written)


      self.stats["ended_at"] = time.time()

      msg_kwargs = { 
        "name": self.name or 'Unnamed',
        "elapsed":  self.stats["ended_at"] - self.stats["started_at"],
        "bytes":    self.stats["data_queue"]["bytes"], 
        "saved":    self.stats["data_queue"]["saved"], 
        "queued":   self.stats["data_queue"]["queued"], 
        "err_bytes":    self.stats["errors_queue"]["bytes"], 
        "err_saved":    self.stats["errors_queue"]["saved"], 
        "err_queued":   self.stats["errors_queue"]["queued"]         
        }
      
      logging.info(
        f"""\n{"-"*10}\n"""
         """<DataPipeline> "{name}" processed jobs :\n 
          items   : saved/queued {saved}/{queued} ({bytes} B) in {elapsed:.6f} seconds
          errors  : saved/queued {err_saved}/{err_queued} ({err_bytes} B) in {elapsed:.6f} seconds
         """
          .format(**msg_kwargs))


  async def enqueue(self, item, is_error=False, **kwargs):
    """ Enqueue a data item to the pipeline 
    and save data if queue limit is reached. """

    if not isinstance(item, dict):
      raise AsyncException(f"item for queue must be a dict, got {type(item)}")
    
    data_queue, output_path, counts = self.switch_queue(is_error)
    data_queue.append({ **item, **kwargs })
    counts.update(queued=1)

    if len(self.data_queue) >= self.data_queue_limit \
      and not self.dry_run:
        
        saved, written = await save_to_csv(data_queue, output_path)
        counts.update(saved=saved, bytes=written)
        data_queue.clear()

  def switch_queue(self, is_error=False):
    """ Set the data or error queue to be the current queue"""

    data_queue, output_path, counts = (
      self.data_queue, self.csv_output_path, self.stats["data_queue"]
    ) if not is_error else (
      self.errors_queue, 
      self.err_output_path,
      self.stats["errors_queue"] 
    )
    return data_queue, output_path, counts
