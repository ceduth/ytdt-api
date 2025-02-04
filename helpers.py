import os
import logging
import pathlib
import traceback


IO_TIMEOUT = 60000          # timeout to scrape/fetch, default 30000
IO_CONCURRENCY_LIMIT = 5    # asyncio semaphore limit
IO_RATE_LIMIT=1             # max number of tasks spawned per second
IO_BATCH_SIZE=3             # I/O size incl. queue length before flushing to storage


__all__ = (
  'IO_TIMEOUT', 'IO_CONCURRENCY_LIMIT', 'IO_RATE_LIMIT', 'IO_BATCH_SIZE',
  'AsyncException', 'VideoError',
  'file_exists', 'remove_file'
  )


logging.basicConfig(
  level=os.environ.get('LOGLEVEL', logging.INFO))


file_exists = lambda path: (os.path.isfile(path) and os.path.getsize(path) > 0)

remove_file = lambda path, missing_ok=True: pathlib.Path(path).unlink(missing_ok)


class AsyncException(Exception):
  """  Prettify-able, self-logging async I/O exception  

  ** Example usage **

    try:
      raise ValueError('not a key')
    except Exception as e:
      err = AsyncException(f'Error scraping video "{1234}"', )
      print(e.__dict__)
  
  """

  def __init__(self, message, exc=None):

    exc = exc or self
    self.message = f"🚫 async error: {message}"
    self.detail = str(exc)
    self.errors = ''.join(traceback.format_exception(exc))

    super().__init__(message)
    logging.debug(self.message)


class VideoError(AsyncException):
  def __init__(self, video_id, *args, **kwargs):

    super().__init__(*args, **kwargs)
    self.video_id = video_id
