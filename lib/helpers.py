import csv
import logging
import os
import traceback
from tqdm.asyncio import tqdm_asyncio


IO_TIMEOUT = 60000          # timeout to scrape/fetch, default 30000
IO_CONCURRENCY_LIMIT = 5    # asyncio semaphore limit
IO_RATE_LIMIT=1             # max number of tasks spawned per second
IO_BATCH_SIZE=3             # I/O size incl. queue length before flushing to storage 


__all__ = (
  'IO_TIMEOUT', 'IO_CONCURRENCY_LIMIT', 'IO_RATE_LIMIT', 'IO_BATCH_SIZE',
  'AsyncException',
  )


logging.basicConfig(
  level=os.environ.get('LOGLEVEL', logging.INFO))


async def save_to_csv(data, csv_output_path, header=None) -> None:
  """
  :param data_queue (List[dict]): items to write
  : param csv_output_path (str): output csv path
  """

  data_batch = []
  data_batch.extend(data)
  if not data_batch:
    return
  
  if not header:
    header = set([e for d in data_batch for e in set(d)])

  file_exists = (os.path.isfile(csv_output_path) 
                 and os.path.getsize(csv_output_path) > 0)

  with open(csv_output_path, mode="a", newline="", encoding="utf-8-sig") as file:

    items_count, bytes_written = 0, 0
    writer = csv.DictWriter(file, fieldnames=header)
    if not file_exists:
      bytes_written += writer.writeheader()

    for data in data_batch:
      reordered_data_dict = { field: data[field] for field in header }
      bytes_written += writer.writerow(reordered_data_dict)
      items_count += 1

    return items_count, bytes_written


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
    self.errors = ''.join(traceback.format_exception(
       etype=type(exc), value=exc, tb=exc.__traceback__))
    
    super().__init__(self, message)
    logging.debug(self.message)


