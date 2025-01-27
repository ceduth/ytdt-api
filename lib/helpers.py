import logging
import asyncio
import aiometer
from tqdm.asyncio import tqdm_asyncio


IO_TIMEOUT = 60000          # timeout to scrape/fetch, default 30000
IO_CONCURRENCY_LIMIT = 5    # asyncio semaphore limit
IO_RATE_LIMIT=1             # max number of tasks spawned per second


__all__ = (
  'IO_TIMEOUT', 'IO_CONCURRENCY_LIMIT', IO_RATE_LIMIT,
  'gather', 'gather_with_concurrency',
  'AsyncException',
  )


class AsyncException(Exception):
  def __int__(self, message):
    super.__init__(self, message)


