import logging
import asyncio
from tqdm.asyncio import tqdm_asyncio


IO_TIMEOUT = 60000          # timeout to scrape/fetch, default 30000
IO_CONCURRENCY_LIMIT = 50   # asyncio semaphore limit


__all__ = (
  'IO_TIMEOUT', 'IO_CONCURRENCY_LIMIT',
  'gather', 'gather_with_concurrency',
  'AsyncException',
   )


class AsyncException(Exception):
  def __int__(self, message):
    super.__init__(self, message)


async def gather(*fs, **kwargs):
    """
    Slightly re-writing `tqdm_asyncio.gather()` to skip async exceptions, 
    since `return_exceptions=True` isn't available with tqdm.
    """
    async def wrap_awaitable(i, f):
        return i, await f

    errs, res = 0, []
    ifs = [wrap_awaitable(i, f) for i, f in enumerate(fs)]
    for aw in tqdm_asyncio.as_completed(ifs, **kwargs):
      try:
        res += [await aw]
      except AsyncException as e:
        logging.debug(f"async task failed: " + str(e))
        errs += 1
      logging.debug(f"async tasks completed. {errs}/{len(fs)} task(s) failed")

    return [i for _, i in sorted(res)]


async def gather_with_concurrency(*coros, limit=IO_CONCURRENCY_LIMIT, **kwargs):
    """ Enrich gather() with default concurrency limit and tasks progress bar """
    
    semaphore = asyncio.Semaphore(limit)
    async def sem_coro(coro):
      async with semaphore:
        return await coro
    return await gather(*map(sem_coro, coros), **kwargs)

