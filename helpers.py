import os
import pathlib


IO_TIMEOUT = 60000            # timeout to scrape/fetch, default 30000
IO_CONCURRENCY_LIMIT = 5      # asyncio semaphore limit
IO_RATE_LIMIT = 1             # max number of tasks spawned per second
IO_BATCH_SIZE = 3             # I/O size incl. queue length before flushing to storage


__all__ = (
  'IO_TIMEOUT', 'IO_CONCURRENCY_LIMIT', 'IO_RATE_LIMIT', 'IO_BATCH_SIZE',
  'file_exists', 'remove_file'
)


file_exists = lambda path: (os.path.isfile(path) and os.path.getsize(path) > 0)

remove_file = lambda path, missing_ok=True: pathlib.Path(path).unlink(missing_ok)


