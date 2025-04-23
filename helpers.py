import os
import pathlib


# timeout to scrape/fetch, default 30000
IO_TIMEOUT = os.getenv("IO_TIMEOUT", 90000)

# asyncio semaphore limit
IO_CONCURRENCY_LIMIT = os.getenv("IO_CONCURRENCY_LIMIT", 5)

# max number of tasks spawned per second
IO_RATE_LIMIT = os.getenv("IO_RATE_LIMIT", 1)

# I/O size incl. queue length before flushing to storage
# also 50 max video ids currently allowed to be requested at once by the YT API.
IO_BATCH_SIZE = os.getenv("IO_BATCH_SIZE", 50)


__all__ = (
  'IO_TIMEOUT', 'IO_CONCURRENCY_LIMIT', 'IO_RATE_LIMIT', 'IO_BATCH_SIZE',
  'file_exists', 'remove_file'
)


file_exists = lambda path: (os.path.isfile(path) and os.path.getsize(path) > 0)

remove_file = lambda path, missing_ok=True: pathlib.Path(path).unlink(missing_ok)


