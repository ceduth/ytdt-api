import os
import pathlib


# timeout to scrape/fetch, default 30000
IO_TIMEOUT = 60000

# asyncio semaphore limit
IO_CONCURRENCY_LIMIT = 5

# max number of tasks spawned per second
IO_RATE_LIMIT = 1

# I/O size incl. queue length before flushing to storage
# also 50 max video ids currently allowed to be requested at once by the YT API.
# recommended value 50
IO_BATCH_SIZE = 3


__all__ = (
  'IO_TIMEOUT', 'IO_CONCURRENCY_LIMIT', 'IO_RATE_LIMIT', 'IO_BATCH_SIZE',
  'file_exists', 'remove_file'
)


file_exists = lambda path: (os.path.isfile(path) and os.path.getsize(path) > 0)

remove_file = lambda path, missing_ok=True: pathlib.Path(path).unlink(missing_ok)


