import logging
import os
import pathlib

from bidict import bidict
from dotenv import load_dotenv


load_dotenv()

# Requires your own API key
YT_API_KEY = os.environ["YT_API_KEY"]

# Set to True to enable debug mode
LOG_LEVEL = int(os.getenv("LOG_LEVEL", logging.INFO))

# timeout to scrape/fetch, default 30000
IO_TIMEOUT = int(os.getenv("IO_TIMEOUT", 90000))

# asyncio semaphore limit
IO_CONCURRENCY_LIMIT = int(os.getenv("IO_CONCURRENCY_LIMIT", 5))

# max number of tasks spawned per second
IO_RATE_LIMIT = int(os.getenv("IO_RATE_LIMIT", 1))

# I/O size incl. queue length before flushing to storage
# also 50 max video ids currently allowed to be requested at once by the YT API.
IO_BATCH_SIZE = int(os.getenv("IO_BATCH_SIZE", 50))


__all__ = (
  'IO_TIMEOUT', 'IO_CONCURRENCY_LIMIT', 'IO_RATE_LIMIT', 'IO_BATCH_SIZE',
  'file_exists', 'remove_file'
)


file_exists = lambda path: (os.path.isfile(path) and os.path.getsize(path) > 0)

remove_file = lambda path, missing_ok=True: pathlib.Path(path).unlink(missing_ok)


def bidirectional_lookup(mapping: dict, key_or_value: str, raise_exc=True):

  bimap = bidict(mapping)
  if key_or_value in bimap:
    return bimap[key_or_value]
  elif key_or_value in bimap.inv:
    return bimap.inv[key_or_value]
  else:
    if not raise_exc:
      return "Unknown"
    raise KeyError(f"{key_or_value} not found in either direction")
  

# Attempt to map language name to code (very basic)
map_language = lambda lang: bidirectional_lookup({
  "English": "en",
  "Spanish": "es",
  "French": "fr",
  "German": "de",
  "Chinese": "zh",
  "Japanese": "ja"
}, lang)


# Extract language and country codes from locale code eg. 'en-US', 'en_US'
parse_locale = lambda code: (*((code or '').replace('_', '-').split('-') + [None]),)[:2]
