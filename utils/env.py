import logging
import os
import pathlib

from bidict import bidict
from dotenv import load_dotenv


__all__ = (
  'YT_API_KEY', 
  'LOG_LEVEL', 'IO_TIMEOUT', 'IO_CONCURRENCY_LIMIT', 'IO_RATE_LIMIT', 'IO_BATCH_SIZE',
  'CORS_ALLOW_ORIGINS', 'CORS_ALLOW_CREDENTIALS',
)



load_dotenv()


# Requires your own API key
YT_API_KEY = os.environ["YT_API_KEY"]

# Set to True to enable debug mode
LOG_LEVEL = int(os.getenv("LOG_LEVEL", logging.INFO))

# Timeout to scrape/fetch, default 30000
IO_TIMEOUT = int(os.getenv("IO_TIMEOUT", 90000))

# asyncio semaphore limit
IO_CONCURRENCY_LIMIT = int(os.getenv("IO_CONCURRENCY_LIMIT", 5))

# Max number of tasks spawned per second
IO_RATE_LIMIT = int(os.getenv("IO_RATE_LIMIT", 1))

# I/O size incl. queue length before flushing to storage
# also 50 max video ids currently allowed to be requested at once by the YT API.
IO_BATCH_SIZE = int(os.getenv("IO_BATCH_SIZE", 50))

# CORS (Cross-Origin Resource Sharing). Permissive by default.
# https://fastapi.tiangolo.com/tutorial/cors/#use-corsmiddleware
CORS_ALLOW_ORIGINS = os.getenv("CORS_ALLOW_ORIGINS", "*")
CORS_ALLOW_CREDENTIALS = os.getenv("CORS_ALLOW_CREDENTIALS", True)



