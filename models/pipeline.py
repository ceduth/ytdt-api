import time
import logging
from collections import Counter

from utils.helpers import remove_file
from lib.exceptions import AsyncException
from utils.csv import save_to_csv, WriteStats
from utils.env import IO_BATCH_SIZE
from utils.helpers import rename_file_with_extension


__all__ = (
    'DataPipeline',
)


class DataPipeline:
    """
    Datatype-agnostic pipeline that batch-saves queued data (dict)
    to a csv file asynchronously.
    Nota: Using no pandas dataframe here, be as fast as possible
    """

    def __init__(self, csv_output_path=None, fields=None,
                 data_queue_limit=IO_BATCH_SIZE,
                 dry_run=False, name=None):

        """ Initialize the data pipeline. """

        self.data_queue = []
        self.errors_queue = []
        self.data_queue_limit = data_queue_limit
        self.dry_run = dry_run
        self.name = name

        self.stats = dict(
            data_queue=Counter(queued=0, saved=0, bytes=0),
            errors_queue=Counter(queued=0, saved=0, bytes=0),
            started_at=None, ended_at=None
        )

        if csv_output_path:
            self.csv_output_path = csv_output_path
            self.err_output_path = rename_file_with_extension(csv_output_path, suffix='error')

    async def __aenter__(self):

        # TODO: backup existing output files
        if not self.dry_run:
            for path in (self.csv_output_path, self.err_output_path):
                remove_file(self.csv_output_path)
                logging.warning(f'Deleted existing data from "{path}"')

        self.stats["started_at"] = time.time()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """ Close pipeline after saving remaining data. """

        if len(self.data_queue) > 0:

            # flush remaining data from all queues
            queues = (self._get_queue(), self._get_queue(is_error=True))
            for queue, output_path, counts in queues:
                written = save_to_csv(queue, output_path)
                counts.update(saved=written.items_written, bytes=written.bytes_written)

            self.stats["ended_at"] = time.time()

            msg_kwargs = {
                "name": self.name or 'Unnamed',
                "elapsed": self.stats["ended_at"] - self.stats["started_at"],
                "bytes": self.stats["data_queue"]["bytes"],
                "saved": self.stats["data_queue"]["saved"],
                "queued": self.stats["data_queue"]["queued"],
                "err_bytes": self.stats["errors_queue"]["bytes"],
                "err_saved": self.stats["errors_queue"]["saved"],
                "err_queued": self.stats["errors_queue"]["queued"]
            }

            logging.info(
                f"""\n{"-" * 10}\n"""
                """<DataPipeline> "{name}" processed jobs :\n 
                    items   : saved/queued {saved}/{queued} ({bytes} B) in {elapsed:.6f} seconds
                    errors  : saved/queued {err_saved}/{err_queued} ({err_bytes} B) in {elapsed:.6f} seconds
         """
                .format(**msg_kwargs))

    async def enqueue(self, item, is_error=False, **kwargs):
        """ Enqueue a data item to the pipeline
        and save data if queue limit is reached.

        :param dict item: item to enqueue
        :param bool is_error: kind of item
        :param dict kwargs: additional fields to save

        returns dict: successfully queued item
        """

        try:
            if not isinstance(item, dict):
                raise AsyncException(f"item for queue must be a dict, got {type(item)}")

            # Set the current queue, error output and counts
            queue, output_path, counts = self._get_queue(is_error)
            queue.append({**item, **kwargs})
            counts.update(queued=1)

            if len(self.data_queue) >= self.data_queue_limit \
                    and not self.dry_run:
                written = save_to_csv(queue, output_path)
                counts.update(saved=written.items_written, bytes=written.bytes_written)
                queue.clear()

            return item

        except Exception as e:
            logging.error(f"Couldn't queue item  {item}: {e}")
            raise

    def _get_queue(self, is_error=False):
        """ Return the data or error queue based on args"""

        data_queue, output_path, counts = (
            self.data_queue, self.csv_output_path, self.stats["data_queue"]
        ) if not is_error else (
            self.errors_queue,
            self.err_output_path,
            self.stats["errors_queue"]
        )
        return data_queue, output_path, counts
