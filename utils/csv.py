import os
import csv
import json
import logging

from typing import List, Dict, Any
from dataclasses import dataclass

from utils.helpers import file_exists


__all__ = (
    'ResumableDictWriter', 'WriteStats',
    'save_to_csv'
)


logging.basicConfig(
    level=os.environ.get('LOGLEVEL', logging.INFO))


@dataclass
class WriteStats:
    """ Statistics about the writing operation. """

    items_written: int = 0
    bytes_written: int = 0
    start_position: int = 0


class ResumableDictWriter:
    """
  A context manager class that provides crash-resistant CSV writing capabilities
  using DictWriter, maintaining checkpoints and tracking writing statistics.
  """

    def __init__(self, output_file: str, fieldnames: List[str],
                 checkpoint_file: str = None, **csv_kwargs):

        self.output_file = output_file
        self.fieldnames = fieldnames
        self.checkpoint_file = checkpoint_file or f"{output_file}.checkpoint"
        self.csv_kwargs = csv_kwargs
        self.last_position = 0
        self.file = None
        self.writer = None
        self.initial_size = 0
        self._load_checkpoint()

    def __enter__(self):
        """ Enter the context manager, setting up CSV DictWriter with appropriate mode."""

        mode = 'a' if (self.last_position > 0 or file_exists(self.output_file)) else 'w'
        self.file = open(self.output_file, mode, newline='', encoding="utf-8-sig")
        self.writer = csv.DictWriter(self.file, fieldnames=self.fieldnames, **self.csv_kwargs)

        # Store initial file size for byte counting
        self.file.flush()
        self.initial_size = self.file.tell()

        # Write header only if we're starting fresh
        if mode == 'w':
            self.writer.writeheader()
            self.file.flush()
            self.initial_size = self.file.tell()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Exit the context manager, ensuring proper cleanup.
    If exception occurred, ensure checkpoint is saved """

        if self.file:
            self.file.close()

        if exc_type is None:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
        else:
            self._save_checkpoint()

    def _load_checkpoint(self):
        """ Load the last successful write position from checkpoint file."""
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint_data = json.load(f)
                    self.last_position = checkpoint_data['position']
        except Exception as e:
            logging.warning(f"Warning: Could not load checkpoint: {e}")
            self.last_position = 0

    def _save_checkpoint(self):
        """Save the current write position to checkpoint file."""
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump({'position': self.last_position}, f)
        except Exception as e:
            logging.debug(f"Warning: Could not save checkpoint: {e}")

    def write_rows(self, rows: List[Dict[str, Any]], start_from: int = None) -> WriteStats:
        """
    Write dictionary rows to CSV with crash recovery support.
    
    Args:
      rows: List of dictionaries to write as CSV rows
      start_from: Optional starting index (overrides checkpoint)
        
    Returns:
      WriteStats: Statistics about the writing operation
    """
        if start_from is not None:
            self.last_position = start_from

        if not self.writer:
            raise RuntimeError("ResumableDictWriter must be used as a context manager")

        start_position = self.last_position
        initial_file_pos = self.file.tell()

        try:

            # Write data respecting columns order
            for i, row in enumerate(rows[self.last_position:], self.last_position):
                self.writer.writerow({f: row[f] for f in self.fieldnames})
                self.file.flush()
                self.last_position = i + 1
                self._save_checkpoint()

            # Calculate final statistics
            final_file_pos = self.file.tell()
            bytes_written = final_file_pos - initial_file_pos
            items_written = self.last_position - start_position

            return WriteStats(
                items_written=items_written,
                bytes_written=bytes_written,
                start_position=start_position
            )

        except Exception as e:
            logging.error(f"Error occurred at position {self.last_position}: {e}")
            self._save_checkpoint()
            raise


def save_to_csv(rows_to_write, csv_output_path, header=None) -> WriteStats:
    """
    :param data_queue (List[dict]): items to write
    :param csv_output_path (str): output csv path
    """

    if not header:
        header = set([e for d in rows_to_write for e in set(d)])

    try:
        with ResumableDictWriter(csv_output_path, fieldnames=header) as writer:

            stats = writer.write_rows(rows_to_write)
            logging.debug(f"Successfully wrote {stats.items_written} items ({stats.bytes_written} bytes)")
            logging.debug(f"Started from position {stats.start_position}")
            return stats

    except Exception as e:
        logging.error(f"Writing failed: {e}")


def load_v():

        # setup dataframe
    df = pd.read_csv(args.csv_input_path)
    df.set_index(args.ids_column, drop=False, inplace=True)

    # select fields from resp. input csv, YT api and scraped videos
    # df = df.reindex(columns=[
    #   *df.columns.tolist(),
    #   *[f.name for f in fields(Video)
    #     if include_fields and f.name in include_fields],
    #   *[f"scraped_{f.name}" for f in fields(Video)
    #     if include_fields and f.name in include_fields]
    # ])

    # drop nan row, assume string cells
    df.dropna(how='all', inplace=True)
    df = df.astype(str)

    # fetch videos asynchronously using the YouTube Data API v3
    video_ids = df[args.ids_column]