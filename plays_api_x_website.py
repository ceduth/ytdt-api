#!/usr/bin/env python3
import asyncio
import argparse
import logging
import pandas as pd 
from collections import Counter

from lib.models import fields, asdict, Video, VideoDataPipeline
from lib.videos import get_multiple_videos
from lib.scraper import scrape_multiple_videos


logging.basicConfig(level=logging.INFO)


if __name__ == '__main__':
    
  """
  plays_api_x_website.py 
    [-h] 
    [--csv_output_path CSV_OUTPUT_PATH] 
    [--include_fields INCLUDE_FIELDS]
    [--ids_column IDS_COLUMN] 
    [--dry_run DRY_RUN] 
    csv_input_path

  Example usage:
    plays_api_x_website.py data/video-ids-three.csv
  """

  description = """
  We could use a python script to compare youtube api plays vs. youtube.com plays 
  for every youtube video
  """

  # Initialize parser
  parser = argparse.ArgumentParser(description = description)
  parser.add_argument('csv_input_path', type=str, help='Input CSV file name. No Excel here yet!')
  parser.add_argument('--csv_output_path', type=str, help='Output XLSX file name')
  parser.add_argument('--ids_column', dest='ids_column', type=str, default='yt_video_id', help="Name of video IDs column")
  parser.add_argument('--include_fields', dest='include_fields', type=str, help="Video fields to exclude (comma separated)")
  parser.add_argument('--dry_run', dest='dry_run', type=bool, help="Whether to write output file")
  
  # read arguments from command line
  args = parser.parse_args()
  csv_output_path = args.csv_output_path or f"{args.csv_input_path}-out.xlsx"
  include_fields = args.include_fields.split(",") if args.include_fields else None

  # setup dataframe
  df = pd.read_csv(args.csv_input_path)
  # df.set_index(args.ids_column, drop=False, inplace=True)

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

  # vars
  video_ids = df[args.ids_column]
  counts = Counter(scraped=0, api=0)


  # Update df with videos from the YT api
  with VideoDataPipeline(
    csv_output_path=f"{args.csv_input_path}-api-out.csv", 
    header=include_fields, dry_run=args.dry_run
  ) as pipeline:
    
    for msg, video in get_multiple_videos(video_ids):
      if not video:
        logging.info(f"Skipping video : {msg}")
        continue
      pipeline.enqueue_video(video)
      counts.update(api=1)

  # Update df with scraped videos 
  with VideoDataPipeline(
    csv_output_path=f"{args.csv_input_path}-scraped-out.csv", 
    header=include_fields, dry_run=args.dry_run
  ) as pipeline:
    
    scraped_videos = asyncio.run(scrape_multiple_videos(video_ids))
    for video in scraped_videos:
      pipeline.enqueue_video(video)
      counts.update(scraped=1)

  # log results  
  logging.info(
    f"Extracted videos: scraped / api / totals = "
    f"{counts['scraped']} / {counts['api']} / {len(df)}"
    f'\n\nDone !'
  )

