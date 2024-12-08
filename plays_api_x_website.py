#!/usr/bin/env python3
import asyncio
import argparse
import logging
import pandas as pd 
from collections import Counter
from dataclasses import dataclass, asdict, fields

from lib.videos import get_video_data, Video
from lib.scraper import scrape_multiple_videos


logging.basicConfig(level=logging.INFO)


if __name__ == '__main__':
    
  """
  Example usage.
  """

  description = """
  We could use a python script to compare youtube api plays vs. youtube.com plays for every youtube video
  """

  # Initialize parser
  parser = argparse.ArgumentParser(description = description)
  parser.add_argument('csv_input_path', type=str, help='Input CSV file name. No Excel here yet!')
  parser.add_argument('--csv_output_path', type=str, help='Output XLSX file name')
  parser.add_argument('--ids_column', dest='ids_column', type=str, default='yt_video_id', help="Name of video IDs column")
  parser.add_argument('--dry_run', dest='dry_run', type=bool, help="Whether to write output file")
  
  # Read arguments from command line
  args = parser.parse_args()
  csv_output_path = args.csv_output_path or f"{args.csv_input_path}-out.xlsx"

  # Setup dataframe
  # TODO add more insight from both scraping and the YT API
  df = pd.read_csv(args.csv_input_path)
  df.set_index(args.ids_column, drop=False, inplace=True)
  df = df.reindex(columns=df.columns.tolist() + ["view_count", "views"]) #+Video.fields)
  df = df.astype(str)
  # df.dropna(inplace=True)

  # vars
  video_ids = df[args.ids_column]
  counts = Counter(scraped=0, api=0)

  # Update df with videos from the YT api
  for msg, video in get_video_data(video_ids):
    if not video:
      logging.info(f"Skipping video : {msg}")
      continue
    df.loc[video.id] = { **df.loc[video.id].to_dict(), **asdict(video) }
    counts.update(api=1)
  

  # Update df with scraped videos 
  scraped_results = asyncio.run(scrape_multiple_videos(video_ids))
  counts.update(scraped=len(df))
  for video in scraped_results:
    video_id = video["video_id"]
    df.loc[video_id] = { **df.loc[video_id].to_dict(), **video }

  # log results  
  logging.info(f"Extracted videos: scraped / api / totals = "
               f"{counts['scraped']} / {counts['api']} / {len(df)}")

  # Write XLSX file
  if not args.dry_run:
    logging.info(f'Writing XLSX file : "{csv_output_path}"...')
    df.rename(columns={ 'views': 'views_count_scraped' }, inplace=True)
    df.to_excel(csv_output_path, engine='xlsxwriter', index=False)
    logging.info(f'Done !')

