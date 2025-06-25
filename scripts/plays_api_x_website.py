#!/usr/bin/env python3
import os
import asyncio
import argparse
import logging
import pandas as pd

from lib.videos import fetch_multiple_videos
from lib.scraper import scrape_multiple_videos
from utils.env import IO_TIMEOUT


logging.basicConfig(
    level=os.environ.get('LOGLEVEL', logging.INFO))


if __name__ == '__main__':
    """
    ** Usage **
        plays_api_x_website.py 
        [-h] 
        [--csv_output_path CSV_OUTPUT_PATH] 
        [--include_fields INCLUDE_FIELDS]
        [--ids_column IDS_COLUMN] 
        [--dry_run DRY_RUN] 
        csv_input_path
        
    ** Example usage **
        plays_api_x_website.py data/video-ids-three.csv
    """

    description = """
    We could use a python script to compare youtube api plays vs. youtube.com plays 
    for every youtube video """

    # Initialize parser
    parser = argparse.ArgumentParser(description=description)
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
    pipeline_kwargs = {"csv_output_path": f"{args.csv_input_path}-api-out.csv",
                       "fields": include_fields, "dry_run": args.dry_run,
                       "name": "Fetch videos using the YouTube Data API v3"}
    fetched_videos = asyncio.run(fetch_multiple_videos(video_ids, **pipeline_kwargs))

    # scrape videos asynchronously
    pipeline_kwargs = {"csv_output_path": f"{args.csv_input_path}-scraped-out.csv",
                       "fields": include_fields, "dry_run": args.dry_run,
                       "name": f"Scrape videos with {IO_TIMEOUT}ms timeout"}
    scraped_videos = asyncio.run(scrape_multiple_videos(video_ids, **pipeline_kwargs))
