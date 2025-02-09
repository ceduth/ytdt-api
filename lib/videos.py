"""
Extract metadata from YouTube videos 
using the [google-api-python-client](https://googleapis.github.io/google-api-python-client/)
Based on YouTube Data API (v3) 

"""

import functools
import os
import logging
import aiometer
import asyncio
from glom import glom
from tqdm import tqdm
from googleapiclient.discovery import build
from dotenv import load_dotenv

from models import DataPipeline, Video, fields, asdict
from helpers import \
    IO_CONCURRENCY_LIMIT, IO_BATCH_SIZE, IO_RATE_LIMIT

load_dotenv()
logging.basicConfig(level=logging.DEBUG)

api_key = os.getenv("YT_API_KEY")
youtube = build('youtube', 'v3', developerKey=api_key)


def parse_video(item):
    """ Parse dict data into Video object """

    video = Video(
        video_id=item['id'],
        title=item['snippet']['title'],
        published_at=item['snippet']['publishedAt'],
        upload_date=glom(item, 'recordingDetails.recordingDate', default=''),
        language_code=glom(item, 'snippet.defaultAudioLanguage', default=''),
        channel_id=glom(item, 'snippet.channelId', default=''),
        channel_name=glom(item, 'snippet.title', default=''),
        thumbnail_url=glom(item, 'snippet.thumbnails.default.url', default=''),
        duration=item['contentDetails']['duration'],
        view_count=item['statistics']['viewCount'],
        # TODO:
        # language_name =
        # country =
    )

    logging.debug(f"Parsed video : {item['id']}", video)
    return video


async def fetch_multiple_videos(video_ids, progress_callback=None, **pipeline_kwargs):
    """
    Return data (metadata and statistics) for a single video.

    Note: Retrieval may fail for some videos so output may have length < len(video_ids)
    Cost of retrieving YT videos is 1 unit per 50 videos capped at 10k units/day.
    https://developers.google.com/youtube/v3/determine_quota_cost
    """

    num_videos = len(video_ids)
    num_batches = min(int(len(video_ids) / IO_BATCH_SIZE), 10000) + 1

    async def _parse_to_pipeline(pipeline, task_index, item):

        try:
            if progress_callback:
                await progress_callback(task_index, item)

            v = parse_video(item)  # validate the data!
            return await pipeline.enqueue(asdict(v))

        except Exception as e:
            # TODO: only AsyncException are currently properly formatted for savin to csv
            await pipeline.enqueue(e.__dict__, is_error=True)

    async def run_tasks(video_ids: [str]):

        results = []

        async with DataPipeline(**pipeline_kwargs) as pipeline:

            for i in range(num_batches):

                # coerces video_ids into an iterable
                if not hasattr(video_ids, '__iter__'):
                    assert isinstance(video_ids, str), f"Invalid arg 'video_ids' {type(video_ids)}"
                    video_ids = video_ids.split(',')

                # prepare fetch request
                # Note: YouTube denies more than 50 video ids per request
                id_list = list(video_ids)[i * IO_BATCH_SIZE: (i + 1) * IO_BATCH_SIZE]
                request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(list(id_list)))

                # fetch batches of IO_DATA_QUEUE_LIMIT videos ...
                response = request.execute()

                batch_desc = 'fetching batch #{current} ie. videos {start}-{end}/{num_videos}: ' \
                    .format(**{"current": i + 1, "start": 1 + i * IO_BATCH_SIZE,
                               "end": min((i + 1) * IO_BATCH_SIZE, num_videos),
                               "num_videos": num_videos})

                tasks = [functools.partial(_parse_to_pipeline, pipeline, i, v)
                         for i, v in enumerate(tqdm(response['items'], desc=batch_desc))]

                results += await aiometer.run_all(
                  tasks, max_per_second=IO_RATE_LIMIT, max_at_once=IO_CONCURRENCY_LIMIT)

        return results

    return await run_tasks(video_ids)


if __name__ == '__main__':
    """
    Example usage.
    """
    import pandas as pd

    async def print_progress(completed: int, current_video: str):
        print(f"Progress: {completed} videos completed. Currently processing: {current_video}")


    dry_run = False
    csv_input_path = '../data/video-ids-three.csv'
    csv_output_path = f"{csv_input_path}-api-out.xlsx"

    pipeline_kwargs = {
        "name": "Fetch videos using the YouTube Data API v3",
        "csv_output_path": csv_output_path,
        "dry_run": False,
    }

    df = pd.read_csv(csv_input_path)
    df.set_index('yt_video_id', drop=False, inplace=True)
    df = df.reindex(columns=df.columns.tolist() + fields(Video))
    df = df.astype(str)
    # df.dropna(inplace=True)

    # update
    video_ids = df['yt_video_id']
    videos = asyncio.run(fetch_multiple_videos(
        video_ids, progress_callback=print_progress, **pipeline_kwargs))

    for item in videos:
        video_id = item["video_id"]
        df.loc[video_id] = {
            **df.loc[video_id].to_dict(), **item}

    # update csv file
    if not dry_run:
        df.to_excel(csv_output_path, engine='xlsxwriter', index=False)
