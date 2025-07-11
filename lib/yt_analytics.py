"""
Extract analytics data from YouTube videos 
using the [google-api-python-client](https://googleapis.github.io/google-api-python-client/)
Based on YouTube Analytics API (v2)

Note: This API requires OAuth2 authentication and only works for channels you own.
"""

import functools
import os
import logging
import argparse
from collections import defaultdict
from datetime import datetime, timedelta

import aiometer
import asyncio
from glom import glom
from tqdm import tqdm
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
from dataclasses import fields as _fields

from models import DataPipeline, Video, fields, asdict
from utils.helpers import safe_dict, rename_file_with_extension

from utils.env import \
    IO_CONCURRENCY_LIMIT, IO_BATCH_SIZE, IO_RATE_LIMIT, \
    LOG_LEVEL, YT_API_KEY, IO_TIMEOUT

logging.basicConfig(level=LOG_LEVEL)

# Scopes for YouTube Analytics API
SCOPES = ['https://www.googleapis.com/auth/youtube.readonly',
          'https://www.googleapis.com/auth/yt-analytics.readonly']



def get_authenticated_service(service_account_file=None):
    """
    Get authenticated YouTube Analytics service.
    
    Args:
        service_account_file: Path to service account JSON file.
                             If None, falls back to OAuth2 flow.
    """
    
    # Try service account authentication first
    if service_account_file and os.path.exists(service_account_file):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                service_account_file, scopes=SCOPES)
            logging.info("✅ Using service account authentication")
            return build('youtubeAnalytics', 'v2', credentials=credentials)
        except Exception as e:
            logging.warning(f"⚠️ Service account authentication failed: {e}")
            logging.info("Falling back to OAuth2 flow...")
    
    # Fallback to OAuth2 flow
    creds = None
    
    # Token file stores the user's access and refresh tokens
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    # If there are no (valid) credentials available, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    logging.info("✅ Using OAuth2 authentication")
    return build('youtubeAnalytics', 'v2', credentials=creds)


def parse_analytics(item, video_id, existing_video_data=None):
    """Parse analytics data and merge with existing Video object or create new one"""
    
    # Create base Video object with analytics data
    # Use existing video data if provided, otherwise create minimal Video
    if existing_video_data:
        video_data = existing_video_data.copy()
    else:
        video_data = {
            'video_id': video_id,
            'title': '',
            'published_at': '',
            'upload_date': '',
            'language_code': '',
            'view_count': str(item.get('views', 0)),
        }
    
    # Update with analytics data, mapping to Video class fields
    analytics_mapping = {
        'views': 'view_count',
        'likes': 'likes',
        'dislikes': 'dislikes', 
        'shares': 'shares',
        'comments': 'comments',
        'subscribersGained': 'subscribers_gained',
        'subscribersLost': 'subscribers_lost',
    }
    
    # Map analytics fields to Video fields
    for analytics_field, video_field in analytics_mapping.items():
        if analytics_field in item:
            video_data[video_field] = str(item[analytics_field])
    
    # Add analytics-specific fields as additional attributes
    analytics_extras = {
        'estimated_minutes_watched': item.get('estimatedMinutesWatched', 0),
        'average_view_duration': item.get('averageViewDuration', 0),
        'average_view_percentage': item.get('averageViewPercentage', 0),
        'subscriber_views': item.get('subscriberViews', 0),
        'non_subscriber_views': item.get('nonSubscriberViews', 0),
        'estimated_revenue': item.get('estimatedRevenue', 0),
        'estimated_ad_revenue': item.get('estimatedAdRevenue', 0),
        'estimated_red_revenue': item.get('estimatedRedRevenue', 0),
        'cpm': item.get('cpm', 0),
        'playback_based_cpm': item.get('playbackBasedCpm', 0),
        'ad_impressions': item.get('adImpressions', 0),
        'monetized_playbacks': item.get('monetizedPlaybacks', 0),
    }
    
    # Merge analytics extras into video data
    video_data.update(analytics_extras)
    
    # Create Video object (it will ignore unknown fields gracefully)
    try:
        video = Video(**{k: v for k, v in video_data.items() if k in [f.name for f in _fields(Video)]})
    except TypeError as e:
        # If Video constructor fails, create a minimal Video and add extras as dict
        video = Video(
            video_id=video_id,
            title=video_data.get('title', ''),
            published_at=video_data.get('published_at', ''),
            upload_date=video_data.get('upload_date', ''),
            language_code=video_data.get('language_code', ''),
            view_count=str(item.get('views', 0))
        )
        # Add analytics data as a dict to be handled by asdict
        video_dict = asdict(video)
        video_dict.update(analytics_extras)
        return video_dict
    
    logging.debug(f"👍 Parsed analytics for video: {video_id} - {video}")
    return video


async def fetch_multiple_analytics(
        video_ids, channel_id, existing_video_data=None, start_date=None, end_date=None, 
        service_account_file=None, progress_callback=None, **pipeline_kwargs):
    """
    Fetch analytics data for multiple YouTube videos using the YouTube Analytics API v2.
    Returns analytics data for videos to `results['analytics']`, errors to `results['errors']`

    Note: This requires OAuth2 authentication and only works for channels you own.
    Analytics data may not be available for all videos.
    
    Args:
        video_ids: List of video IDs to fetch analytics for
        channel_id: Channel ID (required for Analytics API)
        start_date: Start date for analytics period (YYYY-MM-DD format)
        end_date: End date for analytics period (YYYY-MM-DD format)
    """
    
    youtube_analytics = get_authenticated_service(service_account_file)
    
    # Default date range: last 28 days
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if not start_date:
        start_date = (datetime.now() - timedelta(days=28)).strftime('%Y-%m-%d')
    
    num_videos = len(video_ids)
    num_batches = min(int(len(video_ids) / IO_BATCH_SIZE), 10000) + 1

    async def _parse_to_pipeline(pipeline, task_index, video_id, analytics_data, existing_data=None):
        try:
            video = parse_analytics(analytics_data, video_id, existing_data)
            
            if progress_callback:
                await progress_callback(task_index, video_id)
            
            return await pipeline.enqueue(asdict(video)), 0
        
        except Exception as e:
            logging.debug(f"👎 Error parsing analytics for video {video_id}: {e}")
            return await pipeline.enqueue(safe_dict(e.__dict__), is_error=True), -1

    async def run_tasks(video_ids: list[str]):
        results = defaultdict(list)
        
        async with DataPipeline(**pipeline_kwargs) as pipeline:
            
            for i in range(num_batches):
                # Coerce video_ids into an iterable
                if not hasattr(video_ids, '__iter__'):
                    assert isinstance(video_ids, str), f"Invalid arg 'video_ids' {type(video_ids)}"
                    video_ids = video_ids.split(',')
                
                # Prepare batch of video IDs
                id_list = list(video_ids)[i * IO_BATCH_SIZE: (i + 1) * IO_BATCH_SIZE]
                
                batch_desc = 'fetched analytics batch #{current} ie. videos {start}-{end}/{num_videos}: ' \
                    .format(**{"current": i + 1, "start": 1 + i * IO_BATCH_SIZE,
                               "end": min((i + 1) * IO_BATCH_SIZE, num_videos),
                               "num_videos": num_videos})
                
                # Fetch analytics for each video in the batch
                tasks = []
                for idx, video_id in enumerate(tqdm(id_list, desc=batch_desc)):
                    try:
                        # YouTube Analytics API request
                        request = youtube_analytics.reports().query(
                            ids=f'channel=={channel_id}',
                            startDate=start_date,
                            endDate=end_date,
                            metrics='views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,subscriberViews,likes,dislikes,shares,comments,subscribersGained,subscribersLost',
                            dimensions='video',
                            filters=f'video=={video_id}',
                            sort='views'
                        )
                        
                        response = request.execute()
                        
                        # Parse response data
                        analytics_data = {}
                        if 'rows' in response and response['rows']:
                            # Map column headers to values
                            headers = [col['name'] for col in response['columnHeaders']]
                            row_data = response['rows'][0]  # Take first row
                            analytics_data = dict(zip(headers, row_data))
                        
                        existing_data = existing_video_data.get(video_id) if existing_video_data else None
                        tasks.append(functools.partial(_parse_to_pipeline, pipeline, i + idx, video_id, analytics_data, existing_data))
                        
                    except Exception as e:
                        logging.error(f"Error fetching analytics for video {video_id}: {e}")
                        # Add error task
                        error_data = {'error': str(e), 'video_id': video_id}
                        tasks.append(functools.partial(_parse_to_pipeline, pipeline, i + idx, video_id, error_data, None))
                
                # Execute all tasks for this batch
                for result in (await aiometer.run_all(
                    tasks, max_per_second=IO_RATE_LIMIT, max_at_once=IO_CONCURRENCY_LIMIT)
                ):
                    if result is None:
                        continue
                    
                    item, err_code = result
                    key = 'videos' if err_code > -1 else 'errors'
                    item = asdict(item)
                    results[key] += [item]
        
        return results
    
    return await run_tasks(video_ids)


if __name__ == '__main__':
    """
    Example usage:
    
        PYTHONPATH=${PYTHONPATH}:. python lib/yt_analytics.py data/video-ids-demo.csv \
                --csv_output_path data/analytics_output.csv \
                ----service_account cru-ga4-prod-1-63a3434e5a2a.json \
                --channel_id UC_x5XG1OV2P6uZZ5FSM9Ttw \
                --ids_column yt_video_id \
                --start_date 2024-01-01 \
                --end_date 2024-01-31 \
                --data_queue_limit 50 \
                --dry_run
                --xlsx
    """
    import pandas as pd
    
    arg_parser = argparse.ArgumentParser(description="Scrape analytics data from YouTube")
    arg_parser.add_argument('csv_input_path', type=str, help='Input CSV file path with video IDs')
    arg_parser.add_argument('--csv_output_path', type=str, help='Optional output CSV file path')
    arg_parser.add_argument('--service_account', type=str, help='Path to service account JSON file (alternative to OAuth2)')
    arg_parser.add_argument('--channel_id', type=str, required=True, help='YouTube channel ID (required for Analytics API)')
    arg_parser.add_argument('--ids_column', type=str, default='yt_video_id', help="Column name containing video IDs")
    arg_parser.add_argument('--start_date', type=str, help='Start date for analytics (YYYY-MM-DD format)')
    arg_parser.add_argument('--end_date', type=str, help='End date for analytics (YYYY-MM-DD format)')
    arg_parser.add_argument('--data_queue_limit', type=int, default=IO_BATCH_SIZE, help="Data queue limit for pipeline")
    arg_parser.add_argument('--dry_run', action='store_true', help="Run without saving to disk (dry run)")
    arg_parser.add_argument('--xlsx', action='store_true', help='Also saves a XLSX file')
    args = arg_parser.parse_args()
    
    csv_output_path = args.csv_output_path or \
        rename_file_with_extension(args.csv_input_path, suffix='yt_analytics')
    
    pipeline_kwargs = {
        "name": "Fetch analytics using the YouTube Analytics API v2",
        "csv_output_path": csv_output_path,
        "data_queue_limit": args.data_queue_limit,
        "dry_run": args.dry_run,
    }
    
    # Load video IDs from CSV
    df = pd.read_csv(args.csv_input_path)
    df.set_index(args.ids_column, drop=False, inplace=True)
    df = df.reindex(columns=list(set(df.columns.tolist() + fields(Video))))
    df = df.astype(str)
    video_ids = df[args.ids_column]
    
    # Create existing video data dict for merging
    existing_video_data = {}
    for idx, row in df.iterrows():
        existing_video_data[row[args.ids_column]] = row.to_dict()
    
    async def print_progress(completed: int, current_video: str):
        print(f"Progress: {completed} videos completed. Currently processing: {current_video}")
    
    # Fetch analytics using the YouTube Analytics API
    results = asyncio.run(fetch_multiple_analytics(
        video_ids, 
        channel_id=args.channel_id,
        existing_video_data=existing_video_data,
        start_date=args.start_date,
        end_date=args.end_date,
        service_account_file=args.service_account,
        progress_callback=print_progress, 
        **pipeline_kwargs
    ))
    
    # Save fetched video items (now with analytics) to memory dataframe
    for item in results['videos']:
        video_id = item['video_id']
        df.loc[video_id] = {
            **df.loc[video_id].to_dict(), **item
        }
    
    # Optionally save to xlsx file
    if not args.dry_run and args.xlsx:
        xlsx_output_path = rename_file_with_extension(csv_output_path, 'xlsx')
        logging.info(f'👍 saving xlsx file to: {xlsx_output_path}')
        df.to_excel(xlsx_output_path, engine='xlsxwriter', index=False)
