import pandas as pd
import os
import httplib2
from googleapiclient.discovery import build
from oauth2client.file import Storage
from oauth2client.client import flow_from_clientsecrets
from oauth2client.tools import run_flow, argparser

# Setup the YouTube API
CLIENT_SECRETS_FILE = "client_secret.json"  # Path to OAuth 2.0 client secrets.
SCOPES = ['https://www.googleapis.com/auth/yt-analytics.readonly']
API_SERVICE_NAME = 'youtubeAnalytics'
API_VERSION = 'v2'

def get_authenticated_service():
    flow = flow_from_clientsecrets(CLIENT_SECRETS_FILE, scope=SCOPES)
    storage = Storage("%s-oauth2.json" % API_SERVICE_NAME)
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        # Setup the local server to automatically handle the redirect
        flow.redirect_uri = 'http://localhost:8080/'
        flags = argparser.parse_args(args=[])
        credentials = run_flow(flow, storage, flags)
    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)

def get_audience_retention(youtube_analytics, video_id):
    # Call the YouTube Analytics API to fetch audience retention
    response = youtube_analytics.reports().query(
        ids='channel==UCCtcQHR6-mQHQh6G06IPlDA',
        startDate='2021-01-01',
        endDate='2024-01-01',
        metrics='audienceWatchRatio',
        dimensions='elapsedVideoTimeRatio',
        filters=f'video=={video_id}',
        sort='elapsedVideoTimeRatio'
    ).execute()

    if 'rows' in response:
        return pd.DataFrame(response['rows'], columns=['ElapsedVideoTimeRatio', 'AudienceWatchRatio'])
    else:
        return pd.DataFrame(columns=['ElapsedVideoTimeRatio', 'AudienceWatchRatio'])

def find_closest_ratio(retention_data, target_ratio):
    # Find the row with the closest ElapsedVideoTimeRatio to the target ratio
    closest_idx = (retention_data['ElapsedVideoTimeRatio'] - target_ratio).abs().idxmin()
    return retention_data.iloc[closest_idx]['AudienceWatchRatio']

def process_videos(file_path):
    youtube_analytics = get_authenticated_service()
    data = pd.read_csv(file_path)
    results = []

    for index, row in data.iterrows():
        video_id = row['Video ID']
        target_ratio = float(row['ElapsedVideoTimeRatio'].strip('%')) / 100  # Convert percentage to decimal

        retention_data = get_audience_retention(youtube_analytics, video_id)
        if not retention_data.empty:
            closest_watch_ratio = find_closest_ratio(retention_data, target_ratio)
        else:
            closest_watch_ratio = None  # Set to None if no data is found

        # Retain original data and update the audienceWatchRatio
        results.append([
            video_id, 
            row['ElapsedVideoTimeRatio'], 
            closest_watch_ratio, 
            row['Total Views'], 
            row['Retained Views']
        ])

    # Save results with updated audienceWatchRatio
    column_names = ['Video ID', 'ElapsedVideoTimeRatio', 'audienceWatchRatio', 'Total Views', 'Retained Views']
    result_df = pd.DataFrame(results, columns=column_names)
    result_df.to_csv('output_updated.csv', index=False)

# Example usage
file_path = 'sheets_yt_jfm_retention - Sheet6.csv'
process_videos(file_path)
