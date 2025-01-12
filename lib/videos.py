"""
Extract metadata from YouTube videos 
using the [google-api-python-client](https://googleapis.github.io/google-api-python-client/)
Based on YouTube Data API (v3) 

"""

import os
import logging
import logging
from glom import glom
from dataclasses import dataclass, asdict, fields
from googleapiclient.discovery import build
from dotenv import load_dotenv


load_dotenv() 
logging.basicConfig(level=logging.DEBUG)


@dataclass
class Video:
  id: str
  title: str
  published_at: str
  language_code: str
  thumbnail_url: str
  duration: str
  view_count: str

  def __str__(self):
    return f"{self.id} {self.title}"

  @classmethod
  @property
  def fields(self):
    return [field.name for field in fields(Video)]


api_key = os.getenv("YT_API_KEY")
youtube = build('youtube', 'v3', developerKey=api_key)



def get_video_data(video_ids):
  """
  Rwturn data (metadata and statistics) for a single video.

  Note: Retrieval may fail for some videos so output may have length < len(video_ids)
  Cost of retrieving YT videos is 1 unit per 50 videos capped at 10k units/day.
  https://developers.google.com/youtube/v3/determine_quota_cost

  """
  
  num_batches = min(int(len(video_ids)/50), 10000) + 1
  for i in range(num_batches):

    # coerces video_ids into an iterable
    if not hasattr(video_ids, '__iter__'):
      assert isinstance(video_ids, str), f"Invalid arg 'video_ids' {type(video_ids)}"
      video_ids = video_ids.split(',')

    # YT denies more than 50 video ids per request
    id_list = list(video_ids)[i*50: (i+1)*50]
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=','.join(list(id_list)) 
    )

    # adapt response
    response = request.execute()
    for item in response['items']:
      msg, video = None, None

      try:
        video = Video(
          id=item['id'],
          title=item['snippet']['title'],
          published_at=item['snippet']['publishedAt'],
          language_code=glom(item, 'snippet.defaultAudioLanguage', default=''),
          thumbnail_url=glom(item, 'snippet.thumbnails.default.url', default=''),
          duration=item['contentDetails']['duration'],
          view_count=item['statistics']['viewCount'],
        )

        logging.debug(f"Extracted video : {item['id']}", asdict(video))
        msg = None

      except Exception as exc:
        msg, video = str(exc), None
        logging.debug(f"""Error extracting video "{item['id']}": {msg}""")

      yield msg, video



if __name__ == '__main__':
  """
  Example usage.
  """
  import pandas as pd 

    
  dry_run = False
  # csv_input_path = 'data/yt-null-upload-dates.csv'
  csv_input_path = 'data/2024-11-08 BP Wonder Series Videos for Checking.xlsx - Data.csv'
  csv_output_path = f"{csv_input_path}-out.xlsx"

  df = pd.read_csv(csv_input_path)
  df.set_index('yt_video_id', drop=False, inplace=True)
  df = df.reindex(columns=df.columns.tolist()+Video.fields)
  # df.dropna(inplace=True)

  # update 
  video_ids = df['yt_video_id']
  msgs, videos = list(zip(*get_video_data(video_ids)))
  for video in videos:
    df.loc[video.id] = { **df.loc[video.id].to_dict(), **asdict(video) }

  # update csv file
  if not dry_run:
    df.to_excel(csv_output_path, engine='xlsxwriter', index=False)
    
    