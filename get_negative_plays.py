import pandas as pd 

from google.cloud import bigquery
from google.oauth2 import service_account




def query_bigquery(query):
  """

  https://google-auth.readthedocs.io/en/master/reference/google.oauth2.service_account.html
  https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries#python

    query = (
      'select *'
      'FROM `jfp-data-warehouse.prod.wc_jfp_youtube_ds`'
    )
    query_bigquery()
  """

  credentials = service_account.Credentials.from_service_account_file('./jfp-data-warehouse-99fc6d8a8234.json')
  client = bigquery.Client(credentials=credentials)
  query_job = client.query(query)
  results = query_job.result()  

  for row in results[:10]:
      print("{} : {} views".format(row.url, row.view_count))


if __name__ == "__main__":

  dup_negative_plays_video_ids = []
  df_out = pd.DataFrame(columns=['video_id', 'plays', 'negative_plays'])

  df = pd.read_csv('data/wc_jfp_youtube_ds.csv').sort_values(['event_date'])
  negative_plays_ids = set(df[df['plays'] < 0]['video_id'])


  # put side-by-side negative vs. postive plays per each video,
  # goal: delete negative video iff negative plays offset positive ones
  # by some threshold.
  for video_id in negative_plays_ids:
    video_plays = df[df['video_id'] == video_id]
    positive_video_plays = video_plays[video_plays['plays'] > 0] 
    negative_video_plays = video_plays[video_plays['plays'] < 0] 

    if 1 < len(negative_video_plays):
      dup_negative_plays_video_ids += [video_id]

    df_out.loc[len(df_out)] = {
      "video_id": video_id,
      "plays": positive_video_plays['plays'].sum(),
      "negative_plays": negative_video_plays['plays'].sum()
    }
    

  df_out.reset_index(drop=True)
  print("\n")
  print(df_out)
  print("\nDuplicate negative plays", dup_negative_plays_video_ids)







    