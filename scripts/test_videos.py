# import re
# import urllib.request
# from bs4 import BeautifulSoup as bs


# video_id = "Y8JJi_YkXtA"
# with urllib.request.urlopen("https://www.youtube.com/watch?v=" + video_id) as r:
#   html = r.read()
#   yt_html = bs(html, "html.parser")

#   pattern = 'Video unavailable'
#   data = re.search(pattern=pattern, string=yt_html.prettify())

import logging
import to_csv
import os 
from pytube import YouTube
from pytube.exceptions import RegexMatchError, VideoUnavailable


logging.basicConfig(
  level=os.environ.get('LOGLEVEL', logging.INFO))


def download_youtube_video(video_id, csv_output_path="videos/", dry_run=False):

    try:

        status, msg = -1, ""
        yt = YouTube(f"https://www.youtube.com/watch?v={video_id}")
        yt.check_availability()
        status, msg = 0, f"Video is available : {yt.title}"

        if not dry_run:
            stream = yt.streams.get_highest_resolution()
            stream.download(csv_output_path)
            status, msg = 0, f"Successfully downloaded : {yt.title}"

    except RegexMatchError:
        status, msg = -1, "Invalid YouTube URL." 
        # logging.debug(msg)
    except VideoUnavailable as e:
        status, msg =  0, "Video Unavailable"
    except Exception as e:
        status, msg = -1, str(e)
    
    return status, msg
    


if __name__ == '__main__':
    
    # yt_video_ids = { "yt_video_id": "Y8JJi_YkXtA" }
    csv_input_path = '../data/yt-nullchannel.csv'
    csv_output_path = f"{csv_input_path}-out.csv"
    csv_header = ['yt_video_id', 'availability']

    with open(csv_input_path, 'r', newline='') as input_csv, \
         open(csv_output_path, 'w', newline='') as output_csv:

        # print('\n\n')
        # print("{:<20} {:<10}".format("Video ID", "Availability"))
        # print("-"*42)
        writer = csv.DictWriter(output_csv, fieldnames=csv_header)
        writer.writeheader()

        for row in csv.DictReader(input_csv):
            video_id = row['yt_video_id']
            status, msg = download_youtube_video(video_id, dry_run=True)
            row['availability'] = msg
            writer.writerow(row)
            # print("{:<20} {:<10}".format(video_id, msg))




     

     