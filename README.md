# yt-retriever

Library and API server to retrieve YouTube content using web scraping and the YouTube Data API. 


## Preps.

1. Create virtual env

```shell
sudo apt-get update
sudo apt-get install git -y
sudo apt-get install python3-setuptools python3-dev build-essential python3-venv python3-pip -y
python -m venv .venv
```


2. Optionally switch to newest major python release, eg. 3.12 in 2025

Install python 3.12 using pyenv

```shell
pyenv install 3.12
#pyenv global 3.12
```

Fix pip for python 3.12

```bash
python -m ensurepip --upgrade
python -m pip install --upgrade setuptools
pip install --upgrade pip
```

3. Install dependencies


```bash
pip install -r requirements.txt
playwright install-deps
playwright install
```

4. Setup envs

Create an API key in the Console by clicking [Create credentials](https://console.cloud.google.com/apis/credentials)  > API key. 

Note: Cost of retrieving YT videos is 1 unit per 50 videos capped at 10k units/day.
https://developers.google.com/youtube/v3/determine_quota_cost


```shell
YT_API_KEY=XXXX...
```


##  API server

1. Start the server

    ```shell
    cd backend
    PYTHONPATH=$PYTHONPATH:/Users/ceduth/Devl/JFP/yt-retriever/backend/api  \
      uvicorn main:app --reload --app-dir=./api
    ```

2. API routes

    ```shell
    curl -X POST http://localhost:8000/scrape \
      -H "Content-Type: application/json" \
      -d '{
        "video_ids": [
          "9eHseYggb-I",  
          "W7Tkx2oXIyk",  
          "uuo2KqoJxsc",
          "UJfX-ZrDZmU",
          "0_jC8Lg-oxY"
        ]
      }'
    
    # Response example:
    # {"job_id": "20250209_150714"}
    ```

3. Check job status:

    ```shell
    curl http://localhost:8000/status/20250209_150714
    
    # Response example:
    # {
    #   "status": "running",
    #   "progress": {
    #     "completed": 4,
    #     "total": 5,
    #     "current_video": "0_jC8Lg-oxY"
    #   },
    #   "results": null,
    #   "error": null
    # }
    ```

4. Get job results:
    
    ```bash
    curl http://localhost:8000/results/20250209_123456
    
    # Response example:
    # {
    #   "results": [
    #     {
    #       "video_id": "dQw4w9WgXcQ",
    #       "title": "Rick Astley - Never Gonna Give You Up",
    #       "views": 1234567,
    #       "likes": 12345,
    #       "comments": 1234,
    #       "upload_date": "Oct 25, 2009",
    #       "channel_name": "Rick Astley",
    #       "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    #     },
    #     {
    #       "video_id": "jNQXAC9IVRw",
    #       "title": "Me at the zoo",
    #       "views": 7654321,
    #       "likes": 54321,
    #       "comments": 4321,
    #       "upload_date": "Apr 23, 2005",
    #       "channel_name": "jawed",
    #       "url": "https://www.youtube.com/watch?v=jNQXAC9IVRw"
    #     }
    #   ]
    # }
    ```


## Scripts

### Script `plays_api_x_website.py`


Compares YouTube api plays vs. youtube.com plays for every YouTube video \
*TODO*: add more fields e.g. likes/dislikes, comments, shares, subscribers gained/lost, language, etc. 

Usage: 

```shell
plays_api_x_website.py 
  [-h] 
  [--csv_output_path CSV_OUTPUT_PATH] 
  [--include_fields INCLUDE_FIELDS]
  [--ids_column IDS_COLUMN] 
  [--dry_run DRY_RUN] 
  csv_input_path
```

Example:

```bash
chmod +x plays_api_x_website.py

PYTHONPATH=$PYTHONPATH:/Users/ceduth/Devl/JFP/yt-retriever/backend/  \
./scripts/plays_api_x_website.py data/video-ids-three.csv \
  --include_fields=published_at,upload_date,duration,view_count,scraped_published_at,scraped_upload_date,scraped_upload_date,scraped_duration,scraped_view_count
```


### Script `test_videos.py`


Unavailable videos are written to CSV file with following columns:

- video_id: YouTube video ID
- error_message: specific error encountered
- check_date: timestamp when the check was performed


Usage:

The usage remains similar, but the unavailable videos are now output as CSV:
```shell
available_videos.py input_file.txt
  --output/-o: Specify custom output file for available videos (default: available_videos.txt)
  --unavailable/-u: Specify custom output CSV file for unavailable videos (default: unavailable_videos.csv)
  --workers/-w: Set maximum number of concurrent threads (default: 10)
```

Example:

```shell
available_videos.py data/wc_jfp_youtube_video_d.csv -u data/unavailable_videos.csv
```

## Deploy


## Test locally

```shell
PYTHONPATH=$PYTHONPATH:/Users/ceduth/Devl/Projects/yt-retriever uvicorn api.main:app \
  --host 127.0.0.1 --port 8000 --reload
```

```shell
docker build -t yt-retriever .
docker run -p 8000:80 yt-retriever
```

Open http://localhost:8000 



## Deploy to Kubernetes 

1. Add following secrets to GitHub repository

```shell
gh secret set HARBOR_USERNAME --body "your-username"
gh secret set HARBOR_PASSWORD --body "your-password-value"
gh secret set YT_API_KEY --body "your-youtube-api-key"
```
