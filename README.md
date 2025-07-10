# ytdt-api

`ytdt-api` exposes the core functionality of the YouTube Data Tools.
as a python library, a webservice, and several utility scripts. 

**YouTube Data Tools**: ML experimentation toolkit for YouTube data. Easily extract YouTube data, gather video statistics, explore API data, and gain novel audience insights.


## Caveats

- YouTube webpages do not currently expose following data: shares count, dislikes count, upload_date. This is not available by scraping and has to be retrieved by the YT API.

- Scrapping: YouTube employs robust anti-scraping measures, including IP blocking and CAPTCHAs, to prevent automated scraping of its data. 
Tweak the various IO_* environment variables to throttle the various multi-threading async I/O tasks. Please perform ethical and sustainable web scraping. Don't redistribute scraped data, especially not in bulk form. This protects user privacy.


## Dev setup.

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


Required envs:

```shell
YT_API_KEY=XXXX...
```

The following have [presets](./helpers.py):

```shell
export \
  IO_TIMEOUT=60000 \
  IO_RATE_LIMIT=1 \
  IO_BATCH_SIZE=3 \
  IO_CONCURRENCY_LIMIT=5
```

##  API Server

### Start the server locally (dev) or hit [YTDT online server](https://ytdt.ceduth.dev/).

    ```shell
    PYTHONPATH=$PYTHONPATH:. uvicorn api.main:app \
      --host 127.0.0.1 --port 8000 --reload
    ```
    or
    ```shell
    PYTHONPATH=$PYTHONPATH:. uvicorn main:app --reload --app-dir=./api
    ```

### API routes

1. Start a scraping job

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

2. Fetch videos using the YouTube Data API v3

  ```shell
  curl -X POST http://localhost:8000/fetch \
    -H "Content-Type: application/json" \
    -d '{ "video_ids": ["Znm_glAFMUQ"] }'

  # Response example:
  # {"job_id":"20250512_190045"}
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
    ```

    ```json
      {
      "results": [
        {
          "video_id": "dQw4w9WgXcQ",
          "title": "Rick Astley - Never Gonna Give You Up",
          "views": 1234567,
          "likes": 12345,
          "comments": 1234,
          "upload_date": "Oct 25, 2009",
          "channel_name": "Rick Astley",
          "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
        },
        {
          "video_id": "jNQXAC9IVRw",
          "title": "Me at the zoo",
          "views": 7654321,
          "likes": 54321,
          "comments": 4321,
          "upload_date": "Apr 23, 2005",
          "channel_name": "jawed",
          "url": "https://www.youtube.com/watch?v=jNQXAC9IVRw"
        }
      ]
    }
    ```


## Scripts

Every script features a `-h (--help)` option.
First add project dir `ytdt-api/` to PYTHONPATH:

```shell
export \
  PYTHONPATH=${PYTHONPATH}:. \
  LOG_LEVEL=10
```

### Script `yt_data.py`

Extract videos using the YouTube Data API (v3).

```shell
python lib/yt_data.py data/video-ids-demo.csv \
  --ids_column yt_video_id \
  --data_queue_limit 50 \
  --xlsx 
  ```

### Script `scraper.py`

Scrapes video ids. Look for output in: `data/youtube_video_stats.json`
Eg. scrape 5 videos at once, with speed = 5 items/sec, erroring unresponsive items after 1s

```shell 
python lib/scraper.py data/video-ids-demo.csv \
    --ids_column yt_video_id \
    --timeout 1000 \
    --concurrency 10 \
    --max_per_second 5 \
    --data_queue_limit 50 \
    --json
```

Using demo video ids file (incl. unavailable videos for completeness sake)
```shell
python lib/scraper.py data/video-ids-demo.csv \
  --json
```
Eg. results for above demo: 
```json
{
  "errors": [
    {
      "message": "🚫 async error: Error scraping video \"9eHseYggb-I\"",
      "detail": "Page.wait_for_selector: Timeout 90000ms exceeded.\nCall log:\n  - waiting for locator(\"h1.ytd-watch-metadata yt-formatted-string\") to be visible\n",
      "errors": "Traceback (most recent call last):\n  File \"/Users/ceduth/Devl/Projects/ytdt/ytdt-api/lib/scraper.py\", line 194, in scrape_video_stats\n    await page.wait_for_selector('h1.ytd-watch-metadata yt-formatted-string', state='visible', timeout=90000)\n  File \"/Users/ceduth/Devl/Projects/ytdt/ytdt-api/.venv/lib/python3.12/site-packages/playwright/async_api/_generated.py\", line 8162, in wait_for_selector\n    await self._impl_obj.wait_for_selector(\n  File \"/Users/ceduth/Devl/Projects/ytdt/ytdt-api/.venv/lib/python3.12/site-packages/playwright/_impl/_page.py\", line 424, in wait_for_selector\n    return await self._main_frame.wait_for_selector(**locals_to_params(locals()))\n           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/Users/ceduth/Devl/Projects/ytdt/ytdt-api/.venv/lib/python3.12/site-packages/playwright/_impl/_frame.py\", line 323, in wait_for_selector\n    await self._channel.send(\"waitForSelector\", locals_to_params(locals()))\n  File \"/Users/ceduth/Devl/Projects/ytdt/ytdt-api/.venv/lib/python3.12/site-packages/playwright/_impl/_connection.py\", line 61, in send\n    return await self._connection.wrap_api_call(\n           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/Users/ceduth/Devl/Projects/ytdt/ytdt-api/.venv/lib/python3.12/site-packages/playwright/_impl/_connection.py\", line 528, in wrap_api_call\n    raise rewrite_error(error, f\"{parsed_st['apiName']}: {error}\") from None\nplaywright._impl._errors.TimeoutError: Page.wait_for_selector: Timeout 90000ms exceeded.\nCall log:\n  - waiting for locator(\"h1.ytd-watch-metadata yt-formatted-string\") to be visible\n\n",
      "video_id": "9eHseYggb-I"
    },
    {
      "message": "🚫 async error: Error scraping video \"W7Tkx2oXIyk\"",
      "detail": "Page.wait_for_selector: Timeout 90000ms exceeded.\nCall log:\n  - waiting for locator(\"h1.ytd-watch-metadata yt-formatted-string\") to be visible\n",
      "errors": "Traceback (most recent call last):\n  File \"/Users/ceduth/Devl/Projects/ytdt/ytdt-api/lib/scraper.py\", line 194, in scrape_video_stats\n    await page.wait_for_selector('h1.ytd-watch-metadata yt-formatted-string', state='visible', timeout=90000)\n  File \"/Users/ceduth/Devl/Projects/ytdt/ytdt-api/.venv/lib/python3.12/site-packages/playwright/async_api/_generated.py\", line 8162, in wait_for_selector\n    await self._impl_obj.wait_for_selector(\n  File \"/Users/ceduth/Devl/Projects/ytdt/ytdt-api/.venv/lib/python3.12/site-packages/playwright/_impl/_page.py\", line 424, in wait_for_selector\n    return await self._main_frame.wait_for_selector(**locals_to_params(locals()))\n           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/Users/ceduth/Devl/Projects/ytdt/ytdt-api/.venv/lib/python3.12/site-packages/playwright/_impl/_frame.py\", line 323, in wait_for_selector\n    await self._channel.send(\"waitForSelector\", locals_to_params(locals()))\n  File \"/Users/ceduth/Devl/Projects/ytdt/ytdt-api/.venv/lib/python3.12/site-packages/playwright/_impl/_connection.py\", line 61, in send\n    return await self._connection.wrap_api_call(\n           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/Users/ceduth/Devl/Projects/ytdt/ytdt-api/.venv/lib/python3.12/site-packages/playwright/_impl/_connection.py\", line 528, in wrap_api_call\n    raise rewrite_error(error, f\"{parsed_st['apiName']}: {error}\") from None\nplaywright._impl._errors.TimeoutError: Page.wait_for_selector: Timeout 90000ms exceeded.\nCall log:\n  - waiting for locator(\"h1.ytd-watch-metadata yt-formatted-string\") to be visible\n\n",
      "video_id": "W7Tkx2oXIyk"
    }
  ],
  "videos": [
    {
      "video_id": "__5bvLohw5U",
      "title": "Religión Cuarto periodo",
      "published_at": "2020-10-22T00:00:00",
      "upload_date": "Unknown",
      "language_code": "Unknown",
      "view_count": 37,
      "url": "https://www.youtube.com/watch?v=__5bvLohw5U",
      "thumbnail_url": "Unknown",
      "channel_id": "Diblag",
      "channel_name": "Diana Blanco",
      "language_name": "Unknown",
      "country": "Unknown",
      "likes": 3,
      "comments": 0,
      "shares": 0,
      "dislikes": 0,
      "subscribers_gained": 0,
      "subscribers_lost": 0,
      "duration": 440
    },
    {
      "video_id": "__c6BSSKIXs",
      "title": "JESUS Mamasa 50 Jesus Carries His Cross",
      "published_at": "2050-10-04T20:22:00",
      "upload_date": "Unknown",
      "language_code": "Unknown",
      "view_count": 3,
      "url": "https://www.youtube.com/watch?v=__c6BSSKIXs",
      "thumbnail_url": "Unknown",
      "channel_id": "napielite3171",
      "channel_name": "Napi Elite",
      "language_name": "Unknown",
      "country": "Unknown",
      "likes": 0,
      "comments": 0,
      "shares": 0,
      "dislikes": 0,
      "subscribers_gained": 0,
      "subscribers_lost": 0,
      "duration": 215
    },
    {
      "video_id": "uuo2KqoJxsc",
      "title": "God's Rescue Plan",
      "published_at": "2023-04-13T00:00:00",
      "upload_date": "Unknown",
      "language_code": "Unknown",
      "view_count": 4597735,
      "url": "https://www.youtube.com/watch?v=uuo2KqoJxsc",
      "thumbnail_url": "Unknown",
      "channel_id": "Godlife",
      "channel_name": "GodLife.com",
      "language_name": "Unknown",
      "country": "Unknown",
      "likes": 0,
      "comments": 0,
      "shares": 0,
      "dislikes": 0,
      "subscribers_gained": 0,
      "subscribers_lost": 0,
      "duration": 178
    },
    {
      "video_id": "UJfX-ZrDZmU",
      "title": "Neden İsa Mesih Bizim İçin Öldü?",
      "published_at": "2020-11-22T00:00:00",
      "upload_date": "Unknown",
      "language_code": "Unknown",
      "view_count": 1773947,
      "url": "https://www.youtube.com/watch?v=UJfX-ZrDZmU",
      "thumbnail_url": "Unknown",
      "channel_id": "IsaMesihFilm",
      "channel_name": "İsa Mesih Film Projeleri",
      "language_name": "Unknown",
      "country": "Unknown",
      "likes": 848,
      "comments": 765,
      "shares": 0,
      "dislikes": 0,
      "subscribers_gained": 0,
      "subscribers_lost": 0,
      "duration": 180
    },
    {
      "video_id": "0_jC8Lg-oxY",
      "title": "لماذا كان على يسوع أن يموت؟",
      "published_at": "2022-04-26T00:00:00",
      "upload_date": "Unknown",
      "language_code": "Unknown",
      "view_count": 1327205,
      "url": "https://www.youtube.com/watch?v=0_jC8Lg-oxY",
      "thumbnail_url": "Unknown",
      "channel_id": "Talmazaonline",
      "channel_name": "TalmazaOnline",
      "language_name": "Unknown",
      "country": "Unknown",
      "likes": 25,
      "comments": 2,
      "shares": 0,
      "dislikes": 0,
      "subscribers_gained": 0,
      "subscribers_lost": 0,
      "duration": 180
    }
  ]
}
```


### Script `yt_analytics`

Extract videos using the YouTube Analytics API (v2).
Eg. the [JesusFilm](https://www.youtube.com/@jesusfilm) channel queried (requires authentication):

```shell
python lib/yt_analytics.py data/video-ids-demo.csv \
    --service_account ./cru-ga4-prod-1-63a3434e5a2a.json \
    --channel_id UCCtcQHR6-mQHQh6G06IPlDA \
    --ids_column yt_video_id \
    --start_date 2025-01-01 \
    --end_date 2025-07-10 \
    --data_queue_limit 50 
```

### Script `plays_api_x_website.py`

Compares YouTube api plays vs. youtube.com plays for every YouTube video 

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

PYTHONPATH=$PYTHONPATH:.  \
./scripts/plays_api_x_website.py data/video-ids-demo.csv \
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


### Deploy locally


```shell
docker build -t ytdt-api .
docker run -p 8000:80 ytdt-api
```

Open http://localhost:8000 



### Deploy to Kubernetes 

1. Add following secrets to GitHub repository

```shell
gh secret set HARBOR_USERNAME --body "your-username"
gh secret set HARBOR_PASSWORD --body "your-password-value"
gh secret set YT_API_KEY --body "your-youtube-api-key"
```


## Testing the YouTube Data API v3

* With your private YouTube Data API v3 key

  ```shell
  YT_API_KEY=AIzaSyDP5X-your_key_here 
  ```

* Query specific video IDs

  Optional part= values:
  - snippet (title, channel, etc.)
  - statistics (views, likes, etc.)
  - contentDetails (duration, etc.)

  ```shell
  YT_VIDEO_IDS=gIDYvg73RuM,...
  curl "https://www.googleapis.com/youtube/v3/videos?part=snippet,contentDetails,statistics&id=${YT_VIDEO_IDS}&key=${YT_API_KEY}"
  ```


## Known Bugs

* Comments count return from scrapping has 0 value most of the time.
