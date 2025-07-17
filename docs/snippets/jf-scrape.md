


Scrape YouTube JFP video metadata only if not available via API, ie. in following order:

1. Analytics (owned videos) -> script `lib/yt_analytics.py`
2. YouTube Data API v3 -> script `lib/yt_data.py`
3. Scraping -> script `lib/scraper.py`


## Work 

### Step 1. Env setup

```shell
export \
  PYTHONPATH=${PYTHONPATH}:. \
  LOG_LEVEL=10
```

### Step 2.   

```shell
python lib/scraper.py ~/Downloads/unique_video_ids.csv \
    --ids_column video_id \
    --timeout 30000 \
    --concurrency 5 \
    --max_per_second 2 
```

```shell
python lib/yt_data.py  ~/Downloads/bquxjob_702e65cd_197f062db56.csv \
  --ids_column video_id 
```

```shell
python lib/yt_analytics.py ~/Downloads/bquxjob_702e65cd_197f062db56.csv \
    --service_account ./cru-ga4-prod-1-63a3434e5a2a.json \
    --channel_id UCCtcQHR6-mQHQh6G06IPlDA \
    --ids_column video_id \
    --start_date 2024-01-01 \
    --end_date 2025-07-10 \
    --data_queue_limit 50 
```

### Step 2. Scrape 

```shell
# 1. Scrape
python lib/scraper.py ~/Downloads/bquxjob_702e65cd_197f062db56.csv --ids_column video_id 

# 2. Retry errors
python lib/scraper.py ~/Downloads/scraped_errors.csv --ids_column video_id

# 3. Merge
python scripts/merge_csv.py ~/Downloads/scraped.csv ~/Downloads/scraped_errors_scraped.csv
```   
 
Optionally, extract `video_id`s that failed retry, 
ie. are in scraped_errors.csv but not in scraped_errors_scraped.csv 

```shell
scripts/extract_unique_video_ids.py ~/Downloads/scraped_errors.csv ~/Downloads/scraped_errors_scraped.csv 
```

F
```python
with open('video_fields.csv', 'w', newline='') as f:
    write = csv.writer(f)
    write.writerow(fields(Video))
```