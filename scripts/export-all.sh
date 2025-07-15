export \
  PYTHONPATH=${PYTHONPATH}:. \
  LOG_LEVEL=10

python lib/scraper.py ~/Downloads/bquxjob_702e65cd_197f062db56.csv \
    --ids_column video_id 

python lib/scraper.py ~/Downloads/scraped-errors.csv --ids_column video_id
    

ERROR:root:Error occurred at position 10: offset must be a timedelta strictly between -timedelta(hours=24) and timedelta(hours=24).
ERROR:root:Writing failed: offset must be a timedelta strictly between -timedelta(hours=24) and timedelta(hours=24).


python lib/scraper.py ~/Downloads/unique_video_ids.csv \
    --ids_column video_id \
    --timeout 30000 \
    --concurrency 5 \
    --max_per_second 2 


python lib/yt_data.py  ~/Downloads/bquxjob_702e65cd_197f062db56.csv \
  --ids_column video_id 



python lib/yt_analytics.py ~/Downloads/bquxjob_702e65cd_197f062db56.csv \
    --service_account ./cru-ga4-prod-1-63a3434e5a2a.json \
    --channel_id UCCtcQHR6-mQHQh6G06IPlDA \
    --ids_column video_id \
    --start_date 2024-01-01 \
    --end_date 2025-07-10 \
    --data_queue_limit 50 



with open('video_fields.csv', 'w', newline='') as f:
    write = csv.writer(f)
    write.writerow(fields(Video))



