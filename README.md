# Gospelseeds


## Preps.

1. Create virtual env

```shell
sudo apt-get update
sudo apt-get install git -y
sudo apt-get install python3-setuptools python3-dev build-essential python3-venv python3-pip -y



python -m venv venv
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


## script `plays_api_x_website`.

Compares youtube api plays vs. youtube.com plays for every youtube video \
*TODO*: add more fields eg. likes/dislikes, comments, shares, subscribers gained/lost, language, etc. 

Usage: 
```
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
./plays_api_x_website.py data/video-ids-three.csv \
  --include_fields=published_at,upload_date,duration,view_count,scraped_published_at,scraped_upload_date,scraped_upload_date,scraped_duration,scraped_view_count
```