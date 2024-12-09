# Gospelseeds


## Preps.

1. Create virtual env

```shell
sudo apt-get update
sudo apt-get install git -y
sudo apt-get install python3-setuptools python3-dev build-essential python3-venv python3-pip -y

python -m venv venv
```

2. Install dependencies

```bash
pip install -r requirements.txt
playwright install-deps
playwright install
```


## script `plays_api_x_website`.

Compares youtube api plays vs. youtube.com plays for every youtube video \
*TODO*: add more fields than 'views_count'.

Usage: 
```
plays_api_x_website.py 
  [-h] 
  [--csv_output_path CSV_OUTPUT_PATH] 
  [--ids_column IDS_COLUMN] 
  [--dry_run DRY_RUN] 
  csv_input_path
```

Example:
```bash
chmod +x plays_api_x_website.py
./plays_api_x_website.py data/videoids.csv
```