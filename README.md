# Gospelseeds


## Preps.

```bash
pip install -r requirements.txt
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