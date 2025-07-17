Retrieve data from GA4 property and save to BigQuery table.

## Work 


### Step 1. Env setup

```shell
export PYTHONPATH=$PYTHONPATH:. 
```

### Step 2. Extract comprehensive report with date range chunking

```shell
python lib/ga4.py --property-id 320198532 --start-date 2023-03-01 --end-date 2023-07-16 \
    --output-csv data/ga4_comprehensive.csv --cleanup-chunks
```

### Step 3. Extract missing data in smaller chunks

```shell
python lib/ga4.py --property-id 320198532 --start-date 2023-03-31 --end-date 2023-04-09 \
    --output-csv data/ga4_missing.csv --chunk-days 1 --cleanup-chunks
```

### Step 4. Merge and clean the data

```shell

# Outputs data/ga4_comprehensive_merged.csv
python scripts/merge_csv.py data/ga4_comprehensive.csv data/ga4_missing.csv

# Outputs ga4_comprehensive_merged_cleaned.csv
python scripts/clean_ga4_data_for_bigquery.py data/ga4_comprehensive_merged.csv
```

### Step 5. Upload to BigQuery

Upload to Cloud Storage bucket

```shell
gsutil cp data/ga4_comprehensive_merged_cleaned.csv \
    gs://jfp-temp/ga4_comprehensive_merged_cleaned.csv
```

Then from bucket to BigQuery

```sql
LOAD DATA OVERWRITE `jfp-data-warehouse.data_sources.ga4_320198532`
FROM FILES (
  format = 'CSV',
  uris = ['gs://jfp-temp/ga4_comprehensive_merged_cleaned.csv'],
  skip_leading_rows = 1
);
```

