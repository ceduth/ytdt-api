"""
clean_ga4_data_for_bigquery.py
================================
This script cleans the GA4 data CSV file to ensure it can be imported into BigQuery without issues. 
It handles numeric columns that may have formatting problems, replaces problematic values, and ensures
all data types are compatible with BigQuery's requirements.

1. Run the script to clean the GA4 data

2. Upload the cleaned CSV to Google Cloud Storage

```bash
gsutil cp data/ga4_comprehensive_chunked_final_cleaned.csv gs://jfp-temp/ga4_comprehensive_chunked_final_cleaned.csv
```

3. Load the results into BigQuery

```bigquery
LOAD DATA INTO `jfp-data-warehouse.data_sources.ga4_320198532`
FROM FILES (
  format = 'CSV',
  uris = ['gs://jfp-temp/ga4_comprehensive_chunked_final_cleaned.csv'],
  skip_leading_rows = 1
);
```

"""
import pandas as pd
import numpy as np



def clean_ga4_data_for_bigquery(csv_file_path, output_path=None):
    """
    Clean GA4 CSV data to fix BigQuery import issues
    
    Args:
        csv_file_path: Path to the GA4 CSV file
        output_path: Output path (defaults to input_path + '_cleaned.csv')
    """
    
    print(f"🔄 Loading CSV file: {csv_file_path}")
    
    # Read the CSV file
    df = pd.read_csv(csv_file_path)
    print(f"   📊 Original shape: {df.shape}")
    
    # Define columns that should be numeric but might have formatting issues
    numeric_columns = [
        'eventValue', 'activeUsers', 'sessions', 'engagedSessions',
        'userEngagementDuration', 'screenPageViews', 'bounceRate',
        'engagementRate', 'sessionsPerUser', 'averageSessionDuration',
        'newUsers', 'totalUsers'
    ]
    
    # Add custom event metrics
    custom_metrics = [col for col in df.columns if col.startswith('customEvent:')]
    numeric_columns.extend(custom_metrics)
    
    print(f"🔧 Cleaning numeric columns...")
    
    for col in numeric_columns:
        if col in df.columns:
            print(f"   🔄 Processing {col}")
            
            # Convert to string first to handle any weird formats
            df[col] = df[col].astype(str)
            
            # Replace common problematic values
            df[col] = df[col].replace(['NaN', 'nan', 'null', 'NULL', ''], '0')
            
            # Check if column has decimal values
            has_decimals = df[col].str.contains('\.', na=False).any()
            
            if has_decimals:
                print(f"      ⚠️  {col} contains decimal values - converting to float")
                # Convert to float first, then handle NaN
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
                # If eventValue, you might want to keep as float
                if col == 'eventValue':
                    # Keep as float for eventValue
                    df[col] = df[col].astype('float64')
                else:
                    # For other metrics, convert to int if they should be integers
                    df[col] = df[col].round().astype('int64')
            else:
                # Convert directly to integer
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
    
    # Clean string columns
    string_columns = [
        'dateHourMinute', 'customEvent:mediacomponentid', 'languageCode',
        'countryId', 'sessionSource', 'sessionMedium', 'pageLocation', 'eventName'
    ]
    
    print(f"🔧 Cleaning string columns...")
    for col in string_columns:
        if col in df.columns:
            # Replace NaN/null values with empty string or appropriate default
            df[col] = df[col].fillna('(not set)')
            # Ensure it's string type
            df[col] = df[col].astype(str)
    
    # Handle any remaining NaN values
    print(f"🔧 Final cleanup...")
    
    # Replace any remaining NaN values
    df = df.fillna(0)
    
    # Generate output path
    if output_path is None:
        output_path = csv_file_path.replace('.csv', '_cleaned.csv')
    
    # Save cleaned data
    print(f"💾 Saving cleaned data to: {output_path}")
    df.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"✅ Cleaning completed!")
    print(f"   📊 Final shape: {df.shape}")
    print(f"   📁 Output file: {output_path}")
    
    # Show sample of problematic columns
    print(f"\n📊 Sample of eventValue column after cleaning:")
    print(df['eventValue'].describe())
    
    return df

# Usage example:
if __name__ == "__main__":
    # Clean your GA4 data
    cleaned_df = clean_ga4_data_for_bigquery(
        csv_file_path="data/ga4_comprehensive_chunked_final.csv",
        output_path="data/ga4_comprehensive_chunked_final_cleaned.csv"
    )
    
    print("\n🔍 Data types after cleaning:")
    print(cleaned_df.dtypes)