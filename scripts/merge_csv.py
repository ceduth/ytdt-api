"""
scripts/merge-scraped.py
Performs an outer merge on video_id of two CSV files as 'merged_cleaned.csv'. 

Usage:  
    PYTHONPATH=${PYTHONPATH}:~/Devl/Projects/ytdt/ytdt-api/ \
    python scripts/merge_csv.py scraped.csv scraped-errors_scraped.csv
"""
import pandas as pd
import sys
import os
from utils.helpers import rename_file_with_extension

# Ensure proper usage
if len(sys.argv) != 3:
    print("Usage: python merge_csv.py <file1.csv> <file2.csv>")
    sys.exit(1)

file1 = sys.argv[1]
file2 = sys.argv[2]

# Validate file paths
if not os.path.isfile(file1):
    print(f"Error: File '{file1}' does not exist.")
    sys.exit(1)

if not os.path.isfile(file2):
    print(f"Error: File '{file2}' does not exist.")
    sys.exit(1)

# Load both files
df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

# Vertically stack rows using only common columns
merged_df = pd.concat([df1, df2], join="inner", ignore_index=True)

# Save the result
output_csv = rename_file_with_extension(file1, suffix='merged')
merged_df.to_csv(output_csv, index=False)

print(f"✅ Vertically merged CSV saved as '{output_csv}'")
