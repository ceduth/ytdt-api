"""
Command Line Arguments:

files: One or more CSV files to merge (required)
--id-column or -i: Column name for deduplication (default: video_id)
--output or -o: Output filename (default: merged_data.csv)
--no-dedup: Skip deduplication, just concatenate files
--help: Show help message with examples
"""
import pandas as pd
import numpy as np
import argparse
import sys
from datetime import datetime


def merge_csv_files(file_paths, id_column='video_id', output_path='merged_data.csv'):
    """
    Merge multiple CSV files vertically with deduplication based on specified ID column.
    
    Priority for duplicate removal:
    1. Largest view_count
    2. Most columns set (non-null values)
    3. Latest published_at
    
    Args:
        file_paths: List of CSV file paths
        id_column: Column name to use for deduplication (default: 'video_id')
        output_path: Path for output merged file
    
    Returns:
        Merged DataFrame
    """
    
    # Read all CSV files
    dfs = []
    for i, file_path in enumerate(file_paths):
        try:
            df = pd.read_csv(file_path)
            print(f"Loaded {file_path}: {df.shape[0]} rows, {df.shape[1]} columns")
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            continue
    
    if not dfs:
        raise ValueError("No valid CSV files could be loaded")
    
    # Concatenate all dataframes
    merged_df = pd.concat(dfs, ignore_index=True, sort=False)
    print(f"\nCombined data: {merged_df.shape[0]} rows, {merged_df.shape[1]} columns")
    
    # Check for duplicates based on specified ID column
    if id_column not in merged_df.columns:
        print(f"Warning: '{id_column}' column not found. Available columns: {list(merged_df.columns)}")
        print("Returning merged data without deduplication.")
        return merged_df
    
    # Remove rows where ID column is null
    merged_df = merged_df.dropna(subset=[id_column])
    
    # Find duplicates
    duplicates = merged_df[merged_df.duplicated(subset=[id_column], keep=False)]
    print(f"Found {len(duplicates)} rows with duplicate {id_column}s")
    
    if len(duplicates) == 0:
        print("No duplicates found. Returning merged data.")
        return merged_df
    
    # Apply deduplication logic
    def select_best_row(group):
        """Select the best row from a group of duplicates"""
        
        # 1. Priority: Largest view_count
        if 'view_count' in group.columns:
            group = group.copy()
            group['view_count'] = pd.to_numeric(group['view_count'], errors='coerce')
            max_views = group['view_count'].max()
            if not pd.isna(max_views):
                candidates = group[group['view_count'] == max_views]
                if len(candidates) == 1:
                    return candidates.iloc[0]
                group = candidates
        
        # 2. Priority: Most columns set (non-null values)
        group = group.copy()
        group['non_null_count'] = group.notna().sum(axis=1)
        max_non_null = group['non_null_count'].max()
        candidates = group[group['non_null_count'] == max_non_null]
        if len(candidates) == 1:
            return candidates.iloc[0]
        group = candidates
        
        # 3. Priority: Latest published_at
        if 'published_at' in group.columns:
            group = group.copy()
            group['published_at_parsed'] = pd.to_datetime(group['published_at'], errors='coerce')
            latest_date = group['published_at_parsed'].max()
            if not pd.isna(latest_date):
                candidates = group[group['published_at_parsed'] == latest_date]
                if len(candidates) == 1:
                    return candidates.iloc[0]
                group = candidates
        
        # If still tied, return the first row
        return group.iloc[0]
    
    # Apply deduplication
    print("Applying deduplication logic...")
    
    # Separate unique and duplicate rows
    unique_rows = merged_df[~merged_df.duplicated(subset=[id_column], keep=False)]
    duplicate_groups = merged_df[merged_df.duplicated(subset=[id_column], keep=False)]
    
    # Process duplicate groups
    deduped_rows = []
    for group_id, group in duplicate_groups.groupby(id_column):
        best_row = select_best_row(group)
        deduped_rows.append(best_row)
    
    # Combine unique rows with best duplicate rows
    if deduped_rows:
        deduped_df = pd.DataFrame(deduped_rows)
        final_df = pd.concat([unique_rows, deduped_df], ignore_index=True)
    else:
        final_df = unique_rows
    
    print(f"Final dataset: {final_df.shape[0]} rows, {final_df.shape[1]} columns")
    print(f"Removed {merged_df.shape[0] - final_df.shape[0]} duplicate rows")
    
    # Save to file
    final_df.to_csv(output_path, index=False)
    print(f"Merged data saved to {output_path}")
    
    return final_df


if __name__ == "__main__":

    # Set up command line argument parsing
    parser = argparse.ArgumentParser(
        description='Merge CSV files vertically with deduplication',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python merge_csvs.py file1.csv file2.csv file3.csv
  python merge_csvs.py *.csv --id-column user_id --output merged.csv
  python merge_csvs.py data/*.csv --id-column video_id --output final_data.csv
        """
    )
    
    parser.add_argument('files', nargs='+', help='CSV files to merge')
    parser.add_argument('--id-column', '-i', default='video_id', help='Column name for deduplication (default: video_id)')
    parser.add_argument('--output', '-o', default='merged_data.csv', help='Output file name (default: merged_data.csv)')
    parser.add_argument('--no-dedup', action='store_true', help='Skip deduplication step')
    args = parser.parse_args()
    
    # Validate input files
    import os
    valid_files = []
    for file_path in args.files:
        if os.path.exists(file_path):
            valid_files.append(file_path)
        else:
            print(f"Warning: File not found: {file_path}")
    
    if not valid_files:
        print("Error: No valid input files found!")
        sys.exit(1)
    
    print(f"Merging {len(valid_files)} files:")
    for f in valid_files:
        print(f"  - {f}")
    print(f"ID column: {args.id_column}")
    print(f"Output: {args.output}")
    print()
    
    # Merge the files
    if args.no_dedup:
        # Simple concatenation without deduplication
        dfs = []
        for file_path in valid_files:
            df = pd.read_csv(file_path)
            print(f"Loaded {file_path}: {df.shape[0]} rows, {df.shape[1]} columns")
            dfs.append(df)
        
        merged_data = pd.concat(dfs, ignore_index=True, sort=False)
        merged_data.to_csv(args.output, index=False)
        print(f"Merged {len(merged_data)} rows to {args.output} (no deduplication)")
    else:
        merged_data = merge_csv_files(valid_files, args.id_column, args.output)
    
    # Display summary statistics
    print("\n" + "="*50)
    print("MERGE SUMMARY")
    print("="*50)
    print(f"Total rows: {len(merged_data)}")
    print(f"Total columns: {len(merged_data.columns)}")
    if args.id_column in merged_data.columns:
        print(f"Unique {args.id_column}s: {merged_data[args.id_column].nunique()}")
    
    # Show column info
    print("\nColumn Information:")
    print(f"{'Column Name':<30} {'Non-null Count':<15} {'Data Type'}")
    print("-" * 60)
    for col in merged_data.columns:
        non_null_count = merged_data[col].notna().sum()
        dtype = str(merged_data[col].dtype)
        print(f"{col:<30} {non_null_count:<15} {dtype}")
    
    # Show sample of merged data
    print("\nSample of merged data:")
    print(merged_data.head())