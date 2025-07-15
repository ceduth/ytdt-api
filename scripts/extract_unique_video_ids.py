"""
Python script that reads two CSV files, compares the video_id columns, and extracts the video_id values 
from <file1.csv> that are not in <file2.csv>. The result is saved to a new CSV file.

Usage:
    Ensure both files exist and have a video_id column.
    python extract_unique_video_ids.py <file1.csv> <file2.csv> --output unique_video_ids.csv
"""
import pandas as pd
import argparse
import sys

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Extract video_id values from <file1.csv> that are not in <file2.csv>")
    parser.add_argument('scraped_errors_file', help="Path to scraped-errors.csv file")
    parser.add_argument('first_file', help="Path to the first CSV file for comparison")
    parser.add_argument('--output', default='unique_video_ids.csv', help="Output CSV file name (default: unique_video_ids.csv)")

    # Parse arguments
    args = parser.parse_args()

    try:
        # Load the CSV files
        scraped_errors = pd.read_csv(args.scraped_errors_file)
        first_file = pd.read_csv(args.first_file)

        # Check if video_id column exists in both files
        if 'video_id' not in scraped_errors.columns or 'video_id' not in first_file.columns:
            raise ValueError("One or both files are missing the 'video_id' column")

        # Extract video_id values that are in scraped-errors.csv but not in first_file.csv
        unique_video_ids = scraped_errors[~scraped_errors['video_id'].isin(first_file['video_id'])]['video_id']

        # Save the result to a new CSV file
        unique_video_ids.to_csv(args.output, index=False)
        print(f"Unique video_ids have been extracted and saved to '{args.output}'")

    except FileNotFoundError as e:
        print(f"Error: File not found - {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()