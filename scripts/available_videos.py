#!/usr/bin/env python3
# YouTube Video Availability Filter
# This script checks a list of YouTube video IDs and filters out ones that are unavailable

import requests
import json
import time
import argparse
import csv
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


def is_video_available(video_id):
    """
    Check if a YouTube video is available by querying YouTube's oEmbed API
    
    Args:
        video_id (str): The YouTube video ID to check
        
    Returns:
        tuple: (is_available, error_message)
            is_available (bool): True if the video is available, False otherwise
            error_message (str): Error message if video is unavailable, None otherwise
    """
    url = f"https://www.youtube.com/oembed?url=http://www.youtube.com/watch?v={video_id}&format=json"
    
    try:
        response = requests.get(url, timeout=10)
        # If we get a 200 OK response, the video is available
        if response.status_code == 200:
            return True, None
        else:
            return False, f"HTTP {response.status_code}"
    except requests.RequestException as e:
        # If there's an error in the request, consider the video unavailable
        return False, str(e)


def filter_videos(video_ids, max_workers=10):
    """
    Filter a list of video IDs, keeping only the available ones
    
    Args:
        video_ids (list): List of YouTube video IDs to check
        max_workers (int): Maximum number of concurrent workers
        
    Returns:
        tuple: (available_ids, unavailable_data)
            available_ids (list): List of available video IDs
            unavailable_data (list): List of tuples (video_id, error_message) for unavailable videos
    """
    available_ids = []
    unavailable_data = []
    
    print(f"Checking {len(video_ids)} YouTube videos...")
    
    # Use ThreadPoolExecutor to check multiple videos concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Map video IDs to their availability status
        futures = {executor.submit(is_video_available, video_id): video_id for video_id in video_ids}
        
        # Process results as they complete
        total_checked = 0
        for future in futures:
            video_id = futures[future]
            is_available, error_message = future.result()
            total_checked += 1
            
            # Progress update every 10 videos or on the last one
            if total_checked % 10 == 0 or total_checked == len(video_ids):
                print(f"  Progress: {total_checked}/{len(video_ids)} videos checked ({total_checked/len(video_ids)*100:.1f}%)")
            
            if is_available:
                available_ids.append(video_id)
            else:
                unavailable_data.append((video_id, error_message))
    
    return available_ids, unavailable_data


def main():
    parser = argparse.ArgumentParser(description='Filter available YouTube videos from a list of IDs')
    parser.add_argument('input_file', help='Path to file containing YouTube video IDs (one ID per line)')
    parser.add_argument('--output', '-o', default='available_videos.txt', 
                        help='Output file for available video IDs (default: available_videos.txt)')
    parser.add_argument('--unavailable', '-u', default='unavailable_videos.csv',
                        help='Output CSV file for unavailable video IDs (default: unavailable_videos.csv)')
    parser.add_argument('--workers', '-w', type=int, default=10,
                        help='Maximum number of concurrent workers (default: 10)')
    
    args = parser.parse_args()
    
    try:
        # Read video IDs from file
        with open(args.input_file, 'r') as f:
            video_ids = [line.strip() for line in f if line.strip()]
        
        start_time = time.time()
        
        # Filter videos
        available_ids, unavailable_data = filter_videos(video_ids, max_workers=args.workers)
        
        # Write available videos to output file
        with open(args.output, 'w') as f:
            f.write('\n'.join(available_ids))
        
        # Write unavailable videos to CSV file
        with open(args.unavailable, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            # Write header
            csv_writer.writerow(['video_id', 'error_message', 'check_date'])
            # Write data
            check_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for video_id, error_message in unavailable_data:
                csv_writer.writerow([video_id, error_message, check_date])
        
        elapsed_time = time.time() - start_time
        
        # Print summary
        print(f"\nResults:")
        print(f"  Total videos checked: {len(video_ids)}")
        print(f"  Available videos: {len(available_ids)} ({len(available_ids)/len(video_ids)*100:.1f}%)")
        print(f"  Unavailable videos: {len(unavailable_data)} ({len(unavailable_data)/len(video_ids)*100:.1f}%)")
        print(f"  Time taken: {elapsed_time:.2f} seconds")
        print(f"\nAvailable videos saved to: {args.output}")
        print(f"Unavailable videos saved to: {args.unavailable} (CSV format)")
        
    except FileNotFoundError:
        print(f"Error: Input file '{args.input_file}' not found.")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())