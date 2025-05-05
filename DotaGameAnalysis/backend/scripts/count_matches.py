#!/usr/bin/env python
import os
import json
import glob
from datetime import datetime

def count_matches():
    # Path to matches directory
    matches_dir = os.path.join('data', 'raw', 'matches')
    
    # Get all match json files
    match_files = glob.glob(os.path.join(matches_dir, 'match_*.json'))
    
    # Count the number of matches
    match_count = len(match_files)
    
    if match_count == 0:
        print("No matches found.")
        return
    
    # Initialize variables to track earliest and latest timestamps
    earliest_timestamp = float('inf')
    latest_timestamp = 0
    
    # Process each match file to find timestamps
    for match_file in match_files:
        try:
            with open(match_file, 'r') as f:
                match_data = json.load(f)
                
                # Extract start_time from the match data
                if 'start_time' in match_data:
                    timestamp = match_data['start_time']
                    
                    if timestamp < earliest_timestamp:
                        earliest_timestamp = timestamp
                    
                    if timestamp > latest_timestamp:
                        latest_timestamp = timestamp
        except json.JSONDecodeError:
            # Skip files with invalid JSON
            continue
        except Exception as e:
            # Skip files with other errors
            continue
    
    # Convert timestamps to human-readable dates
    earliest_date = datetime.fromtimestamp(earliest_timestamp).strftime('%Y-%m-%d')
    latest_date = datetime.fromtimestamp(latest_timestamp).strftime('%Y-%m-%d')
    
    # Print the results
    print(f"{match_count}")
    print(f"{earliest_date}")
    print(f"{latest_date}")

if __name__ == "__main__":
    count_matches()
