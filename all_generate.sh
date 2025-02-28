#!/bin/bash

# This script generates analysis data for all valid log directories
# For each subdirectory in logs/:
#   - Checks if it contains vine-logs/debug file
#   - If found, processes the log data using generate_data.py
#   - Generates CSV, JSON, PKL files for visualization
#
# Generated directories for each log:
#   - csv-files:  parsed CSV data files
#   - json-files: parsed JSON data files
#   - pkl-files:  cached Python pickle files
#   - svg-files:  graph visualizations

find "logs" -mindepth 1 -maxdepth 1 -type d | sort |
while IFS= read -r subdir; do
    # Check if vine-logs directory exists and contains debug file
    if [ -d "$subdir/vine-logs" ] && [ -f "$subdir/vine-logs/debug" ]; then
        echo "=== processing $subdir..."
        python3 generate_data.py "$subdir"
    else
        echo "=== skipping $subdir (no vine-logs/debug file found)"
    fi
done
