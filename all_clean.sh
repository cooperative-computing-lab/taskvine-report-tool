#!/bin/bash

# This script cleans up all generated data files from the report tool
# It will NOT modify or delete any original log data in vine-logs directories
# It only removes the following generated directories:
#   - csv-files:  parsed CSV data files
#   - json-files: parsed JSON data files
#   - pkl-files:  cached Python pickle files
#   - svg-files:  generated subgraphs

# Define directories to clean
DIRS_TO_CLEAN=("csv-files" "json-files" "pkl-files" "svg-files")

find "logs" -mindepth 1 -maxdepth 1 -type d | sort |
while IFS= read -r subdir; do
    echo "=== cleaning $subdir..."
    for dir in "${DIRS_TO_CLEAN[@]}"; do
        if [ -d "$subdir/$dir" ]; then
            echo "    removing $subdir/$dir"
            rm -rf "$subdir/$dir"
        fi
    done
done

echo "=== clean complete"
