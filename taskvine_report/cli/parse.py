#!/usr/bin/env python3
"""
vine_parse command - Parse TaskVine execution logs

This command parses TaskVine execution logs and generates analysis data.
"""

import argparse
import os
import sys
import fnmatch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from taskvine_report.src.data_parser import DataParser
from taskvine_report import __version__


def remove_duplicates_preserve_order(seq):
    seen = set()
    result = []
    for item in seq:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def find_matching_directories(root_dir, patterns):
    try:
        all_dirs = [d for d in os.listdir(root_dir) 
                   if os.path.isdir(os.path.join(root_dir, d))]
        
        matched_dirs = []
        for pattern in patterns:
            # strip trailing slashes and get basename
            pattern = pattern.rstrip('/')
            pattern = os.path.basename(pattern)
            
            # remove quotes if user accidentally included them
            cleaned_pattern = pattern.strip('\'"')
            
            # check for glob pattern matching
            pattern_matches = [d for d in all_dirs if fnmatch.fnmatch(d, cleaned_pattern)]
            
            if pattern_matches:
                matched_dirs.extend(pattern_matches)
            else:
                print(f"⚠️  Pattern '{cleaned_pattern}' matched no directories")
        
        if not matched_dirs:
            print(f"❌ No directories matched any of the provided patterns in {root_dir}")
            sys.exit(1)
            
        return matched_dirs
        
    except Exception as e:
        print(f"❌ Error scanning directory {root_dir}: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        prog='vine_parse',
        description='Parse TaskVine execution logs and generate analysis data'
    )

    parser.add_argument(
        '--logs-dir',
        default=os.getcwd(),
        help='Base directory containing log folders (default: current directory)'
    )
    
    parser.add_argument(
        '--templates', 
        type=str, 
        nargs='+',
        required=True,
        help='List of log directory names/patterns. Use shell glob expansion without quotes: '
             '--templates exp* test* checkpoint_*. Quotes will be automatically removed if provided. Required.'
    )

    parser.add_argument(
        '-v', '--version',
        action='version',
        version=f'%(prog)s {__version__}'
    )
    
    args = parser.parse_args()

    root_dir = os.path.abspath(args.logs_dir)

    # find directories matching the regex patterns
    matched_dirs = find_matching_directories(root_dir, args.templates)
    
    # remove duplicates while preserving order
    deduped_names = remove_duplicates_preserve_order(matched_dirs)

    # construct full paths
    full_paths = [os.path.join(root_dir, name) for name in deduped_names]

    # check if all directories exist and have vine-logs subdirectory
    missing = []
    no_vine_logs = []
    for path in full_paths:
        if not os.path.exists(path):
            missing.append(path)
        elif not os.path.exists(os.path.join(path, 'vine-logs')):
            no_vine_logs.append(path)
    
    if missing:
        print("❌ The following directories do not exist:")
        for m in missing:
            print(f"  - {m}")
        sys.exit(1)
    
    if no_vine_logs:
        print("⚠️  The following directories do not contain 'vine-logs' subdirectory:")
        for m in no_vine_logs:
            print(f"  - {m}")
        # filter out directories without vine-logs
        full_paths = [p for p in full_paths if p not in no_vine_logs]
        
    if not full_paths:
        print("❌ No valid log directories found to process")
        sys.exit(1)

    print(f"\n✅ The following {len(full_paths)} log directories will be processed:")
    for path in full_paths:
        print(f"  - {path}")

    # process each directory
    for template in full_paths:
        print(f"\n=== Start parsing: {template}")
        try:
            data_parser = DataParser(template)
            data_parser.parse_logs()
            data_parser.generate_subgraphs()
            data_parser.generate_csv_files()
            print(f"✅ Successfully processed: {template}")
        except Exception as e:
            print(f"❌ Error processing {template}: {e}")
            sys.exit(1)

    print("\n🎉 All log directories processed successfully!")


if __name__ == '__main__':
    main() 