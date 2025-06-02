#!/usr/bin/env python3
"""
vine_parse command - Parse TaskVine execution logs

This command parses TaskVine execution logs and generates analysis data.
"""

import argparse
import os
import sys
from pathlib import Path

try:
    from ..src.data_parse import DataParser
    from .. import __version__
except ImportError:
    # Handle direct execution
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
    from taskvine_report.src.data_parse import DataParser
    from taskvine_report import __version__


def remove_duplicates_preserve_order(seq):
    """Remove duplicate items while preserving order"""
    seen = set()
    result = []
    for item in seq:
        name = Path(item).name
        if name not in seen:
            seen.add(name)
            result.append(name)
    return result


def main():
    """Main entry point for vine_parse command"""
    parser = argparse.ArgumentParser(
        prog='vine_parse',
        description='Parse TaskVine execution logs and generate analysis data'
    )
    
    parser.add_argument(
        '-v', '--version',
        action='version',
        version=f'%(prog)s {__version__}'
    )
    
    parser.add_argument(
        'templates', 
        type=str, 
        nargs='*',  # changed from '+' to '*' to make it optional
        help='List of log directories (e.g., log1 log2 log3). Required unless --all is specified.'
    )

    parser.add_argument(
        '--all', 
        action='store_true',
        help='Process all log directories found in --logs-dir'
    )

    parser.add_argument(
        '--subgraphs-only', 
        action='store_true',
        help='Only generate subgraphs (assumes debug file was previously parsed)'
    )
    
    parser.add_argument(
        '--logs-dir',
        default=os.getcwd(),
        help='Base directory containing log folders (default: current directory)'
    )
    
    args = parser.parse_args()

    # Validate arguments
    if args.all and args.templates:
        print("❌ Cannot specify both --all and templates. Choose one.")
        sys.exit(1)
    
    if not args.all and not args.templates:
        print("❌ Must specify either --all or provide templates.")
        sys.exit(1)

    root_dir = os.path.abspath(args.logs_dir)

    if args.all:
        # Find all directories in logs_dir that contain vine-logs subdirectory
        try:
            potential_dirs = [d for d in os.listdir(root_dir) 
                            if os.path.isdir(os.path.join(root_dir, d))]
            templates = []
            for d in potential_dirs:
                vine_logs_path = os.path.join(root_dir, d, 'vine-logs')
                if os.path.exists(vine_logs_path):
                    templates.append(d)
            
            if not templates:
                print(f"❌ No log directories with 'vine-logs' subdirectory found in {root_dir}")
                sys.exit(1)
                
            print(f"🔍 Found {len(templates)} log directories with 'vine-logs' subdirectory:")
            for template in sorted(templates):
                print(f"  - {template}")
            
            # Remove duplicates while preserving order
            deduped_names = remove_duplicates_preserve_order(templates)
        except Exception as e:
            print(f"❌ Error scanning directory {root_dir}: {e}")
            sys.exit(1)
    else:
        # Use provided templates
        deduped_names = remove_duplicates_preserve_order(args.templates)

    # Construct full paths
    full_paths = [os.path.join(root_dir, name) for name in deduped_names]

    # Check if all directories exist
    missing = [p for p in full_paths if not os.path.exists(p)]
    if missing:
        print("❌ The following log directories do not exist:")
        for m in missing:
            print(f"  - {m}")
        sys.exit(1)

    print("✅ The following log directories will be processed:")
    for path in full_paths:
        print(f"  - {path}")

    # Process each directory
    for template in full_paths:
        print(f"\n=== Start parsing: {template}")
        try:
            data_parser = DataParser(template)
            if args.subgraphs_only:
                data_parser.generate_subgraphs()
            else:
                data_parser.parse_logs()
                data_parser.generate_subgraphs()
            print(f"✅ Successfully processed: {template}")
        except Exception as e:
            print(f"❌ Error processing {template}: {e}")
            sys.exit(1)

    print("\n🎉 All log directories processed successfully!")


if __name__ == '__main__':
    main() 