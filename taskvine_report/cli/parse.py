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
except ImportError:
    # Handle direct execution
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
    from taskvine_report.src.data_parse import DataParser


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
        'runtime_templates', 
        type=str, 
        nargs='+',
        help='List of log directories (e.g., log1 log2 log3)'
    )
    
    parser.add_argument(
        '--subgraphs-only', 
        action='store_true',
        help='Only generate subgraphs (assumes debug file was previously parsed)'
    )
    
    parser.add_argument(
        '--logs-dir',
        default='logs',
        help='Base directory containing log folders (default: logs)'
    )
    
    args = parser.parse_args()

    # Remove duplicates while preserving order
    deduped_names = remove_duplicates_preserve_order(args.runtime_templates)

    # Construct full paths
    root_dir = os.path.abspath(args.logs_dir)
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
    for runtime_template in full_paths:
        print(f"\n=== Start parsing: {runtime_template}")
        try:
            data_parser = DataParser(runtime_template)
            if args.subgraphs_only:
                data_parser.generate_subgraphs()
            else:
                data_parser.parse_logs()
                data_parser.generate_subgraphs()
            print(f"✅ Successfully processed: {runtime_template}")
        except Exception as e:
            print(f"❌ Error processing {runtime_template}: {e}")
            sys.exit(1)

    print("\n🎉 All log directories processed successfully!")


if __name__ == '__main__':
    main() 