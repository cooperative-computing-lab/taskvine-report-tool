#!/usr/bin/env python3
"""
vine_parse command - Parse TaskVine execution logs

This command parses TaskVine execution logs and generates analysis data.
"""

import argparse
import os
import sys
import fnmatch
import hashlib
import re
import shutil
import traceback as tb
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from src.vine_parse.data_parser import DataParser
from src.vine_parse.csv_manager import CSVManager
from src.utils import check_pip_updates
from src import __version__


def remove_duplicates_preserve_order(seq):
    seen = set()
    result = []
    for item in seq:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def normalize_template_pattern(raw_pattern):
    """Normalize template input to rightmost name-only pattern."""
    cleaned = str(raw_pattern).strip().strip("'\"")
    normalized_path = os.path.normpath(cleaned.rstrip("/"))
    return os.path.basename(normalized_path)


def find_matching_directories(root_dir, patterns):
    try:
        all_dirs = [d for d in os.listdir(root_dir) 
                   if os.path.isdir(os.path.join(root_dir, d))]
        
        matched_dirs = []
        for pattern in patterns:
            # Always match by rightmost name under --logs-dir only.
            cleaned_pattern = normalize_template_pattern(pattern)
            
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


def find_valid_dirs(root_dir: str):
    root = Path(root_dir)
    results = []

    for path in root.rglob("*"):
        if path.is_dir():
            vine_logs = path / "vine-logs"
            if vine_logs.is_dir():
                if (vine_logs / "debug").is_file():
                    results.append(str(path))
    return results


def debug_file_run_name(path):
    path = Path(path).resolve()
    if path.name != "debug":
        return sanitize_run_name(path.stem)

    timestamp_pid_re = re.compile(
        r"^(\d{4})/(\d{2})/(\d{2})\s+(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?\s+\S+\[(\d+)\]"
    )
    manager_timezone_re = re.compile(
        r"\[(\d+)\].*manager timezone at startup: local_time=(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})"
    )
    digest = hashlib.sha1()
    first_timestamp_name = None
    with open(path, "rb") as file:
        for i, raw_line in enumerate(file):
            if i < 256:
                digest.update(raw_line)
            try:
                line = raw_line.decode("utf-8", errors="ignore")
            except Exception:
                continue
            match = manager_timezone_re.search(line)
            if match:
                pid, year, month, day, hour, minute, second = match.groups()
                return f"taskvine-debug-{year}{month}{day}-{hour}{minute}{second}-{pid}"
            match = timestamp_pid_re.match(line)
            if match and first_timestamp_name is None:
                year, month, day, hour, minute, second, pid = match.groups()
                first_timestamp_name = f"taskvine-debug-{year}{month}{day}-{hour}{minute}{second}-{pid}"
            if i >= 255:
                break

    if first_timestamp_name:
        return first_timestamp_name
    return f"taskvine-debug-{digest.hexdigest()[:12]}"


def sanitize_run_name(name):
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", name).strip(".-")
    return cleaned or "taskvine-debug"


def prepare_debug_file_template(debug_file, logs_dir):
    base = Path(logs_dir).resolve()
    base.mkdir(parents=True, exist_ok=True)

    source = Path(debug_file).expanduser().resolve()
    if not source.is_file():
        raise ValueError(f"debug file does not exist: {source}")

    run_dir = base / debug_file_run_name(source)
    vine_logs_dir = run_dir / "vine-logs"
    vine_logs_dir.mkdir(parents=True, exist_ok=True)
    target = vine_logs_dir / "debug"

    if target.exists() or target.is_symlink():
        target.unlink()

    try:
        target.symlink_to(source)
    except OSError:
        shutil.copy2(source, target)

    return str(run_dir)


def main():
    parser = argparse.ArgumentParser(
        prog='vine_parse',
        description='Parse TaskVine execution logs and generate analysis data'
    )

    parser.add_argument(
        '--logs-dir',
        default=None,
        help='Base directory containing log folders (default: current directory)'
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--templates', 
        type=str, 
        nargs='+',
        help='List of log directory names/patterns (matched under --logs-dir). '
             'Each item is normalized to its rightmost name. '
             'Use quotes to avoid shell pre-expansion, e.g. --templates "exp*" "test*" "checkpoint_*".'
    )

    parser.add_argument(
        '--checkpoint-pkl-files',
        action='store_true',
        help='Checkpoint pkl files'
    )

    parser.add_argument(
        '--load-pkl-files',
        action='store_true',
        help='Load pkl files'
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode'
    )

    group.add_argument(
        '-R', '--recursive',
        action='store_true',
        help='Enable recursive mode'
    )

    group.add_argument(
        '--debug-file',
        type=str,
        help='Standalone debug file. vine_parse will create a minimal run folder in the current directory.'
    )

    parser.add_argument(
        '-v', '--version',
        action='version',
        version=f'%(prog)s {__version__}'
    )

    parser.add_argument(
        '--downsampling',
        type=int,
        default=1,
        help='Enable downsampling (default: 1)'
    )

    parser.add_argument(
        '--downsample-task-count',
        type=int,
        default=100000,
        help='Downsample tasks (default: 100000)'
    )

    parser.add_argument(
        '--downsample-point-count',
        type=int,
        default=10000,
        help='Downsample point count (default: 10000)'
    )

    args = parser.parse_args()

    check_pip_updates()

    if args.debug_file and args.logs_dir:
        print("❌ Use either --debug-file or --logs-dir, not both. For --debug-file, cd to the output directory first.")
        sys.exit(1)

    root_dir = os.path.abspath(args.logs_dir or os.getcwd())

    if args.debug_file:
        try:
            full_paths = [prepare_debug_file_template(args.debug_file, root_dir)]
        except Exception as e:
            print(f"❌ Error preparing debug file: {e}")
            sys.exit(1)
    elif args.recursive:
        full_paths = find_valid_dirs(root_dir)
    else:
        matched_dirs = find_matching_directories(root_dir, args.templates)
        deduped_names = remove_duplicates_preserve_order(matched_dirs)
        full_paths = [os.path.join(root_dir, name) for name in deduped_names]

    # resolve symlinks and deduplicate again after resolution
    full_paths = [str(Path(p).resolve()) for p in full_paths]
    full_paths = remove_duplicates_preserve_order(full_paths)

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
    success = 0
    failed = 0

    for template in full_paths:
        print(f"\n=== Start parsing: {template}")
        try:
            data_parser = DataParser(template, debug_mode=args.debug, 
                                     enablee_checkpoint_pkl_files=args.checkpoint_pkl_files, 
                                    )
            if args.load_pkl_files:
                data_parser.load_pkl_files()
            else:
                data_parser.parse_logs()

            csv_manager = CSVManager(template,
                                     data_parser=data_parser,
                                     downsampling=args.downsampling > 0,
                                     downsample_task_count=args.downsample_task_count,
                                     downsample_point_count=args.downsample_point_count)
            csv_manager.generate_csv_files()
            success += 1
            print(f"✅ Successfully processed: {template}")
        except Exception as e:
            print(f"❌ Error processing {template}")
            failed += 1
            print(tb.format_exc())

    if success > 0:
        print(f"\n🎉 {success} log {'directory' if success == 1 else 'directories'} processed successfully!")
    if failed > 0:
        print(f"❌ {failed} log {'directory' if failed == 1 else 'directories'} failed to process")


if __name__ == '__main__':
    main()
