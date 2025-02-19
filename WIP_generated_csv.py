import argparse
import os
import time
from data_parse import DataParser
from data_process import DataProcessor


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('runtime_template', type=str, help='list of log directories')
    parser.add_argument('--execution-details-only', action='store_true', help='Only generate data for task execution details')
    parser.add_argument('--meta-files', action='store_true', help='include meta files in the file_info.csv')
    parser.add_argument('--restore', action='store_true', help='restore from checkpoint')
    args = parser.parse_args()

    vine_logs_dir = os.path.join(args.runtime_template, 'vine-logs')
    csv_files_dir = os.path.join(args.runtime_template, 'csv-files')
    json_files_dir = os.path.join(args.runtime_template, 'json-files')
    pkl_files_dir = os.path.join(args.runtime_template, 'pkl-files')

    data_parser = DataParser(vine_logs_dir, pkl_files_dir)

    if not args.restore:
        data_parser.parse_logs()
        data_parser.checkpoint()
    else:
        data_parser.restore_from_checkpoint()

    workers = data_parser.workers
    files = data_parser.files
    tasks = data_parser.tasks
    manager = data_parser.manager

    data_process = DataProcessor(workers, files, tasks, manager, csv_files_dir, json_files_dir)
    data_process.generate_data()
