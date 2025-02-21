import argparse
import os
from data_parse import DataParser
from pathlib import Path
from data_process import DataProcessor


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('runtime_template', type=str, help='list of log directories')
    parser.add_argument('--execution-details-only', action='store_true', help='Only generate data for task execution details')
    parser.add_argument('--meta-files', action='store_true', help='include meta files in the file_info.csv')
    parser.add_argument('--restore', action='store_true', help='restore from checkpoint')
    args = parser.parse_args()

    runtime_template = Path(args.runtime_template).name
    runtime_template = os.path.join(os.getcwd(), 'logs', runtime_template)

    print(f"=== parsing data for {runtime_template}")
    data_parser = DataParser(runtime_template)

    if not args.restore:
        data_parser.parse_logs()
        data_parser.checkpoint()

    manager, workers, files, tasks = data_parser.restore_from_checkpoint()

    # print(f"=== processing data for {runtime_template}")
    # data_processor = DataProcessor(runtime_template, manager, workers, files, tasks)
    # data_processor.generate_data()
