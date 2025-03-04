import argparse
import os
from src.data_parse import DataParser
from pathlib import Path


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('runtime_template', type=str, help='list of log directories')
    parser.add_argument('--subgraphs-only', action='store_true', help='had previously parsed the debug file, only generate subgraphs')
    args = parser.parse_args()

    runtime_template = Path(args.runtime_template).name
    runtime_template = os.path.join(os.getcwd(), 'logs', runtime_template)

    print(f"=== Generating data for {runtime_template}")
    data_parser = DataParser(runtime_template)

    if args.subgraphs_only:
        data_parser.parse_logs()
        data_parser.generate_subgraphs()
    else:
        data_parser.parse_logs()
        data_parser.generate_subgraphs()

