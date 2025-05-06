import argparse
import os
from pathlib import Path
from src.data_parse import DataParser


def remove_duplicates_preserve_order(seq):
    seen = set()
    result = []
    for item in seq:
        name = Path(item).name
        if name not in seen:
            seen.add(name)
            result.append(name)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('runtime_templates', type=str, nargs='+',
                        help='list of log directories (e.g., log1 log2 log3)')
    parser.add_argument('--subgraphs-only', action='store_true',
                        help='had previously parsed the debug file, only generate subgraphs')
    args = parser.parse_args()

    deduped_names = remove_duplicates_preserve_order(args.runtime_templates)

    root_dir = os.path.join(os.getcwd(), 'logs')
    full_paths = [os.path.join(root_dir, name) for name in deduped_names]

    missing = [p for p in full_paths if not os.path.exists(p)]
    if missing:
        print("❌ The following log directories do not exist:")
        for m in missing:
            print(f"  - {m}")
        exit(1)

    print("✅ The following log directories will be processed:")
    for path in full_paths:
        print(f"  - {path}")

    for runtime_template in full_paths:
        print(f"\n=== Start parsing: {runtime_template}")
        data_parser = DataParser(runtime_template)
        if args.subgraphs_only:
            data_parser.generate_subgraphs()
        else:
            data_parser.parse_logs()
            data_parser.generate_subgraphs()
