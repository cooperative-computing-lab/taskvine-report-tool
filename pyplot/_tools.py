from _config import *


def get_adjusted_max(actual_max, step=200):
    if actual_max < 0:
        raise ValueError("actual_max must be non-negative.")
    if step <= 0:
        raise ValueError("step must be a positive integer.")

    return math.ceil(actual_max / step) * step


def get_global_max_disk_usage_gb():
    max_disk_usage_global_mb = 0

    for csv_file in DISK_USAGE_CSV_FILES:
        if not os.path.exists(csv_file):
            continue
        df = pd.read_csv(csv_file)
        df['adjusted_time'] = df['when_stage_in_or_out'] - \
            df['when_stage_in_or_out'].min()
        df = df.sort_values(by='adjusted_time')
        df['accumulated_disk_usage_mb'] = df['size(MB)'].cumsum()
        max_disk_usage_global_mb = max(
            max_disk_usage_global_mb, df['accumulated_disk_usage_mb'].max())

    return max_disk_usage_global_mb / 1024


def get_worker_max_disk_usage_gb():
    max_worker_disk_usage_mb = 0

    for csv_file in DISK_USAGE_CSV_FILES:
        if not os.path.exists(csv_file):
            continue
        df = pd.read_csv(csv_file)
        max_worker_disk_usage_mb = max(
            max_worker_disk_usage_mb, df['disk_usage(MB)'].max())

    return max_worker_disk_usage_mb / 1024


def get_global_max_file_count():
    max_file_count_global = 0

    for csv_file in DISK_USAGE_CSV_FILES:
        if not os.path.exists(csv_file):
            continue
        df = pd.read_csv(csv_file)
        df['adjusted_time'] = df['when_stage_in_or_out'] - \
            df['when_stage_in_or_out'].min()
        df = df.sort_values(by='adjusted_time')
        df['file_count'] = df['size(MB)'].apply(
            lambda x: 1 if x > 0 else (-1 if x < 0 else 0)).cumsum()
        max_file_count_global = max(
            max_file_count_global, df['file_count'].max())

    return max_file_count_global


def get_worker_max_file_count():
    max_file_count = 0

    for csv_file in DISK_USAGE_CSV_FILES:
        if not os.path.exists(csv_file):
            continue
        df = pd.read_csv(csv_file)
        df['file_count'] = df['size(MB)'].apply(
            lambda x: 1 if x > 0 else (-1 if x < 0 else 0)).cumsum()
        max_file_count = max(max_file_count, df['file_count'].max())

    return max_file_count


def get_workflow_time_scale(MANAGER_INFO_CSV_FILE):
    min_time = 0
    max_time = 0

    if not os.path.exists(MANAGER_INFO_CSV_FILE):
        return min_time, max_time

    min_time = pd.read_csv(MANAGER_INFO_CSV_FILE)[TIME_WORKFLOW_START].min()
    max_time = pd.read_csv(MANAGER_INFO_CSV_FILE)[TIME_WORKFLOW_END].max()

    return (min_time, max_time)


WORKFLOW_TIME_SCALES = [get_workflow_time_scale(
    MANAGER_INFO_CSV_FILE) for MANAGER_INFO_CSV_FILE in MANAGER_INFO_CSV_FILES]


def get_global_max_execution_time():
    global_max_execution_time = 0

    for MANAGER_INFO_CSV_FILE in MANAGER_INFO_CSV_FILES:
        min_time, max_time = get_workflow_time_scale(MANAGER_INFO_CSV_FILE)
        global_max_execution_time = max(
            max_time - min_time, global_max_execution_time)

    return global_max_execution_time


def get_global_max_manager_disk_usage_gb():
    global_max_manager_disk_usage_mb = 0

    for MANAGER_DISK_USAGE_CSV_FILE in MANAGER_DISK_USAGE_CSV_FILES:
        if not os.path.exists(MANAGER_DISK_USAGE_CSV_FILE):
            continue

        df = pd.read_csv(MANAGER_DISK_USAGE_CSV_FILE)
        global_max_manager_disk_usage_mb = max(
            global_max_manager_disk_usage_mb, df['accumulated_disk_usage(MB)'].max())

    return global_max_manager_disk_usage_mb / 1024
