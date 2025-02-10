import os
import math
import argparse
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import ast
import numpy as np
from matplotlib.gridspec import GridSpec
from adjustText import adjust_text
from matplotlib.ticker import FuncFormatter

PLOT_SETTINGS = {
    "grid_alpha": 0.8,
    "grid_linewidth": 0.3,

    "subplot_width": 8,
    "subplot_height": 6,

    "title_fontsize": 18,
    "label_fontsize": 18,
    "annotate_fontsize": 18,
    "tick_fontsize": 14,
    "legend_fontsize": 14,

    "worker_disk_usage_line_width": 1.2,
    "individual_disk_usage_line_width": 0.6,
    "plot_alpha": 0.7,
    "color_secondary": "g",
    "yticks_count": 5,
    "xticks_count": 5,

    "line_color": "#3b7397",
    "bar_color": "#3b7397",
    "dot_color": "#9cc2db",
    "dot_edgecolor": "#3d80ad",

    "dot_size": 20,

    "group_spacing": 1.5,
    "inner_spacing": 0.02,
    "bar_width": 0.4,
}

ROOT_PATH = '../logs'
# LOGS = ['prefer_file_replication', 'prefer_task_dispatch', 'no_prune']
#LOGS = ['5rep_120s_pruning', '5rep_120s_nopruning']

#LOGS = ['small_original', 'small_unlimited_1', 'small_unlimited_2', 'small_unlimited_3', 'small_unlimited_all']
#LOG_TITLES = ['Original', 'unlimited_1', 'unlimited_2', 'unlimited_3', 'unlimited_all']

# LOGS = ['1w16c', '2w16c', '3w16c', '4w16c', '8w16c']
LOGS = ['worker_transfer_0', "worker_transfer_1"]
# LOG_TITLES = ['big_unlimited_1', 'big_unlimited_2', 'big_unlimited_3', 'unlimited_all']
LOG_TITLES = LOGS

SAVE_TO = "../imgs"

TIME_WORKFLOW_START = 'when_first_task_start_commit'
TIME_WORKFLOW_END = 'when_last_task_done'

DISK_USAGE_CSV_FILES = [os.path.join(ROOT_PATH, log, 'vine-logs', 'worker_disk_usage.csv') for log in LOGS]
FILE_INFO_CSV_FILES = [os.path.join(ROOT_PATH, log, 'vine-logs', 'file_info.csv') for log in LOGS]
MANAGER_INFO_CSV_FILES = [os.path.join(ROOT_PATH, log, 'vine-logs', 'manager_info.csv') for log in LOGS]
TASK_DONE_CSV_FILES = [os.path.join(ROOT_PATH, log, 'vine-logs', 'task_done.csv') for log in LOGS]
TASK_CSV_FILES = [os.path.join(ROOT_PATH, log, 'vine-logs', 'task.csv') for log in LOGS]
WORKER_SUBMARY_CSV_FILES = [os.path.join(ROOT_PATH, log, 'vine-logs', 'worker_summary.csv') for log in LOGS]
# change this if you want to set the title of the logs


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
        df['adjusted_time'] = df['when_stage_in_or_out'] - df['when_stage_in_or_out'].min()
        df = df.sort_values(by='adjusted_time')
        df['accumulated_disk_usage_mb'] = df['size(MB)'].cumsum()
        max_disk_usage_global_mb = max(max_disk_usage_global_mb, df['accumulated_disk_usage_mb'].max())

    return max_disk_usage_global_mb / 1024

def get_worker_max_disk_usage_gb():
    max_worker_disk_usage_mb = 0

    for csv_file in DISK_USAGE_CSV_FILES:
        if not os.path.exists(csv_file):
            continue
        df = pd.read_csv(csv_file)
        max_worker_disk_usage_mb = max(max_worker_disk_usage_mb, df['disk_usage(MB)'].max())

    return max_worker_disk_usage_mb / 1024

def get_global_max_file_count():
    max_file_count_global = 0

    for csv_file in DISK_USAGE_CSV_FILES:
        if not os.path.exists(csv_file):
            continue
        df = pd.read_csv(csv_file)
        df['adjusted_time'] = df['when_stage_in_or_out'] - df['when_stage_in_or_out'].min()
        df = df.sort_values(by='adjusted_time')
        df['file_count'] = df['size(MB)'].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0)).cumsum()
        max_file_count_global = max(max_file_count_global, df['file_count'].max())

    return max_file_count_global

def get_worker_max_file_count():
    max_file_count = 0

    for csv_file in DISK_USAGE_CSV_FILES:
        if not os.path.exists(csv_file):
            continue
        df = pd.read_csv(csv_file)
        df['file_count'] = df['size(MB)'].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0)).cumsum()
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

WORKFLOW_TIME_SCALES = [get_workflow_time_scale(MANAGER_INFO_CSV_FILE) for MANAGER_INFO_CSV_FILE in MANAGER_INFO_CSV_FILES]

def get_global_max_execution_time():
    global_max_execution_time = 0

    for MANAGER_INFO_CSV_FILE in MANAGER_INFO_CSV_FILES:
        min_time, max_time = get_workflow_time_scale(MANAGER_INFO_CSV_FILE)
        global_max_execution_time = max(max_time - min_time, global_max_execution_time)

    return global_max_execution_time