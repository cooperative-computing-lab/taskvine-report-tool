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
LOGS = ['t1']
# LOG_TITLES = ['big_unlimited_1', 'big_unlimited_2', 'big_unlimited_3', 'unlimited_all']
LOG_TITLES = LOGS

SAVE_TO = "../imgs"

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


