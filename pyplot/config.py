import os
import math
import argparse
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import ast
import numpy as np
from matplotlib.gridspec import GridSpec
from adjustText import adjust_text


PLOT_SETTINGS = {
    "grid_alpha": 0.8,
    "grid_linewidth": 0.3,

    "subplot_width": 8,
    "subplot_height": 6,

    "title_fontsize": 18,
    "label_fontsize": 18,
    "annotate_fontsize": 18,
    "tick_fontsize": 14,

    "worker_disk_usage_line_width": 1.2,
    "individual_disk_usage_line_width": 0.6,
    "plot_alpha": 0.8,
    "color_secondary": "g",
    "wspace": 0,
    "yticks_count": 5,
    "xticks_count": 5,

    "line_color": "#3b7397",
    "dot_color": "#9cc2db",
    "dot_edgecolor": "#3d80ad",

    "dot_size": 20,

    "group_spacing": 1.5,
    "inner_spacing": 0.02,
    "bar_width": 0.4,
}

ROOT_PATH = '/Users/jinzhou/applications/taskvine-report-tool/logs'
LOGS = ['bfs+0rep+prune', 'lif+3rep+prune', 'lif+5rep+prune']

SAVE_TO = "/Users/jinzhou/Downloads"

DISK_USAGE_CSV_FILES = [os.path.join(ROOT_PATH, log, 'vine-logs', 'worker_disk_usage.csv') for log in LOGS]
FILE_INFO_CSV_FILES = [os.path.join(ROOT_PATH, log, 'vine-logs', 'file_info.csv') for log in LOGS]
MANAGER_INFO_CSV_FILES = [os.path.join(ROOT_PATH, log, 'vine-logs', 'manager_info.csv') for log in LOGS]

# change this if you want to set the title of the logs
LOG_TITLES = LOGS


def get_adjusted_max(actual_max, step=200):
    if actual_max < 0:
        raise ValueError("actual_max must be non-negative.")
    if step <= 0:
        raise ValueError("step must be a positive integer.")
    
    return math.ceil(actual_max / step) * step