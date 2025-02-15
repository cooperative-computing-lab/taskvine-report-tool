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

# LOGS = ["incluster_3replicas_random", "incluster_3replicas_largest_available_disk",]
# LOGS = ["pfs_random", "incluster_3replicas_random",]
# LOGS = ["incluster_3replicas_random", "incluster_3replicas_largest_available_disk"]

# LOGS = ['incluster_3replicas_largest_available_disk', 'incluster_3replicas_replica_eviction']
# LOGS = ["prune_depth_0", "prune_depth_1", "prune_depth_2"]

LOGS = ["incluster_3replicas_random", "lif_prune_depth_1", "lif_prune_depth_2"]

# LOG_TITLES = ["In-Cluster Random", "In-Cluster Largest Available Disk"]
# LOG_TITLES = ["PFS Transfer", "In-Cluster Transfer (3 replicas)",]
# LOG_TITLES = ["incluster_3replicas_replica_eviction"]
LOG_TITLES = ["PFS Random", "lif_prune_depth_1", "lif_prune_depth_2",]

SAVE_TO = "../imgs"

TIME_WORKFLOW_START = 'when_first_task_start_commit'
TIME_WORKFLOW_END = 'when_last_task_done'

DISK_USAGE_CSV_FILES = [os.path.join(ROOT_PATH, log, 'vine-logs', 'worker_disk_usage.csv') for log in LOGS]
MANAGER_DISK_USAGE_CSV_FILES = [os.path.join(ROOT_PATH, log, 'vine-logs', 'manager_disk_usage.csv') for log in LOGS]
FILE_INFO_CSV_FILES = [os.path.join(ROOT_PATH, log, 'vine-logs', 'file_info.csv') for log in LOGS]
MANAGER_INFO_CSV_FILES = [os.path.join(ROOT_PATH, log, 'vine-logs', 'manager_info.csv') for log in LOGS]
TASK_DONE_CSV_FILES = [os.path.join(ROOT_PATH, log, 'vine-logs', 'task_done.csv') for log in LOGS]
TASK_CSV_FILES = [os.path.join(ROOT_PATH, log, 'vine-logs', 'task.csv') for log in LOGS]
WORKER_SUBMARY_CSV_FILES = [os.path.join(ROOT_PATH, log, 'vine-logs', 'worker_summary.csv') for log in LOGS]

# change this if you want to set the title of the logs
