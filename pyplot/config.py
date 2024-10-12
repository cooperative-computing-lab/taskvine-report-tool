import os


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
    "FRR_line_width": 0.5,
    "individual_disk_usage_line_width": 0.6,
    "plot_alpha": 0.8,
    "color_primary": "#3b7397",
    "color_secondary": "g",
    "wspace": 0,
    "yticks_count": 5,
    "xticks_count": 5,

    "group_spacing": 1.5,
    "inner_spacing": 0.02,
    "bar_width": 0.4,
}

ROOT_PATH = '/Users/jinzhou/applications/taskvine-report-tool/logs'
LOGS = ['bfs+0rep+prune','bfs+0rep+prune2','dfs+sharedfs+0rep','lif+0rep+prune_2','lif+0rep+prune_2']

SAVE_TO = "/Users/jinzhou/Downloads"

DISK_USAGE_CSV_FILES = [os.path.join(ROOT_PATH, log, 'vine-logs', 'worker_disk_usage.csv') for log in LOGS]
FILE_INFO_CSV_FILES = [os.path.join(ROOT_PATH, log, 'vine-logs', 'file_info.csv') for log in LOGS]

LOG_TITLES = LOGS