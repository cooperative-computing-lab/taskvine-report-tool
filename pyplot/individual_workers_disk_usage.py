from config import *
import os
import pandas as pd
import argparse
import numpy as np
import math
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from matplotlib.ticker import FuncFormatter


def get_adjusted_max(actual_max, step=200):
    if actual_max < 0:
        raise ValueError("actual_max must be non-negative.")
    if step <= 0:
        raise ValueError("step must be a positive integer.")
    
    return math.ceil(actual_max / step) * step


def plot_individual_worker_disk_usage(add_peak_line_and_text=True, show=True):
    num_logs = len(LOGS)

    fig_height = PLOT_SETTINGS["subplot_height"]
    fig_width = PLOT_SETTINGS["subplot_width"] * num_logs

    fig = plt.figure(figsize=(fig_width, fig_height))
    gs = GridSpec(1, num_logs, figure=fig, wspace=0.3, left=0.15, right=0.9, top=0.9, bottom=0.1)
    axes = []

    y_max_global_mb = 0
    for csv_file in DISK_USAGE_CSV_FILES:
        if not os.path.exists(csv_file):
            print(f"File {csv_file} does not exist.")
            continue
        df = pd.read_csv(csv_file)
        y_max_global_mb = max(y_max_global_mb, np.ceil(df['disk_usage(MB)'].max()))

    # y_max_global_mb = get_adjusted_max(y_max_global_mb, step=100)
    y_max_global_gb = y_max_global_mb / 1024 * 1.1

    for i, csv_file in enumerate(DISK_USAGE_CSV_FILES):
        if not os.path.exists(csv_file):
            continue

        df = pd.read_csv(csv_file)
        df['worker_id'] = df['worker_id'].astype(str)
        df['adjusted_time'] = df['when_stage_in_or_out'] - df['when_stage_in_or_out'].min()

        ax = fig.add_subplot(gs[0, i])
        axes.append(ax)

        for worker_id, group in df.groupby('worker_id'):
            ax.plot(group['adjusted_time'], group['disk_usage(MB)'] / 1024,
                    linewidth=PLOT_SETTINGS["individual_disk_usage_line_width"],
                    alpha=PLOT_SETTINGS["plot_alpha"])

        y_max_disk_usage_mb = df['disk_usage(MB)'].max()
        y_max_disk_usage_gb = y_max_disk_usage_mb / 1024
        x_max_disk_usage_time = df.loc[df['disk_usage(MB)'].idxmax(), 'adjusted_time']

        if add_peak_line_and_text:
            ax.axhline(y=y_max_disk_usage_gb, color='red', linestyle='--', linewidth=1)
            ax.annotate(f'Peak: {y_max_disk_usage_gb:.2f} GB',
                        xy=(x_max_disk_usage_time, y_max_disk_usage_gb),
                        xytext=(20, 10), textcoords='offset points',
                        arrowprops=dict(facecolor='black', arrowstyle='->', lw=0.5),
                        color='red', fontsize=PLOT_SETTINGS['annotate_fontsize'], ha='center', va='bottom')

        ax.set_title(LOG_TITLES[i], fontsize=PLOT_SETTINGS["title_fontsize"])
        ax.set_xlabel('Time (s)', fontsize=PLOT_SETTINGS["label_fontsize"])
        ax.set_ylabel('WSC (GB)', fontsize=PLOT_SETTINGS["label_fontsize"])
        ax.set_ylim(0, y_max_global_gb)
        ax.set_yticks(np.linspace(0, y_max_global_gb, PLOT_SETTINGS["yticks_count"]))
        ax.set_xlim(0, df['adjusted_time'].max() + 200)
        ax.set_xticks(np.linspace(0, df['adjusted_time'].max() + 200, 4))
        ax.grid(visible=True, linestyle='--', linewidth=0.3, alpha=PLOT_SETTINGS["grid_alpha"])
        ax.tick_params(axis='both', labelsize=PLOT_SETTINGS["tick_fontsize"])

        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:.2f}'))
    
    plt.savefig(os.path.join(SAVE_TO, 'WSC.png'), bbox_inches='tight')

    if show:
        plt.tight_layout()
        plt.show()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-peak', action='store_true', help="Add peak line and text annotations.")
    args = parser.parse_args()

    plot_individual_worker_disk_usage(add_peak_line_and_text=not args.no_peak, show=True)
