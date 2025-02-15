from _config import *
from _tools import *

def plot_manager_disk_usage(show=True):
    num_logs = len(MANAGER_DISK_USAGE_CSV_FILES)

    fig_height = PLOT_SETTINGS["subplot_height"]
    fig_width = PLOT_SETTINGS["subplot_width"] * num_logs

    fig = plt.figure(figsize=(fig_width, fig_height))
    gs = GridSpec(1, num_logs, figure=fig, wspace=0.3, left=0.15, right=0.9, top=0.9, bottom=0.1)
    axes = []

    global_max_execution_time = get_global_max_execution_time()

    global_max_manager_disk_usage_gb = get_global_max_manager_disk_usage_gb()

    for i, csv_file in enumerate(MANAGER_DISK_USAGE_CSV_FILES):
        if not os.path.exists(csv_file):
            print(f"CSV file {csv_file} not found.")
            continue

        df = pd.read_csv(csv_file)
        if df.empty:
            print(f"CSV file {csv_file} is empty.")
            continue

        min_time, max_time = WORKFLOW_TIME_SCALES[i]

        df['adjusted_time'] = df['time_stage_in'] - min_time
        df['accumulated_disk_usage_gb'] = df['accumulated_disk_usage(MB)'] / 1024

        ax = fig.add_subplot(gs[0, i])
        axes.append(ax)

        ax.plot(df['adjusted_time'], df['accumulated_disk_usage_gb'],
                label=os.path.basename(csv_file),
                linewidth=PLOT_SETTINGS["worker_disk_usage_line_width"],
                alpha=PLOT_SETTINGS["plot_alpha"])

        ax.set_xlabel('Time (s)', fontsize=PLOT_SETTINGS["label_fontsize"])
        ax.set_ylabel('Manager Storage Consumption (GB)', fontsize=PLOT_SETTINGS["label_fontsize"])
        ax.set_title(LOG_TITLES[i], fontsize=PLOT_SETTINGS["title_fontsize"])
        ax.grid(visible=True, linestyle='--', linewidth=0.3, alpha=PLOT_SETTINGS["grid_alpha"])
        ax.tick_params(axis='both', labelsize=PLOT_SETTINGS["tick_fontsize"])
        ax.set_xlim(0, global_max_execution_time * 1.1)
        ax.set_ylim(0, global_max_manager_disk_usage_gb * 1.1)

    plt.savefig(os.path.join(SAVE_TO, 'Manager_Disk_Usage_Comparison.png'), bbox_inches='tight')

    if show:
        plt.tight_layout()
        plt.show()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-show', action='store_true', help="Do not display the plot.")
    args = parser.parse_args()

    plot_manager_disk_usage(show=not args.no_show)
