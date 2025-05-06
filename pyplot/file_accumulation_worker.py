from _config import *
from _tools import *
import matplotlib.cm as cm
import matplotlib.colors as mcolors


def plot_individual_worker_disk_usage(add_peak_line_and_text=True, show=True, plot_manager=False, plot_file_count=False):
    num_logs = len(LOGS)

    fig_height = PLOT_SETTINGS["subplot_height"]
    fig_width = PLOT_SETTINGS["subplot_width"] * num_logs

    fig = plt.figure(figsize=(fig_width, fig_height))
    gs = GridSpec(1, num_logs, figure=fig, wspace=0.4,
                  left=0.15, right=0.9, top=0.9, bottom=0.1)
    axes = []

    global_max_disk_usage_gb = get_worker_max_disk_usage_gb()
    global_max_execution_time = get_global_max_execution_time()

    global_max_manager_disk_usage_gb = get_global_max_manager_disk_usage_gb()
    global_max_file_count = get_global_max_file_count()

    for i, csv_file in enumerate(DISK_USAGE_CSV_FILES):
        if not os.path.exists(csv_file):
            continue

        min_time, max_time = WORKFLOW_TIME_SCALES[i]

        df = pd.read_csv(csv_file)
        df['worker_id'] = df['worker_id'].astype(str)
        df['adjusted_time'] = df['when_stage_in_or_out'] - min_time

        ax = fig.add_subplot(gs[0, i])
        axes.append(ax)

        # Use a colormap to assign colors to each worker
        unique_workers = df['worker_id'].unique()
        # colormap = cm.get_cmap('tab20', len(unique_workers))
        colormap = plt.colormaps['tab20'].resampled(len(unique_workers))
        color_norm = mcolors.Normalize(vmin=0, vmax=len(unique_workers))

        for idx, (worker_id, group) in enumerate(df.groupby('worker_id')):
            ax.plot(group['adjusted_time'], group['disk_usage(MB)'] / 1024,
                    color=colormap(color_norm(idx)),
                    linewidth=1.5,
                    alpha=PLOT_SETTINGS["plot_alpha"])

        y_max_disk_usage_mb = df['disk_usage(MB)'].max()
        y_max_disk_usage_gb = y_max_disk_usage_mb / 1024
        x_max_disk_usage_time = df.loc[df['disk_usage(MB)'].idxmax(
        ), 'adjusted_time']

        if add_peak_line_and_text:
            ax.axhline(y=y_max_disk_usage_gb, color='red',
                       linestyle='--', linewidth=1)
            ax.annotate(f'Peak: {y_max_disk_usage_gb:.2f} GB',
                        xy=(x_max_disk_usage_time, y_max_disk_usage_gb),
                        xytext=(20, 10), textcoords='offset points',
                        arrowprops=dict(facecolor='black',
                                        arrowstyle='->', lw=0.5),
                        color='red', fontsize=PLOT_SETTINGS['annotate_fontsize'], ha='center', va='bottom')

        ax.set_title(LOG_TITLES[i], fontsize=PLOT_SETTINGS["title_fontsize"])

        ax.set_xlabel('Time (s)', fontsize=PLOT_SETTINGS["label_fontsize"])
        ax.set_xlim(0, global_max_execution_time * 1.1)

        ax.set_ylabel('Worker Storage Consumption (GB)',
                      fontsize=PLOT_SETTINGS["label_fontsize"])
        ax.set_ylim(0, global_max_disk_usage_gb * 1.1)

        ax.grid(visible=True, linestyle='--', linewidth=0.3,
                alpha=PLOT_SETTINGS["grid_alpha"])
        ax.tick_params(axis='both', labelsize=PLOT_SETTINGS["tick_fontsize"])

        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{int(x)}'))

        # Prioritize manager plot if both are set
        if plot_manager:
            manager_csv_file = MANAGER_DISK_USAGE_CSV_FILES[i]
            if os.path.exists(manager_csv_file):
                df_manager = pd.read_csv(manager_csv_file)
                df_manager['adjusted_time'] = df_manager['time_stage_in'] - min_time
                df_manager['accumulated_disk_usage_gb'] = df_manager[
                    'accumulated_disk_usage(MB)'] / 1024

                # Ensure the first point is at zero
                first_point = pd.DataFrame(
                    {'adjusted_time': [0], 'accumulated_disk_usage_gb': [0]})
                df_manager = pd.concat(
                    [first_point, df_manager], ignore_index=True)

                # Plot the manager's storage consumption
                ax2 = ax.twinx()
                ax2.plot(df_manager['adjusted_time'], df_manager['accumulated_disk_usage_gb'],
                         color='red', linestyle='--', linewidth=3, alpha=0.8, label='Manager')
                ax2.set_ylabel('Manager Storage Consumption (GB)',
                               fontsize=PLOT_SETTINGS["label_fontsize"])
                ax2.set_ylim(0, global_max_manager_disk_usage_gb * 1.1)
                ax2.tick_params(
                    axis='y', labelsize=PLOT_SETTINGS["tick_fontsize"])

                ax2.legend(loc='upper left',
                           fontsize=PLOT_SETTINGS["legend_fontsize"])
        elif plot_file_count:
            ax2 = ax.twinx()
            ax2.spines['right'].set_position(('axes', 1.1))
            ax2.set_frame_on(True)
            ax2.patch.set_visible(False)

            df['file_count'] = df['size(MB)'].apply(
                lambda x: 1 if x > 0 else (-1 if x < 0 else 0)).cumsum()
            ax2.plot(df['adjusted_time'], df['file_count'], color='green',
                     linestyle=':', linewidth=1.5, alpha=0.7, label='File Count')
            ax2.set_ylabel(
                'File Count', fontsize=PLOT_SETTINGS["label_fontsize"])
            ax2.set_ylim(0, global_max_file_count * 1.1)
            ax2.tick_params(axis='y', labelsize=PLOT_SETTINGS["tick_fontsize"])

            ax2.legend(loc='upper left',
                       fontsize=PLOT_SETTINGS["legend_fontsize"])

    plt.savefig(os.path.join(
        SAVE_TO, 'file_accumulation_worker.png'), bbox_inches='tight')

    if show:
        plt.tight_layout()
        plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-peak', action='store_true',
                        help="Add peak line and text annotations.")
    parser.add_argument('--manager', action='store_true',
                        help="Plot manager storage consumption on right y-axis.")
    parser.add_argument('--file-count', action='store_true',
                        help="Plot file count on a secondary y-axis.")
    args = parser.parse_args()

    plot_individual_worker_disk_usage(add_peak_line_and_text=not args.no_peak,
                                      show=True, plot_manager=args.manager, plot_file_count=args.file_count)
