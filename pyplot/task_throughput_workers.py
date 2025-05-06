from _config import *
from _tools import *
import matplotlib.cm as cm
import matplotlib.colors as mcolors


def plot_worker_task_throughput(show=True):
    num_logs = len(LOGS)

    fig_height = PLOT_SETTINGS["subplot_height"]
    fig_width = PLOT_SETTINGS["subplot_width"] * num_logs

    fig = plt.figure(figsize=(fig_width, fig_height))
    gs = GridSpec(1, num_logs, figure=fig, wspace=0.4,
                  left=0.15, right=0.9, top=0.9, bottom=0.1)
    axes = []

    for i, task_csv_file in enumerate(TASK_CSV_FILES):
        if not os.path.exists(task_csv_file):
            continue

        min_time, max_time = WORKFLOW_TIME_SCALES[i]

        task_df = pd.read_csv(task_csv_file)
        task_df['worker_id'] = task_df['worker_id'].astype(str)
        task_df['adjusted_time'] = task_df['time_worker_end'] - min_time

        ax = fig.add_subplot(gs[0, i])
        axes.append(ax)

        # Use a colormap to assign colors to each worker
        unique_workers = task_df['worker_id'].unique()
        colormap = plt.colormaps['tab20'].resampled(len(unique_workers))
        color_norm = mcolors.Normalize(vmin=0, vmax=len(unique_workers))

        for idx, (worker_id, group) in enumerate(task_df.groupby('worker_id')):
            # Calculate cumulative task completions over time
            group = group.sort_values('adjusted_time')
            group['cumulative_tasks'] = range(1, len(group) + 1)
            ax.plot(group['adjusted_time'], group['cumulative_tasks'],
                    color=colormap(color_norm(idx)),
                    linewidth=1.5,
                    alpha=PLOT_SETTINGS["plot_alpha"],
                    label=f'Worker {worker_id}')

        ax.set_title(LOG_TITLES[i], fontsize=PLOT_SETTINGS["title_fontsize"])
        ax.set_xlabel('Time (s)', fontsize=PLOT_SETTINGS["label_fontsize"])
        ax.set_xlim(0, max_time - min_time)
        ax.set_ylabel('Cumulative Tasks Completed',
                      fontsize=PLOT_SETTINGS["label_fontsize"])
        ax.grid(visible=True, linestyle='--', linewidth=0.3,
                alpha=PLOT_SETTINGS["grid_alpha"])
        ax.tick_params(axis='both', labelsize=PLOT_SETTINGS["tick_fontsize"])

        # ax.legend(loc='upper left', fontsize=PLOT_SETTINGS["legend_fontsize"])

    plt.savefig(os.path.join(SAVE_TO, 'worker_task_throughput.png'),
                bbox_inches='tight')

    if show:
        plt.tight_layout()
        plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    plot_worker_task_throughput(show=True)
