from _config import *


def plot_execution_time_cdf(show=True, log_scale=False):
    num_logs = len(LOGS)

    fig_height = PLOT_SETTINGS["subplot_height"]
    fig_width = PLOT_SETTINGS["subplot_width"] * num_logs

    fig = plt.figure(figsize=(fig_width, fig_height))
    gs = GridSpec(1, num_logs, figure=fig, wspace=0.3,
                  left=0.15, right=0.9, top=0.9, bottom=0.1)

    for i, task_done_csv in enumerate(TASK_DONE_CSV_FILES):
        if not os.path.exists(task_done_csv):
            print(f"File {task_done_csv} does not exist.")
            continue

        task_done_df = pd.read_csv(task_done_csv)

        task_done_df['execution_time'] = task_done_df['time_worker_end'] - \
            task_done_df['time_worker_start']

        sns.set_theme(style="whitegrid", rc={
                      'axes.grid': True, 'grid.alpha': PLOT_SETTINGS['grid_alpha']})

        execution_times = task_done_df['execution_time'].values
        execution_times_sorted = np.sort(execution_times)
        cdf = np.arange(1, len(execution_times_sorted) + 1) / \
            len(execution_times_sorted)

        ax = fig.add_subplot(gs[0, i])

        ax.plot(execution_times_sorted, cdf, marker='.', linestyle='none',
                color="#9105a1", alpha=PLOT_SETTINGS["plot_alpha"], markersize=3)

        if log_scale:
            ax.set_xscale('log')
            x_label = "Execution Time (log scale)"
        else:
            x_label = "Execution Time (s)"

        ax.set_title(LOG_TITLES[i], fontsize=PLOT_SETTINGS["title_fontsize"])
        ax.set_xlabel(x_label, fontsize=PLOT_SETTINGS["label_fontsize"])
        ax.set_ylabel('CDF', fontsize=PLOT_SETTINGS["label_fontsize"])

        ax.grid(True, linestyle='--',
                linewidth=PLOT_SETTINGS['grid_linewidth'], alpha=PLOT_SETTINGS['grid_alpha'])

    plt.savefig(os.path.join(
        SAVE_TO, 'task_execution_time_cdf.png'), bbox_inches='tight')

    if show:
        plt.tight_layout()
        plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--log-scale', action='store_true',
                        help="Use log scale in x-axis.")
    args = parser.parse_args()

    plot_execution_time_cdf(show=True, log_scale=args.log_scale)
