from _config import *

# define some colors
PLOT_SETTINGS["ready_line_color"] = "#099652"
PLOT_SETTINGS["committing_line_color"] = "#8327cf"
PLOT_SETTINGS["running_line_color"] = "#5581b0"
PLOT_SETTINGS["retrieving_line_color"] = "#be612a"


def plot_task_states_over_time(show=True):
    num_logs = len(MANAGER_INFO_CSV_FILES)
    fig_height = PLOT_SETTINGS["subplot_height"]
    fig_width = PLOT_SETTINGS["subplot_width"] * num_logs

    fig = plt.figure(figsize=(fig_width, fig_height))
    gs = GridSpec(1, num_logs, figure=fig, wspace=0.3,
                  left=0.15, right=0.9, top=0.8, bottom=0.1)

    global_max_y = 0
    task_counts = []

    for i, manager_info_path in enumerate(MANAGER_INFO_CSV_FILES):
        task_path = TASK_CSV_FILES[i]
        if not os.path.exists(manager_info_path) or not os.path.exists(task_path):
            print(f"File {manager_info_path} or {task_path} does not exist.")
            continue

        task_df = pd.read_csv(task_path)

        worker_summary_df = pd.read_csv(WORKER_SUBMARY_CSV_FILES[i])
        time_start = worker_summary_df['time_connected'].min()
        time_end = worker_summary_df['time_disconnected'].max()
        time_range = np.arange(time_start, time_end, step=1)

        ready_counts = []
        committing_counts = []
        running_counts = []
        retrieving_counts = []

        for current_time in time_range:
            ready_tasks = task_df[
                (task_df['when_ready'] <= current_time) &
                (task_df['when_input_transfer_ready'] > current_time) &
                task_df['when_ready'].notna(
                ) & task_df['when_input_transfer_ready'].notna()
            ]
            committing_tasks = task_df[
                (task_df['when_input_transfer_ready'] <= current_time) &
                (task_df['time_worker_start'] > current_time) &
                task_df['when_input_transfer_ready'].notna(
                ) & task_df['time_worker_start'].notna()
            ]
            running_tasks = task_df[
                (task_df['time_worker_start'] <= current_time) &
                (task_df['time_worker_end'] > current_time) &
                task_df['time_worker_start'].notna(
                ) & task_df['time_worker_end'].notna()
            ]
            retrieving_tasks = task_df[
                (task_df['time_worker_end'] <= current_time) &
                (task_df['when_done'] > current_time) &
                task_df['time_worker_end'].notna(
                ) & task_df['when_done'].notna()
            ]

            ready_counts.append(len(ready_tasks))
            committing_counts.append(len(committing_tasks))
            running_counts.append(len(running_tasks))
            retrieving_counts.append(len(retrieving_tasks))

        if not args.no_ready:
            local_max_y = max(max(ready_counts), max(committing_counts), max(
                running_counts), max(retrieving_counts))
        else:
            local_max_y = max(max(committing_counts), max(
                running_counts), max(retrieving_counts))
        global_max_y = max(global_max_y, local_max_y)

        task_counts.append(
            (time_range, ready_counts, committing_counts, running_counts, retrieving_counts))

    lines = []
    labels = []
    for i, (time_range, ready_counts, committing_counts, running_counts, retrieving_counts) in enumerate(task_counts):
        ax = fig.add_subplot(gs[0, i])
        if not args.no_ready:
            ax.plot(time_range - time_range[0], ready_counts, label="Ready Tasks",
                    linewidth=PLOT_SETTINGS["worker_disk_usage_line_width"],
                    alpha=PLOT_SETTINGS["plot_alpha"],
                    color=PLOT_SETTINGS["ready_line_color"])
        ax.plot(time_range - time_range[0], committing_counts, label="Committing Tasks",
                linewidth=PLOT_SETTINGS["worker_disk_usage_line_width"],
                alpha=PLOT_SETTINGS["plot_alpha"],
                color=PLOT_SETTINGS["committing_line_color"])
        ax.plot(time_range - time_range[0], running_counts, label="Running Tasks",
                linewidth=PLOT_SETTINGS["worker_disk_usage_line_width"],
                alpha=PLOT_SETTINGS["plot_alpha"],
                color=PLOT_SETTINGS["running_line_color"])
        ax.plot(time_range - time_range[0], retrieving_counts, label="Retrieving Tasks",
                linewidth=PLOT_SETTINGS["worker_disk_usage_line_width"],
                alpha=PLOT_SETTINGS["plot_alpha"],
                color=PLOT_SETTINGS["retrieving_line_color"])

        ax.set_ylim(0, global_max_y * 1.1)
        ax.set_title(LOG_TITLES[i], fontsize=PLOT_SETTINGS['title_fontsize'])
        ax.set_xlabel('Time (s)', fontsize=PLOT_SETTINGS['label_fontsize'])
        ax.set_ylabel('Number of Tasks',
                      fontsize=PLOT_SETTINGS['label_fontsize'])
        ax.tick_params(axis='both', labelsize=PLOT_SETTINGS['tick_fontsize'])
        ax.grid(True, alpha=PLOT_SETTINGS["grid_alpha"],
                linewidth=PLOT_SETTINGS["grid_linewidth"])

        if not lines:
            lines, labels = ax.get_legend_handles_labels()

    fig.legend(lines, labels, loc='upper center', ncol=4,
               fontsize=PLOT_SETTINGS['legend_fontsize'])

    plt.savefig(os.path.join(SAVE_TO, 'task_states_over_time.png'),
                bbox_inches='tight')
    if show:
        plt.tight_layout()
        plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-ready', action='store_true',
                        help="No ready tasks.")
    args = parser.parse_args()

    plot_task_states_over_time(show=True)
