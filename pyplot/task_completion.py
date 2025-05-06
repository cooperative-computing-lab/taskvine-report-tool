from _config import *


def plot_cumulative_task_completion_for_multiple_logs(show=True):
    num_logs = len(MANAGER_INFO_CSV_FILES)
    fig_height = PLOT_SETTINGS["subplot_height"]
    fig_width = PLOT_SETTINGS["subplot_width"] * num_logs

    fig = plt.figure(figsize=(fig_width, fig_height))
    gs = GridSpec(1, num_logs, figure=fig,  wspace=0.3,
                  left=0.15, right=0.9, top=0.9, bottom=0.1)

    for i, (manager_info_path, task_done_path) in enumerate(zip(MANAGER_INFO_CSV_FILES, TASK_DONE_CSV_FILES)):
        if not os.path.exists(manager_info_path) or not os.path.exists(task_done_path):
            print(
                f"File {manager_info_path} or {task_done_path} does not exist.")
            continue

        manager_info_df = pd.read_csv(manager_info_path)
        task_done_df = pd.read_csv(task_done_path)

        time_start = manager_info_df['time_start'].min()
        task_done_df['adjusted_time_worker_end'] = task_done_df['time_worker_end'] - time_start

        task_completion_counts = task_done_df.groupby(
            'adjusted_time_worker_end').size()
        cumulative_task_completion = task_completion_counts.cumsum()

        ax = fig.add_subplot(gs[0, i])
        ax.plot(cumulative_task_completion.index, cumulative_task_completion.values,
                linewidth=PLOT_SETTINGS["worker_disk_usage_line_width"],
                alpha=PLOT_SETTINGS["plot_alpha"],
                color=PLOT_SETTINGS["line_color"])

        ax.set_title(LOG_TITLES[i], fontsize=PLOT_SETTINGS['title_fontsize'])
        ax.set_xlabel('Time (s)', fontsize=PLOT_SETTINGS['label_fontsize'])
        ax.set_ylabel('Completed Tasks',
                      fontsize=PLOT_SETTINGS['label_fontsize'])
        ax.tick_params(axis='both', labelsize=PLOT_SETTINGS['tick_fontsize'])
        ax.grid(True, alpha=PLOT_SETTINGS["grid_alpha"],
                linewidth=PLOT_SETTINGS["grid_linewidth"])

    plt.savefig(os.path.join(
        SAVE_TO, 'cumulative_tasks_completed.png'), bbox_inches='tight')
    if show:
        plt.tight_layout()
        plt.show()


if __name__ == '__main__':
    plot_cumulative_task_completion_for_multiple_logs(show=True)
