from _config import *
from _tools import *

def plot_accumulated_disk_usage(show=True, add_peak_line_and_text=True, plot_file_count=True):
    num_logs = len(LOGS)
    fig_height = PLOT_SETTINGS["subplot_height"]
    fig_width = PLOT_SETTINGS["subplot_width"] * num_logs

    fig = plt.figure(figsize=(fig_width, fig_height))

    gs = GridSpec(1, num_logs, figure=fig, wspace=0.4, left=0.1, right=0.85, top=0.8, bottom=0.1)

    axes = []

    global_max_disk_usage_gb = get_global_max_disk_usage_gb()
    global_max_file_count = get_global_max_file_count()

    global_max_execution_time = get_global_max_execution_time()

    for i, csv_file in enumerate(DISK_USAGE_CSV_FILES):
        if not os.path.exists(csv_file):
            continue

        min_time, max_time = WORKFLOW_TIME_SCALES[i]

        df = pd.read_csv(csv_file)
        df['adjusted_time'] = df['when_stage_in_or_out'] - min_time
        df = df.sort_values(by='adjusted_time')
        df['size(GB)'] = df['size(MB)'] / 1024
        df['accumulated_disk_usage_gb'] = df['size(GB)'].cumsum()
        df['file_count'] = df['size(MB)'].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0)).cumsum()

        max_disk_usage_gb = df['accumulated_disk_usage_gb'].max()
        max_disk_usage_time = df.loc[df['accumulated_disk_usage_gb'].idxmax(), 'adjusted_time']

        max_file_count = df['file_count'].max()
        max_file_count_time = df.loc[df['file_count'].idxmax(), 'adjusted_time']

        ax1 = fig.add_subplot(gs[0, i])
        axes.append(ax1)

        line1, = ax1.plot(df['adjusted_time'], df['accumulated_disk_usage_gb'],
                          color=PLOT_SETTINGS["line_color"],
                          linewidth=PLOT_SETTINGS["worker_disk_usage_line_width"],
                          alpha=PLOT_SETTINGS["plot_alpha"], label='ASC')

        ax1.set_xlabel('Time (s)', fontsize=PLOT_SETTINGS["label_fontsize"])
        ax1.set_xlim(0, global_max_execution_time * 1.1)

        ax1.set_ylabel('ASC (GB)', fontsize=PLOT_SETTINGS["label_fontsize"])
        ax1.tick_params(axis='y', labelsize=PLOT_SETTINGS["tick_fontsize"])
        ax1.set_ylim(0, global_max_disk_usage_gb * 1.1)

        if add_peak_line_and_text:
            ax1.axhline(y=max_disk_usage_gb, color='red', linestyle='--', linewidth=1)
            text1 = ax1.annotate(
                f'Peak: {max_disk_usage_gb:.2f} GB',
                xy=(max_disk_usage_time, max_disk_usage_gb),
                xytext=(-20, 10),
                textcoords='offset points',
                arrowprops=dict(facecolor='black', arrowstyle='->', lw=0.5),
                color='red', fontsize=PLOT_SETTINGS['annotate_fontsize'], ha='center', va='bottom',
            )
            all_texts = [text1]
        else:
            all_texts = []
        
        ax1.tick_params(axis='x', labelsize=PLOT_SETTINGS["tick_fontsize"])
        ax1.set_title(LOG_TITLES[i], fontsize=PLOT_SETTINGS["title_fontsize"])
        ax1.grid(visible=True, linestyle='--', linewidth=PLOT_SETTINGS["grid_linewidth"], alpha=PLOT_SETTINGS["grid_alpha"])

        handles = [line1]
        labels = [line1.get_label()]

        if plot_file_count:
            ax2 = ax1.twinx()
            ax2.spines['right'].set_position(('axes', 1.0))
            ax2.set_frame_on(True)
            ax2.patch.set_visible(False)

            line2, = ax2.plot(df['adjusted_time'], df['file_count'], color=PLOT_SETTINGS["color_secondary"],
                            linestyle='--', alpha=PLOT_SETTINGS["plot_alpha"], label='AFC')
            ax2.set_ylabel('AFC', fontsize=PLOT_SETTINGS["label_fontsize"])
            ax2.tick_params(axis='y', labelsize=PLOT_SETTINGS["tick_fontsize"])
            ax2.set_ylim(0, global_max_file_count * 1.1)

            if add_peak_line_and_text:
                ax2.axhline(y=max_file_count, color='red', linestyle='--', linewidth=1)
                text2 = ax2.annotate(
                    f'Peak: {max_file_count}',
                    xy=(max_file_count_time, max_file_count),
                    xytext=(20, -10),
                    textcoords='offset points',
                    arrowprops=dict(facecolor='black', arrowstyle='->', lw=0.5),
                    color='red', fontsize=PLOT_SETTINGS['annotate_fontsize'], ha='center', va='bottom',
                )
                all_texts.append(text2)

            handles.append(line2)
            labels.append(line2.get_label())
            ax2.grid(visible=False)

        if add_peak_line_and_text:
            adjust_text(all_texts, autoalign='xy', only_move={'points': 'y', 'text': 'y'}, ax=ax1)

    fig.legend(handles, labels, loc='upper center', fontsize=PLOT_SETTINGS["label_fontsize"], ncol=2, bbox_to_anchor=(0.5, 0.95))
    plt.savefig(os.path.join(SAVE_TO, 'ASC.png'), bbox_inches='tight')

    if show:
        plt.tight_layout()
        plt.show()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-peak', action='store_true', help="Add peak line and text annotations.")
    parser.add_argument('--no-file-count', action='store_true', help="Do not plot accumulated file count.")
    args = parser.parse_args()

    plot_accumulated_disk_usage(show=True, add_peak_line_and_text=not args.no_peak, plot_file_count=not args.no_file_count)
