from _config import *


def plot_file_consumer_count(show=True):
    num_logs = len(FILE_INFO_CSV_FILES)

    fig_height = PLOT_SETTINGS["subplot_height"]
    fig_width = PLOT_SETTINGS["subplot_width"] * num_logs

    fig = plt.figure(figsize=(fig_width, fig_height))
    gs = GridSpec(1, num_logs, figure=fig, wspace=0.3,
                  left=0.15, right=0.9, top=0.9, bottom=0.1)

    for i, file_info_csv in enumerate(FILE_INFO_CSV_FILES):
        if not os.path.exists(file_info_csv):
            print(f"File {file_info_csv} does not exist.")
            continue

        df = pd.read_csv(file_info_csv)

        df['consumers_count'] = df['consumers'].apply(
            lambda x: len(eval(x)) if pd.notna(x) else 0)

        df_count = df['consumers_count'].value_counts().sort_index()

        ax = fig.add_subplot(gs[0, i])

        bar_plot = sns.barplot(
            x=df_count.index, y=df_count.values, color='steelblue', ax=ax, width=0.5)

        for p in bar_plot.patches:
            ax.text(p.get_x() + p.get_width() / 2, p.get_height(), f'{int(p.get_height())}',
                    ha='center', va='bottom', fontsize=10, color='red')

        ax.set_xlabel('Number of Consumers',
                      fontsize=PLOT_SETTINGS["label_fontsize"])
        ax.set_ylabel('Number of Files',
                      fontsize=PLOT_SETTINGS["label_fontsize"])
        ax.tick_params(axis='both', labelsize=PLOT_SETTINGS["tick_fontsize"])
        ax.set_title(LOG_TITLES[i], fontsize=PLOT_SETTINGS["title_fontsize"])

        ax.grid(True, color='#d3d3d3',
                linewidth=PLOT_SETTINGS['grid_linewidth'], alpha=PLOT_SETTINGS['grid_alpha'])

    plt.savefig(os.path.join(SAVE_TO, 'file_consumer_count.png'),
                bbox_inches='tight')

    if show:
        plt.tight_layout()
        plt.show()


if __name__ == '__main__':
    plot_file_consumer_count(show=True)
