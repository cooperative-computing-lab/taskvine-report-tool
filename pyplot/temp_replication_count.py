from _config import *

def plot_file_count(show=True):
    num_logs = len(FILE_INFO_CSV_FILES)
    fig_height = PLOT_SETTINGS["subplot_height"]
    fig_width = PLOT_SETTINGS["subplot_width"] * num_logs

    fig = plt.figure(figsize=(fig_width, fig_height))
    gs = GridSpec(1, num_logs, figure=fig, wspace=0.3, left=0.15, right=0.9, top=0.9, bottom=0.1)

    for i, file_info_csv in enumerate(FILE_INFO_CSV_FILES):
        if not os.path.exists(file_info_csv):
            print(f"File {file_info_csv} does not exist.")
            continue

        file_info_df = pd.read_csv(file_info_csv)
        file_info_df = file_info_df[file_info_df['filename'].str.startswith('temp-')] # consider temp files only

        if 'num_workers_holding' not in file_info_df.columns:
            print(f"File {file_info_csv} is missing 'num_workers_holding' column.")
            continue

        file_info_df = file_info_df.sort_values(by='num_workers_holding').reset_index(drop=True)

        ax = fig.add_subplot(gs[0, i])

        file_ids = range(1, len(file_info_df) + 1)
        num_workers_holding = file_info_df['num_workers_holding']

        ax.bar(file_ids, num_workers_holding, 
               color=PLOT_SETTINGS["bar_color"], 
               alpha=PLOT_SETTINGS["plot_alpha"])

        # ax.set_title("Per-File Replica Count", fontsize=PLOT_SETTINGS['title_fontsize'])
        ax.set_title(LOG_TITLES[i], fontsize=PLOT_SETTINGS['title_fontsize'])
        ax.set_xlabel('File Index', fontsize=PLOT_SETTINGS['label_fontsize'])
        ax.set_ylabel('Count', fontsize=PLOT_SETTINGS['label_fontsize'])
        ax.tick_params(axis='both', labelsize=PLOT_SETTINGS['tick_fontsize'])
        ax.grid(True, alpha=PLOT_SETTINGS["grid_alpha"], linewidth=PLOT_SETTINGS["grid_linewidth"])

    plt.savefig(os.path.join(SAVE_TO, 'file_workers_holding.png'), bbox_inches='tight')
    if show:
        plt.tight_layout()
        plt.show()

if __name__ == '__main__':
    plot_file_count(show=True)
