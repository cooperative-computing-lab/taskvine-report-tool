from _config import *

def plot_temp_file_size(show=True):
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
        file_info_df = file_info_df[file_info_df['filename'].str.startswith('temp-')] 

        if 'size(MB)' not in file_info_df.columns:
            print(f"File {file_info_csv} is missing 'size(MB)' column.")
            continue

        file_info_df = file_info_df.sort_values(by='size(MB)').reset_index(drop=True)

        ax = fig.add_subplot(gs[0, i])

        file_ids = range(1, len(file_info_df) + 1)
        sizes_mb = file_info_df['size(MB)']

        ax.bar(file_ids, sizes_mb, 
               color=PLOT_SETTINGS["bar_color"], 
               alpha=PLOT_SETTINGS["plot_alpha"])

        ax.set_title(f"{LOG_TITLES[i]}", fontsize=PLOT_SETTINGS['title_fontsize'])
        ax.set_xlabel('File Index', fontsize=PLOT_SETTINGS['label_fontsize'])
        ax.set_ylabel('File Size (MB)', fontsize=PLOT_SETTINGS['label_fontsize'])
        ax.tick_params(axis='both', labelsize=PLOT_SETTINGS['tick_fontsize'])
        ax.grid(True, alpha=PLOT_SETTINGS["grid_alpha"], linewidth=PLOT_SETTINGS["grid_linewidth"])

    plt.savefig(os.path.join(SAVE_TO, 'file_sizes_distribution.png'), bbox_inches='tight')
    if show:
        plt.tight_layout()
        plt.show()

if __name__ == '__main__':
    plot_temp_file_size(show=True)
