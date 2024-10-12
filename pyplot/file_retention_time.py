from config import *


def generate_file_retention_time_csv(file_info_csv, file_retention_time_csv):
    file_info_df = pd.read_csv(file_info_csv)
    manager_info_df = pd.read_csv(file_info_csv.replace('file_info.csv', 'manager_info.csv'))

    manager_time_start = manager_info_df['time_start'].min()
    manager_time_end = manager_info_df['time_end'].min()
    manager_lifetime = manager_time_end - manager_time_start

    data = []
    
    for index, row in file_info_df.iterrows():
        filename = row['filename']
        size = row['size(MB)']
        worker_holding = row['worker_holding']
        worker_holding_list = ast.literal_eval(worker_holding)
        
        min_in = min([worker[1] for worker in worker_holding_list])
        max_out = max([worker[2] for worker in worker_holding_list])
        file_retention_time = max_out - min_in
        
        data.append({
            'filename': filename,
            'size(MB)': size,
            'file_retention_time': file_retention_time,
            'file_retention_rate': file_retention_time / manager_lifetime,
        })
    
    file_retention_time_df = pd.DataFrame(data, columns=['filename', 'size(MB)', 'file_retention_time', 'file_retention_rate'])
    file_retention_time_df.to_csv(file_retention_time_csv, index=False)


def plot_file_retention_time(show=True, x_unit='index'):
    sns.set_theme(style="whitegrid", rc={'axes.grid': True, 'grid.alpha': PLOT_SETTINGS['grid_alpha']})
    
    num_logs = len(LOGS)
    
    fig_height = PLOT_SETTINGS["subplot_height"]
    fig_width = PLOT_SETTINGS["subplot_width"] * num_logs
    
    fig = plt.figure(figsize=(fig_width, fig_height))
    
    gs = GridSpec(1, num_logs, figure=fig, wspace=0.3, left=0.15, right=0.9, bottom=0.1)
    
    axs = []
    max_file_index = 0
    max_file_size = 0

    for csv_file in FILE_INFO_CSV_FILES:
        file_retention_time_csv = csv_file.replace('file_info.csv', 'file_retention_time.csv')
        if not os.path.exists(file_retention_time_csv):
            generate_file_retention_time_csv(csv_file, file_retention_time_csv)
        df = pd.read_csv(file_retention_time_csv)
        max_file_size = max(max_file_size, df['size(MB)'].max())

    max_file_index = get_adjusted_max(len(df), step=10000)
    max_FRR = 1

    for i, csv_file in enumerate(FILE_INFO_CSV_FILES):
        file_retention_time_csv = csv_file.replace('file_info.csv', 'file_retention_time.csv')
        df = pd.read_csv(file_retention_time_csv)

        ax = fig.add_subplot(gs[0, i])
        axs.append(ax)
        
        if x_unit == 'index':
            x_values = range(len(df))
            x_label = 'File Index'
        elif x_unit == 'size':
            x_values = df['size(MB)']
            x_label = 'File Size (MB)'

        sns.scatterplot(
            ax=ax, 
            x=x_values,
            y='file_retention_rate', 
            data=df,
            s=PLOT_SETTINGS["dot_size"],
            alpha=PLOT_SETTINGS["plot_alpha"], 
            color=PLOT_SETTINGS["dot_color"],
            edgecolor=PLOT_SETTINGS["dot_edgecolor"]
        )
        
        ax.set_title(LOG_TITLES[i], fontsize=PLOT_SETTINGS["title_fontsize"])

        ax.set_xlabel(x_label, fontsize=PLOT_SETTINGS["label_fontsize"])
        ax.set_ylabel('FRR (%)', fontsize=PLOT_SETTINGS["label_fontsize"])

        if x_unit == 'index':
            ax.set_xlim(-1, max_file_index)
        elif x_unit == 'size':
            ax.set_xlim(-1, max_file_size)
        ax.set_xticks(np.linspace(0, ax.get_xlim()[1], 4))

        ax.set_ylim(0, max_FRR * 1.1)
        ax.set_yticks(np.linspace(0, max_FRR, PLOT_SETTINGS["yticks_count"]))
        
        ax.tick_params(axis='both', labelsize=PLOT_SETTINGS["tick_fontsize"])
        ax.grid(visible=True, linestyle='--', linewidth=0.3, alpha=PLOT_SETTINGS["grid_alpha"])
    
    plt.savefig(os.path.join(SAVE_TO, 'FRR.png'), bbox_inches='tight')

    if show:
        plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    plot_file_retention_time(show=True, x_unit='index')
