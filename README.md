# TaskVine Report Tool

An interactive visualization tool for [TaskVine](https://github.com/cooperative-computing-lab/cctools), a task scheduler for large workflows to run efficiently on HPC clusters. This tool helps you analyze task execution patterns, file transfers, resource utilization, storage consumption, and other key metrics.

## Installation

### For Users (Recommended)

Install directly from PyPI:

```bash
pip install taskvine-report-tool
```

After installation, you can use the commands `vine_parse` and `vine_report` directly from anywhere.

### For Developers

If you want to contribute to development or modify the source code:

```bash
git clone https://github.com/cooperative-computing-lab/taskvine-report-tool.git
cd taskvine-report-tool
pip install -e .
```

## Usage Guide

The tool provides two main commands:

- 🔍 `vine_parse` - Parse TaskVine logs and generate analysis data
- 🌐 `vine_report` - Start web visualization server

### Command Reference

#### `vine_parse` - Parse TaskVine Logs

**Required Parameters:**
- `--templates`: List of log directory names/patterns (required)

**Optional Parameters:**
- `--logs-dir`: Base directory containing log folders (default: current directory)

**Usage Examples:**

```bash
# Basic usage - parse specific log directories (--templates is required)
vine_parse --templates experiment1 experiment2

# Use glob patterns to match multiple directories
vine_parse --templates exp* test* checkpoint_*

# Specify a different logs directory
vine_parse --logs-dir /path/to/logs --templates experiment1

# Parse directories matching patterns in a specific directory
vine_parse --logs-dir /home/user/logs --templates workflow_* test_*
```

**Default Behavior:**
- If no `--logs-dir` is specified, uses current working directory
- The `--templates` parameter is **required** - the command will fail without it
- Patterns support shell glob expansion (*, ?, [])
- Automatically filters out directories that don't contain `vine-logs` subdirectory

#### `vine_report` - Start Web Server

**All Parameters are Optional:**

- `--logs-dir`: Directory containing log folders (default: current directory)
- `--port`: Port number for the web server (default: 9122)
- `--host`: Host address to bind to (default: 0.0.0.0)

**Usage Examples:**

```bash
# Basic usage - start server with all defaults
vine_report

# Specify custom port and logs directory
vine_report --port 8080 --logs-dir /path/to/logs

# Bind to specific host (restrict access)
vine_report --host 127.0.0.1 --port 9122

# Allow remote access (default behavior)
vine_report --host 0.0.0.0 --port 9122
```

**Default Behavior:**
- Uses current working directory as logs directory
- Starts server on port 9122
- Binds to all interfaces (0.0.0.0) allowing remote access
- Displays all available IP addresses where the server can be accessed

## Quick Start

Follow these steps to use the visualization tool:

### 1. Navigate to Your Log Directory

After running your TaskVine workflow, the logs are automatically saved in the `vine-run-info` directory within your workflow's working directory. Navigate to this directory:

```bash
cd your_workflow_directory/vine-run-info
```

You'll see a structure like this containing your experiment runs:

```
vine-run-info/
├── experiment1/
│   └── vine-logs/
│       ├── debug
│       ├── performance
│       ├── taskgraph
│       ├── transactions
│       └── workflow.json
├── experiment2/
│   └── vine-logs/
└── test_run/
    └── vine-logs/
```

### 2. Parse and Visualize

From within the `vine-run-info` directory:

1. Parse specific experiments (**--templates is required**):
```bash
vine_parse --templates experiment1 experiment2
```

Or parse all experiments matching a pattern:
```bash
vine_parse --templates exp* test_*
```

2. Start the visualization server:
```bash
vine_report
```

3. View the report in your browser at `http://localhost:9122`

Note: In the web interface, you'll only see log collections that have been successfully processed by `vine_parse`.

### 2. Working with Different Log Directories

If your logs are in a different location, you can specify the base directory containing your log folders using `--logs-dir`:

```bash
# If your logs are in a custom location:
# /home/user/custom_logs/
# ├── experiment1/vine-logs/
# ├── experiment2/vine-logs/
# └── test_run/vine-logs/

# Parse specific experiments from custom location
vine_parse --logs-dir /home/user/custom_logs --templates experiment1 experiment2

# Parse all experiments matching pattern from custom location
vine_parse --logs-dir /home/user/custom_logs --templates exp* test*
```

### 3. Customizing TaskVine Log Location

By default, TaskVine creates a `vine-run-info` directory in your working directory. You can customize this location when creating your TaskVine manager:

```python
manager = vine.Manager(
    9123,
    run_info_path="~/my_analysis_directory",     # Path to your analysis directory
    run_info_template="your_workflow_name"       # Name for this run's logs
)
```

This will automatically create the correct directory structure:
```
~/my_analysis_directory/
└── your_workflow_name/
    └── vine-logs/
        ├── debug
        └── transactions
```

After your workflow completes, simply:
1. Navigate to your analysis directory: `cd ~/my_analysis_directory`
2. Parse the logs: `vine_parse --templates your_workflow_name`
3. Start the server: `vine_report`
4. View at `http://localhost:9122`

### 4. Generated Data Structure

After parsing, each experiment will have multiple generated directories:
```
vine-run-info/
└── experiment1/
    ├── vine-logs/          # Original log files
    │   ├── debug
    │   ├── performance
    │   ├── taskgraph
    │   ├── transactions
    │   └── workflow.json
    ├── pkl-files/          # Raw parsed data (generated by vine_parse)
    │   ├── manager.pkl     # Manager information
    │   ├── workers.pkl     # Worker statistics
    │   ├── tasks.pkl       # Task execution details
    │   ├── files.pkl       # File transfer information
    │   └── subgraphs.pkl   # Task dependency graphs
    ├── csv-files/          # Visualization-ready data (generated from pkl-files)
    │   ├── task_concurrency.csv
    │   ├── worker_lifetime.csv
    │   ├── file_transfers.csv
    │   └── ...             # Various CSV files for different charts
    └── svg-files/          # Cached graph visualizations
        ├── task_subgraphs_1.svg
        ├── task_dependencies_graph.svg
        └── ...             # Cached SVG files for complex graphs
```

**Directory Breakdown:**

- **`pkl-files/`**: Contains the raw parsed data extracted directly from log files. These are Python pickle files containing structured data about workers, tasks, files, and other workflow components. This is the primary output of `vine_parse`.

- **`csv-files/`**: Contains visualization-ready data files generated from the pkl-files. The web frontend uses these CSV files as the data source for all charts and graphs. Each CSV file corresponds to a specific visualization module.

- **`svg-files/`**: Contains cached SVG files for complex graph visualizations (such as task dependency graphs and subgraphs). Since building these graphs is computationally expensive and time-consuming, we cache the generated SVG files to avoid rebuilding them on subsequent loads.

**For Developers:**

If you want to work with the raw data programmatically, you can load the pkl files into memory using the `restore_pkl_files()` function. The data structures are defined in the following files:
- `data_parser.py` - Main data parsing logic and file restoration
- `task.py` - Task data structure and methods
- `worker.py` - Worker data structure and methods  
- `file.py` - File data structure and methods
- `manager.py` - Manager data structure and methods

This allows you to build custom visualizations based on the original parsed data. You can also customize the CSV generation logic by editing the `generate_csv_files()` function to create your own visualization-ready data formats.

## Important Notes

1. Ensure correct log folder structure with the required `vine-logs` subdirectory
2. Each log collection must contain complete log files (debug and transactions)
3. Data generation may take some time, especially for large workflows
4. Ensure sufficient disk space for generated data files
5. For workflows with large task graphs, the initial data generation and graph visualization might take significant time (potentially hours on some machines). However, once processed, the results are cached in the `pkl-files` directory, making subsequent loads much faster.

## Features

The tool provides several interactive features to enhance user experience and facilitate detailed analysis:

### Interactive Visualization
- **Zoom**: Use your trackpad or hold Ctrl and scroll with your mouse to zoom in/out. This is especially useful when you have lots of tasks and want to focus on a particular area.
  ![Example](imgs/example_zoom.png)

- **Hover**: Hover over any point or line to see its details. This helps you quickly find slow or failed tasks and check their logs. Other elements will fade out to help you focus.
  ![Example](imgs/example_hover.png)

- **Legend**: Use the checkboxes to show only the data you care about. Mix and match different types of information to create your own view. Click on worker names in the legend to show or hide their data. This helps you focus on specific workers without getting distracted by others.
  ![Example](imgs/example_legend.png)

- **Toolbox** Use the toolbox to customize your plot:
  - Save your charts in different formats:
    - Vector formats (SVG, PDF) - great for papers and reports
    - Image formats (PNG, JPG) - perfect for sharing online
  - Download the raw data as CSV files to:
    - Make your own charts
    - Do your own analysis
    - Use with other tools
  - Adjust the axes:
    - Set your own X and Y axis ranges
    - Focus on specific parts of the data
    - Make the chart look exactly how you want
  ![Example](imgs/example_toolbox.png)

## Visualization Modules

The tool provides various visualization modules to analyze different aspects of your TaskVine workflow. Here's a brief description of each module:

### Task Analysis
- **Task Execution Details**: Comprehensive visualization of task distribution across workers and cores. Each task undergoes three phases: committing (input preparation and process initialization), execution (actual task processing), and retrieval (output transfer to manager). The visualization also tracks task failures, which may occur due to invalid inputs, worker disconnections, or resource exhaustion. Additionally, it monitors recovery tasks that are automatically submitted to handle file losses caused by worker evictions or crashes.
  ![Example](imgs/example_task_execution_details.png)
- **Task Concurrency**: Visualizes task states over time from the manager's perspective, tracking five distinct states: waiting (committed but not dispatched), committing (dispatched but not yet executed), executing (currently running on workers), waiting retrieval (completed with outputs pending retrieval), and done (fully completed, whether succeeded or failed).
  ![Example](imgs/example_task_concurrency.png)
- **Task Response Time**: Measures the duration between task commitment to the manager and its dispatch to a worker. High response times may indicate task queue congestion or scheduler inefficiencies when available cores are significantly outnumbered by waiting tasks.
  ![Example](imgs/example_task_response_time.png)
- **Task Execution Time**: Displays the actual runtime duration of each task, providing insights into computational performance and resource utilization.
  ![Example](imgs/example_task_execution_time.png)
- **Task Retrieval Time**: Tracks the time required to retrieve task outputs, beginning when a task completes and sends its completion message to the manager. This phase ends when outputs are successfully retrieved or an error is identified.
  ![Example](imgs/example_task_retrieval_time.png)
- **Task Completion Percentiles**: Shows the time required to complete specific percentages of the total workflow. For instance, the 10th percentile indicates the time needed to complete the first 10% of all tasks.
  ![Example](imgs/example_task_completion_percentiles.png)
- **Task Dependencies**: Visualizes the number of parent tasks for each task. A task can only execute after all its parent tasks have completed and their outputs have been successfully retrieved by the manager.
  ![Example](imgs/example_task_dependencies.png)
- **Task Dependents**: Shows the number of child tasks that depend on each task's outputs as their inputs.
  ![Example](imgs/example_task_dependents.png)
- **Task Subgraphs**: Displays the workflow's independent Directed Acyclic Graphs (DAGs), where each subgraph represents a set of tasks connected by input-output file dependencies.
  ![Example](imgs/example_task_subgraphs.png)

### Worker Analysis
- **Worker Storage Consumption**: Monitors the actual storage usage of each worker over time, specifically tracking worker cache consumption. Note that this metric excludes task-related sandboxes as they represent virtual resource allocation.
  ![Example](imgs/example_worker_storage_consumption.png)
- **Worker Concurrency**: Tracks the number of active workers over time, providing insights into cluster utilization and scalability.
  ![Example](imgs/example_worker_concurrency.png)
- **Worker Incoming Transfers**: Shows the number of file download requests received by each worker over time. These transfers occur when other workers need files from this worker or when the manager is retrieving task outputs.
  ![Example](imgs/example_worker_incoming_transfers.png)
- **Worker Outgoing Transfers**: Displays the number of file download requests initiated by each worker over time, including transfers from the cloud, other workers, or the manager.
  ![Example](imgs/example_worker_outgoing_transfers.png)
- **Worker Executing Tasks**: Tracks the number of tasks actively running on each worker over time.
  ![Example](imgs/example_worker_executing_tasks.png)
- **Worker Waiting Retrieval Tasks**: Shows the number of completed tasks on each worker that are pending output retrieval.
  ![Example](imgs/example_worker_waiting_retrieval_tasks.png)
- **Worker Lifetime**: Visualizes the active period of each worker throughout the workflow, accounting for varying connection times and potential crashes.
  ![Example](imgs/example_worker_lifetime.png)

### File Analysis
- **File Sizes**: Displays the size of each task-related file, including both input and output files.
  ![Example](imgs/example_file_sizes.png)
- **File Concurrent Replicas**: Shows the maximum number of file replicas at any given time. Higher values indicate better redundancy and fault tolerance. Replication occurs automatically for temporary files when specified by the manager's `temp-replica-count` parameter, or naturally when workers fetch inputs from other workers.
  ![Example](imgs/example_file_concurrent_replicas.png)
- **File Retention Time**: Measures the duration between file creation and removal from the cluster. Longer retention times provide better redundancy but consume more disk space. This can be optimized through the manager's file pruning feature.
  ![Example](imgs/example_file_retention_time.png)
- **File Transferred Size**: Tracks the cumulative size of data transferred between workers over time.
  ![Example](imgs/example_file_transferred_size.png)
- **File Created Size**: Shows the cumulative size of distinct files created during workflow execution.
  ![Example](imgs/example_file_created_size.png)

## Troubleshooting

### Common Issues

If you encounter other issues:
1. Verify the log folder structure is correct
2. Confirm all required Python packages are properly installed
3. Check if log files are complete and not corrupted. Note that this tool can usually parse logs from abnormally terminated runs (e.g., due to system crashes or manual interruption), but in some special cases, parsing might fail if the logs are severely corrupted or truncated

Note: Due to ongoing development of TaskVine, there might be occasional mismatches between TaskVine's development version and this tool's log parsing capabilities. This is normal and will be fixed promptly. If you encounter parsing errors:
1. Save the error message and the relevant section of your log files
2. Open an issue on the repository with these details
3. We will help resolve the parsing issue as quickly as possible