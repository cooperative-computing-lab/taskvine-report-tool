# TaskVine Report Tool

An interactive visualization tool for [TaskVine](https://cctools.readthedocs.io/en/stable/taskvine/), a task scheduler for large workflows to run efficiently on HPC clusters. This tool helps you analyze task execution patterns, file transfers, resource utilization, and other key metrics.

## Quick Install

Install required Python packages via conda:

```bash
conda install -y flask pandas tqdm bitarray python-graphviz
```

## Usage Guide

Follow these steps to use the visualization tool:

### 1. Configure TaskVine Log Location (Recommended)

The easiest way to use this tool is to configure TaskVine to generate logs directly in the correct location. When creating your TaskVine manager, set these parameters:

```python
manager = vine.Manager(
    9123,
    run_info_path="~/taskvine-report-tool",   # Path to this tool's directory
    run_info_template="experiment1"           # Name for this run's logs
)
```

This will automatically create the correct directory structure:
```
~/taskvine-report-tool/
└── logs/
    └── experiment1/
        └── vine-logs/
            ├── debug
            └── transactions
```

After your workflow completes, simply:
1. Navigate to the tool directory: `cd ~/taskvine-report-tool`
2. Generate the visualization data: `python3 generate_data.py logs/experiment1`
3. Refresh your browser to see the new log collection

### 2. Prepare Log Files

All log files should be placed in the `logs` directory. Currently, the tool only parses the `debug` and `transactions` logs, so the other log files are optional. The directory structure should be:

```
logs/
└── your_log_folder_name/
    └── vine-logs/
        ├── debug         (required)
        ├── performance   (optional)
        ├── taskgraph     (optional)
        └── transactions  (required)
```

For example, if you have multiple log collections, your directory structure might look like this:

```
logs/
├── experiment1/
│   └── vine-logs/
│       ├── debug
│       └── transactions
├── large_workflow/
│   └── vine-logs/
│       ├── debug
│       └── transactions
└── test_run/
    └── vine-logs/
        ├── debug
        ├── performance
        ├── taskgraph
        └── transactions
```

### 3. Generate Visualization Data

For each log collection, generate the visualization data by running:

```bash
python3 generate_data.py logs/your_log_folder_name
```

Example:
```bash
python3 generate_data.py logs/experiment1
```

This will create a `pkl-files` directory under your log folder containing the processed data:
```
logs/
└── experiment1/
    ├── vine-logs/
    │   ├── debug
    │   └── transactions
    └── pkl-files/          # Generated data directory
        ├── manager.pkl     # Manager information
        ├── workers.pkl     # Worker statistics
        ├── tasks.pkl       # Task execution details
        ├── files.pkl       # File transfer information
        └── subgraphs.pkl   # Task dependency graphs
```

### 4. Start Visualization Server

After generating the data, start the web server:

```bash
python3 app.py
```

By default, the server runs on port 9122. You can specify a different port using the `--port` argument:

```bash
python3 app.py --port 8080
```

### 5. View Visualization Report

Once the server is running, access the visualization in your browser at:

```
http://localhost:9122
```

On the web interface, you can:
- Switch between different log collections
- View task execution time distributions
- Analyze file transfer patterns
- Monitor resource utilization
- Export visualization charts

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

If you encounter issues:
1. Verify the log folder structure is correct
2. Confirm all required Python packages are properly installed
3. Check if log files are complete and not corrupted. Note that this tool can usually parse logs from abnormally terminated runs (e.g., due to system crashes or manual interruption), but in some special cases, parsing might fail if the logs are severely corrupted or truncated

Note: Due to ongoing development of TaskVine, there might be occasional mismatches between TaskVine's development version and this tool's log parsing capabilities. This is normal and will be fixed promptly. If you encounter parsing errors:
1. Save the error message and the relevant section of your log files
2. Open an issue on the repository with these details
3. We will help resolve the parsing issue as quickly as possible