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

## Visualization Modules

The tool provides various visualization modules to analyze different aspects of your TaskVine workflow. Here's a brief description of each module:

### Task Analysis
- **Task Execution Details**: Comprehensive visualization of task distribution across workers and cores. Each task undergoes three phases: committing (input preparation and process initialization), execution (actual task processing), and retrieval (output transfer to manager). The visualization also tracks task failures, which may occur due to invalid inputs, worker disconnections, or resource exhaustion. Additionally, it monitors recovery tasks that are automatically submitted to handle file losses caused by worker evictions or crashes.
  ![Example](imgs/example_task_execution_details.png)
- **Task Concurrency**: Visualizes task states over time from the manager's perspective, tracking five distinct states: waiting (committed but not dispatched), committing (dispatched but not yet executed), executing (currently running on workers), waiting retrieval (completed with outputs pending retrieval), and done (fully completed, whether succeeded or failed).
  ![Example](imgs/example_task_concurrency.png)
- **Task Response Time**: Distribution of time between task submission and start of execution
  ![Example](imgs/example_task_response_time.png)
- **Task Execution Time**: Distribution of actual task execution durations
  ![Example](imgs/example_task_execution_time.png)
- **Task Retrieval Time**: Time taken to retrieve task dependencies
  ![Example](imgs/example_task_retrieval_time.png)
- **Task Completion Percentiles**: Distribution of task completion times
  ![Example](imgs/example_task_completion_percentiles.png)
- **Task Dependencies**: Visualization of task dependency relationships
  ![Example](imgs/example_task_dependencies.png)
- **Task Dependents**: Shows which tasks depend on each task
  ![Example](imgs/example_task_dependents.png)
- **Task Subgraphs**: Displays task dependency subgraphs
  ![Example](imgs/example_task_subgraphs.png)

### Worker Analysis
- **Worker Storage Consumption**: Storage usage patterns across workers
  ![Example](imgs/example_worker_storage_consumption.png)
- **Worker Concurrency**: Number of concurrent tasks per worker
  ![Example](imgs/example_worker_concurrency.png)
- **Worker Incoming Transfers**: File transfer patterns to workers
  ![Example](imgs/example_worker_incoming_transfers.png)
- **Worker Outgoing Transfers**: File transfer patterns from workers
  ![Example](imgs/example_worker_outgoing_transfers.png)
- **Worker Executing Tasks**: Tasks currently running on each worker
  ![Example](imgs/example_worker_executing_tasks.png)
- **Worker Waiting Retrieval Tasks**: Tasks waiting for file retrieval
  ![Example](imgs/example_worker_waiting_retrieval_tasks.png)
- **Worker Lifetime**: Worker availability and uptime patterns
  ![Example](imgs/example_worker_lifetime.png)

### File Analysis
- **File Sizes**: Distribution of file sizes in the workflow
  ![Example](imgs/example_file_sizes.png)
- **File Concurrent Replicas**: Number of simultaneous file replicas
  ![Example](imgs/example_file_concurrent_replicas.png)
- **File Retention Time**: How long files are kept in the system
  ![Example](imgs/example_file_retention_time.png)
- **File Transferred Size**: Total size of files transferred
  ![Example](imgs/example_file_transferred_size.png)
- **File Created Size**: Size of files created during execution
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