from .runtime_state import runtime_state, check_and_reload_data

import graphviz
import os
import traceback
from pathlib import Path
from flask import Blueprint, jsonify, request


subgraphs_bp = Blueprint('subgraphs', __name__, url_prefix='/api')


@subgraphs_bp.route('/subgraphs')
@check_and_reload_data()
def get_subgraphs():
    try:
        data = {}

        subgraph_id = request.args.get('subgraph_id')
        if not subgraph_id:
            return jsonify({'error': 'Subgraph ID is required'}), 400
        subgraph_id = int(subgraph_id)
        if subgraph_id not in runtime_state.subgraphs.keys():
            return jsonify({'error': 'Invalid subgraph ID'}), 400

        plot_unsuccessful_task = request.args.get(
            'plot_unsuccessful_task', 'true').lower() == 'true'
        plot_recovery_task = request.args.get(
            'plot_recovery_task', 'true').lower() == 'true'

        subgraph = runtime_state.subgraphs[subgraph_id]
        print(f"subgraph: {subgraph_id} has {len(subgraph)} tasks")

        svg_file_path_without_suffix = os.path.join(
            runtime_state.svg_files_dir, f'subgraph-{subgraph_id}-{plot_unsuccessful_task}-{plot_recovery_task}')
        svg_file_path = f'{svg_file_path_without_suffix}.svg'

        if not Path(svg_file_path).exists():
            dot = graphviz.Digraph()

            # Use sets to cache added file nodes and edges to avoid duplication
            added_file_nodes = set()
            added_edges = set()

            num_of_task_nodes = 0
            num_of_file_nodes = 0
            num_of_edges = 0

            # Preprocess to analyze the execution history of each task_id
            task_stats = {}  # {task_id: {"failures": count, "attempts": []}}
            # {task_id: [ordered list of task_try_ids]}
            task_execution_order = {}

            # First build a chronological order of attempts for each task
            for (task_id, task_try_id) in list(subgraph):
                task = runtime_state.tasks[(task_id, task_try_id)]

                if task_id not in task_execution_order:
                    task_execution_order[task_id] = []

                # Add to the execution order list (we'll sort it later)
                task_execution_order[task_id].append({
                    "try_id": task_try_id,
                    "time": task.time_worker_start or 0,  # Use start time for ordering
                    "success": not task.when_failure_happens,
                    "is_recovery": task.is_recovery_task
                })

            # Sort attempts by time for each task
            for task_id, attempts in task_execution_order.items():
                task_execution_order[task_id] = sorted(
                    attempts, key=lambda x: x["time"])

            # Now compute statistics based on the ordered execution history
            for task_id, attempts in task_execution_order.items():
                failures = 0
                latest_successful_try_id = None
                final_status_is_success = False
                is_recovery_task = False

                # Go through attempts in chronological order
                for attempt in attempts:
                    if not attempt["success"]:
                        failures += 1
                    else:
                        latest_successful_try_id = attempt["try_id"]
                        final_status_is_success = True

                    # Check if this is a recovery task
                    if attempt["is_recovery"]:
                        is_recovery_task = True

                # Get the final attempt (chronologically last)
                final_attempt = attempts[-1] if attempts else None

                task_stats[task_id] = {
                    "failures": failures,
                    "latest_successful_try_id": latest_successful_try_id,
                    "final_status_is_success": final_status_is_success,
                    "is_recovery_task": is_recovery_task,
                    "final_attempt": final_attempt,
                    "attempts": attempts
                }

            def plot_task_node(dot, task):
                task_id = task.task_id
                # task_try_id = task.task_try_id
                stats = task_stats[task_id]

                # Use a single node ID based on task_id
                node_id = f'{task_id}'

                # Create a detailed label
                node_label = f'{task_id}'

                # Add recovery task label if applicable
                if stats["is_recovery_task"]:
                    node_label += " (Recovery Task)"

                # Add failed count if applicable
                if stats["failures"] > 0:
                    node_label += f" (Failed: {stats['failures']})"

                # Add information about the number of attempts
                total_attempts = len(stats["attempts"])
                if total_attempts > 1:
                    node_label += f" (Attempts: {total_attempts})"

                # Style based on task type and final status
                if not stats["final_status_is_success"]:
                    # Task ultimately failed
                    style = 'dashed'
                    color = '#FF0000'  # Red
                    fontcolor = '#FF0000'
                    fillcolor = '#FFFFFF'
                elif stats["is_recovery_task"]:
                    # This is a recovery task that succeeded
                    style = 'filled'
                    color = '#000000'
                    fontcolor = '#000000'
                    fillcolor = '#FFC0CB'  # Light pink
                elif stats["failures"] > 0:
                    # Task had failures but succeeded without being a recovery task
                    style = 'filled'
                    color = '#000000'
                    fontcolor = '#000000'
                    fillcolor = '#FFFACD'  # Light yellow
                else:
                    # Normal successful task without failures
                    style = 'solid'
                    color = '#000000'
                    fontcolor = '#000000'
                    fillcolor = '#FFFFFF'

                dot.node(node_id, node_label, shape='ellipse', style=style, color=color,
                         fontcolor=fontcolor, fillcolor=fillcolor)
                return True

            def plot_file_node(dot, file):
                # Skip files not produced by any task
                if len(file.producers) == 0:
                    return

                file_name = file.filename
                # Check if file node has already been added
                if file_name not in added_file_nodes:
                    dot.node(file_name, file_name, shape='box')
                    added_file_nodes.add(file_name)
                    nonlocal num_of_file_nodes
                    num_of_file_nodes += 1

            def plot_task2file_edge(dot, task, file):
                # Skip files not produced by any task
                if len(file.producers) == 0:
                    return
                # Skip unsuccessful tasks (cannot produce files)
                if task.when_failure_happens:
                    return

                # Use only task_id (not try_id) to connect to the aggregated task node
                edge_id = (f'{task.task_id}', file.filename)

                # Check if edge has already been added
                if edge_id in added_edges:
                    return

                task_execution_time = task.time_worker_end - task.time_worker_start
                dot.edge(edge_id[0], edge_id[1],
                         label=f'{task_execution_time:.2f}s')
                added_edges.add(edge_id)
                nonlocal num_of_edges
                num_of_edges += 1

            def plot_file2task_edge(dot, file, task):
                # Skip files not produced by any task
                if len(file.producers) == 0:
                    return

                # Use only task_id (not try_id) to connect to the aggregated task node
                edge_id = (file.filename, f'{task.task_id}')

                # Check if edge has already been added
                if edge_id in added_edges:
                    return

                # Calculate file creation time
                file_creation_time = float('inf')
                for producer_task_id, producer_task_try_id in file.producers:
                    producer_task = runtime_state.tasks[(
                        producer_task_id, producer_task_try_id)]
                    if producer_task.time_worker_end:
                        file_creation_time = min(
                            file_creation_time, producer_task.time_worker_end)
                file_creation_time = file_creation_time - runtime_state.MIN_TIME

                dot.edge(edge_id[0], edge_id[1],
                         label=f'{file_creation_time:.2f}s')
                added_edges.add(edge_id)
                nonlocal num_of_edges
                num_of_edges += 1

            # Process tasks for display (one node per task_id)
            processed_task_ids = set()

            for task_id, stats in task_stats.items():
                if task_id in processed_task_ids:
                    continue

                # For visualization, prefer to use:
                # 1. The latest successful attempt if there is one
                # 2. Otherwise, use the final attempt (which would be a failure)
                if stats["latest_successful_try_id"] is not None:
                    task_try_id = stats["latest_successful_try_id"]
                else:
                    # If no successful attempts, use the final attempt
                    task_try_id = stats["final_attempt"]["try_id"] if stats["final_attempt"] else None

                if task_try_id is None:
                    continue  # Skip if we can't determine which attempt to use

                task = runtime_state.tasks[(task_id, task_try_id)]

                # Skip based on display options
                if task.is_recovery_task and not plot_recovery_task:
                    continue
                if task.when_failure_happens and not plot_unsuccessful_task:
                    continue

                # Plot the node for this task
                if plot_task_node(dot, task):
                    num_of_task_nodes += 1
                    processed_task_ids.add(task_id)

                    # Process input files
                    for file_name in task.input_files:
                        file = runtime_state.files[file_name]
                        plot_file_node(dot, file)
                        plot_file2task_edge(dot, file, task)

                    # Process output files
                    # Only plot outputs if this was a successful attempt
                    if not task.when_failure_happens:
                        for file_name in task.output_files:
                            file = runtime_state.files[file_name]
                            # skip files that haven't been created
                            if len(file.transfers) == 0:
                                continue
                            plot_file_node(dot, file)
                            plot_task2file_edge(dot, task, file)

            print(f"num of task nodes: {num_of_task_nodes}")
            print(f"num of file nodes: {num_of_file_nodes}")
            print(f"num of edges: {num_of_edges}")
            print(f"total nodes: {num_of_task_nodes + num_of_file_nodes}")

            dot.attr(rankdir='TB')
            dot.engine = 'dot'
            dot.render(svg_file_path_without_suffix, format='svg', view=False)

            import time
            print(f"rendering subgraph: {subgraph_id}")
            time_start = time.time()
            dot.render(svg_file_path_without_suffix, format='svg', view=False)
            time_end = time.time()
            print(
                f"rendering subgraph: {subgraph_id} done in {round(time_end - time_start, 4)} seconds")

        data['subgraph_id_list'] = list(runtime_state.subgraphs.keys())
        data['subgraph_num_tasks_list'] = [
            len(subgraph) for subgraph in runtime_state.subgraphs.values()]

        data['subgraph_id'] = subgraph_id
        data['subgraph_num_tasks'] = len(subgraph)
        data['subgraph_svg_content'] = open(svg_file_path, 'r').read()

        return jsonify(data)
    except Exception as e:
        print(f"Error in get_subgraphs: {str(e)}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
