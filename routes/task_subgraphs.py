from .runtime_state import runtime_state, check_and_reload_data
from flask import Blueprint, jsonify, request
import graphviz
import os
from pathlib import Path


task_subgraphs_bp = Blueprint('task_subgraphs', __name__, url_prefix='/api')

@task_subgraphs_bp.route('/task-subgraphs')
@check_and_reload_data()
def get_task_subgraphs():
    try:
        data = {}

        subgraph_id = request.args.get('subgraph_id')
        if not subgraph_id:
            return jsonify({'error': 'Subgraph ID is required'}), 400
        try:
            subgraph_id = int(subgraph_id)
        except Exception:
            return jsonify({'error': 'Invalid subgraph ID'}), 400

        plot_unsuccessful_task = request.args.get('plot_failed_task', 'true').lower() == 'true'
        plot_recovery_task = request.args.get('plot_recovery_task', 'true').lower() == 'true'

        subgraph = runtime_state.subgraphs.get(subgraph_id)
        if not subgraph:
            return jsonify({'error': 'Subgraph not found'}), 404
        task_tries = list(subgraph)

        svg_file_path_without_suffix = os.path.join(
            runtime_state.data_parser.svg_files_dir,
            f'task-subgraph-{subgraph_id}-{plot_unsuccessful_task}-{plot_recovery_task}'
        )
        svg_file_path = f'{svg_file_path_without_suffix}.svg'

        if not Path(svg_file_path).exists():
            dot = graphviz.Digraph()

            def plot_task_node(dot, task):
                node_id = f'{task.task_id}-{task.task_try_id}'
                node_label = f'{task.task_id}'

                if task.when_failure_happens:
                    node_label = f'{node_label} (unsuccessful)'
                    style = 'dashed'
                    color = '#FF0000'
                    fontcolor = '#FF0000'
                else:
                    style = 'solid'
                    color = '#000000'
                    fontcolor = '#000000'

                if task.is_recovery_task:
                    node_label = f'{node_label} (recovery)'
                    style = 'filled,dashed'
                    fillcolor = '#FF69B4'
                else:
                    fillcolor = '#FFFFFF'

                dot.node(node_id, node_label, shape='ellipse', style=style, color=color, fontcolor=fontcolor, fillcolor=fillcolor)

            def plot_file_node(dot, file):
                if len(file.producers) == 0:
                    return
                file_name = file.filename
                dot.node(file_name, file_name, shape='box')

            def plot_task2file_edge(dot, task, file):
                if len(file.producers) == 0:
                    return
                if task.when_failure_happens:
                    return
                else:
                    task_execution_time = task.time_worker_end - task.time_worker_start
                    dot.edge(f'{task.task_id}-{task.task_try_id}', file.filename, label=f'{task_execution_time:.2f}s')

            def plot_file2task_edge(dot, file, task):
                if len(file.producers) == 0:
                    return
                file_creation_time = float('inf')
                for producer_task_id, producer_task_try_id in file.producers:
                    producer_task = runtime_state.tasks[(producer_task_id, producer_task_try_id)]
                    if producer_task.time_worker_end:
                        file_creation_time = min(file_creation_time, producer_task.time_worker_end)
                file_creation_time = file_creation_time - runtime_state.MIN_TIME

                dot.edge(file.filename, f'{task.task_id}-{task.task_try_id}', label=f'{file_creation_time:.2f}s')

            for (tid, try_id) in task_tries:
                task = runtime_state.tasks[(tid, try_id)]
                if task.is_recovery_task and not plot_recovery_task:
                    continue
                if task.when_failure_happens and not plot_unsuccessful_task:
                    continue
                plot_task_node(dot, task)
                for file_name in getattr(task, 'input_files', []):
                    file = runtime_state.files[file_name]
                    plot_file_node(dot, file)
                    plot_file2task_edge(dot, file, task)
                for file_name in getattr(task, 'output_files', []):
                    file = runtime_state.files[file_name]
                    if len(file.transfers) == 0:
                        continue
                    plot_file_node(dot, file)
                    plot_task2file_edge(dot, task, file)
            dot.attr(rankdir='TB')
            dot.engine = 'dot'
            dot.render(svg_file_path_without_suffix, format='svg', view=False)

        data['subgraph_id'] = subgraph_id
        data['num_task_tries'] = len(task_tries)
        data['subgraph_svg_content'] = open(svg_file_path, 'r').read()

        return jsonify(data)
    except Exception as e:
        runtime_state.logger.error(f'Error in get_task_subgraphs: {e}')
        return jsonify({'error': str(e)}), 500
