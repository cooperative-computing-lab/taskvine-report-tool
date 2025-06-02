from flask import Blueprint, jsonify, request, current_app
import graphviz
import os
from pathlib import Path
from io import StringIO
import csv
from flask import make_response
import hashlib
import json
from .utils import *

task_subgraphs_bp = Blueprint('task_subgraphs', __name__, url_prefix='/api')

def sanitize_filename(filename):
    import re
    # remove or replace characters that might cause issues on different filesystems
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # limit length to avoid filesystem limits
    if len(filename) > 200:
        filename = filename[:200]
    return filename

def calculate_subgraph_hash(subgraph_id, task_tries, plot_unsuccessful_task, plot_recovery_task):
    hash_data = {
        'subgraph_id': subgraph_id,
        'plot_unsuccessful_task': plot_unsuccessful_task,
        'plot_recovery_task': plot_recovery_task,
        'tasks': []
    }
    
    # Add task information to hash data
    for (tid, try_id) in sorted(task_tries):
        if (tid, try_id) not in current_app.config["RUNTIME_STATE"].tasks:
            continue
        task = current_app.config["RUNTIME_STATE"].tasks[(tid, try_id)]
        
        task_info = {
            'task_id': tid,
            'task_try_id': try_id,
            'is_recovery_task': getattr(task, 'is_recovery_task', False),
            'when_failure_happens': getattr(task, 'when_failure_happens', False),
            'input_files': sorted(list(getattr(task, 'input_files', []))),
            'output_files': sorted(list(getattr(task, 'output_files', []))),
            'time_worker_start': getattr(task, 'time_worker_start', None),
            'time_worker_end': getattr(task, 'time_worker_end', None)
        }
        hash_data['tasks'].append(task_info)
    
    # Add file information that affects the graph
    hash_data['files'] = {}
    for (tid, try_id) in task_tries:
        if (tid, try_id) not in current_app.config["RUNTIME_STATE"].tasks:
            continue
        task = current_app.config["RUNTIME_STATE"].tasks[(tid, try_id)]
        
        # Handle both list and set types for input_files and output_files
        input_files = getattr(task, 'input_files', [])
        output_files = getattr(task, 'output_files', [])
        
        # Convert to lists if they are sets, then combine
        if isinstance(input_files, set):
            input_files = list(input_files)
        if isinstance(output_files, set):
            output_files = list(output_files)
            
        all_files = input_files + output_files
        
        for file_name in all_files:
            if file_name in current_app.config["RUNTIME_STATE"].files and file_name not in hash_data['files']:
                file = current_app.config["RUNTIME_STATE"].files[file_name]
                hash_data['files'][file_name] = {
                    'filename': file.filename,
                    'producers': sorted(getattr(file, 'producers', [])),
                    'num_transfers': len(getattr(file, 'transfers', []))
                }
    
    # Convert to JSON string and hash
    json_str = json.dumps(hash_data, sort_keys=True, default=str)
    return hashlib.sha256(json_str.encode('utf-8')).hexdigest()

def check_subgraph_cache(svg_file_path, current_hash):
    """Check if cached subgraph is still valid based on hash"""
    hash_file_path = svg_file_path.replace('.svg', '.hash')
    
    # Check if both SVG and hash files exist
    if not (Path(svg_file_path).exists() and Path(hash_file_path).exists()):
        return False
    
    try:
        with open(hash_file_path, 'r') as f:
            cached_hash = f.read().strip()
        return cached_hash == current_hash
    except Exception:
        return False

def save_subgraph_hash(svg_file_path, current_hash):
    """Save the hash for the generated subgraph"""
    hash_file_path = svg_file_path.replace('.svg', '.hash')
    try:
        with open(hash_file_path, 'w') as f:
            f.write(current_hash)
    except Exception as e:
        current_app.config["RUNTIME_STATE"].logger.warning(f"Failed to save subgraph hash: {e}")

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

        subgraph = current_app.config["RUNTIME_STATE"].subgraphs.get(subgraph_id)
        if not subgraph:
            return jsonify({'error': 'Subgraph not found'}), 404
        task_tries = list(subgraph)

        # ensure the SVG directory exists
        svg_dir = current_app.config["RUNTIME_STATE"].data_parser.svg_files_dir
        if not svg_dir:
            return jsonify({'error': 'SVG directory not configured'}), 500
        os.makedirs(svg_dir, exist_ok=True)
        
        # use a safer filename without special characters that might cause I/O issues
        # ensure filename is safe across different filesystems
        base_filename = f'task-subgraph-{subgraph_id}-{str(plot_unsuccessful_task).lower()}-{str(plot_recovery_task).lower()}'
        safe_filename = sanitize_filename(base_filename)
        # normalize path for cross-platform compatibility
        svg_file_path_without_suffix = os.path.normpath(os.path.join(svg_dir, safe_filename))
        svg_file_path = f'{svg_file_path_without_suffix}.svg'

        # Calculate hash for current subgraph configuration
        current_hash = calculate_subgraph_hash(subgraph_id, task_tries, plot_unsuccessful_task, plot_recovery_task)
        
        # Check if we can use cached version
        use_cache = check_subgraph_cache(svg_file_path, current_hash)
        
        if not use_cache:
            current_app.config["RUNTIME_STATE"].logger.info(f"Generating new subgraph {subgraph_id} (hash mismatch or missing cache)")
            # check if graphviz is available on the system
            import shutil
            if not shutil.which('dot'):
                # graphviz not found, return error SVG
                error_svg = '<svg xmlns="http://www.w3.org/2000/svg" width="500" height="100"><text x="10" y="30" font-family="Arial" font-size="14">Error: Graphviz not installed. Please install graphviz package.</text><text x="10" y="50" font-family="Arial" font-size="12">Linux: sudo apt-get install graphviz</text><text x="10" y="70" font-family="Arial" font-size="12">macOS: brew install graphviz</text></svg>'
                with open(svg_file_path, 'w') as f:
                    f.write(error_svg)
            else:
                dot = graphviz.Digraph()
                # set explicit format and engine for better cross-platform compatibility
                dot.format = 'svg'
                dot.engine = 'dot'

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
                        producer_task = current_app.config["RUNTIME_STATE"].tasks[(producer_task_id, producer_task_try_id)]
                        if producer_task.time_worker_end:
                            file_creation_time = min(file_creation_time, producer_task.time_worker_end)
                    file_creation_time = file_creation_time - current_app.config["RUNTIME_STATE"].MIN_TIME

                    dot.edge(file.filename, f'{task.task_id}-{task.task_try_id}', label=f'{file_creation_time:.2f}s')

                # set graph attributes before adding nodes and edges
                dot.attr(rankdir='TB')

                # add all tasks, files, and edges to the graph
                for (tid, try_id) in task_tries:
                    task = current_app.config["RUNTIME_STATE"].tasks[(tid, try_id)]
                    if task.is_recovery_task and not plot_recovery_task:
                        continue
                    if task.when_failure_happens and not plot_unsuccessful_task:
                        continue
                    plot_task_node(dot, task)
                    for file_name in getattr(task, 'input_files', []):
                        file = current_app.config["RUNTIME_STATE"].files[file_name]
                        plot_file_node(dot, file)
                        plot_file2task_edge(dot, file, task)
                    for file_name in getattr(task, 'output_files', []):
                        file = current_app.config["RUNTIME_STATE"].files[file_name]
                        if len(file.transfers) == 0:
                            continue
                        plot_file_node(dot, file)
                        plot_task2file_edge(dot, task, file)
                
                # generate SVG with better error handling for cross-platform compatibility
                svg_generated = False
                error_message = ""
                
                try:
                    # attempt to render the SVG
                    dot.render(svg_file_path_without_suffix, format='svg', view=False, cleanup=True)
                    
                    # verify the generated file is valid
                    if Path(svg_file_path).exists() and Path(svg_file_path).stat().st_size > 0:
                        # check if the content is actually valid SVG
                        with open(svg_file_path, 'r') as f:
                            content = f.read().strip()
                            if content and (content.startswith('<?xml') or content.startswith('<svg')):
                                svg_generated = True
                            else:
                                error_message = "Generated file is not valid SVG"
                    else:
                        error_message = "SVG file was not generated or is empty"
                except Exception as e:
                    error_message = f"Exception during SVG generation: {str(e)}"
                
                # if generation failed, create a fallback SVG
                if not svg_generated:
                    fallback_svg = f'<svg xmlns="http://www.w3.org/2000/svg" width="500" height="150"><rect width="500" height="150" fill="#f8f9fa" stroke="#dee2e6"/><text x="10" y="30" font-family="Arial" font-size="14" fill="#dc3545">Error: Failed to generate subgraph visualization</text><text x="10" y="50" font-family="Arial" font-size="12" fill="#6c757d">Reason: {error_message}</text><text x="10" y="80" font-family="Arial" font-size="12" fill="#6c757d">Subgraph ID: {subgraph_id}</text><text x="10" y="100" font-family="Arial" font-size="12" fill="#6c757d">Tasks: {len(task_tries)}</text><text x="10" y="130" font-family="Arial" font-size="10" fill="#6c757d">Please check graphviz installation and permissions</text></svg>'
                    with open(svg_file_path, 'w', encoding='utf-8') as f:
                        f.write(fallback_svg)
                
                save_subgraph_hash(svg_file_path, current_hash)
        else:
            current_app.config["RUNTIME_STATE"].logger.info(f"Using cached subgraph {subgraph_id} (hash match)")

        data['subgraph_id'] = subgraph_id
        data['num_task_tries'] = len(task_tries)
        
        # check if SVG file exists and has valid content before reading
        if Path(svg_file_path).exists():
            with open(svg_file_path, 'r', encoding='utf-8') as f:
                svg_content = f.read().strip()
                # check if the content is valid SVG (starts with <?xml or <svg)
                if svg_content and (svg_content.startswith('<?xml') or svg_content.startswith('<svg')):
                    data['subgraph_svg_content'] = svg_content
                else:
                    data['subgraph_svg_content'] = '<svg xmlns="http://www.w3.org/2000/svg" width="400" height="100"><text x="10" y="30" font-family="Arial" font-size="14">Error: Invalid SVG content generated</text></svg>'
        else:
            data['subgraph_svg_content'] = '<svg xmlns="http://www.w3.org/2000/svg" width="400" height="100"><text x="10" y="30" font-family="Arial" font-size="14">Error: SVG file could not be generated</text></svg>'

        # legend: list of {'id': str(idx), 'label': ..., 'color': ..., 'checked': bool}, sorted by idx
        data['legend'] = [
            {
                'id': str(idx),
                'label': f"Subgraph {idx} ({count} task{'s' if count != 1 else ''})",
                'color': '',
                'checked': False
            }
            for idx, count in sorted(
                [(k, len(v)) for k, v in current_app.config["RUNTIME_STATE"].subgraphs.items()],
                key=lambda x: x[0]
            )
        ]
        # check the current subgraph (subgraph_id is 1-based)
        if subgraph_id <= len(data['legend']) and subgraph_id > 0:
            data['legend'][subgraph_id - 1]['checked'] = True

        return jsonify(data)
    except Exception as e:
        current_app.config["RUNTIME_STATE"].logger.error(f'Error in get_task_subgraphs: {e}')
        return jsonify({'error': str(e)}), 500


@task_subgraphs_bp.route('/task-subgraphs/export-csv')
@check_and_reload_data()
def export_task_subgraph_csv():
    try:
        rows = []

        for (tid, try_id), task in current_app.config["RUNTIME_STATE"].tasks.items():
            dependent_ids = set()
            for file_name in task.output_files:
                file = current_app.config["RUNTIME_STATE"].files.get(file_name)
                if not file:
                    continue
                for consumer_task_id, _ in file.consumers:
                    dependent_ids.add(consumer_task_id)

            rows.append([tid, ' '.join(str(did) for did in sorted(dependent_ids))])

        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['task_id', 'dependent_task_ids'])
        writer.writerows(rows)
        output.seek(0)

        response = make_response(output.getvalue())
        response.headers["Content-Disposition"] = "attachment; filename=task_subgraphs.csv"
        response.headers["Content-Type"] = "text/csv"
        return response

    except Exception as e:
        current_app.config["RUNTIME_STATE"].logger.error(f'Error in export_task_subgraph_csv: {e}')
        return jsonify({'error': str(e)}), 500
