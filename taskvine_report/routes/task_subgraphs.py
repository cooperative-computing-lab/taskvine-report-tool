from flask import Blueprint, jsonify, request, current_app
import graphviz
import os
from pathlib import Path
from io import StringIO
import csv
from flask import make_response
import json
from taskvine_report.utils import *

task_subgraphs_bp = Blueprint('task_subgraphs', __name__, url_prefix='/api')

def sanitize_filename(filename):
    import re
    # remove or replace characters that might cause issues on different filesystems
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # limit length to avoid filesystem limits
    if len(filename) > 200:
        filename = filename[:200]
    return filename

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

        # Read CSV file
        csv_dir = current_app.config["RUNTIME_STATE"].csv_files_dir
        if not csv_dir:
            return jsonify({'error': 'CSV directory not configured'}), 500

        import pandas as pd
        
        # Read the single CSV file
        df_subgraphs = read_csv_to_fd(os.path.join(csv_dir, 'task_subgraphs.csv'))
        subgraph_tasks = df_subgraphs[df_subgraphs['subgraph_id'] == subgraph_id]
        if subgraph_tasks.empty:
            return jsonify({'error': 'Subgraph not found'}), 404

        # ensure the SVG directory exists
        svg_dir = current_app.config["RUNTIME_STATE"].svg_files_dir
        if not svg_dir:
            return jsonify({'error': 'SVG directory not configured'}), 500
        os.makedirs(svg_dir, exist_ok=True)
        
        # Generate filename for subgraph
        base_filename = f'task-subgraph-{subgraph_id}-{str(plot_unsuccessful_task).lower()}-{str(plot_recovery_task).lower()}'
        safe_filename = sanitize_filename(base_filename)
        svg_file_path_without_suffix = os.path.normpath(os.path.join(svg_dir, safe_filename))
        svg_file_path = f'{svg_file_path_without_suffix}.svg'

        # Check if SVG file already exists
        if Path(svg_file_path).exists():
            current_app.config["RUNTIME_STATE"].logger.info(f"Using existing subgraph {subgraph_id} SVG file")
        else:
            current_app.config["RUNTIME_STATE"].logger.info(f"Generating subgraph {subgraph_id} from CSV")
            
            # check if graphviz is available on the system
            import shutil
            if not shutil.which('dot'):
                error_svg = '<svg xmlns="http://www.w3.org/2000/svg" width="500" height="100"><text x="10" y="30" font-family="Arial" font-size="14">Error: Graphviz not installed. Please install graphviz package.</text><text x="10" y="50" font-family="Arial" font-size="12">Linux: sudo apt-get install graphviz</text><text x="10" y="70" font-family="Arial" font-size="12">macOS: brew install graphviz</text></svg>'
                with open(svg_file_path, 'w') as f:
                    f.write(error_svg)
            else:
                dot = graphviz.Digraph()
                dot.format = 'svg'
                dot.engine = 'dot'
                dot.attr(rankdir='TB')

                # Build task and file data structures from CSV
                tasks_dict = {}
                files_dict = {}
                
                # Process tasks
                for _, task_row in subgraph_tasks.iterrows():
                    task_id = task_row['task_id']
                    
                    # Apply filtering based on parameters
                    is_recovery = bool(task_row.get('is_recovery_task', False))
                    if not plot_recovery_task and is_recovery:
                        continue
                    
                    # Check failure count and apply filtering
                    failure_count = int(task_row.get('failure_count', 0))
                    if not plot_unsuccessful_task and failure_count > 0:
                        continue
                    
                    # Parse input and output files with timing info
                    def parse_files_with_timing(files_str):
                        files_with_timing = []
                        if pd.notna(files_str) and str(files_str).strip():
                            for item in str(files_str).split('|'):
                                if ':' in item:
                                    file_name, timing = item.rsplit(':', 1)
                                    try:
                                        timing = float(timing)
                                    except ValueError:
                                        timing = 0.0
                                    files_with_timing.append((file_name.strip(), timing))
                                else:
                                    files_with_timing.append((item.strip(), 0.0))
                        return files_with_timing
                    
                    input_files = parse_files_with_timing(task_row.get('input_files', ''))
                    output_files = parse_files_with_timing(task_row.get('output_files', ''))
                    
                    # Remove empty strings
                    input_files = [(f, t) for f, t in input_files if f]
                    output_files = [(f, t) for f, t in output_files if f]
                    
                    tasks_dict[task_id] = {
                        'task_id': int(task_id),
                        'is_recovery_task': is_recovery,
                        'failure_count': failure_count,
                        'input_files': input_files,
                        'output_files': output_files
                    }

                # Build file dependencies from task data
                for task_data in tasks_dict.values():
                    # Add files to files_dict
                    for file_name, _ in task_data['input_files'] + task_data['output_files']:
                        if file_name not in files_dict:
                            files_dict[file_name] = {
                                'filename': file_name,
                                'producers': [],
                                'consumers': []
                            }
                    
                    # Add task as producer for output files
                    for file_name, _ in task_data['output_files']:
                        if task_data['task_id'] not in files_dict[file_name]['producers']:
                            files_dict[file_name]['producers'].append(task_data['task_id'])
                    
                    # Add task as consumer for input files  
                    for file_name, _ in task_data['input_files']:
                        if task_data['task_id'] not in files_dict[file_name]['consumers']:
                            files_dict[file_name]['consumers'].append(task_data['task_id'])

                # Plot nodes and edges
                def plot_task_node(dot, task_data):
                    task_id = task_data['task_id']
                    node_id = str(task_id)
                    node_label = str(task_id)

                    style = 'solid'
                    color = '#000000'
                    fontcolor = '#000000'
                    fillcolor = '#FFFFFF'

                    if task_data['is_recovery_task']:
                        node_label = f'{node_label} (recovery)'
                        style = 'filled,dashed'
                        fillcolor = '#FF69B4'

                    dot.node(node_id, node_label, shape='ellipse', style=style, color=color, fontcolor=fontcolor, fillcolor=fillcolor)

                def plot_file_node(dot, file_data):
                    if not file_data['producers']:
                        return
                    filename = file_data['filename']
                    dot.node(filename, filename, shape='box')

                def plot_task2file_edge(dot, task_data, file_data, timing=None):
                    if not file_data['producers']:
                        return
                    label = f"{timing:.2f}s" if timing is not None else ""
                    dot.edge(str(task_data['task_id']), file_data['filename'], label=label)

                def plot_file2task_edge(dot, file_data, task_data, timing=None):
                    if not file_data['producers']:
                        return
                    label = f"{timing:.2f}s" if timing is not None else ""
                    dot.edge(file_data['filename'], str(task_data['task_id']), label=label)

                # Add all tasks and files to the graph
                for task_id, task_data in tasks_dict.items():
                    plot_task_node(dot, task_data)
                    
                    # Add input file edges with waiting times
                    for file_name, waiting_time in task_data['input_files']:
                        if file_name in files_dict and files_dict[file_name]['producers']:
                            plot_file_node(dot, files_dict[file_name])
                            plot_file2task_edge(dot, files_dict[file_name], task_data, waiting_time)
                    
                    # Add output file edges with creation times
                    for file_name, creation_time in task_data['output_files']:
                        if file_name in files_dict and files_dict[file_name]['producers']:
                            plot_file_node(dot, files_dict[file_name])
                            plot_task2file_edge(dot, task_data, files_dict[file_name], creation_time)
                
                # Generate SVG
                svg_generated = False
                error_message = ""
                
                try:
                    dot.render(svg_file_path_without_suffix, format='svg', view=False, cleanup=True)
                    
                    if Path(svg_file_path).exists() and Path(svg_file_path).stat().st_size > 0:
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
                
                if not svg_generated:
                    fallback_svg = f'<svg xmlns="http://www.w3.org/2000/svg" width="500" height="150"><rect width="500" height="150" fill="#f8f9fa" stroke="#dee2e6"/><text x="10" y="30" font-family="Arial" font-size="14" fill="#dc3545">Error: Failed to generate subgraph visualization</text><text x="10" y="50" font-family="Arial" font-size="12" fill="#6c757d">Reason: {error_message}</text><text x="10" y="80" font-family="Arial" font-size="12" fill="#6c757d">Subgraph ID: {subgraph_id}</text><text x="10" y="100" font-family="Arial" font-size="12" fill="#6c757d">Tasks: {len(subgraph_tasks)}</text><text x="10" y="130" font-family="Arial" font-size="10" fill="#6c757d">CSV-based rendering</text></svg>'
                    with open(svg_file_path, 'w', encoding='utf-8') as f:
                        f.write(fallback_svg)

        data['subgraph_id'] = int(subgraph_id)
        data['num_task_tries'] = int(len(subgraph_tasks))
        
        # Read and return SVG content
        if Path(svg_file_path).exists():
            with open(svg_file_path, 'r', encoding='utf-8') as f:
                svg_content = f.read().strip()
                if svg_content and (svg_content.startswith('<?xml') or svg_content.startswith('<svg')):
                    data['subgraph_svg_content'] = svg_content
                else:
                    data['subgraph_svg_content'] = '<svg xmlns="http://www.w3.org/2000/svg" width="400" height="100"><text x="10" y="30" font-family="Arial" font-size="14">Error: Invalid SVG content generated</text></svg>'
        else:
            data['subgraph_svg_content'] = '<svg xmlns="http://www.w3.org/2000/svg" width="400" height="100"><text x="10" y="30" font-family="Arial" font-size="14">Error: SVG file could not be generated</text></svg>'

        # Generate legend from CSV data
        unique_subgraphs = df_subgraphs['subgraph_id'].unique()
        data['legend'] = [
            {
                'id': str(int(sg_id)),
                'label': f"Subgraph {int(sg_id)} ({len(df_subgraphs[df_subgraphs['subgraph_id'] == sg_id])} task{'s' if len(df_subgraphs[df_subgraphs['subgraph_id'] == sg_id]) != 1 else ''})",
                'color': '',
                'checked': bool(int(sg_id) == int(subgraph_id))
            }
            for sg_id in sorted(unique_subgraphs)
        ]

        # Test JSON serialization before returning
        try:
            json.dumps(data)
        except TypeError as e:
            current_app.config["RUNTIME_STATE"].logger.error(f"JSON serialization test failed: {e}")
            current_app.config["RUNTIME_STATE"].logger.error(f"Data keys: {list(data.keys())}")
            for key, value in data.items():
                try:
                    json.dumps({key: value})
                except TypeError as sub_e:
                    current_app.config["RUNTIME_STATE"].logger.error(f"Field '{key}' serialization failed: {sub_e}, type: {type(value)}")

        return jsonify(data)
    except Exception as e:
        current_app.config["RUNTIME_STATE"].logger.error(f'Error in get_task_subgraphs: {e}')
        return jsonify({'error': str(e)}), 500
