from flask import Blueprint, jsonify, request, current_app
import graphviz
import os
from pathlib import Path
from io import StringIO
import csv
from flask import make_response
import json
import shutil
from taskvine_report.utils import *
import re
import pandas as pd
import time

task_subgraphs_bp = Blueprint('task_subgraphs', __name__, url_prefix='/api')

def create_response(legend=None, subgraph_id=0, num_task_tries=0, svg_content='', error=None, status_code=200):
    response_data = {}
    
    if legend is not None:
        response_data['legend'] = legend
    
    response_data.update({
        'subgraph_id': subgraph_id,
        'num_task_tries': num_task_tries,
        'subgraph_svg_content': svg_content
    })
    
    if error:
        response_data['error'] = error
        if status_code == 200:  # if no specific status code provided for error, use 404
            status_code = 404
    
    response = make_response(jsonify(response_data), status_code)
    return response

def sanitize_filename(filename):
    # remove or replace characters that might cause issues on different filesystems
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # limit length to avoid filesystem limits
    if len(filename) > 200:
        filename = filename[:200]
    return filename

def generate_legend(df_subgraphs, selected_subgraph_id=None):
    unique_subgraphs = df_subgraphs['subgraph_id'].unique()
    return [
        {
            'id': str(int(sg_id)),
            'label': f"Subgraph {int(sg_id)} ({len(df_subgraphs[df_subgraphs['subgraph_id'] == sg_id])} task{'s' if len(df_subgraphs[df_subgraphs['subgraph_id'] == sg_id]) != 1 else ''})",
            'color': '',
            'checked': bool(selected_subgraph_id and int(sg_id) == int(selected_subgraph_id))
        }
        for sg_id in sorted(unique_subgraphs)
    ]

def find_subgraph_by_filename(df_subgraphs, filename):
    if not filename:
        return None
    
    filename = filename.strip()
    if not filename:
        return None
    
    # search in both input_files and output_files columns
    for _, row in df_subgraphs.iterrows():
        subgraph_id = row['subgraph_id']
        
        # check input files
        input_files_str = row.get('input_files', '')
        if pd.notna(input_files_str) and str(input_files_str).strip():
            input_files = parse_files_with_timing(input_files_str)
            for file_name, _ in input_files:
                if filename in file_name:  # substring match
                    return int(subgraph_id)
        
        # check output files
        output_files_str = row.get('output_files', '')
        if pd.notna(output_files_str) and str(output_files_str).strip():
            output_files = parse_files_with_timing(output_files_str)
            for file_name, _ in output_files:
                if filename in file_name:  # substring match
                    return int(subgraph_id)
    
    return None

def find_subgraph_by_task_id(df_subgraphs, task_id):
    if not task_id:
        return None
    
    try:
        task_id = int(task_id)
    except (ValueError, TypeError):
        return None
    
    # search in task_id column - ensure both sides are int for comparison
    for _, row in df_subgraphs.iterrows():
        try:
            row_task_id = int(row['task_id'])
            if row_task_id == task_id:
                return int(row['subgraph_id'])
        except (ValueError, TypeError):
            continue  # skip invalid task_id entries
    
    return None

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
                file_name = file_name.strip()
                if file_name:  # filter empty names here
                    files_with_timing.append((file_name, timing))
            else:
                file_name = item.strip()
                if file_name:  # filter empty names here
                    files_with_timing.append((file_name, 0.0))
    return files_with_timing

def build_tasks_and_files(subgraph_tasks):
    tasks_dict = {}
    files_dict = {}
    
    for _, task_row in subgraph_tasks.iterrows():
        task_id = task_row['task_id']
        failure_count = int(task_row.get('failure_count', 0))
        recovery_count = int(task_row.get('recovery_count', 0))
        
        # get task execution time
        task_execution_time = task_row.get('task_execution_time', None)
        if pd.notna(task_execution_time):
            try:
                task_execution_time = float(task_execution_time)
            except (ValueError, TypeError):
                task_execution_time = None
        else:
            task_execution_time = None
        
        input_files = parse_files_with_timing(task_row.get('input_files', ''))
        output_files = parse_files_with_timing(task_row.get('output_files', ''))

        tasks_dict[task_id] = {
            'task_id': int(task_id),
            'failure_count': failure_count,
            'recovery_count': recovery_count,
            'task_execution_time': task_execution_time,
            'input_files': input_files,
            'output_files': output_files
        }

    # build file dependencies from task data
    for task_data in tasks_dict.values():
        for file_name, _ in task_data['input_files'] + task_data['output_files']:
            if file_name not in files_dict:
                files_dict[file_name] = {
                    'filename': file_name,
                    'producers': [],
                    'consumers': []
                }
        
        for file_name, _ in task_data['output_files']:
            if task_data['task_id'] not in files_dict[file_name]['producers']:
                files_dict[file_name]['producers'].append(task_data['task_id'])
        
        for file_name, _ in task_data['input_files']:
            if task_data['task_id'] not in files_dict[file_name]['consumers']:
                files_dict[file_name]['consumers'].append(task_data['task_id'])

    return tasks_dict, files_dict

def plot_task_graph(dot, tasks_dict, files_dict, params_dict=None):
    if params_dict is None:
        params_dict = {}

    label_file_waiting_time = params_dict.get('label_file_waiting_time', False)
    show_failed_count = params_dict.get('show_failed_count', False)
    show_recovery_count = params_dict.get('show_recovery_count', False)

    # plot all task nodes
    for task_data in tasks_dict.values():
        task_id = str(task_data['task_id'])
        
        # create node label
        node_label = task_id
        if show_failed_count:
            failure_count = task_data.get('failure_count', 0)
            if failure_count and failure_count > 0:
                node_label = f"{task_id} (Failure: {failure_count})"
        
        dot.node(task_id, node_label, shape='ellipse', style='solid', color='#000000', fontcolor='#000000', fillcolor='#FFFFFF')
    
    # plot file nodes and edges
    plotted_files = set()
    for task_data in tasks_dict.values():
        # input file edges (file -> task)
        for file_name, waiting_time in task_data['input_files']:
            if file_name in files_dict and files_dict[file_name]['producers']:
                if file_name not in plotted_files:
                    dot.node(file_name, file_name, shape='box')
                    plotted_files.add(file_name)
                
                # use waiting time label based on parameter
                if label_file_waiting_time and waiting_time is not None:
                    label = f"{waiting_time:.2f}s"
                else:
                    label = ""
                dot.edge(file_name, str(task_data['task_id']), label=label)
        
        # output file edges (task -> file)
        for file_name, _ in task_data['output_files']:
            if file_name in files_dict and files_dict[file_name]['producers']:
                if file_name not in plotted_files:
                    # create file node label with recovery count if enabled
                    file_label = file_name
                    if show_recovery_count:
                        recovery_count = task_data.get('recovery_count', 0)
                        if recovery_count and recovery_count > 0:
                            file_label = f"{file_name} (Recovery: {recovery_count})"
                    
                    dot.node(file_name, file_label, shape='box')
                    plotted_files.add(file_name)
                
                # use task execution time as edge label
                execution_time = task_data.get('task_execution_time')
                if execution_time is not None:
                    label = f"{execution_time:.2f}s"
                else:
                    label = ""
                dot.edge(str(task_data['task_id']), file_name, label=label)

def generate_error_svg(message, subgraph_id=None, task_count=None):
    lines = [
        f'<text x="10" y="30" font-family="Arial" font-size="14" fill="#dc3545">Error: {message}</text>'
    ]
    if subgraph_id is not None:
        lines.append(f'<text x="10" y="50" font-family="Arial" font-size="12" fill="#6c757d">Subgraph ID: {subgraph_id}</text>')
    if task_count is not None:
        lines.append(f'<text x="10" y="70" font-family="Arial" font-size="12" fill="#6c757d">Tasks: {task_count}</text>')
    
    return f'<svg xmlns="http://www.w3.org/2000/svg" width="500" height="150"><rect width="500" height="150" fill="#f8f9fa" stroke="#dee2e6"/>{"".join(lines)}</svg>'

def read_valid_svg(svg_file_path):
    if not Path(svg_file_path).exists():
        return None
    with open(svg_file_path, 'r', encoding='utf-8') as f:
        content = f.read().strip()
        if content and (content.startswith('<?xml') or content.startswith('<svg')):
            return content
    return None

def generate_subgraph_metadata(subgraph_tasks, subgraph_id):
    tasks_dict, files_dict = build_tasks_and_files(subgraph_tasks)
    
    # prepare tasks metadata (structure only, ensure consistent types)
    tasks_metadata = []
    for task_data in tasks_dict.values():
        tasks_metadata.append({
            'task_id': int(task_data['task_id']),  # ensure int type
            'input_files': sorted([str(name) for name, _ in task_data['input_files']]),  # ensure string type and sorted for consistency
            'output_files': sorted([str(name) for name, _ in task_data['output_files']])  # ensure string type and sorted for consistency
        })
    
    # sort tasks by task_id for consistent ordering
    tasks_metadata.sort(key=lambda x: x['task_id'])
    
    # prepare files metadata (structure only, ensure consistent types)
    files_metadata = []
    for file_data in files_dict.values():
        files_metadata.append({
            'filename': str(file_data['filename']),  # ensure string type
            'producers': sorted([int(p) for p in file_data['producers']]),  # ensure int type and sorted
            'consumers': sorted([int(c) for c in file_data['consumers']])   # ensure int type and sorted
        })
    
    # sort files by filename for consistent ordering
    files_metadata.sort(key=lambda x: x['filename'])
    
    metadata = {
        'subgraph_id': int(subgraph_id),
        'generated_timestamp': time.time(),
        'num_tasks': len(tasks_metadata),
        'num_files': len(files_metadata),
        'tasks': tasks_metadata,
        'files': files_metadata
    }
    
    return metadata

def write_metadata(metadata, metadata_file_path):
    with open(metadata_file_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

def load_metadata(metadata_file_path):
    if not Path(metadata_file_path).exists():
        return None
    
    with open(metadata_file_path, 'r', encoding='utf-8') as f:
        metadata = json.load(f)
    
    # basic validation
    required_fields = ['subgraph_id', 'num_tasks', 'num_files', 'tasks', 'files']
    for field in required_fields:
        if field not in metadata:
            return None
    
    return metadata

def compare_metadata_structure(metadata1, metadata2):
    if not metadata1 or not metadata2:
        return False
    
    try:
        if (metadata1['subgraph_id'] != metadata2['subgraph_id'] or
            metadata1['num_tasks'] != metadata2['num_tasks'] or
            metadata1['num_files'] != metadata2['num_files']):
            return False
        
        # compare task structures
        tasks1 = {task['task_id']: task for task in metadata1['tasks']}
        tasks2 = {task['task_id']: task for task in metadata2['tasks']}
        
        if set(tasks1.keys()) != set(tasks2.keys()):
            return False
        
        for task_id in tasks1:
            task1, task2 = tasks1[task_id], tasks2[task_id]
            if (task1['input_files'] != task2['input_files'] or
                task1['output_files'] != task2['output_files']):
                return False
        
        # compare file structures
        files1 = {f['filename']: f for f in metadata1['files']}
        files2 = {f['filename']: f for f in metadata2['files']}
        
        if set(files1.keys()) != set(files2.keys()):
            return False
        
        for filename in files1:
            file1, file2 = files1[filename], files2[filename]
            if (file1['producers'] != file2['producers'] or
                file1['consumers'] != file2['consumers']):
                return False
        
        return True
    except (KeyError, TypeError, ValueError) as e:
        current_app.config["RUNTIME_STATE"].logger.info(f"Metadata structure comparison error: {e}")
        return False

def validate_metadata_against_current_data(metadata, subgraph_tasks):
    if not metadata:
        return False
    
    try:
        current_metadata = generate_subgraph_metadata(subgraph_tasks, metadata['subgraph_id'])
        
        if not compare_metadata_structure(metadata, current_metadata):
            current_app.config["RUNTIME_STATE"].logger.info(f"Metadata structure mismatch for subgraph {metadata['subgraph_id']}")
            return False
        
        return True
    except Exception as e:
        current_app.config["RUNTIME_STATE"].logger.info(f"Metadata validation error: {e}")
        return False

def render_svg(subgraph_tasks, svg_file_path, params_dict=None):
    if params_dict is None:
        params_dict = {}

    use_cached = params_dict.get('use_cached_svg', False)
    metadata_file_path = svg_file_path.replace('.svg', '.metadata.json')
    
    # check cache if enabled
    if use_cached:
        svg_content = read_valid_svg(svg_file_path)
        if svg_content:
            metadata = load_metadata(metadata_file_path)
            if metadata and validate_metadata_against_current_data(metadata, subgraph_tasks):
                current_app.config["RUNTIME_STATE"].logger.info(f"Using cached SVG for {svg_file_path}")
                return svg_content
    
    # generate new SVG
    current_app.config["RUNTIME_STATE"].logger.info(f"Cache miss for {svg_file_path}, generating a new SVG")
    if not shutil.which('dot'):
        error_svg = generate_error_svg("Graphviz not installed. Please install graphviz package.")
        with open(svg_file_path, 'w') as f:
            f.write(error_svg)
        return error_svg

    tasks_dict, files_dict = build_tasks_and_files(subgraph_tasks)
    
    dot = graphviz.Digraph()
    dot.format = 'svg'
    dot.engine = 'dot'
    dot.attr(rankdir='TB')

    plot_task_graph(dot, tasks_dict, files_dict, params_dict)

    svg_file_path_without_suffix = svg_file_path.rsplit('.', 1)[0]
    try:
        dot.render(svg_file_path_without_suffix, format='svg', view=False, cleanup=True)

        # validate generated svg
        if not (Path(svg_file_path).exists() and Path(svg_file_path).stat().st_size > 0):
            raise Exception("SVG file was not generated or is empty")
        
        content = read_valid_svg(svg_file_path)
        if not content:
            raise Exception("Generated file is not valid SVG")
        
        # generate and save metadata after successful SVG generation
        subgraph_id = subgraph_tasks.iloc[0]['subgraph_id'] if not subgraph_tasks.empty else 0
        metadata = generate_subgraph_metadata(subgraph_tasks, subgraph_id)
        write_metadata(metadata, metadata_file_path)

        return content
            
    except Exception as e:
        fallback_svg = generate_error_svg(f"SVG generation failed: {str(e)}")
        with open(svg_file_path, 'w', encoding='utf-8') as f:
            f.write(fallback_svg)
        return fallback_svg

@task_subgraphs_bp.route('/task-subgraphs')
@check_and_reload_data()
def get_task_subgraphs():
    try:
        # parse arguments
        subgraph_id = request.args.get('subgraph_id')
        filename = request.args.get('filename')
        task_id = request.args.get('task_id')
        show_failed_count = request.args.get('show_failed_count', 'false').lower() == 'true'
        show_recovery_count = request.args.get('show_recovery_count', 'false').lower() == 'true'
        
        if not subgraph_id:
            return create_response(error='Subgraph ID is required', status_code=400)
        try:
            subgraph_id = int(subgraph_id)
        except Exception:
            return create_response(error='Invalid subgraph ID', status_code=400)

        # read df_subgraphs
        csv_dir = current_app.config["RUNTIME_STATE"].csv_files_dir
        if not csv_dir:
            return create_response(error='CSV directory not configured', status_code=500)

        df_subgraphs = read_csv_to_fd(os.path.join(csv_dir, 'task_subgraphs.csv'))
        
        # handle filename search when subgraph_id=0 and filename is provided
        if subgraph_id == 0 and filename:
            current_app.config["RUNTIME_STATE"].logger.info(f"Searching for filename pattern '{filename}' (substring match) in {len(df_subgraphs)} tasks across {len(df_subgraphs['subgraph_id'].unique())} subgraphs")
            found_subgraph_id = find_subgraph_by_filename(df_subgraphs, filename.strip())
            if found_subgraph_id:
                subgraph_id = found_subgraph_id
                current_app.config["RUNTIME_STATE"].logger.info(f"Found filename pattern '{filename}' in subgraph {subgraph_id}")
            else:
                current_app.config["RUNTIME_STATE"].logger.warning(f"Filename pattern '{filename}' not found")
                return create_response(
                    legend=generate_legend(df_subgraphs),
                    error=f"Filename pattern '{filename}' not found in any subgraph"
                )
        
        # handle task_id search when subgraph_id=0 and task_id is provided
        if subgraph_id == 0 and task_id:
            current_app.config["RUNTIME_STATE"].logger.info(f"Searching for task ID '{task_id}' (exact match) in {len(df_subgraphs)} tasks across {len(df_subgraphs['subgraph_id'].unique())} subgraphs")
            found_subgraph_id = find_subgraph_by_task_id(df_subgraphs, task_id.strip())
            if found_subgraph_id:
                subgraph_id = found_subgraph_id
                current_app.config["RUNTIME_STATE"].logger.info(f"Found task ID '{task_id}' in subgraph {subgraph_id}")
            else:
                current_app.config["RUNTIME_STATE"].logger.warning(f"Task ID '{task_id}' not found")
                return create_response(
                    legend=generate_legend(df_subgraphs),
                    error=f"Task ID '{task_id}' not found in any subgraph"
                )
        
        # get tasks for subgraph_id
        if subgraph_id == 0:
            return create_response(legend=generate_legend(df_subgraphs))

        subgraph_tasks = df_subgraphs[df_subgraphs['subgraph_id'] == subgraph_id]
        if subgraph_tasks.empty:
            return create_response(
                legend=generate_legend(df_subgraphs),
                error='Subgraph not found'
            )

        # setup svg path
        svg_dir = current_app.config["RUNTIME_STATE"].svg_files_dir
        if not svg_dir:
            return create_response(error='SVG directory not configured', status_code=500)
        os.makedirs(svg_dir, exist_ok=True)
        
        # include both show_failed_count and show_recovery_count in filename to ensure different cache files for different states
        base_filename = f'task-subgraph-{subgraph_id}-failed-{show_failed_count}-recovery-{show_recovery_count}'
        safe_filename = sanitize_filename(base_filename)
        svg_file_path = os.path.join(svg_dir, f'{safe_filename}.svg')
        metadata_file_path = os.path.join(svg_dir, f'{safe_filename}.metadata.json')

        # manually specify plotting parameters
        plot_params = {
            'label_file_waiting_time': False,
            'use_cached_svg': True,
            'show_failed_count': show_failed_count,
            'show_recovery_count': show_recovery_count,
        }
        
        # render SVG (with cache check if enabled)
        svg_content = render_svg(subgraph_tasks, svg_file_path, plot_params)

        if not svg_content:
            svg_content = generate_error_svg("SVG file could not be generated")

        return create_response(
            legend=generate_legend(df_subgraphs, subgraph_id),
            subgraph_id=int(subgraph_id),
            num_task_tries=int(len(subgraph_tasks)),
            svg_content=svg_content
        )
    except Exception as e:
        current_app.config["RUNTIME_STATE"].logger.error(f'Error in get_task_subgraphs: {e}')
        return create_response(error=str(e), status_code=500)
