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

task_subgraphs_bp = Blueprint('task_subgraphs', __name__, url_prefix='/api')

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

def parse_files_with_timing(files_str):
    """parse file string and filter empty names"""
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
    """build tasks_dict and files_dict from csv data"""
    tasks_dict = {}
    files_dict = {}
    
    for _, task_row in subgraph_tasks.iterrows():
        task_id = task_row['task_id']
        failure_count = int(task_row.get('failure_count', 0))
        
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
    
    # plot all task nodes
    for task_data in tasks_dict.values():
        task_id = str(task_data['task_id'])
        dot.node(task_id, task_id, shape='ellipse', style='solid', 
                color='#000000', fontcolor='#000000', fillcolor='#FFFFFF')
    
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
                    dot.node(file_name, file_name, shape='box')
                    plotted_files.add(file_name)
                
                # use task execution time as label
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

def read_valid_svg(path):
    if not Path(path).exists():
        return None
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read().strip()
        if content and (content.startswith('<?xml') or content.startswith('<svg')):
            return content
    return None

def render_svg(subgraph_tasks, svg_file_path, params_dict=None):
    if not shutil.which('dot'):
        error_svg = generate_error_svg("Graphviz not installed. Please install graphviz package.")
        with open(svg_file_path, 'w') as f:
            f.write(error_svg)
        return

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
            
    except Exception as e:
        fallback_svg = generate_error_svg(f"SVG generation failed: {str(e)}")
        with open(svg_file_path, 'w', encoding='utf-8') as f:
            f.write(fallback_svg)

@task_subgraphs_bp.route('/task-subgraphs')
@check_and_reload_data()
def get_task_subgraphs():
    try:
        # parse arguments
        subgraph_id = request.args.get('subgraph_id')
        if not subgraph_id:
            return jsonify({'error': 'Subgraph ID is required'}), 400
        try:
            subgraph_id = int(subgraph_id)
        except Exception:
            return jsonify({'error': 'Invalid subgraph ID'}), 400

        # read df_subgraphs
        csv_dir = current_app.config["RUNTIME_STATE"].csv_files_dir
        if not csv_dir:
            return jsonify({'error': 'CSV directory not configured'}), 500

        df_subgraphs = read_csv_to_fd(os.path.join(csv_dir, 'task_subgraphs.csv'))
        
        # get tasks for subgraph_id
        if subgraph_id == 0:
            return jsonify({
                'legend': generate_legend(df_subgraphs),
                'subgraph_id': 0,
                'num_task_tries': 0,
                'subgraph_svg_content': ''
            })

        subgraph_tasks = df_subgraphs[df_subgraphs['subgraph_id'] == subgraph_id]
        if subgraph_tasks.empty:
            return jsonify({'error': 'Subgraph not found'}), 404

        # setup svg path
        svg_dir = current_app.config["RUNTIME_STATE"].svg_files_dir
        if not svg_dir:
            return jsonify({'error': 'SVG directory not configured'}), 500
        os.makedirs(svg_dir, exist_ok=True)
        
        base_filename = f'task-subgraph-{subgraph_id}'
        safe_filename = sanitize_filename(base_filename)
        svg_file_path = os.path.join(svg_dir, f'{safe_filename}.svg')

        # manually specify plotting parameters
        plot_params = {
            'label_file_waiting_time': False,
            'overwrite_existing_svg': True
        }
        
        svg_content = None
        if not plot_params['overwrite_existing_svg']:
            svg_content = read_valid_svg(svg_file_path)
        
        if not svg_content:
            current_app.config["RUNTIME_STATE"].logger.info(f"Generating subgraph {subgraph_id} from CSV")
            render_svg(subgraph_tasks, svg_file_path, plot_params)
            svg_content = read_valid_svg(svg_file_path)
        else:
            current_app.config["RUNTIME_STATE"].logger.info(f"Using existing subgraph {subgraph_id} SVG file")

        if not svg_content:
            svg_content = generate_error_svg("SVG file could not be generated")

        return jsonify({
            'subgraph_id': int(subgraph_id),
            'num_task_tries': int(len(subgraph_tasks)),
            'subgraph_svg_content': svg_content,
            'legend': generate_legend(df_subgraphs, subgraph_id)
        })
    except Exception as e:
        current_app.config["RUNTIME_STATE"].logger.error(f'Error in get_task_subgraphs: {e}')
        return jsonify({'error': str(e)}), 500
