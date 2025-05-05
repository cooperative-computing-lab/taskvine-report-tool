from flask import Flask, render_template, jsonify, Response, request, send_from_directory, Blueprint
import os
import argparse
import pandas as pd
from typing import Dict, Any
from pathlib import Path
from collections import defaultdict
import graphviz
from src.data_parse import DataParser
import numpy as np
import random
import traceback
import functools
import time

LOGS_DIR = 'logs'
TARGET_POINTS = 10000  # at lease 3: the beginning, the end, and the global peak
TARGET_TASK_BARS = 100000   # how many task bars to show


def check_and_reload_data():
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if runtime_state.check_pkl_files_changed():
                runtime_state.reload_data()
            return func(*args, **kwargs)
        return wrapper
    return decorator

def all_subfolders_exists(parent: str, folder_names: list[str]) -> bool:
    parent_path = Path(parent).resolve()
    for folder_name in folder_names:
        target_path = parent_path / folder_name
        if not target_path.is_dir():
            return False
    return True

# calculate the file size unit, the default is MB
def get_unit_and_scale_by_max_file_size_mb(max_file_size_mb) -> tuple[str, float]:
    if max_file_size_mb < 1 / 1024:
        return 'Bytes',  1024 * 1024
    elif max_file_size_mb < 1:
        return 'KB', 1024
    elif max_file_size_mb > 1024:
        return 'GB', 1 / 1024
    elif max_file_size_mb > 1024 * 1024:
        return 'TB', 1 / (1024 * 1024)
    else:
        return 'MB', 1


class RuntimeState:
    def __init__(self):
        # full path to the runtime template
        self.runtime_template = None
        self.data_parser = None

        self.manager = None
        self.workers = None
        self.files = None
        self.tasks = None
        self.subgraphs = None

        # for storing the graph files
        self.svg_files_dir = None

        self.MIN_TIME = None
        self.MAX_TIME = None

        self.tick_size = 12

        self.pkl_files_info = {}

    def get_file_stat(self, file_path):
        try:
            stat = os.stat(file_path)
            return {
                'mtime': stat.st_mtime,
                'size': stat.st_size
            }
        except Exception as e:
            print(f"Error getting file info for {file_path}: {e}")
            return None

    def check_pkl_files_changed(self):
        if not self.runtime_template or not self.data_parser:
            return False

        pkl_dir = self.data_parser.pkl_files_dir
        pkl_files = ['workers.pkl', 'files.pkl', 'tasks.pkl', 'manager.pkl', 'subgraphs.pkl']
        
        for pkl_file in pkl_files:
            file_path = os.path.join(pkl_dir, pkl_file)
            current_stat = self.get_file_stat(file_path)
            
            if not current_stat:
                continue

            if (file_path not in self.pkl_files_info or 
                current_stat['mtime'] != self.pkl_files_info[file_path]['mtime'] or 
                current_stat['size'] != self.pkl_files_info[file_path]['size']):
                print(f"Detected changes in {pkl_file}")
                return True

        return False

    def update_pkl_files_info(self):
        if not self.runtime_template or not self.data_parser:
            return

        pkl_dir = self.data_parser.pkl_files_dir
        pkl_files = ['workers.pkl', 'files.pkl', 'tasks.pkl', 'manager.pkl', 'subgraphs.pkl']
        
        for pkl_file in pkl_files:
            file_path = os.path.join(pkl_dir, pkl_file)
            info = self.get_file_stat(file_path)
            if info:
                self.pkl_files_info[file_path] = info

    def reload_data(self):
        try:
            print("Reloading data from checkpoint...")
            self.data_parser.restore_from_checkpoint()
            self.manager = self.data_parser.manager
            self.workers = self.data_parser.workers
            self.files = self.data_parser.files
            self.tasks = self.data_parser.tasks
            self.subgraphs = self.data_parser.subgraphs

            self.MIN_TIME = self.manager.when_first_task_start_commit
            self.MAX_TIME = self.manager.time_end

            self.update_pkl_files_info()
            print("Data reload completed successfully")
        except Exception as e:
            print(f"Error reloading data: {e}")
            traceback.print_exc()

    def change_runtime_template(self, runtime_template):
        if not runtime_template:
            return
        if self.runtime_template and Path(runtime_template).name == Path(self.runtime_template).name:
            print(f"Runtime template already set to: {runtime_template}")
            return
        self.runtime_template = os.path.join(os.getcwd(), LOGS_DIR, Path(runtime_template).name)
        print(f"Restoring data for: {runtime_template}")

        self.data_parser = DataParser(self.runtime_template)
        self.svg_files_dir = self.data_parser.svg_files_dir

        self.data_parser.restore_from_checkpoint()
        self.manager = self.data_parser.manager
        self.workers = self.data_parser.workers
        self.files = self.data_parser.files
        self.tasks = self.data_parser.tasks
        self.subgraphs = self.data_parser.subgraphs

        self.MIN_TIME = self.manager.when_first_task_start_commit
        self.MAX_TIME = self.manager.time_end

        self.update_pkl_files_info()


runtime_state = RuntimeState()