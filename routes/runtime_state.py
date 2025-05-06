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
from src.logger import Logger
from src.utils import *
import threading
from collections import defaultdict
import queue

LOGS_DIR = 'logs'
SAMPLING_POINTS = 10000  # at lease 3: the beginning, the end, and the global peak
SAMPLING_TASK_BARS = 100000   # how many task bars to show

SERVICE_API_LISTS = [
    'task-execution-details',
    'task-execution-time',
    'task-concurrency',
    'storage-consumption',
    'file-transfers',
    'file-sizes',
    'file-replicas',
    'subgraphs',
]


def check_and_reload_data():
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if runtime_state.check_pkl_files_changed():
                runtime_state.reload_data()
            return func(*args, **kwargs)
        return wrapper
    return decorator


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

        # set logger
        self.logger = Logger()

        # last time process the template change
        self.last_template_change_time = 0
        self.is_processing_template_change = False
        self.template_change_lock = threading.Lock()

        self.api_requested = defaultdict(int)
        self.api_responded = defaultdict(int)

    @property
    def log_prefix(self):
        return f"[{self.runtime_template}]"

    def log_info(self, message):
        self.logger.info(f"{self.log_prefix} {message}")

    def log_error(self, message):
        self.logger.error(f"{self.log_prefix} {message}")

    def log_warning(self, message):
        self.logger.warning(f"{self.log_prefix} {message}")

    def check_pkl_files_changed(self):
        if not self.runtime_template or not self.data_parser:
            return False

        pkl_dir = self.data_parser.pkl_files_dir
        pkl_files = ['workers.pkl', 'files.pkl', 'tasks.pkl', 'manager.pkl', 'subgraphs.pkl']
        
        for pkl_file in pkl_files:
            file_path = os.path.join(pkl_dir, pkl_file)
            current_stat = get_file_stat(file_path)
            
            if not current_stat:
                continue

            if (file_path not in self.pkl_files_info or 
                current_stat['mtime'] != self.pkl_files_info[file_path]['mtime'] or 
                current_stat['size'] != self.pkl_files_info[file_path]['size']):
                self.log_info(f"Detected changes in {pkl_file}")
                return True

        return False
    
    def reload_data(self):
        try:
            self.log_info(f"Reloading data from checkpoint...")
            self.data_parser.restore_from_checkpoint()
            self.manager = self.data_parser.manager
            self.workers = self.data_parser.workers
            self.files = self.data_parser.files
            self.tasks = self.data_parser.tasks
            self.subgraphs = self.data_parser.subgraphs

            self.MIN_TIME = self.manager.when_first_task_start_commit
            self.MAX_TIME = self.manager.time_end

            # update the pkl files info
            pkl_dir = self.data_parser.pkl_files_dir
            pkl_files = ['workers.pkl', 'files.pkl', 'tasks.pkl', 'manager.pkl', 'subgraphs.pkl']
            for pkl_file in pkl_files:
                file_path = os.path.join(pkl_dir, pkl_file)
                info = get_file_stat(file_path)
                if info:
                    self.pkl_files_info[file_path] = info
            
            self.log_info(f"Data reload completed successfully")
        except Exception as e:
            self.log_error(f"Error reloading data: {e}")
            traceback.print_exc()

    def change_runtime_template(self, runtime_template):
        if not runtime_template:
            return False
        if self.runtime_template and Path(runtime_template).name == Path(self.runtime_template).name:
            self.log_info(f"Runtime template already set to: {runtime_template}")
            return True
        
        with self.template_change_lock:
            if self.is_processing_template_change:
                self.log_info(f"Busy with another template change, skipping...")
                return False
            
            self.is_processing_template_change = True
            self.last_template_change_time = time.time()

        self.runtime_template = os.path.join(os.getcwd(), LOGS_DIR, Path(runtime_template).name)
        self.log_info(f"Restoring data for runtime template: {runtime_template}")

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

        self.reload_data()

        self.log_info(f"Runtime template changed to: {runtime_template}")

        return True


runtime_state = RuntimeState()
