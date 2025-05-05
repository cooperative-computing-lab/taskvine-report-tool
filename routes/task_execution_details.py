from .runtime_state import *

task_execution_details_bp = Blueprint('task_execution_details', __name__, url_prefix='/api')


@task_execution_details_bp.route('/execution-details')
@check_and_reload_data()
def get_execution_details():
    try:
        data: Dict[str, Any] = {}

        data['xMin'] = 0
        data['xMax'] = runtime_state.MAX_TIME - runtime_state.MIN_TIME

        # tasks information
        data['successfulTasks'] = []
        data['unsuccessfulTasks'] = []
        data['num_of_status'] = defaultdict(int)
        data['num_successful_recovery_tasks'] = 0
        data['num_unsuccessful_recovery_tasks'] = 0
        for task in runtime_state.tasks.values():
            if task.task_status == 0:
                # note that the task might have not been retrieved yet
                if not task.when_retrieved:
                    continue
                if task.is_library_task:
                    continue
                if len(task.core_id) == 0:
                    raise ValueError(f"Task {task.task_id} has no core_id, but when_running is {task.when_running}, when_failure_happens is {task.when_failure_happens}, when_waiting_retrieval is {task.when_waiting_retrieval}, when_retrieved is {task.when_retrieved}, when_done is {task.when_done}")
                data['num_of_status'][task.task_status] += 1
                if task.is_recovery_task:
                    data['num_successful_recovery_tasks'] += 1
                done_task_info = {
                    'task_id': task.task_id,
                    'worker_ip': task.worker_ip,
                    'worker_port': task.worker_port,
                    'worker_id': task.worker_id,
                    'core_id': task.core_id[0],
                    'is_recovery_task': task.is_recovery_task,
                    'num_input_files': len(task.input_files),
                    'num_output_files': len(task.output_files),
                    'task_status': task.task_status,
                    'category': task.category,
                    'when_ready': task.when_ready - runtime_state.MIN_TIME,
                    'when_running': task.when_running - runtime_state.MIN_TIME,
                    'time_worker_start': task.time_worker_start - runtime_state.MIN_TIME,
                    'time_worker_end': task.time_worker_end - runtime_state.MIN_TIME,
                    'execution_time': task.time_worker_end - task.time_worker_start,
                    'when_waiting_retrieval': task.when_waiting_retrieval - runtime_state.MIN_TIME,
                    'when_retrieved': task.when_retrieved - runtime_state.MIN_TIME,
                }
                data['successfulTasks'].append(done_task_info)
            else:
                if len(task.core_id) == 0:    # not run at all
                    continue
                if task.is_recovery_task:
                    data['num_unsuccessful_recovery_tasks'] += 1
                data['num_of_status'][task.task_status] += 1
                unsuccessful_task_info = {
                    'task_id': task.task_id,
                    'worker_ip': task.worker_ip,
                    'worker_port': task.worker_port,
                    'worker_id': task.worker_id,
                    'core_id': task.core_id[0],
                    'is_recovery_task': task.is_recovery_task,
                    'num_input_files': len(task.input_files),
                    'num_output_files': len(task.output_files),
                    'task_status': task.task_status,
                    'category': task.category,
                    'when_ready': task.when_ready - runtime_state.MIN_TIME,
                    'when_running': task.when_running - runtime_state.MIN_TIME,
                    'when_failure_happens': task.when_failure_happens - runtime_state.MIN_TIME,
                    'execution_time': task.when_failure_happens - task.when_running,
                }
                data['unsuccessfulTasks'].append(unsuccessful_task_info)

        # filter successfulTasks to keep only top 100,000 by execution time if there are more than 100,000 tasks
        if len(data['successfulTasks']) > TARGET_TASK_BARS:
            # sort tasks by execution time in descending order and keep top 100,000
            data['successfulTasks'] = sorted(data['successfulTasks'], 
                                          key=lambda x: x['execution_time'],
                                          reverse=True)[:TARGET_TASK_BARS]
        # filter unsuccessfulTasks to keep only top 100,000 by execution time if there are more than 100,000 tasks
        if len(data['unsuccessfulTasks']) > TARGET_TASK_BARS:
            # sort tasks by execution time in descending order and keep top 100,000
            data['unsuccessfulTasks'] = sorted(data['unsuccessfulTasks'], 
                                          key=lambda x: x['execution_time'],
                                          reverse=True)[:TARGET_TASK_BARS]
        
        data['workerInfo'] = []
        for worker in runtime_state.workers.values():
            if not worker.hash:
                continue
            # it means the worker didn't exit normally or hasn't exited yet
            if len(worker.time_disconnected) != len(worker.time_connected):
                # set the time_disconnected to the max time
                worker.time_disconnected = [runtime_state.MAX_TIME] * (len(worker.time_connected) - len(worker.time_disconnected))
            worker_info = {
                'hash': worker.hash,
                'id': worker.id,
                'worker_ip_port': f"{worker.ip}:{worker.port}",
                'time_connected': [max(t - runtime_state.MIN_TIME, 0) for t in worker.time_connected],
                'time_disconnected': [max(t - runtime_state.MIN_TIME, 0) for t in worker.time_disconnected],
                'cores': worker.cores,
                'memory_mb': worker.memory_mb,
                'disk_mb': worker.disk_mb,
                'gpus': worker.gpus,
            }
            data['workerInfo'].append(worker_info)

        # ploting parameters
        data['tickFontSize'] = runtime_state.tick_size
        data['xTickValues'] = [
            round(data['xMin'], 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.25, 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.5, 2),
            round(data['xMin'] + (data['xMax'] - data['xMin']) * 0.75, 2),
            round(data['xMax'], 2)
        ]
        
        # Calculate yTickValues for worker IDs
        worker_ids = [worker['id'] for worker in data['workerInfo']]
        
        if worker_ids:
            min_worker_id = 1  # Start with worker ID 1
            max_worker_id = max(worker_ids)
            
            print(f"Min worker ID: {min_worker_id}, Max worker ID: {max_worker_id}")
            
            # Generate 5 evenly distributed tick values
            if min_worker_id == max_worker_id:
                data['yTickValues'] = [min_worker_id]
            else:
                step = (max_worker_id - min_worker_id) / 4  # To get 5 points total
                data['yTickValues'] = [
                    min_worker_id,
                    round(min_worker_id + step, 0),
                    round(min_worker_id + 2 * step, 0),
                    round(min_worker_id + 3 * step, 0),
                    max_worker_id
                ]
                # Convert to integers
                data['yTickValues'] = [int(tick) for tick in data['yTickValues']]
                # Remove duplicates while preserving order
                data['yTickValues'] = list(dict.fromkeys(data['yTickValues']))
                
            print(f"Generated yTickValues: {data['yTickValues']}")
        else:
            data['yTickValues'] = [1]  # Default if no workers
            print("No workers, using default yTickValues: [1]")

        return jsonify(data)

    except Exception as e:
        error_message = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        print(error_message)
        return jsonify({'error': str(e), 'details': error_message}), 500
