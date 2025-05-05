from routes.runtime_state import *


app = Flask(__name__)


def check_and_reload_data():
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if runtime_state.check_pkl_files_changed():
                runtime_state.reload_data()
            return func(*args, **kwargs)
        return wrapper
    return decorator


# tasks
from routes.task_execution_details import task_execution_details_bp
app.register_blueprint(task_execution_details_bp)
from routes.task_execution_time import task_execution_time_bp
app.register_blueprint(task_execution_time_bp)
from routes.task_concurrency import task_concurrency_bp
app.register_blueprint(task_concurrency_bp)

# files
from routes.file_replicas import file_replicas_bp
app.register_blueprint(file_replicas_bp)
from routes.file_sizes import file_sizes_bp
app.register_blueprint(file_sizes_bp)
from routes.file_transfers import file_transfers_bp
app.register_blueprint(file_transfers_bp)

# storage
from routes.storage_consumption import storage_consumption_bp
app.register_blueprint(storage_consumption_bp)

# subgraphs
from routes.subgraphs import subgraphs_bp
app.register_blueprint(subgraphs_bp)

# runtime template
from routes.runtime_template_management import runtime_template_management_bp
app.register_blueprint(runtime_template_management_bp)



@app.route('/')
def index():
    log_folders = [name for name in os.listdir(LOGS_DIR) if os.path.isdir(os.path.join(LOGS_DIR, name))]
    log_folders_sorted = sorted(log_folders)
    return render_template('index.html', log_folders=log_folders_sorted)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', default=9122, help='Port number')
    args = parser.parse_args()
    
    app.run(host='0.0.0.0', port=args.port, debug=True, use_reloader=False)