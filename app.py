from routes.runtime_state import *


app = Flask(__name__)

def setup_request_logging(app):
    @app.before_request
    def log_request_info():
        logger.log_request(request)
        request._start_time = time.time()
    
    @app.after_request
    def log_response_info(response):
        if hasattr(request, '_start_time'):
            duration = time.time() - request._start_time
            logger.log_response(response, request, duration)
        else:
            logger.log_response(response, request)
        return response
        
    return app

setup_request_logging(app)

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
from routes.runtime_template import runtime_template_bp
app.register_blueprint(runtime_template_bp)


@app.route('/')
def index():
    log_folders = [name for name in os.listdir(LOGS_DIR) if os.path.isdir(os.path.join(LOGS_DIR, name))]
    log_folders_sorted = sorted(log_folders)
    return render_template('index.html', log_folders=log_folders_sorted)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', default=9122, help='Port number')
    args = parser.parse_args()
    
    logger.info(f"Starting application on port {args.port}")
    app.run(host='0.0.0.0', port=args.port, debug=True, use_reloader=False)