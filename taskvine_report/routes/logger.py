import os
import logging


class Logger:
    _instance = None

    def __new__(cls, log_file_name='taskvine_report.log'):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._initialize(log_file_name)
        return cls._instance

    def _initialize(self, log_file_name):
        self.log_file = os.path.join(os.getcwd(), log_file_name)
        self.logger = logging.getLogger('taskvine-report-tool')
        self.logger.setLevel(logging.INFO)

        try:
            if os.path.exists(self.log_file):
                os.remove(self.log_file)
                self.info(f"Removed existing log file: {self.log_file}")
        except Exception as e:
            self.warning(f"Could not remove existing log file: {e}")

        self.info(f"Initializing logger with log file: {self.log_file}")

        if self.logger.handlers:
            self.logger.handlers.clear()

        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(logging.INFO)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s')

        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        self.info("Application started - Log file initialized")

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def warning(self, message):
        self.logger.warning(message)

    def debug(self, message):
        self.logger.debug(message)

    def log_response(self, response, request, duration=None):
        path = request.path
        status_code = response.status_code

        if path.startswith('/api/'):
            if duration:
                self.info(
                    f"API Response: {status_code} for {path} - completed in {duration:.4f}s")
            else:
                self.info(f"API Response: {status_code} for {path}")
        elif status_code >= 400:
            self.warning(f"HTTP Error Response: {status_code} for {path}")
