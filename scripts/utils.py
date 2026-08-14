import logging
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime

def setup_logging(robot_id: str, posegraph: str, log_dir: str = "logs"):
    """
    Sets up thread-safe logging to both console and a rotating log file.
    """
    
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(log_dir, f"{posegraph}_{robot_id}_{timestamp}.log")

    # Log format with timestamp, thread name, log level, and message
    formatter = logging.Formatter(
        "[%(asctime)s.%(msecs)03d] [%(levelname)s] [%(threadName)s] [%(filename)s:%(lineno)d]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers.clear()
    
    # 1. File Handler (Max 10 MB per file, keeps up to 5 backup logs)
    file_handler = RotatingFileHandler(
        log_filename, maxBytes=10 * 1024 * 1024, backupCount=5
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)

    # 2. Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)
    
    logging.info(f"Logging initialized. Output file: {log_filename}")