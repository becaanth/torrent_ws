import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import threading
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

    # Hooks to capture crashed threads/main thread & flush logs
    sys.excepthook = handle_uncaught_exception
    threading.excepthook = lambda args: handle_uncaught_exception(args.exc_type, args.exc_value, args.exc_traceback)

    logging.info(f"Logging initialized. Output file: {log_filename}")

def flush_logs():
    for handler in logging.getLogger().handlers:
        handler.flush()

def handle_uncaught_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        # Allow standard CTRL+C exit without logging stack trace
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    # Log the thread name and full stack trace
    logging.critical(
        f"Thread crashed! Crashing orchestrator process...",
        exc_info=(exc_type, exc_value, exc_traceback)
    )
    
    # Guarantee disk write before exit
    flush_logs()
    
    # Hard exit process immediately to force orchestrator shutdown
    os._exit(1)