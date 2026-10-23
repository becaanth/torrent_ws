import logging
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener
import os
import queue
import sys
import threading
from datetime import datetime
import time
import json

# Console output goes through a queue so worker threads never block on a
# stalled terminal (e.g. the SSH client disappeared). See _DropQueueHandler.
_CONSOLE_QUEUE_SIZE = 10000
_console_listener = None


class _DropQueueHandler(QueueHandler):
    """
    Puts log records on a bounded queue without ever waiting.
    If the queue is full (console stalled), the record is dropped for the
    console only -- the file handler still gets it.
    """
    def enqueue(self, record):
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            pass


def setup_logging(robot_id: str, posegraph: str, log_dir: str = "logs"):
    """
    Sets up thread-safe logging to both console and a rotating log file.
    File writes happen directly in the calling thread.
    Console writes are handed to a background thread via a bounded queue,
    so a blocked terminal can never stall the orchestrator threads.
    """
    global _console_listener

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

    # 1. File Handler (Max 10 MB per file, keeps up to 5 backup logs) -- unchanged
    file_handler = RotatingFileHandler(
        log_filename, maxBytes=10 * 1024 * 1024, backupCount=50
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)

    # 2. Console: worker threads -> queue -> listener thread -> stdout
    #    The queue handler formats the line in the calling thread (so the
    #    thread name / file:line are correct); the listener just prints it.
    if _console_listener is not None:
        _console_listener.stop()
    console_queue = queue.Queue(maxsize=_CONSOLE_QUEUE_SIZE)

    queue_handler = _DropQueueHandler(console_queue)
    queue_handler.setFormatter(formatter)
    queue_handler.setLevel(logging.INFO)
    root_logger.addHandler(queue_handler)

    console_handler = logging.StreamHandler(sys.stdout)  # prints the pre-formatted line as-is
    _console_listener = QueueListener(console_queue, console_handler)
    _console_listener.start()  # runs in its own daemon thread

    # Hooks to capture crashed threads/main thread & flush logs
    sys.excepthook = handle_uncaught_exception
    threading.excepthook = lambda args: handle_uncaught_exception(args.exc_type, args.exc_value, args.exc_traceback)

    logging.info(f"Logging initialized. Output file: {log_filename}")


def flush_logs():
    # Only the root logger's own handlers: the file handler and the queue handler.
    # Deliberately NOT flushing/stopping the console listener: if the terminal is
    # stalled, that would block forever (e.g. inside the crash hook before os._exit).
    for handler in logging.getLogger().handlers:
        handler.flush()


def evt(event_kind: str, /, **fields):
    """
    Structured event for the analysis scripts (ttr_events.py):
        ... [INFO] [Thread] [file:line]: EVT {"k":"peer","t":1792700000.123456,...}
    t is this robot's wall clock (chrony-synced) in seconds, microsecond resolution.
    Keep field values JSON-serialisable: ints, floats, strings, lists.
    """
    logging.info("EVT " + json.dumps({**fields, "k": event_kind, "t": round(time.time(), 6)},
                                     separators=(",", ":"), default=str))

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