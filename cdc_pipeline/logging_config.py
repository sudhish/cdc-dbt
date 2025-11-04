"""
Centralized logging configuration for the CDC pipeline
Provides structured logging with both file and console output
"""
import os
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for console output"""

    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
        'RESET': '\033[0m'       # Reset
    }

    def format(self, record):
        # Add color to level name
        if record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}{self.COLORS['RESET']}"
        return super().format(record)


def setup_logger(
    name: str,
    log_level: str = None,
    log_dir: str = None,
    enable_file_logging: bool = True
) -> logging.Logger:
    """
    Setup and configure a logger with both console and file handlers

    Args:
        name: Logger name (typically __name__)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir: Directory for log files
        enable_file_logging: Whether to enable file logging

    Returns:
        Configured logger instance
    """
    # Get configuration from environment variables
    if log_level is None:
        log_level = os.getenv('LOG_LEVEL', 'INFO').upper()

    if log_dir is None:
        log_dir = os.getenv('LOG_DIR', '/data/logs')

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level))

    # Remove existing handlers to avoid duplicates
    logger.handlers = []

    # Console handler with colors
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level))

    console_format = ColoredFormatter(
        fmt='%(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # File handler with rotation (if enabled)
    if enable_file_logging:
        try:
            # Create log directory
            Path(log_dir).mkdir(parents=True, exist_ok=True)

            # Separate log files for different components
            log_file = os.path.join(log_dir, f"{name.split('.')[-1]}.log")

            # Rotating file handler (10MB per file, keep 5 backups)
            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=10 * 1024 * 1024,  # 10MB
                backupCount=5
            )
            file_handler.setLevel(logging.DEBUG)  # Always DEBUG for files

            file_format = logging.Formatter(
                fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(file_format)
            logger.addHandler(file_handler)

        except Exception as e:
            logger.warning(f"Failed to setup file logging: {e}")

    # Prevent propagation to root logger
    logger.propagate = False

    return logger


class LogContext:
    """Context manager for logging operations with timing"""

    def __init__(self, logger: logging.Logger, operation: str, level: int = logging.INFO):
        self.logger = logger
        self.operation = operation
        self.level = level
        self.start_time = None

    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.log(self.level, f"Starting: {self.operation}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = (datetime.now() - self.start_time).total_seconds()

        if exc_type is None:
            self.logger.log(self.level, f"Completed: {self.operation} (took {duration:.2f}s)")
        else:
            self.logger.error(f"Failed: {self.operation} (took {duration:.2f}s) - {exc_val}")

        return False  # Don't suppress exceptions


class MetricsLogger:
    """Helper class for logging metrics and statistics"""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.metrics = {}

    def record(self, key: str, value: any):
        """Record a metric"""
        self.metrics[key] = value

    def increment(self, key: str, amount: int = 1):
        """Increment a counter metric"""
        self.metrics[key] = self.metrics.get(key, 0) + amount

    def log_summary(self):
        """Log all recorded metrics"""
        if not self.metrics:
            return

        self.logger.info("=== Metrics Summary ===")
        for key, value in sorted(self.metrics.items()):
            if isinstance(value, float):
                self.logger.info(f"  {key}: {value:.2f}")
            else:
                self.logger.info(f"  {key}: {value}")

    def reset(self):
        """Reset all metrics"""
        self.metrics = {}


# Convenience function to get logger
def get_logger(name: str) -> logging.Logger:
    """Get or create a logger with standard configuration"""
    return setup_logger(name)


if __name__ == "__main__":
    # Test the logging configuration
    logger = get_logger("test_logger")

    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")

    # Test LogContext
    with LogContext(logger, "Test operation"):
        import time
        time.sleep(1)

    # Test MetricsLogger
    metrics = MetricsLogger(logger)
    metrics.record("records_processed", 100)
    metrics.record("processing_time_seconds", 5.23)
    metrics.increment("errors_count", 2)
    metrics.log_summary()
