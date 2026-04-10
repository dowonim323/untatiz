"""Logging helpers."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional


def setup_logging(log_path: Path, level: int = logging.INFO, logger_name: Optional[str] = None) -> logging.Logger:
    """Configure logging for the application.

    Args:
        log_path: Log file path.
        level: Logging level.
        logger_name: Optional logger name. If None, configure root.

    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

    file_handler = logging.FileHandler(str(log_path))
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger


class StreamToLogger:
    """Redirect stdout/stderr to logger."""

    def __init__(self, logger: logging.Logger, log_level: int = logging.INFO):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf: str):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    def flush(self):
        pass


__all__ = ["setup_logging", "StreamToLogger"]
