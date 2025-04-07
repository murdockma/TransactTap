"""
Logger Module

Standardized logging functionality for the application
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

_LOGGERS: Dict[str, logging.Logger] = {}


def setup_logger(name: str, level: int = logging.INFO, 
                log_to_file: bool = True) -> logging.Logger:
    """
    Set up and configure logger
    
    :param name: Name for the logger
    :param level: Logging level (e.g., logging.INFO, logging.DEBUG)
    :param log_to_file: Whether to log to file in addition to console
    :return: Configured logging.Logger instance
    """
    if name in _LOGGERS:
        return _LOGGERS[name]
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False
    
    # Clear any existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Add file handler
    if log_to_file:
        log_dir = Path(__file__).parents[2] / "logs"
        os.makedirs(log_dir, exist_ok=True)
        
        date_str = datetime.now().strftime('%Y%m%d')
        log_file = log_dir / f"{date_str}_{name}.log"

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
    
    _LOGGERS[name] = logger
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get an existing logger or create new one
    
    :param name: Name of the logger
    :return: Logger instance
    """
    if name in _LOGGERS:
        return _LOGGERS[name]
    else:
        return setup_logger(name)


def set_global_log_level(level: int) -> None:
    """
    Set the log level for all existing loggers
    
    :param level: Logging level (e.g., logging.INFO, logging.DEBUG)
    """
    for logger_name, logger in _LOGGERS.items():
        logger.setLevel(level)
        for handler in logger.handlers:
            handler.setLevel(level)
