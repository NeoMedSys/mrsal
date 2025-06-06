import logging
import colorlog
from logging.handlers import TimedRotatingFileHandler
import os
from pathlib import Path


class MrsalLogger:
    """
    Custom logger for Mrsal with colored console output and file rotation.
    Compatible replacement for neolibrary.monitoring.logger.NeoLogger.
    """
    
    def __init__(self, name: str, rotate_days: int = 7, log_level: str = "INFO"):
        """
        Initialize the Mrsal logger.
        
        :param name: Logger name (usually __name__)
        :param rotate_days: Number of days to keep log files
        :param log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Prevent duplicate handlers
        if self.logger.handlers:
            return
            
        # Create logs directory
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        # Console handler with colors
        console_handler = colorlog.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Color formatter for console
        console_formatter = colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s | %(name)s | %(levelname)-8s | %(message)s%(reset)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'white', 
                'SUCCESS': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red',
            }
        )
        console_handler.setFormatter(console_formatter)
        
        # File handler with rotation
        log_file = log_dir / f"{name.replace('.', '_')}.log"
        file_handler = TimedRotatingFileHandler(
            log_file,
            when='midnight',
            interval=1,
            backupCount=rotate_days,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        
        # Simple formatter for file
        file_formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        
        # Add handlers
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
        
        # Add custom SUCCESS level
        if not hasattr(logging, 'SUCCESS'):
            logging.addLevelName(25, 'SUCCESS')
    
    def log(self, level, message, *args, exc_info=None, **kwargs):
        """Log method for compatibility with tenacity and other libraries."""
        self.logger.log(level, message, *args, exc_info=exc_info, **kwargs)
        
    def __call__(self, message: str, level: int = logging.INFO):
        """Make logger callable for compatibility with tenacity before_sleep_log."""
        self.logger.log(level, message)
            
    def debug(self, message: str, *args, **kwargs):
        """Log debug message."""
        self.logger.debug(message, *args, **kwargs)
        
    def info(self, message: str, *args, **kwargs):
        """Log info message."""
        self.logger.info(message, *args, **kwargs)
        
    def success(self, message: str, *args, **kwargs):
        """Log success message (custom level)."""
        self.logger.log(25, message, *args, **kwargs)  # SUCCESS = 25
        
    def warning(self, message: str, *args, **kwargs):
        """Log warning message."""
        self.logger.warning(message, *args, **kwargs)
        
    def error(self, message: str, *args, exc_info=False, **kwargs):
        """Log error message."""
        self.logger.error(message, *args, exc_info=exc_info, **kwargs)
        
    def critical(self, message: str, *args, **kwargs):
        """Log critical message."""
        self.logger.critical(message, *args, **kwargs)


# Convenience function for easy import
def get_logger(name: str, rotate_days: int = 7, log_level: str = "INFO") -> MrsalLogger:
    """
    Get a Mrsal logger instance.
    
    Usage:
        from mrsal.logger import get_logger
        log = get_logger(__name__)
        log.info("Hello world!")
        log.success("Operation completed!")
    """
    return MrsalLogger(name, rotate_days, log_level)
