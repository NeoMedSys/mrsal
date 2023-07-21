import logging
import os
from pathlib import Path

from colorlog import ColoredFormatter

# TODO: inlcude comments

# Setting color logs
SUCCESS = 21
FAIL = 22
PIPE = 55
LOG_LEVEL = logging.INFO
LOG_LEVEL_E = logging.WARNING
LOGFORMAT = '%(log_color)s[%(asctime)s] [%(levelname)-8s] %(reset)s | \
    %(log_color)s%(message)s%(reset)s'
LOGFORMAT_ERROR = '%(log_color)s%(asctime)-8s%(reset)s | \
    %(log_color)s%(levelname)-8s%(reset)s | %(log_color)s%(message)s%(reset)s'

ROOT = os.getcwd()  # os.environ['PYTHONPATH']

# Adding file path for error logs and levels above
FILE_PATH = os.path.join(ROOT, 'docs/logs/level_errors_logs.log')
# It is sad with non existent parents
Path(FILE_PATH).parent.mkdir(exist_ok=True, parents=True)

logging.addLevelName(SUCCESS, 'SUCCESS')


def success(self, message, *args, **kws):
    if self.isEnabledFor(SUCCESS):
        self._log(SUCCESS, message, args, **kws)


logging.addLevelName(FAIL, 'FAIL')


def fail(self, message, *args, **kws):
    if self.isEnabledFor(FAIL):
        self._log(FAIL, message, args, **kws)


logging.addLevelName(PIPE, 'PIPE')


def pipe(self, message, *args, **kws):
    if self.isEnabledFor(PIPE):
        self._log(PIPE, message, args, **kws)


logging.Logger.success = success
logging.Logger.fail = fail
logging.Logger.pipe = pipe
logging.root.setLevel(LOG_LEVEL)

formatter = ColoredFormatter(
    LOGFORMAT,
    log_colors={
        'DEBUG': 'white,bg_black',
        'INFO': 'cyan',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_black',
        'SUCCESS': 'bold_green',
        'FAIL': 'white,bg_black',
        'PIPE': 'green'
    })
e_formatter = ColoredFormatter(
    LOGFORMAT_ERROR,
    log_colors={
        'DEBUG': 'white,bg_black',
        'INFO': 'cyan',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red,bg_black',
        'SUCCESs': 'bold_green',
        'FAIL': 'white, bg_black',
        'PIPE': 'green'
    })

stream = logging.StreamHandler()
file_handler = logging.FileHandler(FILE_PATH)

stream.setLevel(LOG_LEVEL)
# TODO: Only save loglevel error to file
file_handler.setLevel(LOG_LEVEL)

stream.setFormatter(formatter)
file_handler.setFormatter(e_formatter)


def get_logger(name):
    log = logging.getLogger(name)
    log.setLevel(LOG_LEVEL)
    if (log.hasHandlers()):
        log.handlers.clear()
    log.addHandler(stream)
    log.addHandler(file_handler)

    return log
