import logging

import os
from logging.handlers import TimedRotatingFileHandler

LOG_FORMAT = os.getenv('LOG_FORMAT', '%(asctime)s.%(msecs)03d  %(levelname)-9s %(process)-d [%(threadName)+18s][%(filename)s:%(funcName)s - %(lineno)d] : [%(task_id)s][%(camera_id)s - %(camera_name)s] %(message)s')
# LOG_FORMAT = os.getenv('LOG_FORMAT', '%(asctime)s.%(msecs)03d  %(levelname)-9s %(process)-d [%(threadName)+18s][%(filename)s:%(funcName)s - %(lineno)d] :  %(message)s')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'DEBUG')
LOG_DIR = os.getenv('LOG_DIR', './logs')
LOG_FILE = os.getenv('LOG_FILE', 'app.log')


class TaskLoggingFilter(logging.Filter):
    def __init__(self):
        import threading
        super().__init__()
        self._local = threading.local()  # 添加线程局部存储

    def set_context(self, task_id, camera_id, camera_name):
        # 设置当前线程的上下文信息
        self._local.task_id = task_id
        self._local.camera_id = camera_id
        self._local.camera_name = camera_name

    def filter(self, record):
        # 从线程局部存储获取上下文
        record.task_id = getattr(self._local, 'task_id', '')
        record.camera_id = getattr(self._local, 'camera_id', '')
        record.camera_name = getattr(self._local, 'camera_name', '')
        return True


def setup_logging(logger_name=__name__,
                  log_file=None,
                  level=LOG_LEVEL,
                  backup_count=7,
                  log_format=LOG_FORMAT,
                  console_collector=True,
                  console_level=logging.DEBUG
                  ):
    # 创建日志记录器
    logger = logging.getLogger(logger_name)
    # 设置日志级别
    logger.setLevel(level)

    log_path = os.path.join(LOG_DIR, LOG_FILE if log_file is None else log_file)
    os.makedirs(LOG_DIR, exist_ok=True)
    date_format = '%Y-%m-%d %H:%M:%S'

    # 清空已有处理器（防止重复添加）
    if logger.hasHandlers():
        logger.handlers.clear()

    # 定义日志格式
    formatter = logging.Formatter(log_format, date_format)

    # 创建handler，设置日志文件和备份数量
    handler = TimedRotatingFileHandler(log_path, when='d', interval=1, backupCount=backup_count)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.addFilter(TaskLoggingFilter())
    if console_collector:
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)
        # 将处理器添加到日志记录器
        logger.addHandler(console_handler)
    return logger
