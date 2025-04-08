import logging

import os
from logging.handlers import TimedRotatingFileHandler

# LOG_FORMAT = os.getenv('LOG_FORMAT', '%(asctime)s  %(levelname)s %(process)d [(%(threadName)s - %(thread)d]- [%(task_id)s][%(camera_id)s - %(camera_name)s] %(message)s')
LOG_FORMAT = os.getenv('LOG_FORMAT', '%(asctime)s  %(levelname)-9s %(process)-d [%(threadName)+18s][%(filename)s:%(funcName)s - %(lineno)d] :  %(message)s')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'DEBUG')
LOG_DIR = os.getenv('LOG_DIR', './logs')
LOG_FILE = os.getenv('LOG_FILE', 'app.log')


class TaskLoggingFilter(logging.Filter):
    def __init__(self, camera_task=None):
        super().__init__()
        self.camera_task = camera_task

    def filter(self, record):
        record.task_id = self.camera_task.taskId if self.camera_task is not None else ""
        record.camera_id = self.camera_task.camera.indexCode if self.camera_task is not None else ""
        record.camera_name = self.camera_task.camera.name if self.camera_task is not None else ""
        return True


def setup_logging(logger_name=__name__,
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

    log_path = os.path.join(LOG_DIR, LOG_FILE)
    os.makedirs(LOG_DIR, exist_ok=True)
    date_format = '%Y-%m-%d %H:%M:%S'

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


if __name__ == '__main__':
    logger = setup_logging(console_collector=True)
    print("22222")
    logger.info('Hello')
