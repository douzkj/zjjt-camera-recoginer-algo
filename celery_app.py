import asyncio
import json
import os
import shutil
import threading
import time
from functools import wraps
from pathlib import Path
# from celery.concurrency import Singleton

import cv2
import fcntl
from celery import Celery
from dotenv import load_dotenv

import db
from algorithm import recognize_image_with_label, general_annotation
from capture import FrameStorageConfig, FrameReadConfig
from db import Session, Signal
from entity import SignalConfig
from mq import MQSender
from recognizer import read_shapes
from setup import setup_logging

load_dotenv()  # 加载环境变量

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'guest')
RABBITMQ_VHOST = os.getenv('RABBITMQ_VHOST', '/')
# QUEUE_RECOGNIZER_COLLECTION = os.getenv('QUEUE_RECOGNIZER_COLLECTION', 'zjjt:camera_recognizer:collection:v2')
QUEUE_RECOGNIZER_COLLECTION = 'zjjt:camera_recognizer:collection:v2'
amqp_url = f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}{RABBITMQ_VHOST}"
STORAGE_GENERAL_BACKUP_FOLDER = os.getenv('STORAGE_GENERAL_BACKUP_FOLDER', './storages/general_test')

# 创建 Celery 实例
celery_app = Celery('recognizer_tasks', broker=amqp_url)
celery_app.conf.broker_transport_options = {
    'max_retries': 3,  # 最大重试次数
    'interval_start': 0,  # 初始重试间隔（秒）
    'interval_step': 0.2,  # 每次重试间隔增加量（秒）
    'interval_max': 0.5,  # 最大重试间隔（秒）
}

celery_app.conf.worker_concurrency = 1
celery_app.conf.worker_disable_rate_limits = True

f_lock = threading.Lock()

# 启用手动 ack
celery_app.conf.task_acks_late = False
# 当 worker 丢失时拒绝消息，让消息重新放回队列
celery_app.conf.task_reject_on_worker_lost = True

celery_app.conf.update(
    task_create_missing_queues=True,
)
logger = setup_logging("recognizer")

existing_tasks = {}
import heapq
class RtspBlackManager:
    def __init__(self):
        self._store = {}
        self._heap = []
        self._lock = threading.Lock()
        self.cleaner = threading.Thread(target=self._cleanup, daemon=True)
        self.cleaner.start()

    def _cleanup(self):
        while True:
            with self._lock:
                now = int(time.time() * 1000)
                while self._heap and self._heap[0][0] < now:
                    expire, addr = heapq.heappop(self._heap)
                    # 校验是否为最新过期时间
                    if self._store.get(addr) == expire:
                        logger.warning("del blacklist: {}".format(addr))
                        del self._store[addr]
            # 动态休眠时间优化
            next_expire = self._heap[0][0] if self._heap else None
            sleep_time = min(
                (next_expire - time.time()) if next_expire else 10,
                10
            )
            time.sleep(max(sleep_time, 0.1))

    def check(self, rtsp_addr: str) -> bool:
        """
        检查地址是否在黑名单中
        """
        with self._lock:
            expire = self._store.get(rtsp_addr)
            if not expire:
                return False
            if time.time() > expire:
                del self._store[rtsp_addr]
                return False
            return True

    def __contains__(self, rtsp_addr: str) -> bool:
        return self.check(rtsp_addr)

    def __del__(self):
        self.cleaner.join()

    def add(self, rtsp_url, expired_time=5 * 60 * 1000):
        with self._lock:
            # 更新最新过期时间
            if rtsp_url in self._store:
                if expired_time <= self._store[rtsp_url]:
                    return
            self._store[rtsp_url] = expired_time
            heapq.heappush(self._heap, (expired_time, rtsp_url))

black_manager = RtspBlackManager()

def des_signal_config(config: str):
    if config is None or len(config) == 0:
        return None
    try:
        return SignalConfig.model_validate_json(config)
    except Exception as e:
        logger.exception(f"解析信号配置时出错: {e}")
    return None


class PathwayConcurrencyControl:
    """基于pathway_id的分布式并发控制器"""
    LOCK_TIMEOUT = 60 * 5  # 锁超时时间（秒）

    def __init__(self, lock_dir='locks'):
        self.lock_dir = lock_dir
        os.makedirs(lock_dir, exist_ok=True)

    def _get_lock_path(self, pathway_id):
        return Path(os.path.join(self.lock_dir, f"pathway_{pathway_id}.lock"))

    def acquire(self, pathway_id):
        lock_file = self._get_lock_path(pathway_id)
        try:
            fd = open(lock_file, 'w')
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            return False

    def release(self, pathway_id):
        lock_file = self._get_lock_path(pathway_id)
        if lock_file.exists():
            lock_file.unlink()

pathway_control = PathwayConcurrencyControl()


def pathway_concurrency_limit(func):

    global pathway_control
    """并发控制装饰器"""

    @wraps(func)
    def wrapper(pathway_id, *args, **kwargs):
        # 尝试获取锁
        if not pathway_control.acquire(pathway_id):
            # 触发任务重试
            raise func.retry(
                args=(pathway_id, *args),
                kwargs=kwargs,
                countdown=10,  # 重试间隔
                max_retries=3  # 最大重试次数
            )

        try:
            return func(pathway_id, *args, **kwargs)
        finally:
            # 确保释放锁
            pathway_control.release(pathway_id)

    return wrapper

@celery_app.task(
    name='celery_app.perform_recognition',  # 确保模块路径正确
    bind=True,
    # base=Singleton,
    # unique_on=['pathway_id'],
    # raise_on_duplicate=True  # 重复任务直接抛出异常
)
def perform_recognition(cl, pathway_id):
    global black_manager
    session = Session()
    try:
        pathway = session.query(Signal).get(pathway_id)
        if not pathway:
            logger.warning(f"未找到通路 ID 为 {pathway_id} 的记录")
            return

        if pathway.status != 1:
            logger.info(f"通路 {pathway_id} 采集状态非活跃，跳过识别任务")
            return
        collect_sender = MQSender(amqp_url, QUEUE_RECOGNIZER_COLLECTION)
        signal_config = des_signal_config(pathway.config)
        frame_config = signal_config.frame
        frame_storage_config = FrameStorageConfig(store_folder=frame_config.storage.frameStoragePath,
                                            image_suffix=frame_config.storage.frameImageSuffix)
        frame_read_config = FrameReadConfig(frame_interval_seconds=frame_config.read.frameIntervalSeconds,
                                            frame_retry_times=frame_config.read.frameRetryTimes,
                                            frame_retry_interval=frame_config.read.frameRetryInterval,
                                            frame_window=frame_config.read.frameWindow
                                            )

        task_id = pathway.current_task_id
        cameras = session.query(db.Camera).filter(db.Camera.signal_id == pathway_id).all()
        for camera in cameras:
            try:
                if camera.is_rtsp_expired():
                    logger.warning(f"[{task_id}][{camera.index_code} - {camera.name}] RTSP地址已过期: {camera.latest_rtsp_url}")
                    continue
                if black_manager.check(camera.latest_rtsp_url):
                    logger.warning(f"[{task_id}][{camera.index_code} - {camera.name}] 存在黑名单中. RTSP访问: {camera.latest_rtsp_url}")
                    continue
                pathway = session.query(Signal).get(pathway_id)
                if not pathway:
                    logger.warning(f"未找到通路 ID 为 {pathway_id} 的记录")
                    continue

                if pathway.status != 1:
                    logger.info(f"通路 {pathway_id} 采集状态非活跃，跳过识别任务")
                    continue
                storage_folder = frame_storage_config.get_storage_folder()
                ts = int(time.time() * 1000)
                image_path = os.path.join(storage_folder, f"{pathway.id}-{get_frame_date_format(ts=ts)}-{camera.index_code}.{frame_storage_config.image_suffix}")
                # 读取frame
                ret, frame = read_rtsp_frame(camera.latest_rtsp_url)
                if ret is False:
                    black_manager.add(camera.latest_rtsp_url, camera.latest_rtsp_expired_time)
                    continue
                collector = {}
                # 保存帧图片
                os.makedirs(storage_folder, exist_ok=True)
                with f_lock:
                    cv2.imwrite(image_path, frame)
                if pathway.is_general():
                    logger.info("# 通用图片拷贝备份至 general_test")
                    os.makedirs(STORAGE_GENERAL_BACKUP_FOLDER, exist_ok=True)
                    shutil.copy2(image_path, STORAGE_GENERAL_BACKUP_FOLDER)
                collector['frame'] = {"frameImagePath": image_path,
                                      'timestamp': int(time.time() * 1000) if ts is None else ts}
                if signal_config.algo.label.enabled:
                    try:
                        tag_image, tag_json = None, None
                        if pathway.is_general():
                            ret = general_annotation(
                                os.path.join(STORAGE_GENERAL_BACKUP_FOLDER, os.path.basename(image_path)))
                            if ret is not None:
                                tag_image, tag_json = ret[0], ret[1]
                        else:
                            logger.info("# 识别图像（带label）")
                            label_images = os.path.join(storage_folder, "label_images")
                            tag_image, tag_json = recognize_image_with_label(image_path, output_path=label_images)
                        shapes = read_shapes(tag_json)
                        collector['label'] = {'labelImagePath': tag_image, 'shapes': shapes,
                                              'labelJsonPath': tag_json,
                                              'timestamp': int(time.time() * 1000)}
                    except Exception as e:
                        logger.exception("recognize_image_with_label error")
                # ret, collection = read_and_recognize(rtsp_url=camera.latest_rtsp_url,
                #                               storage_folder=frame_storage_config.get_storage_folder(),
                #                               image_path=image_path,
                #                               algo_label_opened=,
                #                                      is_general=pathway.is_general(),
                #                                      ts=ts)

                message = {
                    "camera": {"indexCode": camera.index_code},
                    'signal': {"signalId": pathway_id},
                    'taskId': task_id,
                    "collect": collector,
                    "timestamp": int(time.time() * 1000),
                }
                logger.info(f"send collect message: {json.dumps(message)}")
                asyncio.run(collect_sender.send_message(message))
            except Exception as e:
                logger.exception(f"执行[{camera.index_code} - {camera.name}]识别任务时出错: {e}")
    except Exception as e:
        logger.exception(f"执行识别任务时出错: {e}")
    finally:
        session.close()

def get_frame_date_format(format='%Y%m%d%H%M%S', ts=None):
    import datetime, pytz
    # 将时间戳转换为datetime对象
    dt_object = datetime.datetime.fromtimestamp(int(time.time()) if ts is None else int(ts / 1000))
    dt_object = dt_object.astimezone(pytz.timezone('Asia/Shanghai'))
    return dt_object.strftime(format)

def read_frame(rtsp_url):
    cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
    cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 30 * 1000)
    cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
    cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 10 * 1000)  # 新增连接超时设置
    # 硬件解码优化
    cap.set(cv2.CAP_PROP_HW_ACCELERATION, cv2.VIDEO_ACCELERATION_ANY)
    max_times = 3
    read_times = 0
    # # 开启异步线程并读取指定帧
    if not cap.isOpened():
        logger.error("could not open rtsp stream, please check url address. rtstp_url: {}".format(rtsp_url))
        raise Exception("could not open rtsp stream, please check url address. rtstp_url: {}".format(rtsp_url))
    fps = cap.get(cv2.CAP_PROP_FPS)
    logger.info(f"视频流[{rtsp_url}]帧率：{fps}")
    if fps > 10000:
        raise Exception(f"视频流[{rtsp_url}]帧率异常：{fps}")
    while read_times < max_times:
        ret, frame = cap.read()
        if not ret:
            logger.error(f"Failed to read frame from {rtsp_url}")
            read_times += 1
            continue
        return frame
    return None


def read_rtsp_frame(rtsp_url):
    try:
        frame = read_frame(rtsp_url)
    except Exception as e:
        logger.exception(f"读取帧时出错: {e}")
        return False, None
    if frame is None:
        logger.error(f"Failed to read frame from {rtsp_url}")
        return False, None
    return True, frame

def read_and_recognize(rtsp_url, storage_folder, image_path, algo_label_opened=False, ts=None, is_general=False):
    collector = {}
    try:
        frame = read_frame(rtsp_url)
    except Exception as e:
        logger.exception(f"读取帧时出错: {e}")
        return False, None
    if frame is None:
        logger.error(f"Failed to read frame from {rtsp_url}")
        return False, None
    os.makedirs(storage_folder, exist_ok=True)
    with f_lock:
        cv2.imwrite(image_path, frame)
    collector['frame'] = {"frameImagePath": image_path, 'timestamp': int(time.time() * 1000) if ts is None else ts}
    if algo_label_opened:
        logger.info("# 识别图像（带label）")
        label_images = os.path.join(storage_folder, "label_images")
        try:
            tag_image, tag_json = recognize_image_with_label(image_path, output_path=label_images)
            shapes = read_shapes(tag_json)
            collector['label'] = {'labelImagePath': tag_image, 'shapes': shapes, 'labelJsonPath': tag_json,
                                   'timestamp': int(time.time() * 1000)}
        except Exception as e:
            logger.exception("recognize_image_with_label error")
    return True, collector



class TaskScheduler:
    _lock = threading.Lock()

    def __init__(self):
        self._schedule = {}

    def gen_schedule_entry(self, frequency, pid):
        return {
            "task": "celery_app.perform_recognition",
            "schedule": frequency,
            "args": (pid,),
            'options': {'queue': f'pathway_{pid}'}
        }

    def refresh_tasks(self):
        """从数据库加载最新任务配置"""
        print(f"app scheduler: {celery_app.conf.beat_schedule}")
        with self._lock:
            session = Session()
            try:
                active_pathways = session.query(Signal).filter(Signal.status == 1).all()
                new_schedule = {}

                for p in active_pathways:
                    signal_config: SignalConfig = des_signal_config(p.config)
                    task_name = f'pathway_{p.id}_recognition'
                    if task_name in self._schedule.keys():
                        # 如果任务已存在，检查频率是否变化
                        if signal_config and self._schedule[task_name]['schedule'] != signal_config.get_frequency():
                            # 频率变化，先移除旧任务
                            logger.warning("频率变化，先移除旧任务")
                            del celery_app.conf.beat_schedule[task_name]
                            new_schedule[task_name] = self.gen_schedule_entry(signal_config.get_frequency(), p.id)
                        else:
                            new_schedule[task_name] = self._schedule[task_name]
                    else:
                        # 若为新任务
                        entry = self.gen_schedule_entry(signal_config.get_frequency(), p.id)
                        logger.info(f"add new schedule. task={task_name}. entry={entry}")
                        new_schedule[task_name] = entry

                    # 移除已不存在的任务
                for task_name in set(self._schedule.keys()) - set(new_schedule.keys()):
                    logger.warning(f"delete schedule. task={task_name}")
                    del celery_app.conf.beat_schedule[task_name]
                # 原子操作更新任务表
                celery_app.conf.beat_schedule.update(new_schedule)
                self._schedule = new_schedule
            except Exception as e:
                logger.exception("refresh task error")
            finally:
                session.close()

scheduler = TaskScheduler()

# 配置定时任务
@celery_app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    # global scheduler
    # scheduler.refresh_tasks()
    # update_periodic_tasks(sender)
    # 每5秒隔一段时间检查并更新任务
    # sender.add_periodic_task(10, lambda : update_periodic_tasks(sender), name='update_periodic_tasks')
    sender.add_periodic_task(10, refresh_pathway_tasks.s(), name='refresh_pathway_tasks')


@celery_app.task
def refresh_pathway_tasks():
    global scheduler
    logger.info("refresh_pathway_tasks")
    scheduler.refresh_tasks()

# @beat_init.connect
# def init_scheduler(**kwargs):
#     """Beat启动时初始化"""
#     scheduler.refresh_tasks()

@celery_app.task
def update_periodic_tasks(sender):
    global existing_tasks
    logger.info('update_periodic_tasks...')
    session = Session()
    try:
        # 筛选开启状态的记录
        pathways = session.query(Signal).filter(Signal.status == 1).all()
        logger.info("opened pathways: {}".format(len(pathways)))
        new_existing_tasks = {}

        for pathway in pathways:
            signal_config: SignalConfig = des_signal_config(pathway.config)
            task_name = f'pathway_{pathway.id}_recognition'
            if task_name in existing_tasks.keys():
                # 如果任务已存在，检查频率是否变化
                if signal_config and existing_tasks[task_name]['frequency'] != signal_config.get_frequency():
                    # 频率变化，先移除旧任务
                    sender.remove_periodic_task(existing_tasks[task_name]['entry'])
                    # 添加新任务
                    logger.info(f"add new task: {task_name}, frequency: {signal_config.get_frequency()}")
                    entry = sender.add_periodic_task(
                        schedule=signal_config.get_frequency(),
                        sig=perform_recognition.s(pathway.id),
                        name=task_name
                    )
                    new_existing_tasks[task_name] = {
                        'frequency': signal_config.get_frequency(),
                        'entry': entry
                    }
                else:
                    # 频率未变化，保留旧任务
                    new_existing_tasks[task_name] = existing_tasks[task_name]
            else:
                # 新任务，添加到调度中
                logger.info(f"add new task: {task_name}, frequency: {signal_config.get_frequency()}")
                entry = sender.add_periodic_task(
                    schedule=signal_config.get_frequency(),
                    sig=perform_recognition.s(pathway.id),
                    name=task_name
                )
                new_existing_tasks[task_name] = {
                    'frequency': signal_config.get_frequency(),
                    'entry': entry
                }

        # 移除已不存在的任务
        for task_name in set(existing_tasks.keys()) - set(new_existing_tasks.keys()):
            sender.remove_periodic_task(existing_tasks[task_name]['entry'])

        existing_tasks = new_existing_tasks

    except Exception as e:
        logger.exception(f"更新定时任务时出错: {e}")
    finally:
        session.close()


def test_result(r):
    if r == 1:
        return
    return [1, 2]


if __name__ == '__main__':
    r1 = test_result(1)
    print(r1)
    r3=test_result(2)
    print(r3)
    # dtg = get_frame_date_format()
    # print(dtg)
