import asyncio
import heapq
import os
import threading
import time

import cv2
from celery import Celery
from dotenv import load_dotenv

import db
from algorithm import recognize_image_with_label
from capture import FrameStorageConfig, FrameReadConfig, RtspReadConfig, CaptureFrame
from db import Session, Signal, Camera
from entity import SignalConfig, Collector, FrameCollectorValue, LabelCollectorValue
from mq import MQSender
from recognizer import read_shapes
from setup import setup_logging, TaskLoggingFilter

load_dotenv()  # 加载环境变量

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'guest')
RABBITMQ_VHOST = os.getenv('RABBITMQ_VHOST', '/')
# QUEUE_RECOGNIZER_COLLECTION = os.getenv('QUEUE_RECOGNIZER_COLLECTION', 'zjjt:camera_recognizer:collection:v2')
QUEUE_RECOGNIZER_COLLECTION = 'zjjt:camera_recognizer:collection:v2'
amqp_url = f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}{RABBITMQ_VHOST}"

# 创建 Celery 实例
celery_app = Celery('recognizer_tasks', broker=amqp_url)
celery_app.conf.broker_transport_options = {
    'max_retries': 3,  # 最大重试次数
    'interval_start': 0,  # 初始重试间隔（秒）
    'interval_step': 0.2,  # 每次重试间隔增加量（秒）
    'interval_max': 0.5,  # 最大重试间隔（秒）
}

f_lock = threading.Lock()

# 启用手动 ack
celery_app.conf.task_acks_late = True
# 当 worker 丢失时拒绝消息，让消息重新放回队列
celery_app.conf.task_reject_on_worker_lost = True

celery_app.conf.update(
    task_create_missing_queues=True,
)
logger = setup_logging("recognizer")

existing_tasks = {}


class CameraAppLogContext:
    """摄像头日志上下文管理器"""
    def __init__(self, logger, task_id, camera):
        self.logger = logger
        self.task_id = task_id
        self.camera = camera
        # 查找过滤器实例
        self.filter = next(f for f in self.logger.filters if isinstance(f, TaskLoggingFilter))

    def __enter__(self):
        # 设置当前上下文信息
        if self.filter:
            self.filter.set_context(
                self.task_id,
                self.camera.index_code,
                self.camera.name
            )
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 清理上下文信息
        if self.filter:
            self.filter.set_context('', '', '')


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
collect_sender = MQSender(amqp_url, QUEUE_RECOGNIZER_COLLECTION)

class CameraReader:
    rtsp_url = None
    cap = None
    fps = None

    def __init__(self, rtsp_url: str, frame_read_config: FrameReadConfig, rtsp_read_config: RtspReadConfig = None):
        self.rtsp_url = rtsp_url
        self.frame_read_config = frame_read_config
        self.rtsp_read_config = rtsp_read_config if rtsp_read_config is not None else RtspReadConfig()
        self._create_cap()

    def _create_cap(self):
        cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
        cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, self.rtsp_read_config.timeout * 1000)
        cap.set(cv2.CAP_PROP_BUFFERSIZE, self.rtsp_read_config.buffer_size)
        cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, self.rtsp_read_config.open_timeout * 1000)  # 新增连接超时设置
        # 硬件解码优化
        cap.set(cv2.CAP_PROP_HW_ACCELERATION, cv2.VIDEO_ACCELERATION_ANY)
        self.cap = cap

    def rtsp_valid(self):
        if not self.cap.isOpened():
            logger.error("could not open rtsp stream, please check url address. rtstp_url: {}".format(self.rtsp_url))
            raise Exception("could not open rtsp stream, please check url address. rtstp_url: {}".format(self.rtsp_url))
        fps = self.cap.get(cv2.CAP_PROP_FPS)
        logger.info(f"视频流[{self.rtsp_url}]帧率：{fps}")
        if fps > 10000:
            raise Exception(f"视频流[{self.rtsp_url}]帧率异常：{fps}")
        self.fps = fps

    def read_frame(self):
        self.rtsp_valid()
        retries = 0
        max_retries = self.frame_read_config.frame_retry_times

        while retries < max_retries:
            if not self.cap.isOpened():
                print("could not open rtsp stream, please check url address. rtstp_url: {}".format(self.rtsp_url))
                return None
            ret, frame = self.cap.read()
            if ret and frame is not None:
                return CaptureFrame(id=retries, frame=frame)
            print(f"尝试读取帧「{self.rtsp_url}」失败，重试 {retries + 1}/{max_retries}...")
            retries += 1
            time.sleep(self.frame_read_config.frame_retry_interval)  # 等待 1 秒后重试
        return None


class RecognizerApp:
    task_id = None
    camera_reader = None
    sender = None
    collector = Collector()
    black_manager = None

    def __init__(self,
                 task_id,
                 camera: Camera,
                 pathway: Signal,
                 signal_config: SignalConfig,
                 frame_storage_config: FrameStorageConfig,
                 frame_read_config: FrameReadConfig,
                 rtsp_black_manager: RtspBlackManager
                 ):
        self.task_id = task_id
        self.camera = camera
        self.pathway = pathway
        self.signal_config = signal_config
        self.frame_storage_config = frame_storage_config
        self.camera_reader = CameraReader(
            rtsp_url=self.camera.latest_rtsp_url,
            frame_read_config=frame_read_config
        )
        self.black_manager = rtsp_black_manager

    def read_and_recognize(self):
        try:
            camera_frame = self.camera_reader.read_frame()
        except Exception as e:
            logger.exception(f"[{self.task_id}][{self.camera.index_code} - {self.camera.name}] 读取帧失败: {e}")
            self.black_manager.add(rtsp_url=self.camera.latest_rtsp_url,
                                   expired_time=self.camera.latest_rtsp_expired_time)
            return False
        if camera_frame is None:
            self.black_manager.add(rtsp_url=self.camera.latest_rtsp_url,
                                   expired_time=self.camera.latest_rtsp_expired_time)
            return False
        storage_folder = self.frame_storage_config.get_storage_folder()
        os.makedirs(storage_folder, exist_ok=True)
        image_filename =  f"{self.pathway.id}-{camera_frame.get_frame_date_format()}-{self.camera.index_code}.{self.frame_storage_config.image_suffix}"
        image_path = os.path.join(storage_folder, image_filename)
        with f_lock:
            cv2.imwrite(image_path, camera_frame.frame)
        self.collector.add('frame', FrameCollectorValue(frameImagePath=image_path, timestamp=camera_frame.timestamp))
        if self.signal_config.is_labels_enabled():
            logger.info(f"[{self.task_id}][{self.camera.index_code} - {self.camera.name}] # 识别图像（带label）")
            label_images = os.path.join(storage_folder, "label_images")
            try:
                tag_image, tag_json = recognize_image_with_label(image_path, output_path=label_images)
                shapes = read_shapes(tag_json)
                self.collector.add('label', LabelCollectorValue(shapes=shapes, labelJsonPath=tag_json,
                                                                labelImagePath=label_images))
            except Exception as e:
                logger.exception(f"[{self.task_id}][{self.camera.index_code} - {self.camera.name}] recognize_image_with_label error")
        return True

    def get_collection(self):
        return self.collector.attr


def des_signal_config(config: str):
    if config is None or len(config) == 0:
        return None
    try:
        return SignalConfig.model_validate_json(config)
    except Exception as e:
        logger.exception(f"解析信号配置时出错: {e}")
    return None


@celery_app.task(
    name='celery_app.perform_recognition',  # 确保模块路径正确
    bind=True,
    # base=Singleton,
    # unique_on=['pathway_id'],
    # raise_on_duplicate=True  # 重复任务直接抛出异常
)
def perform_recognition(cl, pathway_id):
    """
    :param cl:
    :param pathway_id: 通路ID
    :return:
    """
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
            # with CameraAppLogContext(logger, task_id, camera) as ctx_logger:
            #     ctx_logger.info(f"开始识别设备")
            #     # logger.info(f"开始识别设备:[{camera.index_code} - {camera.name}]")
            #     try:
            #         if camera.is_rtsp_expired():
            #             # logger.warning(
            #             #     f"[{task_id}][{camera.index_code} - {camera.name}] RTSP地址已过期: {camera.latest_rtsp_url}")
            #             ctx_logger.warning(
            #                 f" RTSP地址已过期: {camera.latest_rtsp_url}")
            #             continue
            #         if black_manager.check(camera.latest_rtsp_url):
            #             # logger.warning(
            #             #     f"[{task_id}][{camera.index_code} - {camera.name}] 存在黑名单中. RTSP访问: {camera.latest_rtsp_url}")
            #             ctx_logger.warning('[{camera.latest_rtsp_url}]存在黑名单中. 禁止访问')
            #             continue
            #         # storage_folder = frame_storage_config.get_storage_folder()
            #         # image_path = os.path.join(storage_folder, f"{pathway.id}-{get_frame_date_format()}-{camera.index_code}.{frame_storage_config.image_suffix}")
            #         # ret, collection = read_and_recognize(rtsp_url=camera.latest_rtsp_url,
            #         #                               storage_folder=frame_storage_config.get_storage_folder(),
            #         #                               image_path=image_path,
            #         #                               algo_label_opened=signal_config.algo.label.enabled)
            #         # if ret is False:
            #         #     black_manager.add(camera.latest_rtsp_url, camera.latest_rtsp_expired_time)
            #         #     continue
            #         # message = {
            #         #     "camera": {"indexCode": camera.index_code},
            #         #     'signal': {"signalId": pathway_id},
            #         #     'taskId': task_id,
            #         #     "collect": collection,
            #         #     "timestamp": int(time.time() * 1000),
            #         # }
            #         # logger.info(f"send collect message: {json.dumps(message)}")
            #         r_app = RecognizerApp(
            #             task_id=task_id,
            #             camera=camera,
            #             pathway=pathway,
            #             signal_config=signal_config,
            #             frame_storage_config=frame_storage_config,
            #             frame_read_config=frame_read_config,
            #             rtsp_black_manager=black_manager,
            #         )
            #         ret = r_app.read_and_recognize()
            #         if not ret:
            #             ctx_logger.warning(
            #                 f" read_and_recognize failed.")
            #             continue
            #         message = {
            #             "camera": {"indexCode": camera.index_code},
            #             'signal': {"signalId": pathway_id},
            #             'taskId': task_id,
            #             "collect": r_app.get_collection(),
            #             "timestamp": int(time.time() * 1000),
            #         }
            #         asyncio.run(collect_sender.send_message(message))
            #         del r_app
            #         time.sleep(0.001)
            #     except Exception as e:
            #         ctx_logger.exception(f"执行[{camera.index_code} - {camera.name}]识别任务时出错: {e}")
            logger.info(f"开始识别设备")
            # logger.info(f"开始识别设备:[{camera.index_code} - {camera.name}]")
            try:
                if camera.is_rtsp_expired():
                    logger.warning(
                        f"[{task_id}][{camera.index_code} - {camera.name}] RTSP地址已过期: {camera.latest_rtsp_url}")
                    continue
                if black_manager.check(camera.latest_rtsp_url):
                    logger.warning(
                        f"[{task_id}][{camera.index_code} - {camera.name}] 存在黑名单中. RTSP禁止访问: {camera.latest_rtsp_url}")
                    continue
                # storage_folder = frame_storage_config.get_storage_folder()
                # image_path = os.path.join(storage_folder, f"{pathway.id}-{get_frame_date_format()}-{camera.index_code}.{frame_storage_config.image_suffix}")
                # ret, collection = read_and_recognize(rtsp_url=camera.latest_rtsp_url,
                #                               storage_folder=frame_storage_config.get_storage_folder(),
                #                               image_path=image_path,
                #                               algo_label_opened=signal_config.algo.label.enabled)
                # if ret is False:
                #     black_manager.add(camera.latest_rtsp_url, camera.latest_rtsp_expired_time)
                #     continue
                # message = {
                #     "camera": {"indexCode": camera.index_code},
                #     'signal': {"signalId": pathway_id},
                #     'taskId': task_id,
                #     "collect": collection,
                #     "timestamp": int(time.time() * 1000),
                # }
                # logger.info(f"send collect message: {json.dumps(message)}")
                r_app = RecognizerApp(
                    task_id=task_id,
                    camera=camera,
                    pathway=pathway,
                    signal_config=signal_config,
                    frame_storage_config=frame_storage_config,
                    frame_read_config=frame_read_config,
                    rtsp_black_manager=black_manager,
                )
                ret = r_app.read_and_recognize()
                if not ret:
                    logger.warning(
                        f"[{task_id}][{camera.index_code} - {camera.name}] read_and_recognize failed.")
                    continue
                message = {
                    "camera": {"indexCode": camera.index_code},
                    'signal': {"signalId": pathway_id},
                    'taskId': task_id,
                    "collect": r_app.get_collection(),
                    "timestamp": int(time.time() * 1000),
                }
                asyncio.run(collect_sender.send_message(message))
                del r_app
                time.sleep(0.001)
            except Exception as e:
                logger.exception(f"[{task_id}][{camera.index_code} - {camera.name}] 识别任务时出错: {e}")
    except Exception as e:
        logger.exception(f"执行识别任务时出错: {e}")
    finally:
        session.close()


def get_frame_date_format(format='%Y%m%d%H%M%S'):
    import datetime, pytz
    # 将时间戳转换为datetime对象
    dt_object = datetime.datetime.fromtimestamp(int(time.time()))
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


def read_and_recognize(rtsp_url, storage_folder, image_path, algo_label_opened=False):
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
    collector['frame'] = {"frameImagePath": image_path, 'timestamp': int(time.time() * 1000)}
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


if __name__ == '__main__':
    pathway_intervals = {}
    interval = 5
    while True:
        session = Session()
        try:
            active_pathways = session.query(Signal).filter(Signal.status == 1).all()
            for pathway in active_pathways:
                rt = celery_app.send_task(
                    'celery_app.perform_recognition',
                    args=(pathway.id,),
                )
                print("Rt", rt)
            time.sleep(5)
        finally:
            session.close()
