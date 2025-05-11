import asyncio
import json
import logging
import os
import sys
import threading
import time
from typing import List

import cv2
from dotenv import load_dotenv

from algorithm import recognize_image_with_label
from capture import CameraRtspCapture, FrameStorageConfig, FrameReadConfig
from entity import CameraRecognizerTask, Camera, CameraRtsp, FrameCollectorValue, \
    LabelCollectorValue, Collector, Signal
from mq import MQSender
from setup import setup_logging, TaskLoggingFilter

load_dotenv()  # 加载环境变量

ALGO_DIR = os.getenv("ALGO_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), 'algo'))
STORAGE_LABEL_IMAGE_FOLDER = os.getenv("STORAGE_LABEL_IMAGE_FOLDER", 'label_images')
ALGO_IMAGE_ENHANCE_ROOT = os.getenv("ALGO_IMAGE_ENHANCE_ROOT", './algo/dataset_image_classification/train')

# 将 algo 目录添加到系统路径中
sys.path.append(ALGO_DIR)

logger = setup_logging("recognizer")


# 定义一个线程锁
file_lock = threading.Lock()

def read_shapes(json_path):
    try:
        # 使用 with 语句打开文件
        with open(json_path, 'r', encoding='utf-8') as file:
            # 读取并解析 JSON 文件内容
            data = json.load(file)
            return data.get("shapes", [])
    except FileNotFoundError:
        logging.error(f"指定的label json  {json_path} 未找到。")
    except json.JSONDecodeError:
        logging.error(f"解析 {json_path} 时出错，文件可能不是有效的 JSON 格式。")
    except Exception as e:
        logging.error(f"发生未知错误: {e}")
    return []


class RecognizeTask(object):
    task: CameraRecognizerTask = None
    camera: Camera = None
    rtsp: CameraRtsp = None
    frame_storage_config: FrameStorageConfig = None
    frame_read_config: FrameReadConfig = None

    # 相似度阈值
    similarity_threshold: float = 0.9
    running = False
    last_read_seconds:int = 0

    def is_active(self):
        return self.rtsp is not None and not self.rtsp.is_expired()

    def __init__(self, camera_task: CameraRecognizerTask,
                 frame_storage_config: FrameStorageConfig,
                 frame_read_config: FrameReadConfig,
                 collect_sender: MQSender=None
                 ):
        self.task = camera_task
        self.camera = camera_task.camera
        self.rtsp = camera_task.rtsp
        self.frame_storage_config = frame_storage_config
        self.frame_read_config = frame_read_config
        self.sender = collect_sender
        logger.addFilter(TaskLoggingFilter(camera_task))

    def create_cap(self):
        return CameraRtspCapture(self.rtsp.url, frame_read_config=self.frame_read_config)


    async def read_and_recognize(self):
        collector = Collector()
        try:
            cap = self.create_cap()
            image_dir = self.frame_storage_config.get_storage_folder()
            os.makedirs(image_dir, exist_ok=True)
            frame = await cap.read_single_frame(last_frame_seconds=self.last_read_seconds)
            if frame is None:
                logger.warning(f"[{self.camera.indexCode} - {self.camera.name}][{self.rtsp.url}]视频帧读取失败")
                return
            self.last_read_seconds = int(time.time())
            image_path = os.path.join(image_dir, f"{self.task.signal.signalId}-{frame.get_frame_date_format()}-{self.camera.indexCode}.{self.frame_storage_config.image_suffix}")
            # with file_lock:  # 使用线程锁保护文件写入操作
            cv2.imwrite(image_path, frame.frame)
            collector.add("frame", FrameCollectorValue(frameImagePath=image_path, timestamp=frame.get_timestamp_ms()))
            logger.info(
                f"[{self.camera.indexCode} - {self.camera.name}][{self.rtsp.url}]视频帧 Image saved to {image_path}")
            await self.do_recognizer_algo(image_path, collector)
        finally:
            await self.send_collect_message(collector)

    async def do_recognizer_algo(self, image_path, collector):
        # img_64 = self.cap.frame_to_image64(frame)
        # if img_64 is None:
        #     print("frame转image64失败")
        #     return
        # # 1. 去重识别，对同类别下的文件夹内的图片做相似度比对
        # print("# 1. 去重识别，对同类别下的文件夹内的图片做相似度比对")
        # for exist_img_64 in read_exist_img_64():
        #     compare_result = calculate_similarity(img_64, exist_img_64)
        #     #
        #     if compare_result > self.similarity_threshold:
        #         print(
        #             "图片相似度大于{}, img_64={}, exist_img_64={}".format(self.similarity_threshold,
        #                                                                            img_64, exist_img_64))
        #         return
        # 2. 增强图像
        # logger.info("# 2. 增强图像")
        # relative_task_image_path = os.path.join(self.camera.indexCode, self.task.taskId)
        # enhance_image_dir = os.path.join(ALGO_IMAGE_ENHANCE_ROOT, relative_task_image_path)
        # copy_and_rename_folder(image_dir, enhance_image_dir)
        # enhance_image(relative_task_image_path)
        # 3. 识别图像（带label）
        alog_config = self.task.get_algo_config()
        if alog_config is None:
            logger.warning("通路算法配置为空")
            return
        if alog_config.label is not None and alog_config.label.enabled:
            logger.info("# 识别图像（带label）")
            label_images = os.path.join(self.frame_storage_config.get_storage_folder(), "label_images")
            os.makedirs(label_images, exist_ok=True)
            tag_image, tag_json = recognize_image_with_label(image_path, output_path=label_images)
            shapes = read_shapes(tag_json)
            collector.add("label", LabelCollectorValue(labelImagePath=tag_image, labelJsonPath=tag_json, shapes=shapes))
        # print("识别结果: label_img64={}, labels={}".format(label_img64, labels))
        # 将识别后的结果推送至消息队列

    # 发送采集消息
    async def send_collect_message(self, collector):
        message = {
            "task": self.task,
            "collect": collector.attr if collector is not None else {},
            "timestamp": int(time.time() * 1000),
        }
        if self.sender is not None:
            try:
                await self.sender.send_message(message)
                logger.info(f"发送采集任务成功. taskId={self.task.taskId}, collection={collector.attr}")
            except Exception as e:
                logger.error(f"[{self.camera.indexCode} - {self.camera.name}][{self.rtsp.url}]发送采集消息失败", e)

    def update(self, task):
        self.task = task.task
        self.camera = task.camera
        self.rtsp = task.rtsp
        self.frame_storage_config = task.frame_storage_config
        self.frame_read_config = task.frame_read_config


class RecognizeTaskV2:
    def __init__(self,
                 task_id: str,
                 camera: Camera,
                 rtsp: CameraRtsp,
                 signal: Signal,
                 frame_storage_config: FrameStorageConfig,
                 frame_read_config: FrameReadConfig,
                 collect_sender: MQSender=None):
        self.task_id = task_id
        self.camera = camera
        self.rtsp = rtsp
        self.signal = signal
        self.frame_storage_config = frame_storage_config
        self.frame_read_config = frame_read_config
        self.sender = collect_sender
        self.collector = Collector()

    def _create_cap(self):
        return CameraRtspCapture(self.rtsp.url, frame_read_config=self.frame_read_config)

    def read_and_algo(self):
        # try:
            cap = self._create_cap()
            image_dir = self.frame_storage_config.get_storage_folder()
            os.makedirs(image_dir, exist_ok=True)
            frame = cap._read_frame_sync()
            if frame is None:
                logger.warning(f"[{self.camera.indexCode} - {self.camera.name}][{self.rtsp.url}]视频帧读取失败")
                return
            image_path = os.path.join(image_dir, f"{self.signal.signalId}-{frame.get_frame_date_format()}-{self.camera.indexCode}.{self.frame_storage_config.image_suffix}")
            with file_lock:  # 使用线程锁保护文件写入操作
                cv2.imwrite(image_path, frame.frame)
            self.collector.add("frame", {"frameImagePath": image_path, 'timestamp': frame.get_timestamp_ms()})
            logger.info(
                f"[{self.camera.indexCode} - {self.camera.name}][{self.rtsp.url}]视频帧 Image saved to {image_path}")
            self.do_recognizer_algo(image_path)
        # finally:
        #     self.send_collect_message()

    async def read_and_recognize(self):
        try:
            # cap = self._create_cap()
            # image_dir = self.frame_storage_config.get_storage_folder()
            # os.makedirs(image_dir, exist_ok=True)
            await asyncio.to_thread(self.read_and_algo)
            # frame = await asyncio.to_thread(self.read_and_algo)
            # if frame is None:
            #     logger.warning(f"[{self.camera.indexCode} - {self.camera.name}][{self.rtsp.url}]视频帧读取失败")
            #     return
            # image_path = os.path.join(image_dir, f"{self.signal.signalId}-{frame.get_frame_date_format()}-{self.camera.indexCode}.{self.frame_storage_config.image_suffix}")
            # # with file_lock:  # 使用线程锁保护文件写入操作
            # cv2.imwrite(image_path, frame.frame)
            # self.collector.add("frame", FrameCollectorValue(frameImagePath=image_path, timestamp=frame.get_timestamp_ms()))
            # logger.info(
            #     f"[{self.camera.indexCode} - {self.camera.name}][{self.rtsp.url}]视频帧 Image saved to {image_path}")
            # await self.do_recognizer_algo(image_path)
        finally:
            await self.send_collect_message()

    async def do_recognizer_algo(self, image_path):
        # img_64 = self.cap.frame_to_image64(frame)
        # if img_64 is None:
        #     print("frame转image64失败")
        #     return
        # # 1. 去重识别，对同类别下的文件夹内的图片做相似度比对
        # print("# 1. 去重识别，对同类别下的文件夹内的图片做相似度比对")
        # for exist_img_64 in read_exist_img_64():
        #     compare_result = calculate_similarity(img_64, exist_img_64)
        #     #
        #     if compare_result > self.similarity_threshold:
        #         print(
        #             "图片相似度大于{}, img_64={}, exist_img_64={}".format(self.similarity_threshold,
        #                                                                            img_64, exist_img_64))
        #         return
        # 2. 增强图像
        # logger.info("# 2. 增强图像")
        # relative_task_image_path = os.path.join(self.camera.indexCode, self.task.taskId)
        # enhance_image_dir = os.path.join(ALGO_IMAGE_ENHANCE_ROOT, relative_task_image_path)
        # copy_and_rename_folder(image_dir, enhance_image_dir)
        # enhance_image(relative_task_image_path)
        # 3. 识别图像（带label）
        alog_config = self.signal.config.algo
        if alog_config is None:
            logger.warning("通路算法配置为空")
            return
        if alog_config.label is not None and alog_config.label.enabled:
            logger.info("# 识别图像（带label）")
            label_images = os.path.join(self.frame_storage_config.get_storage_folder(), "label_images")
            os.makedirs(label_images, exist_ok=True)
            tag_image, tag_json = recognize_image_with_label(image_path, output_path=label_images)
            shapes = read_shapes(tag_json)
            # self.collector.add("label", {}, LabelCollectorValue(labelImagePath=tag_image, labelJsonPath=tag_json, shapes=shapes))
            self.collector.add("label", {"labelImagePath": tag_image, "labelJsonPath": label_images, "shapes": shapes, "timestamp": int(time.time() * 1000)})
        # print("识别结果: label_img64={}, labels={}".format(label_img64, labels))
        # 将识别后的结果推送至消息队列

    # 发送采集消息
    async def send_collect_message(self):
        message = {
            "camera": self.camera,
            'signal': self.signal,
            'taskId': self.task_id,
            "collect": self.collector.attr if self.collector.attr is not None else {},
            "timestamp": int(time.time() * 1000),
        }
        if self.sender is not None:
            try:
                await self.sender.send_message(message)
                logger.info(f"[{self.camera.indexCode} - {self.camera.name}]发送采集任务成功. taskId={self.task_id}, collection={self.collector.attr}")
            except Exception as e:
                logger.exception(f"[{self.camera.indexCode} - {self.camera.name}][{self.rtsp.url}]发送采集消息失败", e)

class TaskManager:
    signal_tasks: dict = None
    camera_task: dict = None
    # 添加线程锁
    lock = threading.Lock()

    def __init__(self):
        self.signal_tasks = {}
        self.signal_threads = {}
        self.camera_task = {}
        self.black_tasks = {}

    def add_task(self, task: RecognizeTask):
        with self.lock:
            signal_id = task.task.signal.signalId
            camera_index_code = task.camera.indexCode
            if signal_id not in self.signal_tasks.keys():
                self.signal_tasks[signal_id] = []
            if camera_index_code not in self.camera_task.keys():
                self.signal_tasks[signal_id].append(task)
                self.camera_task[camera_index_code] = task
            else:
                self.camera_task[camera_index_code].update(task)
            if signal_id not in self.signal_threads.keys():
                signal_thread = threading.Thread(target=self.run_tasks, args=signal_id)
                self.signal_threads[signal_id] = signal_thread
                signal_thread.start()

    def get_signal_active_tasks(self, signal_id):
        active_tasks = []
        tasks = self.signal_tasks.get(signal_id, [])
        for t in tasks:
            if t.is_active():
                active_tasks.append(t)
            else:
                self.add_black_task(signal_id, t)
        return active_tasks

    async def run_pathway_tasks(self, task_list: List[RecognizeTask]):
        # 顺序执行同一通路的任务
        for task in task_list:
            try:
                await task.read_and_recognize()
            except Exception as e:
                logger.error(f"[{task.camera.indexCode} - {task.camera.name}][{task.rtsp.url}]read_and_recognize error. ", e)
            await asyncio.sleep(0.001)

    def add_black_task(self,signal_id, t):
        if signal_id not in self.black_tasks:
            self.black_tasks[signal_id] = []
        self.black_tasks[signal_id].append(t)

    def run_tasks(self, signal_id):
        # 创建一个新的事件循环
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            while True:
                tasks = self.get_signal_active_tasks(signal_id)
                if len(tasks) > 0:
                    # 运行异步方法
                    loop.run_until_complete(self.run_pathway_tasks(tasks))
                time.sleep(0.001)
        finally:
            self.remove_tasks()
            self.black_tasks = {}
            # 关闭事件循环
            loop.close()

    def remove_tasks(self):
        with self.lock:
            for signal_id, tasks in self.black_tasks.items():
                if tasks is not None and len(tasks) > 0:
                    for task in tasks:
                        try:
                            self.signal_tasks[signal_id].remove(task)
                            if task.camera.indexCode in self.camera_task:
                                del self.camera_task[task.camera.indexCode]
                        except ValueError:
                            logger.warning(f"Task {task} not found in signal_tasks[{signal_id}]")

