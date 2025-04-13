import asyncio
import os
import sys
import threading
import time
from typing import List

import cv2
from dotenv import load_dotenv

from algorithm import recognize_image_with_label
from capture import CameraRtspCapture, FrameStorageConfig, FrameReadConfig
from entity import CameraRecognizerTask, Camera, CameraRtsp
from setup import setup_logging, TaskLoggingFilter

load_dotenv()  # 加载环境变量

ALGO_DIR = os.getenv("ALGO_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), 'algo'))
STORAGE_LABEL_IMAGE_FOLDER = os.getenv("STORAGE_LABEL_IMAGE_FOLDER", 'label_images')
ALGO_IMAGE_ENHANCE_ROOT = os.getenv("ALGO_IMAGE_ENHANCE_ROOT", './algo/dataset_image_classification/train')

# 将 algo 目录添加到系统路径中
sys.path.append(ALGO_DIR)

logger = setup_logging("recognizer")


class RecognizeTask(object):
    task: CameraRecognizerTask = None
    camera: Camera = None
    rtsp: CameraRtsp = None
    frame_storage_config: FrameStorageConfig = None
    frame_read_config: FrameReadConfig = None

    # 相似度阈值
    similarity_threshold: float = 0.9
    running = False

    def is_active(self):
        return self.rtsp is not None and not self.rtsp.is_expired()

    def __init__(self, camera_task: CameraRecognizerTask, frame_storage_config: FrameStorageConfig, frame_read_config: FrameReadConfig):
        self.task = camera_task
        self.camera = camera_task.camera
        self.rtsp = camera_task.rtsp
        self.frame_storage_config = frame_storage_config
        self.frame_read_config = frame_read_config
        self.cap = CameraRtspCapture(self.rtsp.url, frame_read_config=frame_read_config)
        logger.addFilter(TaskLoggingFilter(camera_task))

    async def read_and_recognize(self):
        try:
            self.cap.open()
            image_dir = self.frame_storage_config.get_storage_folder()
            os.makedirs(image_dir, exist_ok=True)
            frame = await self.cap.read_single_frame()
            if frame is None:
                logger.warning(f"[{self.camera.indexCode} - {self.camera.name}][{self.rtsp.url}]视频帧读取失败")
                return
            image_path = os.path.join(image_dir, f"{self.camera.indexCode}-{frame.get_frame_date_format()}.{self.frame_storage_config.image_suffix}")
            cv2.imwrite(image_path, frame.frame)
            logger.info(
                f"[{self.camera.indexCode} - {self.camera.name}][{self.rtsp.url}]视频帧 Image saved to {image_path}")
            await self.do_recognizer_algo(image_path)
        finally:
            self.cap.stop()

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
        alog_config = self.task.get_algo_config()
        if alog_config is None:
            logger.warning("通路算法配置为空")
            return
        if alog_config.label is not None and alog_config.label.enabled:
            logger.info("# 识别图像（带label）")
            label_images = os.path.join(self.frame_storage_config.get_storage_folder(), "label_images")
            os.makedirs(label_images, exist_ok=True)
            recognize_image_with_label(image_path, output_path=label_images)
        # print("识别结果: label_img64={}, labels={}".format(label_img64, labels))
        # 将识别后的结果推送至消息队列

    def update(self, task):
        self.task = task.task
        self.camera = task.camera
        self.rtsp = task.rtsp
        self.frame_storage_config = task.frame_storage_config
        self.frame_read_config = task.frame_read_config
        self.cap.update(rtsp_url=self.rtsp.url, rtsp_read_config=task.cap.rtsp_read_config, frame_read_config=self.frame_read_config)


class TaskManager:
    signal_tasks: dict = None
    camera_task: dict = None
    thread: threading.Thread = None

    def __init__(self):
        self.signal_tasks = {}
        self.camera_task = {}
        self.thread = threading.Thread(target=self.run_tasks, daemon=True)
        self.thread.start()

    def add_task(self, task: RecognizeTask):
        signal_id = task.task.signal.signalId
        camera_index_code = task.camera.indexCode
        if signal_id not in self.signal_tasks:
            self.signal_tasks[signal_id] = []
        if camera_index_code not in self.camera_task:
            self.signal_tasks[signal_id].append(task)
            self.camera_task[camera_index_code] = task
        else:
            self.camera_task[camera_index_code].update(task)


    def remove_task(self,signal_id, task: RecognizeTask):
        if signal_id in self.signal_tasks:
            try:
                self.signal_tasks[signal_id].remove(task)
                if task.camera.indexCode in self.camera_task:
                    del self.camera_task[task.camera.indexCode]
            except ValueError:
                logger.warning(f"Task {task} not found in signal_tasks[{signal_id}]")

    def get_signal_active_tasks(self, signal_id):
        active_tasks = []
        tasks = self.signal_tasks.get(signal_id, [])
        for t in tasks:
            if t.is_active():
                active_tasks.append(t)
            else:
                self.remove_task(signal_id, t)
        return active_tasks

    async def run_pathway_tasks(self, task_list: List[RecognizeTask]):
        # 顺序执行同一通路的任务
        for task in task_list:
            try:
                await task.read_and_recognize()
            except Exception as e:
                logger.error(f"[{task.camera.indexCode} - {task.camera.name}][{task.rtsp.url}]read_and_recognize error. ", e)
        await asyncio.sleep(0.001)

    def run_tasks(self):
        # 创建一个新的事件循环
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            while True:
                task_runners = []
                for signal_id in self.signal_tasks.keys():
                    tasks = self.get_signal_active_tasks(signal_id)
                    if len(tasks) > 0:
                        task_runners.append(self.run_pathway_tasks(tasks))
                    # else:
                        # logger.warning(f"当前通路[{signal_id}]无有效设备")
                if len(task_runners) > 0:
                    # 运行异步方法
                    loop.run_until_complete(asyncio.gather(*task_runners))
                time.sleep(0.001)
        finally:
            # 关闭事件循环
            loop.close()


task_manager = TaskManager()



