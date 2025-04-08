import asyncio
import os
import queue
import time
from queue import Queue

import cv2
from typing_extensions import AsyncIterable

from setup import setup_logging

os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "pixel_format;yuv420p|max_delay;1000000"

logger = setup_logging("capture")

# cv2.setLogLevel(2)


# 读取视频帧配置
class FrameReadConfig(object):
    # 帧读取间隔
    frame_interval_seconds: int = 5
    # 读取重试次数
    frame_retry_times: int = 3
    # 帧重试间隔
    frame_retry_interval: int = 3
    # 帧窗口大小，一共采集多少帧
    frame_window: int = 3

    def __init__(self, frame_interval_seconds=5, frame_retry_times=3, frame_retry_interval=3, frame_window=3):
        self.frame_retry_times = frame_retry_times
        self.frame_interval_seconds = frame_interval_seconds
        self.frame_window = frame_window
        self.frame_retry_interval = frame_retry_interval

    def can_continue_read(self, save_frame_count):
        if self.frame_window <= 0:
            return True
        return save_frame_count < self.frame_window


class FrameStorageConfig(object):
    # 存储文件夹
    store_folder: str = './data'
    image_suffix: str = 'jpg'

    def __init__(self, store_folder='./data/', image_suffix='jpg'):
        self.store_folder = store_folder
        self.image_suffix = image_suffix

    def get_storage_path(self, filename):
        # 确保存储文件夹存在
        return os.path.join(self.store_folder, filename)

    def get_storage_folder(self, sub_folder=None):
        # 确保存储文件夹存在
        if sub_folder is not None:
            return os.path.join(self.store_folder, sub_folder)
        return self.store_folder


DEFAULT_FRAME_READ_CONFIG = FrameReadConfig()


class CaptureFrame(object):
    id = None
    frame = None
    timestamp: int = None

    def __init__(self, id, frame, timestamp=None):
        self.id = id
        self.frame = frame
        self.timestamp = timestamp if timestamp is not None else int(time.time())

    def get_frame_id(self) -> str:
        return str(self.id)

    def get_frame_date_format(self, format='%Y%m%d%H%M%S'):
        import datetime, pytz
        # 将时间戳转换为datetime对象
        dt_object = datetime.datetime.fromtimestamp(self.timestamp)
        dt_object = dt_object.astimezone(pytz.timezone('Asia/Shanghai'))
        return dt_object.strftime(format)


class RtspReadConfig:
    buffer_size = 1
    open_timeout = 5
    timeout = 30
    ffmpeg = 1

    def __init__(self, buffer_size=1, open_timeout=5, timeout=30, ffmpeg=1):
        self.buffer_size = buffer_size
        self.open_timeout = open_timeout
        self.timeout = timeout
        self.ffmpeg = ffmpeg


class CameraRtspCapture:
    is_capturing = False
    buffer_size = 1
    open_timeout = 5
    timeout = 30
    ffmpeg = 1
    fps = None
    cap = None
    thread = None
    queue: Queue = Queue(1)
    saved_frame_count = 0
    last_frame_seconds = 0
    frame_count = 0

    def __init__(self, rtsp_url,
                 rtsp_read_config=None,
                 frame_read_config=None):
        self.rtsp_url = rtsp_url
        self.frame_read_config = frame_read_config if frame_read_config is not None else DEFAULT_FRAME_READ_CONFIG
        self.rtsp_read_config = rtsp_read_config if rtsp_read_config is not None else RtspReadConfig()
        self._create_cap()

    def _create_cap(self):
        cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
        cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, self.rtsp_read_config.timeout * 1000)
        cap.set(cv2.CAP_PROP_BUFFERSIZE, self.rtsp_read_config.buffer_size)
        cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, self.rtsp_read_config.open_timeout * 1000)  # 新增连接超时设置
        self.cap = cap
        self.rtsp_valid()

    def rtsp_valid(self):
        # # 开启异步线程并读取指定帧
        if not self.cap.isOpened():
            logger.error("could not open rtsp stream, please check url address. rtstp_url: {}".format(self.rtsp_url))
            raise Exception("could not open rtsp stream, please check url address. rtstp_url: {}".format(self.rtsp_url))
        fps = self.cap.get(cv2.CAP_PROP_FPS)
        logger.info(f"视频流[{self.rtsp_url}]帧率：{fps}")
        if fps > 10000:
            raise Exception(f"视频流[{self.rtsp_url}]帧率异常：{fps}")
        self.fps = fps

    # def start(self):
    #     if self.cap is None:
    #         self._create_cap()
    #     self.is_capturing = True
    #     # 启动视频读取线程
    #     self.thread = threading.Thread(target=self._read_frame_async, daemon=True)
    #     self.thread.start()

    def open(self):
        if self.cap is None or not self.cap.isOpened():
            self._create_cap()

    def stop(self):
        self.is_capturing = False
        if self.thread is not None:
            self.thread.join()
        if self.cap.isOpened():
            self.cap.release()

    def can_read(self):
        return self.cap.isOpened()

    def log_read_time(self):
        self.last_frame_seconds = int(time.time())

    async def _read_sleep(self):
        # 计算读取间隔
        interval_seconds = int(time.time()) - self.last_frame_seconds
        if interval_seconds < self.frame_read_config.frame_interval_seconds:
            await asyncio.sleep(max(self.frame_read_config.frame_interval_seconds - interval_seconds, 1))


    async def read_single_frame(self) -> CaptureFrame|None:
        frame_count = 0
        while self.can_read():
            # 计算读取间隔
            await self._read_sleep()
            frame = await self.read_frame()
            self.log_read_time()
            if frame is None:
                print(f"read frame failed. frame count:{self.frame_count}, saved frame count:{self.saved_frame_count}")
                return None
            else:
                print(
                    f"read frame [{self.rtsp_url}] success. frame count:{frame_count}, saved frame count:{self.saved_frame_count}")
                capture_frame = CaptureFrame(id=self.saved_frame_count, frame=frame)
                self.saved_frame_count += 1
                self.last_frame_seconds = int(time.time())
                return capture_frame

    async def read(self):
        while True:
            if self.is_capturing is False:
                logger.warning(f"[{self.rtsp_url}]视频帧读取closed")
                raise Exception(f"[{self.rtsp_url}] 视频帧读取closed")
            try:
                return self.queue.get_nowait()
            except queue.Empty:
                continue

    async def read_frame_async_iter(self) -> AsyncIterable[CaptureFrame]:
        while True:
            if self.is_capturing is False:
                logger.warning(f"[{self.rtsp_url}]视频帧读取closed")
                break
            try:
                yield self.queue.get_nowait()
            except queue.Empty:
                continue

    def _read_frame_async(self):
        saved_frame_count = 0
        frame_count = 0
        try:
            while self.is_capturing and self.frame_read_config.can_continue_read(saved_frame_count):
                can_save = frame_count % (self.fps * self.frame_read_config.frame_interval_seconds) == 0
                frame = self._read_single_frame()
                if frame is None:
                    print(f"read frame failed. frame count:{frame_count}, saved frame count:{saved_frame_count}")
                    break
                else:
                    if can_save:
                        print(
                            f"read frame [{self.rtsp_url}] success. frame count:{frame_count}, saved frame count:{saved_frame_count}")
                        capture_frame = CaptureFrame(id=saved_frame_count, frame=frame)
                        # 更新队列中的帧（保留最新一帧）
                        if self.queue.full():
                            try:
                                self.queue.get_nowait()
                            except queue.Empty:
                                pass
                        self.queue.put(capture_frame)
                        saved_frame_count += 1
                frame_count += 1
        finally:
            self.stop()
            print(f"Saved {saved_frame_count} frames. total {frame_count} frames")

    async def read_frame(self):
        retries = 0
        max_retries = self.frame_read_config.frame_retry_times
        while retries < max_retries:
            if not self.cap.isOpened():
                print("could not open rtsp stream, please check url address. rtstp_url: {}".format(self.rtsp_url))
                return None
            ret, frame = self.cap.read()
            if ret and frame is not None:
                return frame
            print(f"尝试读取帧「{self.rtsp_url}」失败，重试 {retries + 1}/{max_retries}...")
            retries += 1
            await asyncio.sleep(1)  # 等待 1 秒后重试
        return None

    def _read_single_frame(self):
        retries = 0
        max_retries = self.frame_read_config.frame_retry_times
        while retries < max_retries:
            if not self.cap.isOpened():
                print("could not open rtsp stream, please check url address. rtstp_url: {}".format(self.rtsp_url))
                return None
            ret, frame = self.cap.read()
            if ret and frame is not None:
                return frame
            print(f"尝试读取帧「{self.rtsp_url}」失败，重试 {retries + 1}/{max_retries}...")
            retries += 1
            time.sleep(1)  # 等待 1 秒后重试
        return None

    def update(self, rtsp_url,rtsp_read_config=None, frame_read_config=None):
        self.rtsp_url = rtsp_url
        if rtsp_read_config:
            self.rtsp_read_config = rtsp_read_config
        if self.frame_read_config:
            self.frame_read_config = frame_read_config
        self.stop()
        self.open()


if __name__ == "__main__":
    storage = FrameStorageConfig(
        store_folder='./storages/images/test/{}'.format(int(time.time())))
    frame_read_config = FrameReadConfig(frame_interval_seconds=10, frame_window=-1)
    cap = CameraRtspCapture("rtsp://video.hibuilding.cn:554/openUrl/L6onUCA",
                            frame_read_config=frame_read_config)
    try:
        while True:
            frame = cap.read()
            print(frame)
    except KeyboardInterrupt:
        print("结束")


