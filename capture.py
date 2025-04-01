import asyncio
import logging
import os
import threading
import time
from queue import Queue

import cv2
from typing_extensions import AsyncIterable


# 自定义日志回调函数
def log_callback(level, msg):
    print(f"日志级别: {level}, 日志信息: {msg}")


cv2.setLogLevel(logging.DEBUG)


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


class FrameStorageConfig(object):
    # 存储文件夹
    store_folder: str = './data'
    image_suffix: str = '.jpg'

    def __init__(self, store_folder='./data/', image_suffix='.jpg'):
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


class CameraRtspCapture:
    current_frame = None
    is_capturing = False
    fps = 30.0
    buffer_size = 1
    open_timeout = 5
    timeout = 30
    ffmpeg = 1

    def __init__(self, rtsp_url, timeout=30, ffmpeg=1,  buffersize=1, open_timeout=5,
                 frame_read_config: FrameReadConfig = DEFAULT_FRAME_READ_CONFIG, frame_storage_config=None):
        self.rtsp_url = rtsp_url
        self.frame_read_config = frame_read_config if frame_read_config is not None else DEFAULT_FRAME_READ_CONFIG
        self.frame_storage_config = frame_storage_config if frame_storage_config is not None else FrameStorageConfig()
        self.reader = RtspReader(rtsp_url,
                                 timeout=timeout,
                                 ffmpeg=ffmpeg,
                                 buffersize=buffersize,
                                 open_timeout=open_timeout,
                                 frame_read_config=self.frame_read_config
                                 )

    async def read_frame_iter(self) -> AsyncIterable[CaptureFrame]:
        self.reader.start()
        saved_frame_count = 0
        self.is_capturing = True
        frame_count = 0
        try:
            while saved_frame_count < self.frame_read_config.frame_window and self.reader.is_reading():
                if not self.reader.queue.empty():
                    frame = self.reader.queue.get()
                    print(f"读取到视频帧[{self.rtsp_url}]-{saved_frame_count}")
                    saved_frame_count += 1
                    yield CaptureFrame(saved_frame_count, frame)
        finally:
            self.is_capturing = False
            self.reader.stop()
            print(f"Saved {saved_frame_count} frames. total {frame_count} frames")

    # async def read_frame(self):
    #     retries = 0
    #     max_retries = self.frame_read_config.frame_retry_times
    #     while retries < max_retries:
    #         if not cap.isOpened():
    #             print("could not open rtsp stream, please check url address. rtstp_url: {}".format(self.rtsp_url))
    #             return None
    #         ret, frame = cap.read()
    #         if ret and frame is not None:
    #             return frame
    #         print(f"尝试读取帧「{self.rtsp_url}」失败，重试 {retries + 1}/{max_retries}...")
    #         retries += 1
    #         await asyncio.sleep(1)  # 等待 1 秒后重试
    #     return None


class RtspReader:
    retry_times = 0
    cap = None
    reading = False
    frame = None
    latest_frame=None
    read_thread = None

    def __init__(self, rtsp_url,
                 timeout=30,
                 ffmpeg=1,
                 buffersize=1,
                 open_timeout=5,
                 frame_read_config=None):

        self.rtsp_url = rtsp_url
        self.buffer_size = buffersize
        self.open_timeout = open_timeout
        self.timeout = timeout
        self.ffmpeg = ffmpeg
        self.frame_read_config = frame_read_config if frame_read_config is not None else DEFAULT_FRAME_READ_CONFIG
        self.cap = self._create_cap()
        self.queue = Queue(self.frame_read_config.frame_window)

    def _create_cap(self):
        cap = cv2.VideoCapture(self.rtsp_url)
        self.fps = cap.get(cv2.CAP_PROP_FPS)
        # cap.set(cv2.CAP_FFMPEG, self.ffmpeg)
        print("视频流帧率：", self.fps)
        cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, self.timeout * 1000)
        cap.set(cv2.CAP_PROP_BUFFERSIZE, self.buffer_size)
        cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, self.open_timeout * 1000)  # 新增连接超时设置
        return cap

    def start(self):
        if self.reading:
            print("采集正在运行...")
            return
        self.reading = True
        self.read_thread = threading.Thread(target=self._read_frame_async)
        self.read_thread.start()

    def stop(self):
        self.reading = False
        self.read_thread.join()
        if self.cap:
            self.cap.release()

    def is_reading(self):
        return self.reading

    def read_frame(self):
        return self.latest_frame

    def _read_frame_async(self):
        if not self.cap.isOpened():
            print("could not open rtsp stream, please check url address. rtstp_url: {}".format(self.rtsp_url))
            raise Exception("could not open rtsp stream, please check url address. rtstp_url: {}".format(self.rtsp_url))
        saved_frame_count = 0
        frame_count = 0
        last_process_time = time.time()
        real_fps = 0
        while self.reading:
            try:
                ret, frame = self.cap.read()
                if ret:
                    frame_count += 1
                    current_time = time.time()
                    # remaining = self.frame_read_config.frame_interval_seconds - (current_time - last_process_time)
                    # if remaining > 0:
                    #     time.sleep(max(min(remaining, 0.5), 0.02))
                    #     continue
                    can_save = frame_count % (self.fps * self.frame_read_config.frame_interval_seconds) == 0
                    if can_save:
                        saved_frame_count += 1
                        print(f"read frame success. frame count:{frame_count}, saved frame count:{saved_frame_count}")
                        last_process_time = current_time
                        self.queue.put(frame)
                    self.latest_frame = frame
                else:
                    time.sleep(0.1)  # 降低重试频率
                    self._reconnect()
            except ConnectionError as e:
                print("could not open rtsp stream, please check url address. rtstp_url: {}".format(self.rtsp_url))
                raise e
            except Exception as e:
                print(f"读取帧失败: {e}")
                self._reconnect()
        self.cap.release()

    def _reconnect(self):
        max_retry_times = self.frame_read_config.frame_retry_times
        retry_interval = self.frame_read_config.frame_retry_interval
        if self.retry_times >= max_retry_times:
            print(f"Reached maximum retry attempts ({max_retry_times}), stopping capture.")
            return
            # raise ConnectionError(f"Reached maximum retry attempts ({max_retry_times}), stopping capture.")
        if self.cap:
            self.cap.release()
        print(f"Attempting to reconnect to RTSP stream (attempt {self.retry_times + 1}/{max_retry_times})...")
        self.cap = self._create_cap()
        self.retry_times += 1
        if not self.cap.isOpened():
            print(f"Reconnection failed. Retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)
        else:
            print("Reconnection successful.")


if __name__ == "__main__":
    storage = FrameStorageConfig(
        store_folder='./storages/images/e4e19628c8354c3db8debd4eaf24963a/{}'.format(int(time.time())))
    frame_read_config = FrameReadConfig(frame_interval_seconds=30)
    cap = CameraRtspCapture("rtsp://video.hibuilding.cn:554/openUrl/PLLJXry", frame_read_config=frame_read_config,
                            frame_storage_config=storage)
