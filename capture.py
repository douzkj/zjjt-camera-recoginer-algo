import asyncio
import logging
import os
import time

import cv2
from typing_extensions import AsyncIterable
import os

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

    def __init__(self, rtsp_url,
                 rtsp_read_config=None,
                 frame_read_config=None,
                 frame_storage_config=None):
        self.rtsp_url = rtsp_url
        self.frame_read_config = frame_read_config if frame_read_config is not None else DEFAULT_FRAME_READ_CONFIG
        self.frame_storage_config = frame_storage_config if frame_storage_config is not None else FrameStorageConfig()
        self.rtsp_read_config = rtsp_read_config if rtsp_read_config is not None else RtspReadConfig()

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
            print("could not open rtsp stream, please check url address. rtstp_url: {}".format(self.rtsp_url))
            raise Exception("could not open rtsp stream, please check url address. rtstp_url: {}".format(self.rtsp_url))
        fps = self.cap.get(cv2.CAP_PROP_FPS)
        print(f"视频流[{self.rtsp_url}]帧率：", fps)
        if fps > 10000:
            raise Exception(f"视频流[{self.rtsp_url}]帧率异常：", fps)
        self.fps = fps

    async def read_frame_iter(self) -> AsyncIterable[CaptureFrame]:
        if self.cap is None:
            self._create_cap()
        saved_frame_count = 0
        frame_count = 0
        self.is_capturing = True
        try:
            while self.is_capturing and saved_frame_count < self.frame_read_config.frame_window:
                can_save = frame_count % (self.fps * self.frame_read_config.frame_interval_seconds) == 0
                frame = await self.read_frame()
                if frame is None:
                    print(f"read frame failed. frame count:{frame_count}, saved frame count:{saved_frame_count}")
                    break
                else:
                    if can_save:
                        print(f"read frame success. frame count:{frame_count}, saved frame count:{saved_frame_count}")
                        capture_frame = CaptureFrame(id=saved_frame_count, frame=frame)
                        yield capture_frame
                        saved_frame_count += 1
                frame_count += 1
        finally:
            self.is_capturing = False
            self.cap.release()
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


if __name__ == "__main__":
    storage = FrameStorageConfig(
        store_folder='./storages/images/test/{}'.format(int(time.time())))
    frame_read_config = FrameReadConfig(frame_interval_seconds=30)
    cap = CameraRtspCapture("rtsp://video.hibuilding.cn:554/openUrl/8Ke0DpT",
                            frame_read_config=frame_read_config,
                            frame_storage_config=storage)
