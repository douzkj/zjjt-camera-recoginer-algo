import asyncio
import base64
import logging
import os
import time
from datetime import datetime

import cv2


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
    # 帧窗口大小，一共采集多少帧
    frame_window: int = 3

    def __init__(self, frame_interval_seconds=5, frame_retry_times=3, frame_window=3):
        self.frame_retry_times = frame_retry_times
        self.frame_interval_seconds = frame_interval_seconds
        self.frame_window = frame_window
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
    frames: list[CaptureFrame] = []
    is_capturing = False
    fps = 30.0

    def __init__(self, rtsp_url, timeout=30, ffmpeg=1, fps=25.0, buffersize=1, open_timeout=5, num_threads=8,
                 frame_read_config: FrameReadConfig = DEFAULT_FRAME_READ_CONFIG, frame_storage_config=None):
        self.rtsp_url = rtsp_url
        self.cap = cv2.VideoCapture(self.rtsp_url)
        self.cap.set(cv2.CAP_FFMPEG, ffmpeg)
        self.fps = self.cap.get(cv2.CAP_PROP_FPS)
        print("视频流帧率：", self.fps)
        # self.cap.set(cv2.CAP_PROP_FPS, fps)
        cv2.setNumThreads(num_threads)
        self.cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, timeout * 1000)
        self.cap.set(cv2.CAP_PROP_BUFFERSIZE, buffersize)
        self.cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, open_timeout * 1000)  # 新增连接超时设置
        self.frame_read_config = frame_read_config if frame_read_config is not None else DEFAULT_FRAME_READ_CONFIG
        self.frame_storage_config = frame_storage_config if frame_storage_config is not None else FrameStorageConfig()

    async def start_capture(self):
        # # 开启异步线程并读取指定帧
        if not self.cap.isOpened():
            print("could not open rtsp stream, please check url address. rtstp_url: {}".format(self.rtsp_url))
            raise Exception("could not open rtsp stream, please check url address. rtstp_url: {}".format(self.rtsp_url))
        self.is_capturing = True
        frame_idx = 0
        try:
            while self.is_capturing and frame_idx < self.frame_read_config.frame_window:
                if frame_idx != 0:
                    await asyncio.sleep(self.frame_read_config.frame_interval_seconds)
                frame = await self.read_frame()
                if frame is not None:
                    print("[{}] read frame success.".format(datetime.now()))
                    self.frames.append(CaptureFrame(id=frame_idx, frame=frame))
                frame_idx += 1
            self.is_capturing = False
        finally:
            self.cap.release()

    async def read_frame_iter(self):
        while self.cap.isOpened():
            if cv2.__version__ >= '4.0.0':
                print("刷新缓冲区")
                self.cap.grab()
            ret, frame = self.cap.retrieve()
            if not ret:
                print("could not read frame, please check url address. rtstp_url: {}".format(self.rtsp_url))
                break
            if ret and frame is not None:
                yield frame
                await asyncio.sleep(0)

    async def read_frame(self):
        retries = 0
        max_retries = self.frame_read_config.frame_retry_times
        while retries < max_retries:
            if not self.cap.isOpened():
                print("could not open rtsp stream, please check url address. rtstp_url: {}".format(self.rtsp_url))
                return None
            if cv2.__version__ >= '4.0.0':
                print("刷新缓冲区")
                self.cap.grab()
            ret, frame = self.cap.retrieve()
            if ret and frame is not None:
                self.current_frame = frame
                return frame
            print(f"尝试读取帧失败，重试 {retries + 1}/{max_retries}...")
            retries += 1
            await asyncio.sleep(1)  # 等待 1 秒后重试
        return None

    def read_image64(self):
        frame = self.read_frame()
        if frame is None:
            print("未读取到帧")
            return None
        return self.frame_to_image64(frame)

    def frame_to_image64(self, frame):
        _, img_encoded = cv2.imencode(self.frame_storage_config.image_suffix, frame)
        img_bytes = img_encoded.tobytes()
        return base64.b64encode(img_bytes).decode('utf-8')

async def run_capture(camera_capture: CameraRtspCapture):
    await camera_capture.start_capture()
    os.makedirs(camera_capture.frame_storage_config.get_storage_folder(), exist_ok=True)
    for frame in camera_capture.frames:
        image_path = camera_capture.frame_storage_config.get_storage_path("{}-{}.jpg".format(frame.get_frame_id(), frame.timestamp))
        cv2.imwrite(image_path, frame.frame)
        print("save img success: ", image_path)


if __name__ == "__main__":
    storage = FrameStorageConfig(store_folder='./storages/images/e4e19628c8354c3db8debd4eaf24963a/{}'.format(int(time.time())))
    frame_read_config = FrameReadConfig(frame_interval_seconds=30)
    cap = CameraRtspCapture("rtsp://video.hibuilding.cn:554/openUrl/m8cYaRy", frame_read_config=frame_read_config, frame_storage_config=storage)
    asyncio.run(run_capture(cap))