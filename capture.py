import cv2
import numpy as np
import base64
import time
import logging

import os
os.environ['LD_LIBRARY_PATH'] = '/Users/rangerdong/bin:$LD_LIBRARY_PATH'
# 自定义日志回调函数
def log_callback(level, msg):
    print(f"日志级别: {level}, 日志信息: {msg}")

cv2.setLogLevel(logging.DEBUG)

class CameraRtspCapture:

    current_frame = None
    def __init__(self, rtsp_url, timeout=30, ffmpeg=1, fps=10, buffersize=1, open_timeout=5):
        self.rtsp_url = rtsp_url
        self.cap = cv2.VideoCapture(self.rtsp_url)
        # self.cap.set(cv2.CAP_FFMPEG, ffmpeg)
        # self.cap.set(cv2.CAP_PROP_FPS, fps)
        # self.cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, timeout * 1000)
        self.cap.set(cv2.CAP_PROP_BUFFERSIZE, buffersize)
        # self.cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, open_timeout * 1000)  # 新增连接超时设置

    
    
    def read_frame(self, max_retries=3):
        time.sleep(2)
        # if not self.cap.isOpened():
        #     print("could not open rtsp stream, please check url address. rtstp_url: {}".format(self.rtsp_url))
        #     return None
        retries = 0
        while retries < max_retries:
            ret, frame = self.cap.retrieve()
            if ret and frame is not None:
                self.current_frame = frame
                return frame
            print(f"尝试读取帧失败，重试 {retries + 1}/{max_retries}...")
            retries += 1
            time.sleep(1)  # 等待 1 秒后重试
        print(f"错误: 无法从视频流中读取帧。RTSP URL: {self.rtsp_url}")
        self.cap.release()
        return None

    def read_image64(self):
        frame = self.read_frame()
        if frame is None:
            print("未读取到帧")
            return None
        return self.frame_to_image64(frame)
   
    
    def frame_to_image64(self, frame):
        _, img_encoded = cv2.imencode('.jpg', frame)
        img_bytes = img_encoded.tobytes()
        return base64.b64encode(img_bytes).decode('utf-8')


if __name__ == "__main__":
    img64 = CameraRtspCapture("rtsp://video.hibuilding.cn:554/openUrl/3huPGa4", buffersize=1).read_image64()
    print("img64: ", img64)