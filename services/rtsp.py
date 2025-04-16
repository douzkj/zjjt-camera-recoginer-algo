import base64

import cv2
from fastapi import APIRouter, Query

from capture import FrameReadConfig, CameraRtspCapture
from common.entity import Response

router = APIRouter(prefix="/rtsp")


def frame_to_base64(frame):
    # 将 OpenCV 帧（BGR格式）转为 JPEG 二进制数据
    _, buffer = cv2.imencode('.jpg', frame)
    # 将二进制数据转为 Base64 字符串
    base64_str = base64.b64encode(buffer).decode('utf-8')
    return base64_str


@router.get("/frame")
async def get_frame(url: str=Query(..., description="URL of the video file")):
    """
    获取rtsp流的图片帧
    :param url: 流地址
    """
    frame_read_config = FrameReadConfig(frame_interval_seconds=10, frame_window=-1)
    cap = CameraRtspCapture(url, frame_read_config=frame_read_config)
    frame_ret = await cap.read_single_frame()
    if frame_ret is None:
        raise Exception("读取视频图片异常")

    return Response.ok(frame_to_base64(frame_ret.frame))




