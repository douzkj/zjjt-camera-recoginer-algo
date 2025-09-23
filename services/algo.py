import asyncio
import base64
import json
import os
import tempfile
import time
import uuid
from typing import List

import cv2
from fastapi import APIRouter, Query, UploadFile, File, Form
from pydantic import BaseModel

from algorithm import general_annotation
from capture import FrameReadConfig, CameraRtspCapture
from common.entity import Response
from setup import setup_logging
from concurrent.futures import ProcessPoolExecutor  # 添加进程池导入


router = APIRouter(prefix="/algo")

RECO_TEMP_DIR = os.getenv("RECO_TEMP_DIR", '')
ALGO_CLASS_FILE = os.getenv("ALGO_CLASS_FILE", '')
os.makedirs(RECO_TEMP_DIR, exist_ok=True)

logger = setup_logging("algo_server", log_file='algo_server.log')



class FileItem(BaseModel):
    id: str

async def process_file(file: UploadFile, file_id: str = None):
    saved_path = None
    try:
        if file_id is None:
            file_id = str(uuid.uuid4())
        # 生成唯一文件名（结合ID和时间戳）
        filename = f"{file_id}_{int(time.time())}{os.path.splitext(file.filename)[-1]}"
        saved_path = os.path.join(RECO_TEMP_DIR, filename)

        # 写入文件
        contents = await file.read()
        with open(saved_path, "wb") as f:
            f.write(contents)

        # 调用算法（使用线程池避免阻塞事件循环）
        loop = asyncio.get_event_loop()
        start_time = int(time.time() * 1000)
        # with ProcessPoolExecutor() as executor:
            # 修改算法调用部分
            # ret = await loop.run_in_executor(executor, general_annotation, saved_path, True)
        # ret = await loop.run_in_executor(None, general_annotation, saved_path, True)
        ret = general_annotation(saved_path, True)
        end_time = int(time.time() * 1000)
        logger.info(f"execute algo reco {end_time - start_time} ms")
        # ret = await loop.run_in_executor(None, general_annotation, saved_path, True)
        if ret is None:
            return {"id": file_id, "error": "算法处理失败"}

        tag_image, tag_json = ret[0], ret[1]
        metrics_map = {}
        points = []
        if tag_json is None or os.path.exists(tag_json) is False:
            raise Exception("算法识别json结果文件异常")
        # 使用 with 语句打开文件
        with open(tag_json, 'r', encoding='utf-8') as file:
            # 读取并解析 JSON 文件内容
            data = json.load(file)
            shapes = data.get("shapes", [])
            for shape in shapes:
                label = shape['label']
                if label not in metrics_map:
                    metrics_map[label] = 0
                metrics_map[shape["label"]] += 1
                points.append(shape)
        return {
            "id": file_id,
            "labelMetrics": [{"label": k, "count": v} for k,v in metrics_map.items()],
            "labelPoints": points
        }
    except Exception as e:
        logger.exception(f"处理上传文件 {file_id} 时出错: {e}")
        return {"id": file_id, "error": str(e)}
    finally:
        if saved_path and os.path.exists(saved_path):
            os.unlink(saved_path)


@router.post("/reco")
async def reco(file: UploadFile = File(...)):
    """
    获取rtsp流的图片帧
    """
    ret = await process_file(file)
    if 'error' in ret:
        return Response.fail(ret['error'])
    return Response.ok(ret)


@router.post("/reco/batch")
async def reco_batch(ids: List[str] = Form(..., description="图片文件ID列表"), files: List[UploadFile] = File(...)):
    """
    获取rtsp流的图片帧
    :param ids: 文件id列表
    :param files: 文件列表
    """
    # 创建临时文件（保留后缀名以便算法处理）
    if len(ids) != len(files):
        return Response.fail("文件ID列表与文件列表数量不一致")
    # 并发处理所有文件
    results = await asyncio.gather(
        *(process_file(file, id) for file, id in zip(files, ids))
    )

    return Response.ok(results)