import asyncio
import base64
import json
import os
import tempfile
import time
import uuid
from typing import List

import cv2
from aiofiles.os import remove, unlink
from celery.bin.result import result
from fastapi import APIRouter, Query, UploadFile, File, Form, Body
from pydantic import BaseModel
import aiofiles

from algorithm import general_annotation
from common.entity import Response
from setup import setup_logging
from concurrent.futures import ProcessPoolExecutor  # 添加进程池导入

router = APIRouter(prefix="/algo")

RECO_TEMP_DIR = os.getenv("RECO_TEMP_DIR", '')
ALGO_CLASS_FILE = os.getenv("ALGO_CLASS_FILE", '')
os.makedirs(RECO_TEMP_DIR, exist_ok=True)

logger = setup_logging("algo_server", log_file='algo_server-2.log')

from concurrent.futures import ThreadPoolExecutor


# 在合适的地方初始化，例如 FastAPI 的 startup 事件
# thread_pool = ThreadPoolExecutor(max_workers=128) # 数量需要根据实际情况测试调整


class FileItem(BaseModel):
    id: str


def do_general_annnotation(img_path, ex=True):
    start_time = int(time.time() * 1000)
    try:
        return general_annotation(img_path, ex)
    finally:
        end_time = int(time.time() * 1000)
        logger.info(f"execute {img_path}. algo reco {end_time - start_time} ms")


def do_annotation_and_read_ret(img_path, file_id, ex=True):
    try:
        ret = do_general_annnotation(img_path, ex)
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
            "labelMetrics": [{"label": k, "count": v} for k, v in metrics_map.items()],
            "labelPoints": points
        }
    except Exception as e:
        logger.exception(f"处理上传文件 {file_id} 时出错: {e}")
        return {"id": file_id, "error": str(e)}


async def process_file(file: UploadFile, file_id: str = None):
    saved_path = None
    try:
        if file_id is None:
            file_id = str(uuid.uuid4())
        logger.info(f"[fileid: {file_id}]starting reco file. size={round(file.size / (1024 * 1024), 2)} mb")
        # 生成唯一文件名（结合ID和时间戳）
        filename = f"{file_id}_{int(time.time())}{os.path.splitext(file.filename)[-1]}"
        saved_path = os.path.join(RECO_TEMP_DIR, filename)

        # 写入文件
        contents = await file.read()
        async with aiofiles.open(saved_path, "wb") as f:
            await f.write(contents)

        logger.info(f"[fileid: {file_id}]saved file to tmp. {saved_path}")

        # 调用算法（使用线程池避免阻塞事件循环）
        loop = asyncio.get_event_loop()
        start_time = int(time.time() * 1000)
        # with ProcessPoolExecutor(max_workers=32) as executor:
        # 修改算法调用部分
        # ret = await loop.run_in_executor(executor, general_annotation, saved_path, True)
        # ret = await loop.run_in_executor(thread_pool, do_general_annnotation, saved_path, True)
        logger.info(f"[fileid: {file_id}]start general_annotation")

        ret = await loop.run_in_executor(None, general_annotation, saved_path, True)
        # ret = general_annotation(saved_path, True)
        end_time = int(time.time() * 1000)
        logger.info(f"[fileid: {file_id}] general_annotation done. cost= {end_time - start_time} ms")
        # ret = await loop.run_in_executor(None, general_annotation, saved_path, True)
        if ret is None:
            return {"id": file_id, "error": "算法处理失败"}

        tag_image, tag_json = ret[0], ret[1]
        metrics_map = {}
        points = []
        if tag_json is None or os.path.exists(tag_json) is False:
            raise Exception("算法识别json结果文件异常")
        # 使用 with 语句打开文件
        async with aiofiles.open(tag_json, 'r', encoding='utf-8') as f:
            # 读取并解析 JSON 文件内容
            content = await f.read()
            data = json.loads(content)
            shapes = data.get("shapes", [])
            for shape in shapes:
                label = shape['label']
                if label not in metrics_map:
                    metrics_map[label] = 0
                metrics_map[shape["label"]] += 1
                points.append(shape)
        return {
            "id": file_id,
            "labelMetrics": [{"label": k, "count": v} for k, v in metrics_map.items()],
            "labelPoints": points
        }
    except Exception as e:
        logger.exception(f"处理上传文件 {file_id} 时出错: {e}")
        return {"id": file_id, "error": str(e)}
    finally:
        if saved_path and os.path.exists(saved_path):
            # 使用异步方式删除文件
            try:
                await unlink(saved_path)
            except Exception as e:
                logger.exception(f"删除临时文件 {saved_path} 时出错: {e}")



@router.post("/reco/annotation")
async def reco_annotation(
        filepath: str = Body(..., description="图片路径"),
        id: str = Body(..., description="图片id")
):
    """
    获取rtsp流的图片帧
    """
    # with ThreadPoolExecutor(max_workers=128) as executor:
    #     ret = executor.submit(do_annotation_and_read_ret, filepath, id)
    ret = do_annotation_and_read_ret(filepath, id)
    return Response.ok(ret)


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
    logger.info(f"starting reco files. file count: {len(files)}")
    start_time = int(time.time() * 1000)
    results = await asyncio.gather(
        *(process_file(file, id) for file, id in zip(files, ids))
    )
    end_time = int(time.time() * 1000)
    logger.info(f"reco files done. cost= {end_time - start_time} ms")

    return Response.ok(results)