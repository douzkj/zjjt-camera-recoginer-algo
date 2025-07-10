import asyncio
import logging
import threading
import time

from fastapi import APIRouter, Query, BackgroundTasks, Body

from common.entity import Response
from setup import setup_logging

logger = setup_logging("task")

router = APIRouter(prefix="/task")
# 新增全局状态和锁
cleanup_task_state = {"is_running": False}
cleanup_lock = asyncio.Lock()


def async_cleanup_task(folder, start_time, end_time, cutoff):
    try:
        from algorithm import cleanup_similar_images
        from clean_up import cleanup_images_records_concurrency
        import datetime, pytz
        start_time_object = datetime.datetime.strptime(start_time, "%Y%m%d%H%M%S")
        end_time_object = datetime.datetime.strptime(end_time, "%Y%m%d%H%M%S")
        start_time_ms = int(start_time_object.astimezone(pytz.timezone('Asia/Shanghai')).timestamp() * 1000)
        end_time_ms = int(end_time_object.astimezone(pytz.timezone('Asia/Shanghai')).timestamp() * 1000)
        r, images = cleanup_similar_images(folder, start_time, end_time, cutoff)
        if r:
            logger.info(f"执行去重算法成功.... 开始删除重复图片记录. images={images}")
            deleted_records_count = cleanup_images_records_concurrency(folder_pattern=folder, start_time_ms=start_time_ms, end_time_ms=end_time_ms)
            cleanup_task_state.update({
                "deletedRecordsCount": deleted_records_count,
                "similarImagesCount": len(images) if images is not None else 0,
                "is_completed": True
            })
        else:
            cleanup_task_state["error"] = "执行算法失败"
    except Exception as e:
        logger.exception(f"异步清理任务失败: {str(e)}")
        cleanup_task_state["error"] = str(e)
    finally:
        cleanup_task_state["is_running"] = False

@router.post("/cleanup")
async def cleanup_similar(
        background_tasks: BackgroundTasks,
        folder: str=Body(..., description="清理的路径"),
        start: str=Body(..., description="开始时间"),
        end: str=Body(..., description="结束时间"),
        cutoff: int=Body(15, description="相似度阈值")
):
    """
    清理重复图片
    :param folder: 流地址
    """
    async with (cleanup_lock):
        if cleanup_task_state["is_running"]:
            return Response.fail("已有清理任务正在进行")

        cleanup_task_state.update({
            "is_running": True,
            "is_completed": False,
            "start_time": int(time.time() * 1000),
            "deletedRecordsCount": 0,
            "similarImagesCount": 0,
            "error": None
        })
        thread = threading.Thread(target=async_cleanup_task, args=(folder, start, end, cutoff))
        thread.start()
        # background_tasks.add_task(
        #     async_cleanup_task,
        #     folder, start, end
        # )

        return Response.ok({"message": "清理任务已开始后台执行"})

@router.get("/cleanup/state")
async def cleanup_state():
    return Response.ok({
        "is_running": cleanup_task_state["is_running"],
        "is_completed": cleanup_task_state.get("is_completed", False),
        "start_time": cleanup_task_state.get("start_time"),
        "deletedRecordsCount": cleanup_task_state.get("deletedRecordsCount", 0),
        "similarImagesCount": cleanup_task_state.get("similarImagesCount", 0),
        "error": cleanup_task_state.get("error")
    })




if __name__ == '__main__':
    print(time.time())