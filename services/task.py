import logging

from fastapi import APIRouter, Query

from common.entity import Response

router = APIRouter(prefix="/task")


@router.get("/cleanup/similar")
async def cleanup_similar(folder: str=Query(..., description="清理的路径")):
    """
    清理重复图片
    :param folder: 流地址
    """
    from algorithm import cleanup_similar_images
    from clean_up import cleanup_images_records_concurrency
    r, images = cleanup_similar_images(folder)
    if r:
        # 删除图片记录
        logging.info(f"开始删除重复图片记录. images={images}")
        deleted_records_count = cleanup_images_records_concurrency(folder_pattern=folder)
        return Response.ok({
            "deletedRecordsCount": deleted_records_count,
            "similarImagesCount": len(images),
        })
    return Response.fail("算法模型删除失败")


