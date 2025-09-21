
import os
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import Column, Integer, String, func

from clean_up import TaskDetail
from db import Session, Base
from recognizer import read_shapes
from setup import setup_logging

logger = setup_logging("general_compensate")

# 通用通路的历史记录补偿

def add_suffix_to_filename(original_name, suffix="_result"):
    """
    在文件名的扩展名前插入指定后缀

    参数:
        original_name: 原始文件名（带扩展名）
        suffix: 要插入的后缀（默认为"_result"）

    返回:
        修改后的完整文件名
    """
    # 使用 os.path.splitext 分割文件名和扩展名
    name_part, ext = os.path.splitext(original_name)

    # 在文件名部分添加后缀，然后重新与扩展名组合
    return f"{name_part}{suffix}{ext}"

label_image_folder = '/data/zjjt_camera_recognizer/algo/dataset_raw/test_inference'
label_json_folder = '/data/zjjt_camera_recognizer/algo/dataset_raw/test'


max_id = 13324976
with Session() as session:
    query = session.query(TaskDetail).filter(
        TaskDetail.id <= max_id,
    ).order_by(TaskDetail.id.asc()).limit(2000)
    records = query.all()
    if not records:
        logger.info(f"处理ID批次为【{max_id}】当前批次无可清理图片.")
    for record in records:
        if record.signal_id >= 15:
            # 如果是通路采集的图片，则判断是否有对应的打标文件和打标json
            frame_image_path = record.frame_image_path
            image_filename = os.path.basename(frame_image_path)
            label_image_path =  add_suffix_to_filename(image_filename)
            label_image_path = os.path.join(label_image_folder, label_image_path)

            label_json_base = os.path.splitext(frame_image_path)[0]  # 移除原扩展名
            label_json_path = os.path.join(label_json_folder, label_json_base + ".json")

            # 判断图片是否存在
            modify = False
            modify_data = {}
            if os.path.exists(label_image_path) or os.path.exists(label_json_path):
                modify = True









