# 统计当前打标数据
import json
import os
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import Column, Integer, String, func

from db import Session, Base
from recognizer import read_shapes
from setup import setup_logging

logger = setup_logging("cleanup", log_file='statistics.log')

output_json = 'sys-data.json'


class TaskDetail(Base):
    __tablename__ = 'task_detail'

    id = Column(Integer, primary_key=True)
    task_id = Column(String)  # 当前任务ID
    frame_image_path = Column(String)  # 帧图片路径
    frame_time_ms = Column(Integer)  # 帧时间戳
    label_image_path = Column(String)
    label_json_path = Column(String)
    label_types = Column(String)
    label_time_ms = Column(Integer)


def statistics():
    """
    1. 顶部总数计算逻辑：
       1）图片总数仅统计被打标的图片量
       2）实例总数为各标签的实例数量之和
    2. 标签的数量计算逻辑：
       1）实例数量为图片中被标记的标签数量之和
       2）图片数量为携带此标签的图片数量之和，一个图片可能有多个标签实例，同一张图片只计算一次
    """
    num_images = 0
    num_instances = 0
    number_of_instances_per_category = {}
    number_of_images_per_category = {}

    chunk = 2000

    with Session() as session:
        last_min_id = 0
        # 1.防止一直出现拉取，记录当前最大的ID，在区间内进行统计
        stats = session.query(func.max(TaskDetail.id).label('max_id'),
                              func.min(TaskDetail.id).label('min_id'),
                              func.count(TaskDetail.id).label('total')
                              ).filter(
            TaskDetail.label_time_ms > 0
        ).one()
        if stats is None:
            logger.info("没有可统计的记录.")
            return
        logger.info(f"打标的记录总数: {stats.total} [ID范围 {stats.min_id}-{stats.max_id}]")
        last_min_id = stats.min_id
        while last_min_id <= stats.max_id:
            # 2.取N条记录，并更新记录
            query = session.query(TaskDetail).filter(
                TaskDetail.label_time_ms > 0,
                TaskDetail.id >= last_min_id,
                TaskDetail.id <= stats.max_id
            ).order_by(TaskDetail.id.asc()).limit(chunk)

            records = query.all()
            batch_res_count = len(records)
            logger.info(f"执行范围[> {last_min_id}]批次. 响应总数: {batch_res_count}")
            if records is None or len(records) == 0:
                return
            last_min_id = max(records, key=lambda x: x.id).id + 1
            logger.info(f"当前批次的最后的ID为：{last_min_id}")
            for record in records:
                label_json_path = record.label_json_path
                if label_json_path is None or len(label_json_path) == 0 or not os.path.exists(label_json_path):
                    logger.warn(
                        f"当前[{record.id}]json文件记录为空 或 json文件不存在. path={label_json_path}, label_ms={record.label_time_ms}")
                    continue
                shapes = read_shapes(label_json_path)
                num_images += 1
                if shapes is None or len(shapes) == 0:
                    logger.warn(f"当前[{record.id}]json文件读取异常，shapes为空. path={label_json_path}")
                    continue
                # last_min_id = record.id + 1
                num_instances += len(shapes)
                # 获取shapes数组中的每个label，并对其计数。number_of_instances_per_category根据每个label确定计数，number_of_images_per_category跟图片计数
                shape_label_set = []
                for shape in shapes:
                    label = shape['label']
                    # 对每个实例都计数
                    if label not in number_of_instances_per_category:
                        number_of_instances_per_category[label] = 0
                    if label not in number_of_images_per_category:
                        number_of_images_per_category[label] = 0
                    number_of_instances_per_category[label] += 1
                    if label not in shape_label_set:
                        shape_label_set.append(label)
                        number_of_images_per_category[label] += 1

    data_obj = {
        "number_of_instances_per_category": number_of_instances_per_category,
        "number_of_images_per_category": number_of_images_per_category,
        "num_images": num_images,
        "num_instances": num_instances,
    }
    with open(output_json, 'w') as outfile:
        outfile.write(json.dumps(data_obj, indent=4))


if __name__ == '__main__':
    statistics()