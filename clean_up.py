import os
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import Column, Integer, String, func

from db import Session, Base


class TaskDetail(Base):
    __tablename__ = 'task_detail'

    id = Column(Integer, primary_key=True)
    task_id = Column(String)    # 当前任务ID
    frame_image_path = Column(String)  # 帧图片路径

def cleanup_records(start_id, end_id, parent_folder = None, folder_pattern = None, deleted_images=None):
    session = Session()
    try:
        query = session.query(TaskDetail).filter(
            TaskDetail.id >= start_id,
            TaskDetail.id <= end_id,
        ).order_by(TaskDetail.id.asc())
        records = query.all()
        if not records:
            print(f"处理ID批次为【{start_id} - {end_id}】当前批次无可清理图片.")
            return 0
        to_delete = []
        for record in records:
            if record.frame_image_path is not None and len(record.frame_image_path) > 0:
                full_path = os.path.join(parent_folder,
                                         record.frame_image_path) if parent_folder is not None else record.frame_image_path
                can_deleted = True if folder_pattern is None else full_path.startswith(folder_pattern)
                can_deleted = can_deleted if deleted_images is None or len(deleted_images) == 0 else full_path in deleted_images
                if can_deleted and not os.path.exists(full_path):
                    to_delete.append(record.id)

            else:
                to_delete.append(record.id)
        deleted_cont = 0
        # 批量删除
        if to_delete:
            delete_stmt = TaskDetail.__table__.delete().where(
                TaskDetail.id.in_(to_delete)
            )
            session.execute(delete_stmt)
            deleted_cont =  len(to_delete)
            session.commit()  # 每批提交一次
        print(f"处理ID批次为【{start_id} - {end_id}】当前批次删除{deleted_cont}条.")
        return deleted_cont

    finally:
        session.close()

def cleanup_images_records_concurrency(batch_size = 1000, concurrency = 16, parent_folder = None, folder_pattern = None, deleted_images=None):
    session = Session()
    try:
        stats = session.query(func.max(TaskDetail.id).label('max_id'),
                func.min(TaskDetail.id).label('min_id'),
                func.count(TaskDetail.id).label('total')
            ).one()
        print(f"总记录数: {stats.total} [ID范围 {stats.min_id}-{stats.max_id}]")
        # 生成分块范围
        chunks = []
        current_start = stats.min_id
        while current_start <= stats.max_id:
            current_end = min(current_start + batch_size - 1, stats.max_id)
            chunks.append((current_start, current_end))
            current_start = current_end + 1
        # 按照batch size chunk出每步任务
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = []
            for start_id, end_id in chunks:
                # 提交清理任务
                future = executor.submit(
                    cleanup_records,
                    start_id=start_id,
                    end_id=end_id,
                    parent_folder=parent_folder,
                    folder_pattern=folder_pattern,
                    deleted_images=deleted_images
                )
                futures.append(future)
                print(f"已提交任务 {start_id}-{end_id}")
        # 等待所有任务完成
        total_deleted = sum(f.result() for f in futures if f.result() is not None)
        print(f"并发清理完成，总计删除 {total_deleted} 条记录")
        return total_deleted
    finally:
        session.close()


def cleanup_deleted_images(batch_size = 1000, max_workers = 10, latest_deleted_id=0, parent_folder = None, folder_pattern = None):
    session = Session()
    try:
        total_deleted = 0
        last_id = latest_deleted_id

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                # 分页查询（使用ID范围提高性能）
                query = session.query(TaskDetail).filter(
                    TaskDetail.id > last_id
                ).order_by(TaskDetail.id.asc()).limit(batch_size)
                records = query.all()
                if not records:
                    break
                # 并行检查文件存在性
                futures = {}
                for record in records:
                    if record.frame_image_path is not None and len(record.frame_image_path) > 0:
                        full_path = os.path.join(parent_folder, record.frame_image_path) if parent_folder is not None else record.frame_image_path
                        if folder_pattern is not None and full_path.startswith(folder_pattern):
                            futures[record.id] = executor.submit(os.path.exists, full_path)

                # 收集需要删除的记录ID
                to_delete = [
                    i
                    for i, future in futures.items()
                    if not future.result()
                ]
                current_deleted_count = len(to_delete)
                # 批量删除
                if to_delete:
                    delete_stmt = TaskDetail.__table__.delete().where(
                        TaskDetail.id.in_(to_delete)
                    )
                    session.execute(delete_stmt)
                    total_deleted += current_deleted_count

                # 更新最后处理的ID
                last_id = records[-1].id
                session.commit()  # 每批提交一次
                print(f"已处理到ID {last_id}, 当前批次删除{current_deleted_count}条. 累计删除 {total_deleted} 条")

    finally:
        session.close()


if __name__ == '__main__':
    cleanup_images_records_concurrency(batch_size=1000)
