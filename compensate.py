import asyncio
import os

from celery_app import QUEUE_RECOGNIZER_COLLECTION, amqp_url
from mq import MQSender
from recognizer import read_shapes

collect_sender = MQSender(amqp_url, QUEUE_RECOGNIZER_COLLECTION)

async def send_frame_message(camera_index_code, pathway_id,  task_id, frame_image_path, ts, collect=None):
    import  time
    message = {
        "camera": {"indexCode": camera_index_code},
        'signal': {"signalId": pathway_id},
        'taskId': task_id,
        "collect": {'frame': {'frameImagePath': frame_image_path, 'timestamp': ts}},
        "timestamp": int(time.time() * 1000),
    }
    if collect is not None:
        message['collect'].update(collect)
    await collect_sender.send_message(message)

async def main(history_path, compensate_path, time_range=None):
    for root, dirs, files in os.walk(history_path):
        for file in files:
            if not file.endswith('.jpg'):
                continue
            # 解析文件名 {pathway_id}-{task_id}-{camera_index_code}.jpg
            try:
                filename = os.path.splitext(file)[0]
                pathway_id, task_id, camera_index_code = filename.split('-')
            except ValueError:
                print(f"文件名格式错误: {file}")
                continue
            # 构建目标路径
            compensate_file = os.path.join(compensate_path, file)
            # 如果补偿路径不存在该文件
            if not os.path.exists(compensate_file):
                src_file = os.path.join(root, file)
                # 使用文件创建时间作为时间戳（毫秒级）
                ts = int(os.path.getmtime(src_file) * 1000)  # 修改此行
                # 发送补偿消息
                await send_frame_message(
                    camera_index_code=camera_index_code,
                    pathway_id=pathway_id,
                    task_id=task_id,
                    frame_image_path=compensate_file,
                    ts=ts
                )
                print(f"已补偿处理: {src_file}")

def get_file_mtime(file_path):
    """获取文件的修改时间（毫秒级）"""
    return int(os.path.getmtime(file_path) * 1000)


async def main_general(general_image_dir, time_range, label_image_dir=None, label_image_json_dir=None):
    # 解析时间范围
    start_time_ms, end_time_ms = time_range[0], time_range[1]
    for file in os.listdir(general_image_dir):
        if not file.endswith('.jpg'):
            continue
        # 解析文件名 {pathway_id}-{task_id}-{camera_index_code}.jpg
        try:
            filename = os.path.splitext(file)[0]
            pathway_id, task_id, camera_index_code = filename.split('-')
        except ValueError:
            print(f"文件名格式错误: {file}")
            continue
        src_file = os.path.join(general_image_dir, file)
        # 使用文件创建时间作为时间戳（毫秒级）
        ts = int(os.path.getmtime(src_file) * 1000)  # 修改此行
        if ts < start_time_ms or ts > end_time_ms:
            continue
        label = {}
        # collector['label'] = {'labelImagePath': tag_image, 'shapes': shapes,
        #                       'labelJsonPath': tag_json,
        #                       'timestamp': int(time.time() * 1000)}
        if label_image_dir is not None:
            # 判定是否存在label信息
            label_image_path = os.path.join(label_image_dir, filename + '_result.jpg')
            if os.path.exists(label_image_path):
                label['labelImagePath'] = label_image_path
        if label_image_json_dir is not None:
            label_json_path = os.path.join(label_image_json_dir, filename + '.json')
            if os.path.exists(label_json_path):
                label['labelJsonPath'] = label_json_path
                label['shapes'] = read_shapes(label_json_path)
        label.update({
            'timestamp': ts
        })
        # 发送补偿消息
        await send_frame_message(
            camera_index_code=camera_index_code,
            pathway_id=pathway_id,
            task_id=task_id,
            frame_image_path=src_file,
            ts=ts,
            collect={'label': label}
        )
        print(f"已补偿处理: {src_file}")




if __name__ == '__main__':
    history_path = '/Users/rangerdong/codes/douzkj/zjjt-camera-recoginer/zjjt-camera-recoginer-algo/storages/dataset_raw/hook'
    compensate_path = '/data/zjjt_camera_recognizer/algo/dataset_raw/general'
    asyncio.run(main_general(general_image_dir=history_path, time_range=(1750986733146, 1751385600000), label_image_dir='/data/zjjt_camera_recognizer/algo/dataset_raw/general/inference', label_image_json_dir='/data/zjjt_camera_recognizer/algo/dataset_raw/general'))
