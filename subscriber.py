import asyncio
import json
import os

import aio_pika
from dotenv import load_dotenv

from capture import CameraRtspCapture, FrameStorageConfig, FrameReadConfig
from entity import CameraRecognizerTask, Camera, CameraRtsp
from recognizer import RecognizeTask

load_dotenv()  # 加载环境变量

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'guest')
RABBITMQ_VHOST = os.getenv('RABBITMQ_VHOST', '/')
QUEUE_RECOGNIZER_TASK = os.getenv('QUEUE_RECOGNIZER_TASK', 'zjjt:camera_recognizer:task')

STORAGE_FRAME_IMAGE_FOLDER = os.getenv('STORAGE_FRAME_IMAGE_FOLDER', './storages/images')


class Subscriber(object):
    def __init__(self, host, port, user, password, vhost):
        self.queue = {}
        self.connection = None
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.vhost = vhost


    def add_queue(self, queue_name, handler_fn):
        self.queue[queue_name] = handler_fn
    
    async def run(self):
        self.connection = await aio_pika.connect_robust(
            host=self.host,
            port=self.port,
            login=self.user,
            password=self.password,
            virtualhost=self.vhost
        )
        async with self.connection:
            channel = await self.connection.channel()
            for queue_name, handler_fn in self.queue.items():
                queue = await channel.declare_queue(name=queue_name, durable=True)
                # 设置一个异步的回调函数来处理消息
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        # 处理消息
                        print(f"Received: {message.body}")
                        try:
                            await handler_fn(message.body)
                            await message.ack()
                            # 确认消息已被处理
                        except Exception as e:
                            print(f"Error processing message: {e}")
                            await message.nack()  # 处理消息失败，重新入队列


async def recognizer_task_handler(message):
    # 处理消息的逻辑
    print(f"处理识别任务: {message}")
    # 转化为json对象
    task_obj = json.loads(message)
    task = CameraRecognizerTask(**task_obj)
    camera: Camera = task.camera
    rtsp: CameraRtsp = task.rtsp

    # 判定当前地址是否有效
    if rtsp.is_expired():
        print(f"RTSP地址已过期: {rtsp.url}")
        return

    storage_config = FrameStorageConfig(store_folder=os.path.join(STORAGE_FRAME_IMAGE_FOLDER, camera.indexCode))
    frame_read_config = FrameReadConfig(frame_interval_seconds=10, frame_window=3)
    capture = CameraRtspCapture(rtsp.url, frame_storage_config=storage_config, frame_read_config=frame_read_config)
    task = RecognizeTask(cap=capture, camera_task=task)
    # 处理识别
    await task.do_recognizing()
        




# 使用示例
if __name__ == "__main__":
    subscriber = Subscriber(RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASSWORD, RABBITMQ_VHOST)
    # 注册队列及处理器
    subscriber.add_queue(QUEUE_RECOGNIZER_TASK, recognizer_task_handler)
    # 订阅
    asyncio.run(subscriber.run())
    # message = '{"taskId":"9617ff0f-f421-49fa-b10d-c794221cfcb4","camera":{"indexCode":"0830b36563804ce390db5fb02810102a","name":"塔机吊钩可视化系统","hkMeta":"{\\"indexCode\\":\\"0830b36563804ce390db5fb02810102a\\",\\"resourceType\\":\\"camera\\",\\"name\\":\\"塔机吊钩可视化系统\\",\\"chanNum\\":5,\\"parentIndexCode\\":\\"7e1112105847499688f5a8969bc07d8a\\",\\"cameraType\\":0,\\"capability\\":\\"@io@event_face@event_rule@event_veh_compare@remote_vss@event_veh@event_veh_recognition@event_ias@event_heat@vss@record@event_io@net@maintenance@event_device@status@\\",\\"channelType\\":\\"digital\\",\\"regionIndexCode\\":\\"717c876a-af07-4ce9-af2b-4a10406e7924\\",\\"regionPath\\":\\"@root000000@f5fb3551-8f87-4696-b918-0d5c917a615d@717c876a-af07-4ce9-af2b-4a10406e7924@\\",\\"transType\\":1,\\"treatyType\\":\\"isup5_reg\\",\\"createTime\\":\\"2024-08-01T18:14:46.094+08:00\\",\\"updateTime\\":\\"2025-03-29T11:15:58.611+08:00\\",\\"disOrder\\":16624,\\"decodeTag\\":\\"hikvision\\",\\"cameraRelateTalk\\":\\"1f1aa6861dfc4bc3a2ce62512619c6ab\\",\\"regionName\\":\\"数智交院生产能力提升及创新研究中心建设项目  (杭政储出[202\\",\\"regionPathName\\":\\"根节点/浙江省三建建设集团有限公司/数智交院生产能力提升及创新研究中心建设项目  (杭政储出[202\\"}"},"rtsp":{"url":"rtsp://video.hibuilding.cn:554/openUrl/9ijStTW","rtspCreatedTimeMs":1743353085912,"rtspExpiredTimeMs":1743653385912},"createdTimeMs":1743353085912}'
    # asyncio.run(recognizer_task_handler(message))
