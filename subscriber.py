import asyncio
import json
import os

import aio_pika
from dotenv import load_dotenv

from capture import CameraRtspCapture, FrameStorageConfig, FrameReadConfig
from entity import CameraRecognizerTask, Camera, CameraRtsp
from recognizer import RecognizeTask, task_manager
from setup import setup_logging, TaskLoggingFilter

load_dotenv()  # 加载环境变量

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'guest')
RABBITMQ_VHOST = os.getenv('RABBITMQ_VHOST', '/')
QUEUE_RECOGNIZER_TASK = os.getenv('QUEUE_RECOGNIZER_TASK', 'zjjt:camera_recognizer:task')

STORAGE_FRAME_IMAGE_FOLDER = os.getenv('STORAGE_FRAME_IMAGE_FOLDER', './storages/images')

FRAME_READ_INTERVAL_SECONDS = int(os.getenv("FRAME_READ_INTERVAL_SECONDS", 10))
FRAME_READ_WINDOW = int(os.getenv("FRAME_READ_WINDOW", 3))

SUBSCRIBER_PREFETCH_COUNT = int(os.getenv("SUBSCRIBER_PREFETCH_COUNT", 10))

logger = setup_logging("subscriber")

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

    async def consume_queue(self, queue_name, handler_fn):
        channel = await self.connection.channel()
        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue(name=queue_name, durable=True,
                                            arguments={
                                                # 'x-dead-letter-exchange': 'dlx_exchange',  # 绑定死信交换机
                                                'x-max-retries': 3  # 自定义重试参数（需手动实现）
                                            }
                                            )
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                try:
                    asyncio.create_task(handler_fn(message.body))  # 创建异步任务处理消息
                    # await handler_fn(message.body)
                    await message.ack()
                except Exception as e:
                    retries = message.headers.get('x-retry-count', 0)
                    logger.error(f"Error processing message: {e}.  retrying {retries}....")
                    if retries < 3:  # 最大重试次数设为3次
                        new_headers = message.headers.copy()
                        new_headers["x-retry-count"] = retries + 1  # 递增重试次数
                        try:
                            ret = await channel.default_exchange.publish(
                                aio_pika.Message(
                                    body=message.body,
                                    headers=new_headers,  # 传递重试次数
                                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                                ),
                                routing_key=queue.name,
                            )
                            logger.warning("Retrying message .ret={}".format(ret))
                            await message.ack()  # 需要 `ack()` 否则 RabbitMQ 认为消息未处理
                        except Exception as retry_error:
                            logger.error(f"Error retrying message: {retry_error}")
                            await message.nack()
                        # # 拒绝消息并重新入队
                        # await message.nack(requeue=False)
                    else:
                        # 超过最大重试次数, 过滤消息
                        await message.ack()


    async def start_consume(self, queue_name, handler_fn):
        connection = await aio_pika.connect_robust(
            host=self.host,
            port=self.port,
            login=self.user,
            password=self.password,
            virtualhost=self.vhost,
            reconnect_interval=5,  # 重试间隔5秒
            reconnect_timeout=300,  # 总重试时间300秒
            heartbeat_interval=60,
        )
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=1)
            queue = await channel.declare_queue(name=queue_name, durable=True,
                                                arguments={
                                                    # 'x-dead-letter-exchange': 'dlx_exchange',  # 绑定死信交换机
                                                    'x-max-retries': 3  # 自定义重试参数（需手动实现）
                                                }
                                                )
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    try:
                        asyncio.create_task(handler_fn(message.body))  # 创建异步任务处理消息
                        # await handler_fn(message.body)
                        await message.ack()
                    except Exception as e:
                        retries = message.headers.get('x-retry-count', 0)
                        logger.error(f"Error processing message: {e}.  retrying {retries}....")
                        if retries < 3:  # 最大重试次数设为3次
                            new_headers = message.headers.copy()
                            new_headers["x-retry-count"] = retries + 1  # 递增重试次数
                            try:
                                ret = await channel.default_exchange.publish(
                                    aio_pika.Message(
                                        body=message.body,
                                        headers=new_headers,  # 传递重试次数
                                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                                    ),
                                    routing_key=queue.name,
                                )
                                logger.warning("Retrying message .ret={}".format(ret))
                                await message.ack()  # 需要 `ack()` 否则 RabbitMQ 认为消息未处理
                            except Exception as retry_error:
                                logger.error(f"Error retrying message: {retry_error}")
                                await message.nack()
                            # # 拒绝消息并重新入队
                            # await message.nack(requeue=False)
                        else:
                            # 超过最大重试次数, 过滤消息
                            await message.ack()
    async def run(self, consumer_count=5):
        for queue_name, handler_fn in self.queue.items():
            consumers = [self.start_consume(queue_name, handler_fn) for _ in range(consumer_count)]
            await asyncio.gather(*consumers)


async def recognizer_task_handler(message):
    # 处理消息的逻辑
    logger.debug(f"处理识别任务: {message}")
    # 转化为json对象
    task_obj = json.loads(message)
    task = CameraRecognizerTask(**task_obj)
    camera: Camera = task.camera
    rtsp: CameraRtsp = task.rtsp
    logger.addFilter(TaskLoggingFilter(task))

    # 判定当前地址是否有效
    if rtsp.is_expired():
        logger.warning(f"[{task.taskId}][{camera.indexCode} - {camera.name}] RTSP地址已过期: {rtsp.url}")
        return
    frame_config = task.get_frame_config()
    if frame_config is not None:
        logger.warning(f"[{task.taskId}][{camera.indexCode} - {camera.name}] 帧读取配置: {frame_config.model_dump_json()}")
        storage_config = FrameStorageConfig(store_folder=frame_config.storage.frameStoragePath, image_suffix=frame_config.storage.frameImageSuffix)
        frame_read_config = FrameReadConfig(frame_interval_seconds=frame_config.read.frameIntervalSeconds,
                                            frame_retry_times=frame_config.read.frameRetryTimes,
                                            frame_retry_interval=frame_config.read.frameRetryInterval,
                                            frame_window=frame_config.read.frameWindow
                                            )
    else:
        storage_config = FrameStorageConfig(store_folder=os.path.join(STORAGE_FRAME_IMAGE_FOLDER, camera.indexCode))
        frame_read_config = FrameReadConfig(frame_interval_seconds=FRAME_READ_INTERVAL_SECONDS,
                                            frame_window=FRAME_READ_WINDOW)
    # capture = CameraRtspCapture(rtsp.url, frame_storage_config=storage_config, frame_read_config=frame_read_config)
    task = RecognizeTask(camera_task=task, frame_storage_config=storage_config, frame_read_config=frame_read_config)
    task_manager.add_task(task)
    # # 处理识别
    # await task.do_recognizing()


# 使用示例
if __name__ == "__main__":
    subscriber = Subscriber(RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASSWORD, RABBITMQ_VHOST)
    # 注册队列及处理器
    subscriber.add_queue(QUEUE_RECOGNIZER_TASK, recognizer_task_handler)
    # 订阅
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(subscriber.run())
    # loop.close()
    asyncio.run(subscriber.run())
    # message = '{"taskId":"20250407004349","camera":{"indexCode":"0830b36563804ce390db5fb02810102a","name":"塔机吊钩可视化系统","hkMeta":"{\\"indexCode\\":\\"0830b36563804ce390db5fb02810102a\\",\\"resourceType\\":\\"camera\\",\\"name\\":\\"塔机吊钩可视化系统\\",\\"chanNum\\":5,\\"parentIndexCode\\":\\"7e1112105847499688f5a8969bc07d8a\\",\\"cameraType\\":0,\\"capability\\":\\"@io@event_face@event_rule@event_veh_compare@remote_vss@event_veh@event_veh_recognition@event_ias@event_heat@vss@record@event_io@net@maintenance@event_device@status@\\",\\"channelType\\":\\"digital\\",\\"regionIndexCode\\":\\"717c876a-af07-4ce9-af2b-4a10406e7924\\",\\"regionPath\\":\\"@root000000@f5fb3551-8f87-4696-b918-0d5c917a615d@717c876a-af07-4ce9-af2b-4a10406e7924@\\",\\"transType\\":1,\\"treatyType\\":\\"isup5_reg\\",\\"createTime\\":\\"2024-08-01T18:14:46.094+08:00\\",\\"updateTime\\":\\"2025-03-29T11:15:58.611+08:00\\",\\"disOrder\\":16624,\\"decodeTag\\":\\"hikvision\\",\\"cameraRelateTalk\\":\\"1f1aa6861dfc4bc3a2ce62512619c6ab\\",\\"regionName\\":\\"数智交院生产能力提升及创新研究中心建设项目  (杭政储出[202\\",\\"regionPathName\\":\\"根节点/浙江省三建建设集团有限公司/数智交院生产能力提升及创新研究中心建设项目  (杭政储出[202\\"}"},"rtsp":{"url":"rtsp://video.hibuilding.cn:554/openUrl/Fo6gJUs","rtspCreatedTimeMs":1743957830322,"rtspExpiredTimeMs":1743958130322},"signal":{"signalId":"13","signalName":"塔吊可视化","config":{"frame":{"storage":{"frameStoragePath":"./storages/dataset_raw/hook","frameImageSuffix":"jpg"},"read":{"frameIntervalSeconds":1,"frameRetryTimes":3,"frameRetryInterval":1,"frameWindow":-1}}}},"createdTimeMs":1743957830325}'
    # asyncio.run(recognizer_task_handler(message))
