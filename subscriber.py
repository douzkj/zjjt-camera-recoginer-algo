import aio_pika
import json
import asyncio
import time
from recognizer import RecognizeTask


RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5672
RABBITMQ_USER = 'admin'
RABBITMQ_PASSWORD = 'admin'
RABBITMQ_VHOST = '/'
QUEUE_RECOGNIZER_TASK = 'zjjt:recognizer:task'


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
                            handler_fn(message.body)
                            await message.ack()
                            # 确认消息已被处理
                        except Exception as e:
                            print(f"Error processing message: {e}")
                            await message.nack()  # 处理消息失败，重新入队列


def recognizer_task_handler(message):
    # 处理消息的逻辑
    print(f"处理识别任务: {message}")
    # 转化为json对象
    # {"cameraIndexCode":"xxxxx","rtspUrl":"rtsp://wowzaec2demo.streamlock.net/vod/mp4:BigBuckBunny_115k.mov","rtspCreatedTime":null, "rtspExpiredTime": xxxxxxxx}
    task_obj = json.loads(message)
    rtsp_create_time = task_obj['rtspCreatedTime']
    rtsp_expired_time = task_obj['rtspExpiredTime']
    rtsp_url = task_obj['rtspUrl']

    # 判定当前地址是否有效
    if rtsp_expired_time is not None and (rtsp_expired_time / 1000) < int(time.time()):
        print(f"RTSP地址已过期: {task_obj['rtspUrl']}")
        return
    
    task = RecognizeTask(camera=task_obj.get('camera', None), rtsp_url=rtsp_url)
    # 处理识别
    task.do_recognizing()
        




# 使用示例
if __name__ == "__main__":
    subscriber = Subscriber(RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASSWORD, RABBITMQ_VHOST)
    # 注册队列及处理器
    subscriber.add_queue(QUEUE_RECOGNIZER_TASK, recognizer_task_handler)
    # 订阅
    asyncio.run(subscriber.run())
