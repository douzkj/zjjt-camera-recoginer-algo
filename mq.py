import json
import logging

from aio_pika import connect, Message
from pydantic import BaseModel


class MQSender:
    def __init__(self, amqp_url='amqp://guest:guest@localhost/', queue_name='zjjt:camera_recognizer:collection'):
        self.amqp_url = amqp_url
        self.connection = None
        self.channel = None
        self.queue_name = queue_name

    async def connect(self):
        if self.connection is None or self.connection.is_closed:
            self.connection = await connect(self.amqp_url)
        if self.channel is None or self.channel.is_closed:
            self.channel = await self.connection.channel()

    def convert_base_model(self, obj):
        if isinstance(obj, dict):
            return {key: self.convert_base_model(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_base_model(item) for item in obj]
        elif isinstance(obj, BaseModel):
            return obj.model_dump()
        else:
            return obj


    async def send_message(self, message):
        try:
            await self.connect()
            serialized_message = self.convert_base_model(message)
            body = json.dumps(serialized_message, default=str).encode('utf-8')
            await self.channel.default_exchange.publish(
                Message(body=body),
                routing_key=self.queue_name
            )
        except Exception as e:
            logging.error(f"发送消息到 MQ 时出错: {e}")
            raise e
        finally:
            await self.close()

    async def close(self):
        if self.connection:
            await self.connection.close()


if __name__ == '__main__':
    import asyncio
    amqp_url = f"amqp://admin:123456@localhost:5672/"
    sender = MQSender(amqp_url)
    message = '{"task":{"taskId":"20250420213139","camera":{"indexCode":"21dcdcc16dad445e94dea9962e4eb63b","name":"Camera 01","hkMeta":"{\\"indexCode\\":\\"21dcdcc16dad445e94dea9962e4eb63b\\",\\"resourceType\\":\\"camera\\",\\"name\\":\\"Camera 01\\",\\"chanNum\\":3,\\"parentIndexCode\\":\\"97f919474213466bbde98f4e5e0c4c4a\\",\\"cameraType\\":0,\\"capability\\":\\"@event_vss@io@vss@record@ptz@remote_vss@maintenance@event_device@status@\\",\\"channelType\\":\\"digital\\",\\"regionIndexCode\\":\\"86eec9fc-db42-4621-88da-058eaaed8948\\",\\"regionPath\\":\\"@root000000@f5fb3551-8f87-4696-b918-0d5c917a615d@86eec9fc-db42-4621-88da-058eaaed8948@\\",\\"transType\\":1,\\"treatyType\\":\\"ehome_reg\\",\\"createTime\\":\\"2024-07-29T15:45:38.895+08:00\\",\\"updateTime\\":\\"2024-07-29T15:46:03.205+08:00\\",\\"disOrder\\":15776,\\"decodeTag\\":\\"hikvision\\",\\"cameraRelateTalk\\":\\"1da6f06654324405944430c657fff696\\",\\"regionName\\":\\"\u6587\u6210\u53bf\u9769\u547d\u8001\u6839\u636e\u5730\u7ea2\u8272\u6587\u5316\u7814\u5b66\u4e2d\u5fc3\u9879\u76ee\\",\\"regionPathName\\":\\"\u6839\u8282\u70b9/\u6d59\u6c5f\u7701\u4e09\u5efa\u5efa\u8bbe\u96c6\u56e2\u6709\u9650\u516c\u53f8/\u6587\u6210\u53bf\u9769\u547d\u8001\u6839\u636e\u5730\u7ea2\u8272\u6587\u5316\u7814\u5b66\u4e2d\u5fc3\u9879\u76ee\\"}"},"rtsp":{"url":"rtsp://video.hibuilding.cn:554/openUrl/A5T4Vqw","rtspCreatedTimeMs":1745155906688,"rtspExpiredTimeMs":1745156206688},"signal":{"signalName":"\u901a\u75284","signalId":"18","config":{"frame":{"storage":{"frameStoragePath":"/data/zjjt_camera_recognizer/algo/dataset_raw/general","frameImageSuffix":"jpg"},"read":{"frameIntervalSeconds":5,"frameRetryTimes":3,"frameRetryInterval":2,"frameWindow":-1}},"algo":{"label":{"enabled":false}}}},"createdTimeMs":1745155906688},"collect":{"frame":{"timestamp":1745156223000,"frameImagePath":"/data/zjjt_camera_recognizer/algo/dataset_raw/general/15-20250420213703-2f516257a75c40bdac3fb9533685b7f5.jpg"},"label":{"timestamp":1745155025532,"labelImagePath":"/data/zjjt_camera_recognizer/algo/dataset_raw/hook/label_images/13-20250420213657-d165f78b266b44898c661a7ba61fa2df_result.jpg","labelJsonPath":"/data/zjjt_camera_recognizer/algo/dataset_raw/hook/13-20250420213657-d165f78b266b44898c661a7ba61fa2df.json","shapes":[{"label":"hook","points":[[916,514],[908,516],[901,523],[896,526],[892,528],[890,541],[892,584],[894,606],[896,624],[898,649],[900,673],[902,685],[904,695],[906,709],[908,719],[910,729],[912,738],[914,748],[916,755],[918,763],[920,771],[922,782],[924,795],[926,804],[928,813],[930,821],[932,827],[934,833],[936,841],[938,847],[940,857],[942,867],[944,877],[946,882],[948,886],[952,892],[961,894],[965,892],[970,890],[977,887],[985,889],[995,890],[999,884],[1001,860],[999,833],[997,814],[995,776],[993,735],[991,715],[989,701],[987,679],[985,667],[983,657],[981,649],[979,642],[977,635],[975,628],[973,621],[971,611],[969,602],[967,594],[965,589],[962,584],[959,579],[957,574],[955,570],[953,564],[951,551],[949,535],[947,529],[945,524],[943,519],[939,515],[937,514]],"group_id":null,"shape_type":"polygon","flags":{},"probability":0.9945501685142517}]}},"timestamp":1745156225812}'
    asyncio.run(sender.send_message(message))