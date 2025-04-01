import json

from pydantic import BaseModel, Field


class Camera(BaseModel):
    indexCode: str = Field(..., description="摄像头唯一识别码")
    name: str = Field(..., description="摄像头名称")
    hkMeta: str = Field(..., description="海康meta信息")


class CameraRtsp(BaseModel):
    url:str = Field(..., description="rtsp地址")
    rtspCreatedTimeMs: int = Field(..., description="rtsp创建时间戳")
    rtspExpiredTimeMs: int = Field(..., description="rtsp过期时间戳")

    def is_expired(self):
        import time
        return self.rtspExpiredTimeMs is not None and (self.rtspExpiredTimeMs / 1000) < int(time.time())


class CameraRecognizerTask(BaseModel):
    taskId: str = Field(..., description="任务运行ID")
    camera: Camera = None
    rtsp: CameraRtsp = None
    createdTimeMs: int = Field(..., description="任务创建时间戳")


if __name__ == '__main__':
    js = '{"taskId":"f1d51844-0a1f-46cf-9c34-2a6b19f8d53f","camera":{"indexCode":"8d51eb8f488249d0b1575c0b4cdff52d","name":"生活区东侧","hkMeta":"{\\"indexCode\\":\\"8d51eb8f488249d0b1575c0b4cdff52d\\",\\"resourceType\\":\\"camera\\",\\"name\\":\\"生活区东侧\\",\\"chanNum\\":14,\\"parentIndexCode\\":\\"2568e96ce8b747bf8ab1bf3724a2ba1e\\",\\"cameraType\\":0,\\"capability\\":\\"@ISUPHttpPassthrough@io@event_face@event_rule@event_veh_compare@remote_vss@event_objects_thrown_detection@event_veh@childmanage@event_veh_recognition@event_ias@event_heat@vss@record@event_io@net@maintenance@event_device@status@\\",\\"channelType\\":\\"digital\\",\\"regionIndexCode\\":\\"10efb716-7b55-4f09-b4c9-90e54e67101a\\",\\"regionPath\\":\\"@root000000@516bad70-af4c-4f1a-8be0-af5b0002bd5b@10efb716-7b55-4f09-b4c9-90e54e67101a@\\",\\"transType\\":1,\\"treatyType\\":\\"isup5_reg\\",\\"createTime\\":\\"2025-03-21T15:50:18.934+08:00\\",\\"updateTime\\":\\"2025-03-29T11:13:30.112+08:00\\",\\"disOrder\\":21828,\\"decodeTag\\":\\"hikvision\\",\\"regionName\\":\\"国能河北定州发电有限责任公司三期2×660MW机组扩建工程\\",\\"regionPathName\\":\\"根节点/浙江省二建建设集团有限公司/国能河北定州发电有限责任公司三期2×660MW机组扩建工程\\"}"},"rtsp":{"url":"rtsp://video.hibuilding.cn:554/openUrl/lrKoxHi","rtspCreatedTimeMs":1743274943075,"rtspExpiredTimeMs":1743275243075},"createdTimeMs":1743274943075}'
    obj = json.loads(js)
    task = CameraRecognizerTask(**obj)
    print(task)