import os
import sys

import cv2
from dotenv import load_dotenv

from algorithm import enhance_image, recognize_image_with_label
from capture import CameraRtspCapture
from entity import CameraRecognizerTask, Camera, CameraRtsp
from utils import copy_and_rename_folder

load_dotenv()  # 加载环境变量

ALGO_DIR = os.getenv("ALGO_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), 'algo'))
STORAGE_LABEL_IMAGE_FOLDER = os.getenv("STORAGE_LABEL_IMAGE_FOLDER", './storages/label_images')
ALGO_IMAGE_ENHANCE_ROOT = os.getenv("ALGO_IMAGE_ENHANCE_ROOT", './algo/dataset_image_classification/train')

# 将 algo 目录添加到系统路径中
sys.path.append(ALGO_DIR)


def rtsp_to_image64(rtsp_url):
    print("rtsp转image64, rtsp={}".format(rtsp_url))
    return None



def read_exist_img_64():
    # 读取同类别下的文件夹内的图片
    print("读取同类别下的文件夹内的图片")
    return []



class RecognizeTask(object):

    task: CameraRecognizerTask = None

    camera: Camera = None
    rtsp: CameraRtsp = None

    # 相似度阈值
    similarity_threshold: float = 0.9

    def __init__(self, cap: CameraRtspCapture, camera_task: CameraRecognizerTask):
        self.task = camera_task
        self.camera = camera_task.camera
        self.rtsp = camera_task.rtsp
        self.cap = cap

    async def do_recognizing(self):
        image_dir = self.cap.frame_storage_config.get_storage_folder(self.task.taskId)
        os.makedirs(image_dir, exist_ok=True)
        read_success = False
        async for frame in self.cap.read_frame_iter():
            image_path = os.path.join(image_dir, f"{frame.get_frame_id()}.jpg")
            cv2.imwrite(image_path, frame.frame)
            print("save img success: ", image_path)
            read_success = True
        if read_success:
            await self.do_recognizer_algo(image_dir)

    async def do_recognizer_algo(self, image_dir):
        # img_64 = self.cap.frame_to_image64(frame)
        # if img_64 is None:
        #     print("frame转image64失败")
        #     return
        # # 1. 去重识别，对同类别下的文件夹内的图片做相似度比对
        # print("# 1. 去重识别，对同类别下的文件夹内的图片做相似度比对")
        # for exist_img_64 in read_exist_img_64():
        #     compare_result = calculate_similarity(img_64, exist_img_64)
        #     #
        #     if compare_result > self.similarity_threshold:
        #         print(
        #             "图片相似度大于{}, img_64={}, exist_img_64={}".format(self.similarity_threshold,
        #                                                                            img_64, exist_img_64))
        #         return
        # 2. 增强图像
        print("# 2. 增强图像")
        relative_task_image_path = os.path.join(self.camera.indexCode, self.task.taskId)
        enhance_image_dir = os.path.join(ALGO_IMAGE_ENHANCE_ROOT, relative_task_image_path)
        copy_and_rename_folder(image_dir, enhance_image_dir)
        enhance_image(relative_task_image_path)
        # 3. 识别图像（带label）
        print("# 3. 识别图像（带label）")
        alog_label_image_tmp_path = os.path.join(STORAGE_LABEL_IMAGE_FOLDER, relative_task_image_path)
        dst_folder = copy_and_rename_folder(image_dir, alog_label_image_tmp_path, "tmp")
        recognize_image_with_label(dst_folder, output_path=dst_folder)
        # print("识别结果: label_img64={}, labels={}".format(label_img64, labels))
        # 将识别后的结果推送至消息队列

