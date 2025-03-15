
from algorithm import calculate_similarity, enhance_image, recognize_image_with_label, recognize_image_without_label
from capture import CameraRtspCapture


def rtsp_to_image64(rtsp_url):
    print("rtsp转image64, rtsp={}".format(rtsp_url))
    return None



def read_exist_img_64():
    # 读取同类别下的文件夹内的图片
    print("读取同类别下的文件夹内的图片")
    return []



class RecognizeTask(object):
    # 设备信息
    camera: dict = None
    # rtsp地址  
    rtsp_url: str = None

    # 相似度阈值
    similarity_threshold: float = 0.9
    
    original_img_64: str = None
    enhance_img_64: str = None

    def __init__(self, camera, rtsp_url):
        self.camera = camera
        self.rtsp_url = rtsp_url
        self.cap = CameraRtspCapture(rtsp_url)

    def do_recognizing(self):
        rtsp_url = self.rtsp_url
        print("开始识别, rtsp={}".format(rtsp_url))
        # 将rtsp转image
        img_64 = self.cap.read_image64()
        self.original_img_64 = img_64
        if img_64 is None:
            print("rtsp转image64失败, rtsp={}".format(rtsp_url))
            return
        # 1. 去重识别，对同类别下的文件夹内的图片做相似度比对
        for exist_img_64 in read_exist_img_64():
            compare_result = calculate_similarity(img_64, exist_img_64)
            # 
            if compare_result > self.similarity_threshold:
                print("图片相似度大于{}, rtsp={}, img_64={}, exist_img_64={}".format(self.similarity_threshold, rtsp_url, img_64, exist_img_64))
                return
        # 2. 增强图像
        enhance_img_64 = enhance_image(img_64)
        self.enhance_img_64 = enhance_img_64
        # 3. 识别图像（带label）
        label_img64, labels = recognize_image_with_label(enhance_img_64)
        print("识别结果: label_img64={}, labels={}".format(label_img64, labels))
        # 将识别后的结果推送至消息队列





