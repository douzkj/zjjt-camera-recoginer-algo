# 算法服务
# 去重算法：输入两张图（base64），输出相似度（double）
# 增强算法：输入输出都是图片（base64）
# 识别算法（带打标）：输入1张图片（base64）
# 识别算法（不打标）：输入1张图片（base64）
import os
import sys

from dotenv import load_dotenv

load_dotenv()  # 加载环境变量

ALGO_DIR = os.getenv("ALGO_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), 'algo'))
ALGO_WEIGHT_PATH = os.getenv("ALGO_LABEL_WEIGHT_PATH", "weights")

# 将 algo 目录添加到系统路径中
sys.path.append(ALGO_DIR)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import logging



# 计算两张图片的相似度
def calculate_similarity(image1_base64, image2_base64) -> float:
    # 实现相似度计算的逻辑
    return 0.0

# 增强算法：输入输出都是图片（base64）
def enhance_image(relative_image_path):
    """

    :param relative_image_path: 相对路径，相对于目录 algo/dataset_image_classification/train
    """
    # 保存当前工作目录
    original_cwd = os.getcwd()
    logging.info("测试")
    try:
        # 切换到 algo 目录
        os.chdir(ALGO_DIR)
        from algo_001_build_dataset_for_supervised_image_classification import data_augmentation
        data_augmentation([relative_image_path])
    finally:
        # 切换回原始目录
        os.chdir(original_cwd)


# 识别算法（带打标）：输入1张图片（base64）
def recognize_image_with_label(image_path, output_path, num_class=2):
    # 保存当前工作目录
    original_cwd = os.getcwd()
    try:
        # 切换到 algo 目录
        os.chdir(ALGO_DIR)
        from algo_008_automatically_build_dataset import generate_json_annotation_for_raw_frame
        config = 'Misc/cascade_mask_rcnn_R_50_FPN_3x.yaml'
        weight_path = ALGO_WEIGHT_PATH
        os.makedirs(output_path, exist_ok=True)
        generate_json_annotation_for_raw_frame(image_path,output_path, num_class)
    except Exception as e:
        print(f"Error occurred while calling generate_json_annotation_for_raw_frame: {e}")
        raise e
    finally:
        # 切换回原始目录
        os.chdir(original_cwd)

# 识别算法（不打标）：输入1张图片（base64）
def recognize_image_without_label(image_base64) -> str:
    return image_base64
