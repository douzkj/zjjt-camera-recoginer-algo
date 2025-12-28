# 算法服务
# 去重算法：输入两张图（base64），输出相似度（double）
# 增强算法：输入输出都是图片（base64）
# 识别算法（带打标）：输入1张图片（base64）
# 识别算法（不打标）：输入1张图片（base64）
import os
import sys

from dotenv import load_dotenv

from setup import setup_logging

load_dotenv()  # 加载环境变量

logger = setup_logging("algorithm", log_file='algorithm.log')

ALGO_DIR = os.getenv("ALGO_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), 'algo'))
ALGO_WEIGHT_PATH = os.getenv("ALGO_LABEL_WEIGHT_PATH", "weights")

# 将 algo 目录添加到系统路径中
sys.path.append(ALGO_DIR)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import logging

PREDICTORS = None


# 计算两张图片的相似度
def calculate_similarity(image1_base64, image2_base64) -> float:
    # 实现相似度计算的逻辑
    return 0.0


def cleanup_similar_images(folder, start_time, end_time, cutoff=15):
    # 保存当前工作目录
    original_cwd = os.getcwd()
    try:
        # 切换到 algo 目录
        os.chdir(ALGO_DIR)
        logger.info(
            f"execute algo_006_build_pseudo_IS_dataset.find_similar_images_mix. folder={folder}, start_time={start_time}, end_time={end_time}, cutoff={cutoff}")
        from algo_006_0_build_IS_dataset import find_similar_images_mix
        deleted_images = find_similar_images_mix(folder, start_time=start_time, end_time=end_time, cutoff=cutoff)
        return True, deleted_images
    except Exception as e:
        logger.exception(
            f"Error occurred executing algo_006_build_pseudo_IS_dataset.calling find_similar_images. folder={folder}")
        return False, []
    finally:
        # 切换回原始目录
        os.chdir(original_cwd)


def load_predictor():
    # 保存当前工作目录
    original_cwd = os.getcwd()
    try:
        # 切换到 algo 目录
        os.chdir(ALGO_DIR)
        logger.debug(
            f"execute algo_006_build_pseudo_IS_dataset.load_predictor. ")
        from algo_006_0_build_IS_dataset import load_predictor
        return load_predictor()
    except Exception as e:
        logger.exception(
            f"Error occurred executing algo_006_build_pseudo_IS_dataset.calling load_predictor. ")
        raise e
    finally:
        # 切换回原始目录
        os.chdir(original_cwd)


def load_predictors():
    # 保存当前工作目录
    original_cwd = os.getcwd()
    try:
        # 切换到 algo 目录
        os.chdir(ALGO_DIR)
        logger.debug(
            f"execute algo_006_build_pseudo_IS_dataset.load_predictor. ")
        from algo_006_1_build_dataset import load_predictors
        return load_predictors()
    except Exception as e:
        logger.exception(
            f"Error occurred executing algo_006_build_pseudo_IS_dataset.calling load_predictor. ")
        raise e
    finally:
        # 切换回原始目录
        os.chdir(original_cwd)


def setup_predictors(re_init=False):
    global PREDICTORS
    if re_init or PREDICTORS is None:
        PREDICTORS = load_predictors()


def clear_predictors():
    global PREDICTORS
    if PREDICTORS is not None:
        import torch, gc
        PREDICTORS = None
        gc.collect()
        torch.cuda.empty_cache()


def general_annotation(image_path, throw_ex=False):
    # from app import PREDICTORS
    global PREDICTORS
    # 保存当前工作目录
    original_cwd = os.getcwd()
    try:
        # 切换到 algo 目录
        os.chdir(ALGO_DIR)
        logger.debug(
            f"execute algo_006_build_pseudo_IS_dataset.generate_annotation. PREDICTOR={PREDICTORS}")
        # from algo_006_0_build_IS_dataset import generate_annotation
        from algo_006_1_build_dataset import dataset_generator
        return dataset_generator(image_path, 0, PREDICTORS)
        # return generate_annotation(image_path, PREDICTORS)
    except Exception as e:
        logger.exception(
            f"Error occurred executing algo_006_build_pseudo_IS_dataset.calling generate_annotation. image_path={image_path}")
        if throw_ex:
            raise e
        return None
    finally:
        # 切换回原始目录
        os.chdir(original_cwd)


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
    """

    :param image_path:
    :param output_path:
    :param num_class:
    :return: tag_image, tag_json
    """
    # 保存当前工作目录
    original_cwd = os.getcwd()
    try:
        # 切换到 algo 目录
        os.chdir(ALGO_DIR)
        from algo_008_automatically_build_dataset import generate_json_annotation_for_raw_frame
        os.makedirs(output_path, exist_ok=True)
        return generate_json_annotation_for_raw_frame(image_path, output_path, num_class)
    except Exception as e:
        logging.exception(f"Error occurred while calling generate_json_annotation_for_raw_frame: {e}")
        raise e
    finally:
        # 切换回原始目录
        os.chdir(original_cwd)


# 识别算法（不打标）：输入1张图片（base64）
def recognize_image_without_label(image_base64) -> str:
    return image_base64
