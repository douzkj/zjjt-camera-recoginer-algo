import os, shutil


def copy_and_rename_folder(src_folder, dst_folder, new_name=None):
    """
    复制文件夹并重命名
    :param src_folder: 源文件夹路径
    :param dst_folder: 目标文件夹路径
    :param new_name: 新文件夹名称
    """
    try:
        # 构造目标文件夹的完整路径
        new_folder_path = os.path.join(dst_folder, new_name) if new_name is not None else dst_folder
        os.makedirs(new_folder_path, exist_ok=True)
        # 如果目标文件夹已存在，先删除
        if os.path.exists(new_folder_path):
            shutil.rmtree(new_folder_path)

        # 复制文件夹
        shutil.copytree(src_folder, new_folder_path)
        print(f"Folder copied and renamed to: {new_folder_path}")
        return new_folder_path
    except Exception as e:
        print(f"Error: {e}")
        return None
