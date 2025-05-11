import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

load_dotenv()

# 从环境变量获取数据库配置
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "123456")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "zjjt_camera_recognizer")

# 创建数据库引擎
engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
Base = declarative_base()
Session = sessionmaker(bind=engine)


# 通路模型
class Signal(Base):
    __tablename__ = 'signals'

    id = Column(Integer, primary_key=True)
    name = Column(String)    # 通路名称
    config = Column(String)  # 采集配置
    status = Column(Integer)  # 通路状态
    current_task_id = Column(String)    # 当前任务ID


# 设备模型
class Camera(Base):
    __tablename__ = 'cameras'
    id = Column(Integer, primary_key=True)
    signal_id = Column(Integer)
    index_code =  Column(String)  # 设备编号
    name = Column(String)    # 设备名称
    # hk_meta = Column(String)  # 海康设备元数据
    latest_rtsp_url = Column(String)            # 最新的RTSP地址
    latest_rtsp_created_time = Column(Integer)      # 最后一次RTSP请求时间
    latest_rtsp_expired_time = Column(Integer)      # 最后一次RTSP过期时间


    def is_rtsp_expired(self):
        import time
        return self.latest_rtsp_expired_time < int(time.time() * 1000)


