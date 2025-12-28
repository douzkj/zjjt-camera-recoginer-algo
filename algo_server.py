# import torch.multiprocessing as mp
# mp.set_start_method('spawn', force=True) # 设置启动方法为 spawn
import logging
import os
import threading
import time
from contextlib import asynccontextmanager

import uvicorn
from dotenv import load_dotenv

from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import JSONResponse
from algorithm import load_predictors,load_predictor,setup_predictors
from celery_app import celery_app
from db import Session, Signal
from services import rtsp, task, algo
from setup import setup_logging
logger = setup_logging('algo_server', log_file='algo_server-2.log')
# 在Flask启动前设置

load_dotenv()  # 加载环境变量

SERVER_PORT = int(os.getenv('SERVER_PORT', 9001))

pathway_intervals = {}
interval = 5
# PREDICTORS = load_predictor()
# PREDICTORS = None


@asynccontextmanager
async def lifespan(fastapi: FastAPI):
    # global PREDICTORS
    # pathway_thread.start()
    setup_predictors()
    from algorithm import PREDICTORS
    logger.info(f"predictor loaded successfully. predictors = {PREDICTORS}")

    yield
app = FastAPI(lifespan=lifespan)
app.include_router(algo.router)

@app.exception_handler(Exception)
async def http_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        {"code": 500, "msg": str(exc)},
        status_code=500)

if __name__ == '__main__':
    import argparse

    # 添加命令行参数解析
    parser = argparse.ArgumentParser(description='启动algo_server服务')
    parser.add_argument('--port', type=int, default=9003, help='服务端口号，默认9003')
    parser.add_argument('--workers', type=int, default=1, help='进程数量，默认1')
    args = parser.parse_args()
    uvicorn.run(app="algo_server:app", host="0.0.0.0", port=args.port, workers=args.workers)