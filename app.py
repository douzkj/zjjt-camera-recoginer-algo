import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
import os

from dotenv import load_dotenv
from starlette.requests import Request
from starlette.responses import JSONResponse

from common.entity import Response
from services import rtsp
from subscriber import Subscriber, recognizer_task_handler

load_dotenv()  # 加载环境变量

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'guest')
RABBITMQ_VHOST = os.getenv('RABBITMQ_VHOST', '/')
QUEUE_RECOGNIZER_TASK = os.getenv('QUEUE_RECOGNIZER_TASK', 'zjjt:camera_recognizer:task')
SERVER_PORT = int(os.getenv('SERVER_PORT', 9001))



@asynccontextmanager
async def lifespan(fastapi: FastAPI):
    subscriber = Subscriber(RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASSWORD, RABBITMQ_VHOST)
    # 注册队列及处理器
    subscriber.add_queue(QUEUE_RECOGNIZER_TASK, recognizer_task_handler)
    task = asyncio.create_task(subscriber.run())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("Queue listener stopped")

app = FastAPI(lifespan=lifespan)
app.include_router(rtsp.router)


@app.exception_handler(Exception)
async def http_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        {"code": 500, "message": str(exc)},
        status_code=500)

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=SERVER_PORT)