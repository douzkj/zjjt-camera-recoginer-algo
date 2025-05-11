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

from celery_app import celery_app
from db import Session, Signal
from services import rtsp, task

# 在Flask启动前设置

load_dotenv()  # 加载环境变量

SERVER_PORT = int(os.getenv('SERVER_PORT', 9001))

pathway_intervals = {}
interval = 5


def pathway_listener():
    while True:
        session = Session()
        try:
            active_pathways = session.query(Signal).filter(Signal.status == 1).all()
            for pathway in active_pathways:
                rt = celery_app.send_task(
                    'celery_app.perform_recognition',
                    args=(pathway.id,),
                    queue=f'pathway_{pathway.id}'
                )
                logging.info(f"send pathway task. celery_app.perform_recognition({pathway.id}). rt={rt}")
            time.sleep(5)
        finally:
            session.close()
pathway_thread = threading.Thread(target=pathway_listener, daemon=True)

@asynccontextmanager
async def lifespan(fastapi: FastAPI):
    pathway_thread.start()
    yield
app = FastAPI(lifespan=lifespan)
app.include_router(rtsp.router)
app.include_router(task.router)


@app.exception_handler(Exception)
async def http_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        {"code": 500, "msg": str(exc)},
        status_code=500)

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=SERVER_PORT)