from typing import TypeVar, Optional

from pydantic import BaseModel

T = TypeVar("T")

class Response(BaseModel):
    code: int = 200
    msg: str = "OK"
    data: Optional[T] = None


    @classmethod
    def ok(cls, data: Optional[T] = None):
        return cls(code=200, data=data)

    @classmethod
    def fail(cls,  message:str = None, data: Optional[T] = None):
        return cls(code=500, msg=message, data=data)
