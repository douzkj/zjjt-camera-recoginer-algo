from typing import TypeVar, Optional

from pydantic import BaseModel

T = TypeVar("T")

class Response(BaseModel):
    code: int = 200
    message: str = "OK"
    data: Optional[T] = None


    @classmethod
    def ok(cls, data: Optional[T] = None):
        return cls(code=200, data=data)

    @classmethod
    def fail(cls, data: Optional[T] = None, message:str = None):
        return cls(code=500, message=message, data=data)
