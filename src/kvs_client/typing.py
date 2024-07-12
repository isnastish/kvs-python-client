from typing import Optional
from dataclasses import dataclass, field

@dataclass
class BaseResult:
    status_code: int = field(default=0)
    status_msg: str = field(default="")
    remote_error: Optional[str] = None


@dataclass
class StrResult(BaseResult):
    result: str = field(default="")

@dataclass
class IntResult(BaseResult):
    result: int = field(default=0)


@dataclass
class BoolResult(BaseResult):
    result: bool = False

@dataclass 
class DictResult(BaseResult):
    result: dict[str, str] = {}

