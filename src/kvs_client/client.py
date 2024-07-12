from typing import Optional
from dataclasses import dataclass, field 
from http import HTTPStatus
from yarl import URL

# logging.config.dictConfig(LOGGING_CONFIG)

# _log = logging.getLogger(__name__)

_HTTP_RETRY_STATUSES = [
    HTTPStatus.BAD_GATEWAY,
    HTTPStatus.TOO_MANY_REQUESTS,
    HTTPStatus.TOO_EARLY,
    HTTPStatus.GATEWAY_TIMEOUT,
    HTTPStatus.REQUEST_TIMEOUT,
    HTTPStatus.SERVICE_UNAVAILABLE,
]


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
