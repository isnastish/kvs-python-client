from types import TracebackType 
import typing as t
from dataclasses import dataclass, field
from contextlib import AsyncExitStack
from http import HTTPStatus
from yarl import URL
from enum import IntEnum
from aiohttp import (
    ClientSession, 
    ClientConnectionError, 
    ClientOSError, 
    ClientResponse
)
import logging
import asyncio

from .typing import *

KVS_SERVICE_DEFAULT_URL = "http://127.0.0.1"
KVS_DEFAULT_PORT = 8080


_logger = logging.getLogger(__name__)

# TODO: BaseResult shold contain the command name which was invoked and parameters
@dataclass
class BaseResult:
    status_code: int = field(default=0)
    status_msg: str = field(default="")
    error: t.Optional[str] = None


@dataclass
class StrResult(BaseResult):
    result: str = field(default="")


@dataclass
class IntResult(BaseResult):
    result: int = field(default=0)


@dataclass
class BoolResult(BaseResult):
    result: bool = field(default=False)


@dataclass
class FloatResult:
    result: float = field(default=0.0)


@dataclass 
class DictResult(BaseResult):
    # https://stackoverflow.com/questions/53632152/why-cant-dataclasses-have-mutable-defaults-in-their-class-attributes-declaratio
    result: dict[str, str] = field(default_factory=dict)

class HttpMethod(IntEnum):
    GET = 1
    POST = 2
    PUT = 3
    PATCH = 4
    HEAD = 5
    DELETE = 6


_HTTP_RETRY_STATUSES = [
    HTTPStatus.BAD_GATEWAY,
    HTTPStatus.TOO_MANY_REQUESTS,
    HTTPStatus.TOO_EARLY,
    HTTPStatus.GATEWAY_TIMEOUT,
    HTTPStatus.REQUEST_TIMEOUT,
    HTTPStatus.SERVICE_UNAVAILABLE,
]

class KVSClient:
    _defaut_headers = {"user-agent": "kvs-client"}
    _service_name = "kvs"
    _service_version = "v1.0.0"

    # NOTE: Instead of passing both the url and a port, we can pass the whole url
    def __init__(
        self, base_url: URL | str, retries_count: int = 3, delay: float = 2.0
    ) -> None:
        self._base_url = self._build_base_url(base_url)
        self._retries_count = retries_count
        self._delay = delay
        self._exit_stack: AsyncExitStack = None
        self._client: ClientSession = None

    def _build_base_url(self, base_url: URL | str) -> URL:
        return URL(base_url) / self._service_name / self._service_version.replace(".", "-")

    def __enter__(self):
        raise ValueError("Not supported, use async context instead")

    # linter won't pass here!
    def __exit__(
        self,
        exc_type,
        exc_val: t.Optional[BaseException],
        exc_tb: t.Optional[TracebackType],
    ):
        pass

    async def __aenter__(self) -> "KVSClient":
        self._exit_stack = AsyncExitStack()
        self._client = await self._exit_stack.enter_async_context(
            #  Pass all the necessary params
            ClientSession()
        )
        return self

    async def __aexit__(
        self,
        exc_type,  # this has to a be a type of Optional[BaseException]
        exc_val: t.Optional[BaseException],
        exc_tb: t.Optional[TracebackType],
    ) -> None:
        if self._exit_stack:
            await self._client.close()
            self._exit_stack = None

    async def echo(self, input: str, /) -> StrResult:
        res = StrResult()
        headers = self._defaut_headers | {"content-length": str(len(input))}
        async with self._client.post(
            self._base_url / "echo", data=input, headers=headers
        ) as resp:
            res.status_code = resp.status
            if (resp.status != HTTPStatus.OK) and resp.content_length:
                # The body will contain an error if the response status is not 200
                res.remote_error = await resp.text()
                return res
            else:
                res.result = await resp.text()
            return res

    async def hello(self) -> StrResult:
        res = StrResult()
        async with self._client.post(self._base_url / "hello") as resp:
            res.status_code = resp.status
            if (resp.status != HTTPStatus.OK) and resp.content_length:
                # The body will contain an error if the response status is not 200
                res.remote_error = await resp.text()
                return res
            else:
                res.result = await resp.text()
            return res

    async def fibo(self, n: int, /) -> IntResult:
        url = (self._base_url / "fibo").with_query({"n": n})
        result = IntResult()

        async with self._client.post(url) as resp:
            result.status_code = resp.status
            if resp.status != HTTPStatus.OK:
                result.remote_error = await resp.text()
                return result

            result.result = int(await resp.text(), base=10)
            return result

    # https://stackoverflow.com/questions/24735311/what-does-the-slash-mean-when-help-is-listing-method-signatures
    # All arguments prior to / are positional only arguments, 
    # and all arguments after / could be positional or keyword
    async def int_add(self, key: str, value: int, /) -> BoolResult:
        """"""

    async def int_get(self, key: str, /) -> IntResult:
        """"""

    async def int_del(self, key: str, /) -> BoolResult:
        """"""

    async def float_add(self, key: str, value: float, /) -> BoolResult:
        """"""

    async def float_get(self, key: str, /) -> FloatResult:
        """"""

    async def float_del(self, key: str, /) -> BoolResult:
        """"""

    async def str_add(self, key: str, value: str, /) -> BoolResult:
        """"""
    
    async def str_get(self, key: str, /) -> StrResult:
        """"""
        
    async def str_del(self, key: str, /) -> BoolResult:
        """"""

    # TODO: Once the KVS service is capable of storing arbitrary types, 
    # such as dict[str, int/float/str] etc, this function has to be adjusted accordingly
    async def dict_add(self, key: str, value: dict[str, str], /) -> BoolResult:
        """"""

    async def dict_get(self, key: str, /) -> DictResult:
        """"""
    
    async def dict_del(self, key: str, /) -> BoolResult:
        """"""

    async def _make_http_request(
        self, url: URL, method: HttpMethod, headers: t.Optional[dict[str, str]] = None
    ) -> ClientResponse:
        # TODO: Instead of iterating over each method, pass client.get/post/delete/put function directly
        # as a callable object. The only problem in that case would be that we won't be able to see the function signature
        retry_attempt = 0
        resp: ClientResponse
        while retry_attemt <= self._retries_count:
            # TODO: Increment the sleep count gradually
            retry_attemt += 1
            try:
                match method:
                    case HttpMethod.GET:
                        resp = await self._client.get(url, headers=headers)
                        
                    case HttpMethod.POST:
                        resp = await self._client.post(url, headers=headers)

                    case HttpMethod.PUT:
                        resp = await self._client.put(url, headers=headers)

                    case HttpMethod.HEAD:
                        resp = await self._client.head(url, headers=headers)
                    
                    case HttpMethod.DELETE:
                        resp = await self._client.head(url, headers=headers)

                if resp.status not in _HTTP_RETRY_STATUSES:
                    return resp
                
            # Handle possible low-level connections problems
            except (ClientConnectionError, ClientOSError) as e:
                if retry_attemt > self._retries_count:
                    raise
                # _log.error("Connection failure, retry attempt %i", retry_attempt)

            if retry_attempt <= self._retries_count:
                # _log.info("Attempt %s failed, retrying in %ss", self._delay)
                await asyncio.sleep(self._delay)

        # Do we want to throw an exception if we run out of retries?
        return resp


async def kvs_echo(service_url: URL | str, input: str, /) -> None:
    """_summary_
    """
    async with KVSClient(service_url) as client:
        res: StrResult = await client.echo(input)
        _logger.info("res %s", res.result)


async def kvs_int_add(service_url: URL | str, key: str, value: int, /) -> None:
    """Put value into the remote storage using the specified `key`.
    Later, the value can be retrieved from the storage using the same key.

    :param service_url: url to the kvs service.
    :param key: hash key used to store the value.
    :param value: value to be stored.
    """
    async with KVSClient(service_url) as client:
        res: BoolResult = await client.int_add(key, value)
        _logger.info("res %s", res.result)


async def kvs_int_get(service_url: URL | str, key: str, /) -> None:
    """_summary_
    """
    async with KVSClient(service_url) as client:
        res: IntResult = await client.int_get(key)
        return res

async def kvs_int_del() -> None:
    pass

