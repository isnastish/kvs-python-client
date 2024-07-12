from types import TracebackType 
from typing import Optional
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
import asyncio

from .typing import *
from .log import LOGGING_CONFIG

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

    def __init__(
        self, base_url: URL | str, port: int, retries_count: int = 3, delay: float = 2.0
    ) -> None:
        self._base_url = self._build_base_url(base_url, port)
        self._retries_count = retries_count
        self._delay = delay
        self._exit_stack: AsyncExitStack = None
        self._client: ClientSession = None

    def _build_base_url(self, base: URL | str, port: int) -> URL:
        res = (
            URL(base).with_port(port)
            / self._service_name
            / self._service_version.replace(".", "-")
        )
        return res

    def __enter__(self):
        raise ValueError("Not supported, use async context instead")

    # linter won't pass here!
    def __exit__(
        self,
        exc_type,
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
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
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if self._exit_stack:
            await self._client.close()
            self._exit_stack = None

    async def echo(self, input: str) -> StrResult:
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

    async def int_add(self, val: int) -> BoolResult:
        async with self._client.post() as resp:
            pass

    async def int_get(self) -> IntResult:
        ...

    async def int_del(self) -> BoolResult:
        ...

    async def _make_http_request(
        self, url: URL, method: HttpMethod, headers: Optional[dict[str, str]] = None
    ) -> ClientResponse:
        # TODO: Instead of iterating over each method, pass client.get/post/delete/put function directly
        # as a callable object. The only problem in that case would be that we won't be able to see the function signature
        retry_attempt = 0
        resp: aiohttp.ClientResponse
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
            except (aiohttp.ClientConnectionError, aiohttp.ClientOSError) as e:
                if retry_attemt > self._retries_count:
                    raise
                _log.error("Connection failure, retry attempt %i", retry_attempt)

            if retry_attempt <= self._retries_count:
                _log.info("Attempt %s failed, retrying in %ss", self._delay)
                await asyncio.sleep(self._delay)

        # Do we want to throw an exception if we run out of retries?
        return resp
