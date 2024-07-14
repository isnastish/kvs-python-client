from types import TracebackType 
import typing as t
from contextlib import AsyncExitStack
from http import HTTPStatus
from yarl import URL
from enum import IntEnum
from aiohttp import (
    ClientSession, 
    ClientConnectionError, 
    ClientOSError, 
    ClientResponse, 
    ClientTimeout,
)
from opentelemetry.instrumentation.aiohttp_client import create_trace_config

import logging
import asyncio

from .result import *

_logger = logging.getLogger(__name__)

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
        self, base_url: URL | str, retries_count: int=5, delay: float=2.0
    ) -> None:
        self._base_url = self._build_base_url(base_url)
        self._retries_count = retries_count
        self._delay = delay
        self._exit_stack: AsyncExitStack = None
        self._client: ClientSession = None
        self._request_timeout = ClientTimeout(total=15) # seconds 

    def _build_base_url(self, base_url: URL | str) -> URL:
        return URL(base_url) / self._service_name / self._service_version.replace(".", "-")

    def __enter__(self):
        raise ValueError("Not supported, use async context instead")

    def __exit__(self, exc_type, exc_val: t.Optional[BaseException], 
                 exc_tb: t.Optional[TracebackType]) -> None:
        pass

    async def __aenter__(self) -> "KVSClient":
        """
        """
        self._exit_stack = AsyncExitStack()
        self._client = await self._exit_stack.enter_async_context(
            ClientSession(timeout=self._request_timeout, trace_configs=[create_trace_config()])
        )
        return self


    async def __aexit__(self, exc_type,
        exc_val: t.Optional[BaseException],
        exc_tb: t.Optional[TracebackType],
    ) -> None:
        """"""
        if self._exit_stack:
            await self._client.close()
            self._exit_stack = None


    async def echo(self, input: str, /) -> StrResult:
        """Invoke echo remote procedural call. 
        Mainly used for testing the connection between kvs service and a client, 
        and doesn't modify the state of the storage.
        
        :param input: string passed to echo rpc.
        """
        async with self._client.post(self._base_url / "echo", data=input) as resp:
            echo_res = StrResult(url=resp.url, status=resp.status, params=[input])
            if resp.status != HTTPStatus.OK:
                echo_res.error = await resp.text()
                return echo_res
            echo_res.result = await resp.text()
            return echo_res


    async def hello(self) -> StrResult:
        """Invoke hello remote procedural call.
        """
        async with self._client.post(self._base_url / "hello") as resp:
            hello_res = StrResult(url=resp.url, status=resp.status)
            if resp.status != HTTPStatus.OK:
                hello_res.error = await resp.text()
                return hello_res
            hello_res.result = await resp.text()
            return hello_res


    async def fibo(self, n: int, /) -> IntResult:
        """Invoke fibo remote procedural callback 
        to compute the fibonacci number with index n.
        Used exclusively for testing the connection and doesn't modify the state 
        of the remote storage.
        
        :param n: fibonacci sequence index.
        """
        url = URL(self._base_url / "fibo").with_query({"n": str(n)})
        fibo_res = IntResult(url=url, params=(n))

        try:
            async with self._client.post(url) as resp:
                fibo_res.status = resp.status
                if resp.status != HTTPStatus.OK:
                    fibo_res.error = await resp.text()
                    return fibo_res
                fibo_res.result = int(await resp.text(), base=10)
                return fibo_res
        except asyncio.TimeoutError as e:
            fibo_res.error = e.__doc__
            return fibo_res


    async def incr(self, key: str, /) -> IntResult:
        """_summary_

        :param key: 
        """
        # TODO: Change endpoint on the service side to `incr`
        async with self._client.put(self._base_url / f"intincr/{key}") as resp:
            incr_res = IntResult(status=resp.status, url=resp.url, params=(key))
            if resp.status != HTTPStatus.OK:
                incr_res.error = await resp.text()
            else:
                # The body should contain a value (before the increment)
                incr_res.result = int(await resp.text())
            return incr_res


    async def incr_by(self, key: str, value: int, /) -> IntResult:
        """_summary_

        :param key: 
        :param value: 
        :returns 
        """ 
        async with self._client.put(self._base_url / f"intincrby/{key}", data=str(value), headers=self._defaut_headers) as resp:
            incrby_res = IntResult(status=resp.status, url=resp.url, params=(key, value))
            if resp.status != HTTPStatus.OK:
                incrby_res.error = await resp.text()
            else:
                incrby_res.result = int(await resp.text(), base=10)
            return incrby_res


    async def int_add(self, key: str, value: int, /) -> BoolResult:
        """_summary_
        
        :param key:
        :param value:
        :returns
        """
        async with self._client.put(self._base_url / f"intadd/{key}", data=str(value), headers=self._defaut_headers) as resp:
            bool_res = BoolResult(url=resp.url, status=resp.status, params=(value))
            if resp.status != HTTPStatus.OK: # Or HTTPStatus.CREATED?
                bool_res.error = await resp.text()
                return bool_res
            bool_res.result = True
            return bool_res 


    async def int_get(self, key: str, /) -> IntResult:
        """
        """
        async with self._client.get(self._base_url / f"intget/{key}") as resp:
            int_res = IntResult(status=resp.status, url=resp.url, params=key)
            if resp.status != HTTPStatus.OK:
                int_res.error = await resp.text()
            else:
                int_res.result = int(await resp.text())
            return int_res


    async def int_del(self, key: str, /) -> BoolResult:
        """Delete key if exists from the remote storage.

        :param key: key to be deleted. 
        :returns BoolResult class with result set to true if the key was deleted.
        """
        async with self._client.delete(self._base_url / f"intdel/{key}") as resp:
            bool_res = BoolResult(status=resp.status, url=resp.url, params=key)
            if resp.status != HTTPStatus.OK:
                bool_res.error = await resp.text()
            else:
                if resp.headers.get("Deleted"): 
                    bool_res.result = True
            return bool_res


    async def float_add(self, key: str, value: float, /) -> BoolResult:
        """_summary_
        
        :param key:
        :param value:
        :return: 
        """

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
        while retry_attempt <= self._retries_count:
            # TODO: Increment the sleep count gradually
            retry_attempt += 1
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
                if retry_attempt > self._retries_count:
                    raise
                # _log.error("Connection failure, retry attempt %i", retry_attempt)

            if retry_attempt <= self._retries_count:
                # _log.info("Attempt %s failed, retrying in %ss", self._delay)
                await asyncio.sleep(self._delay)

        # Do we want to throw an exception if we run out of retries?
        return resp
