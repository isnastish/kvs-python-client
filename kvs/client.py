import numpy as np

import os
import logging
import json
import asyncio
import typing as t
from types import TracebackType 
from functools import cache
from contextlib import AsyncExitStack
from yarl import URL
from aiohttp import (
    ClientSession, 
    ClientTimeout,
    TCPConnector, 
)
from opentelemetry.instrumentation.aiohttp_client import create_trace_config

from .results import (
    StrResult, 
    IntResult,
    BoolResult,
    FloatResult,
    DictResult, 
    UintResult,
)

_KVS_SERVICE_URL = os.getenv("KVS_SERVICE_URL", "http://localhost:8080")

_logger = logging.getLogger(__name__)


class Client:
    _defaut_headers = {"user-agent": "kvs-client"}
    _service_name = "kvs"
    _service_version = "v1.0.0"

    def __init__(
        self, base_url: t.Optional[URL | str]=_KVS_SERVICE_URL, retries_count: int=5, delay: float=2.0
    ) -> None:
        self._base_url = self._build_base_url(base_url)
        self._retries_count = retries_count
        self._delay = delay
        self._exit_stack: AsyncExitStack = None
        self._client: ClientSession = None
        self._tcp_connector = TCPConnector(force_close=True)

    def _build_base_url(self, base_url: URL | str) -> URL:
        return URL(base_url) / self._service_name / self._service_version.replace(".", "-")


    def __enter__(self):
        raise ValueError("Not supported, use async context instead")


    def __exit__(self, exc_type, exc_val: t.Optional[BaseException], 
                 exc_tb: t.Optional[TracebackType]) -> None:
        pass


    async def __aenter__(self) -> "Client":
        self._exit_stack = AsyncExitStack()
        self._client = await self._exit_stack.enter_async_context(
            ClientSession(
                timeout=ClientTimeout(total=100),
                trace_configs=[create_trace_config()], 
                headers=self._defaut_headers,
                # connector=self._tcp_connector,
            )
        )
        return self


    async def __aexit__(self, exc_type,
        exc_val: t.Optional[BaseException],
        exc_tb: t.Optional[TracebackType],
    ) -> None:
        if self._exit_stack:
            await self._client.close()
            self._exit_stack = None

        # Wait a bit for the underlying SSL connection to close
        await asyncio.sleep(0.250)


    @cache
    async def echo(self, input: str, /) -> StrResult:
        """Execute echo remote procedural call.
        Results are cached.
         
        :param input: input to the the echo rpc.
        :returns: StrResult containing the result value if succeeded.
                Otherwise check the status and the error members. 
        """
        res: StrResult
        async with self._client.post(self._base_url / "echo", data=input, headers={"content-length": str(len(input))}) as r:
            res = StrResult(status=r.status, url=r.url, params=(input,))
            if not r.ok: res.error = await r.text()
            else: res.result = await r.text()
        return res


    @cache
    async def hello(self) -> StrResult:
        """Execute hello remote procedural call.
        Calls are cached.
        
        :returns: StrResult containing the result of hello rpc if succeeded.
                Otherwise check the status and the error members.
        """
        res: StrResult
        async with self._client.post(self._base_url / "hello") as r:
            res = StrResult(url=r.url, status=r.status, params=("none",))
            if not r.ok: res.error = await r.text()
            else: res.result = await r.text()
        return res


    @cache
    async def fibo(self, n: int, /) -> IntResult:
        """Execute fibo remote procedural call. 
        Calls are cached.
        
        :param n: fibonacci sequence index.
        :returns: IntResult containing the result value if succeeded.
                Otherwise the status member will contain the response status, 
                and the error member will contain the error message, if any.
        """
        res: IntResult

        url = URL(self._base_url / "fibo").with_query({"n": str(n)})
        async with self._client.post(url=url) as r:
            res = IntResult(status=r.status, url=r.url, params=(n,))
            if not r.ok: res.error = await r.text() 
            else: res.result = int(await r.read(), base=10)
        return res


    async def incr(self, key: str, /) -> IntResult:
        """Increment value by one in the remote integer storage.

        :param key: key holding a value. If the key doesn't exist, 
                a new one will be inserted and value incremented.
        :returns: IntResult holding the previous value if succeeded.
                Otherwise check the status and the error members.
        """
        res: IntResult
        async with self._client.put(self._base_url / f"int-incr/{key}") as r:
            res = IntResult(status=r.status, url=r.url, params=(key,))
            if not r.ok(): res.error = await r.text()
            else: res.result = int(await r.text(), base=10) 
        return res


    async def incr_by_d(self, kv_pair: dict[str, int], /) -> IntResult:
        """Increment value in the remote integer storage.

        :param kv_pair: dictionary containing key-value pair.
        :returns: InResult. See `incr_by`.
        :raises KeyError: if the dictionary is empty.
        """
        key, value = kv_pair.popitem()
        return await self.incr_by(key, value)


    async def incr_by(self, key: str, value: int, /) -> IntResult:
        """Increment value in the integer storage.

        :param key: key holding a value.
        :param value: amount by which to increment.
        :returns: IntResult containing a previous value before increment if succeeded.
                If the key didn't exist before, the value returned is 0.
                Otherwise, check the status and the error members. 
        """
        res: IntResult
        async with self._client.put(
            self._base_url / f"int-incrby/{key}",
            data=str(value),
            headers={"content-length": str(len(f"{value}"))},
        ) as r:
            res = IntResult(status=r.status, url=r.url, params=(key, value))
            if not r.ok(): res.error = await r.text()
            else: res.result = int(await r.read(), base=10)
        return res

        
    async def int_put_d(self, kv_pair: dict[str, int], /) -> IntResult:
        """Put integer value into the remote integer storage.

        :param kv_pair: dictionary holding key-value pair. 
        :returns: IntResult. See `int_put`.
        :raises KeyError: if the dictionary is empty.
        """
        key, value = kv_pair.popitem()
        return await self.int_put(key, value)


    async def int_put(self, key: str, value: np.int32, /) -> IntResult:
        """Put key into the integer storage.

        :param key: key to be inserted into the storage. 
                If the key already exists, it will be updated with a new value.
        :param value: a new value.
        :returns: IntResult with result member set to true if the key was deleted.
                Otherwise check the status and the error members.
        """
        _logger.info("int storage put, key: %s, value: %s", key, value)

        res: IntResult
        async with self._client.put(self._base_url / f"int-put/{key}", data=str(value)) as r:
            res = IntResult(status=r.status, url=r.url, params=(key, value))
            if not r.ok: res.error = await r.text()
            else: res.result = r.status
        return res


    async def int_get(self, key: str, /) -> IntResult:
        """Get value from the remote integer storage.

        :param key: key that references a value in a remote storage.
        :returns: IntResult containing the value if succeeded. 
                If the key doesn't exist, status will be set to 404.
                Check status and error members.
        """
        _logger.info("int storage get, key: %s", key)

        res: IntResult
        async with self._client.get(self._base_url / f"int-get/{key}") as r:
            res = IntResult(status=r.status, url=r.url, params=(key,)) 
            if not r.ok: res.error = await r.text() 
            else: res.result = int(await r.read(), base=10)
        return res


    async def int_del(self, key: str, /) -> BoolResult:
        """Delete key from the integer storage.

        :param key: key to be deleted.
        :returns: BoolResult with result member set to true if the key was deleted.
                Otherwise check the status and the error members.
        """
        _logger.info("int  storage del, key: %s", key)
        
        res: BoolResult
        async with self._client.delete(self._base_url / f"int-del/{key}") as r:
            res = BoolResult(status=r.status, url=r.url, params=(key,))
            if not r.ok: res.error = await r.text() 
            else:
                if r.headers.get("Deleted"): 
                    res.result = True
        return res


    async def float_put_d(self, kv_pair: dict[str, float], /) -> IntResult:
        """Put float value into the remote float storage.

        :param kv_pair: dictionary holding the key and the value.
        :returns: IntResult. See `float_put`.
        :raises KeyError: if dictionary is empty.
        """
        key, value = kv_pair.popitem()
        return await self.float_put(key, value)


    async def float_put(self, key: str, value: np.float32, /) -> IntResult:
        """Put float value into the remote float storage.
    
        :param key: key to be inserted into the float storage.
                If the key already exists, the value will be updated.
        :param value: a new value.
        :return: IntResult with result member set to true if the operation succeeded.
                Otherwise check status and error members.
        """
        _logger.info("float storage put, key: %s, value %s", key, value)

        res: IntResult
        async with self._client.put(self._base_url / f"float-put/{key}", data=str(value)) as r:
            res = IntResult(status=r.status, url=r.url, params=(key, value))
            if not r.ok: res.error = await r.text()
            else: res.result = r.status
        return res


    async def float_get(self, key: str, /) -> FloatResult:
        """Get float from the float storage.

        :param key: key which references value in a remote storage.
        :returns: FloatResult containing the value if succeeded.
                Otherwise, check the status and error members.
        """
        _logger.info("float storage get, key: %s", key)
        
        res: FloatResult
        async with self._client.get(self._base_url / f"float-get/{key}") as r:
            res = FloatResult(status=r.status, url=r.url, params=(key,))
            if not r.ok: res.error = await r.text()
            else: res.result = float(await r.read())
        return res


    async def float_del(self, key: str, /) -> BoolResult:
        """Delete key from the float storage.

        :param key: key to be deleted.
        :returns: BoolResult with result set to true if the key was deleted.
                Otherwise, the status will contain the response status, 
                and the error member will contain an error, if any.
        """
        _logger.info("float storage del, key: %s", key)

        res: BoolResult
        async with self._client.delete(self._base_url / f"float-del/{key}") as r:
            res = BoolResult(status=r.status, url=r.url, params=(key,))
            if not r.ok: res.error = await r.text()
            else: 
                if r.headers.get("Deleted"):
                    res.result = True
        return res


    async def str_put_d(self, kv_pair: dict[str, str], /) -> IntResult:
        """Put string into the remote string storage.
        
        :param kv_pair: dictionary holding a key and a value.
        :returns: IntResult. See `str_put`.
        :raises: KeyError if dictionary is empty.
        """
        key, value = kv_pair.popitem()
        return await self.str_put(key, value)


    async def str_put(self, key: str, value: str, /) -> IntResult:
        """Put string into the remote string storage.

        :param key: key to be inserted into the string storage. 
                If the key already exists, the value is updated.
        :param value: new value.
        :returns: BoolResult with result member set to true, 
                if the operation succeeded. Otherwise check
                the status and error members.
        """
        _logger.info("str storage put, key: %s, value: %s", key, value)
        
        res: IntResult
        async with self._client.put(self._base_url / f"str-put/{key}", data=value) as r:
            res = IntResult(status=r.status, url=r.url, params=(key, value))
            if not r.ok: res.error = await r.text() 
            else: res.result = r.status
        return res


    async def str_get(self, key: str, /) -> StrResult:
        """Get string value from the string storage.

        :param key: key holding a value in a remote storage.
        :returns: StrResult containing the result of operation.
                If succeeded, the result member holds the desired string.
                Otherwise status will contain the response status, 
                and an error member will contain an error message, if any.
        """
        _logger.info("str storage get, key: %s", key)
        
        res: StrResult
        async with self._client.get(self._base_url / f"str-get/{key}") as r:
            res = StrResult(status=r.status, url=r.url, params=(key,))
            if not r.ok: res.error = await r.text() 
            else: res.result = await r.text()
        return res


    async def str_del(self, key: str, /) -> BoolResult:
        """Delete key from the string storage.

        :param key: key to be deleted.
        :returns: BoolResult containing the result of operation.
                If succeeded, the result member is set to true.
                Otherwise status will contain the response status, 
                and an error member will contain an error message, if any.
        """
        _logger.info("str storage del, key: %s", key)
        
        res: BoolResult
        async with self._client.delete(self._base_url / f"str-del/{key}") as r:
            res = BoolResult(status=r.status, url=r.url, params=(key,))
            if not r.ok: res.error = await r.text()
            else: 
                if r.headers.get("Deleted"):
                    res.result = True
        return res


    async def dict_put_d(self, kv_pair: dict[str, dict[str, str]], /) -> IntResult:
        """Put map int the remote map storage.

        :param kv_pair: dictionary containing a key and a value (nested dictionary) to be inserted. 
        :returns: BoolResult holding the result of operation. See `dict_put`.
        :raises KeyError: if the dictionary is empty.
        """
        key, value = kv_pair.popitem()
        return await self.dict_put(key, value)


    async def dict_put(self, key: str, value: dict[str, str], /) -> IntResult:
        """Put map into the remote map storage with the following key.
        
        :param key: a new key to be inserted. If the key already exists
                it will be updated with a new map value.
        :param value: a new map value.
        :returns: BoolResult with result member set to true if the operation succeeded. 
                Otherwise the status member will contain the response status, 
                and the error member will contain the error message, if any.
        :raises TypeError: if dictionary contains non-pod keys.
        """
        _logger.info("map storage put, key: %s, value: %s", key, value)
        
        obj = json.dumps(obj=value, skipkeys=True)
        
        res: IntResult
        async with self._client.put(self._base_url / f"map-put/{key}", data=obj) as r:
            res = IntResult(status=r.status, url=r.url, params=(key, value))
            if not r.ok: res.error = await r.text()
            else: 
                res.result = r.status
        return res


    async def dict_get(self, key: str, /) -> DictResult:
        """Get map from the remote map storage with the following key.

        :param key: key referencing a map in the remote storage.
        :returns: DictResult containing a map in its result member if succeeded.
                Otherwise check the status and the error members.
        :raises JSONDecodeError: If the response contains invalid json.
        """
        _logger.info("map storage get, key: %s", key)
        
        res: DictResult
        async with self._client.get(self._base_url / f"map-get/{key}") as r:
            res = DictResult(status=r.status, url=r.url, params=(key,))
            if not r.ok: res.error = await r.text()
            else: 
                d = json.loads(s=await r.read())
                _logger.info("Deserialized object %s", d)
                res.result = d
        return res


    async def dict_del(self, key: str, /) -> BoolResult:
        """Delete map from the remote storage with the following key.
        
        :param key: key to be deleted.
        :returns: BoolResult with result member set to true if the key was deleted.
                Otherwise check the status and the error members.
        """
        _logger.info("map storage del, key: %s", key)
        
        res: BoolResult
        async with self._client.delete(self._base_url / f"map-del/{key}") as r:
            res = BoolResult(status=r.status, url=r.url, params=(key,))
            if not r.ok: res.error = await r.text()
            else: 
                if r.headers.get("Deleted"):
                    res.result = True
        return res


    async def uint_put(self, key: str, value: np.uint32, /) -> IntResult:
        """_summary_
        Returns:
            BoolResult: _description_
        """
        _logger.info("uint storage put, key: %s, value: %s", key, value)

        
        res: IntResult
        async with self._client.put(self._base_url / f"uint/put/{key}", data=str(value)) as r:
            res = IntResult(status=r.status, url=r.url, params=(key, value))
            if not r.ok: res.error = await r.text()
            else: 
                res.result = r.status
        return res
    
    
    async def uint_get(self, key: str, /) -> UintResult:
        """"""
        _logger.info("uint storage get, key: %s", key)

        res: UintResult
        async with self._client.get(self._base_url / f"uint/get/{key}") as r:
            res = UintResult(status=r.status, url=r.url, params=(key,))
            if not r.ok: res.error = await r.text()
            else:
                res.result = np.array([await r.read()]).astype(np.uint32)[0]
        return res
        
        
    async def uint_del(self, key: str, /) -> BoolResult:
        """"""
        _logger.info("uint storage del, key: %s", key)
        
        res: BoolResult
        async with self._client.delete(self._base_url / f"uint/del/{key}")as r:
            res = BoolResult(status=r.status, url=r.url, params=(key,))
            if not r.ok: res.error = await r.text()
            else:
                if r.headers.get("Deleted"):
                    res.result = True
        return res