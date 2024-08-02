import asyncio
import typing as t
from collections.abc import Generator
from functools import wraps
from yarl import URL

import numpy as np
import click
from aiohttp import ServerDisconnectedError

from kvs.client import Client
from kvs.results import (
    StrResult, 
    IntResult, 
    BoolResult, 
    FloatResult, 
    DictResult,
    UintResult,
)


_ResultT: t.TypeAlias = t.Union[FloatResult|IntResult|BoolResult|StrResult|DictResult|UintResult]



def echo_error(status: int, error: str, url: URL, /) -> None:
    """Echo error message.
    
    :param status: request status code
    :param error: error message, if any
    :param url: request URL
    """
    click.echo(f"failed with status: {status}, error: {error.strip()}, url: {url}")


def _handle_del_result(res: BoolResult | list[BoolResult], /) -> None:
    """"""
    if not isinstance(res, list): res = [res]
    for r in res:
        if r.error: echo_error(r.status, r.error, r.url)
        else: 
            click.echo(f"key: \"{r.params[0]}\", deleted: {r.result}")


def _handle_put_result(res: IntResult | list[IntResult], /) -> None:
    """"""
    if not isinstance(res, list): res = [res]
    for r in res:
        if r.error: echo_error(r.status, r.error, r.url)
        else:
            click.echo(f"key \"{r.params[0]}\", status: {r.result}")


def _handle_get_result(res: _ResultT | list[_ResultT], /) -> None:
    """Helper function for displaying response results.
    :param results: response returned by the remote storage.
    """
    if not isinstance(res, list): res = [res]
    for r in res:
        if r.error: echo_error(r.status, r.error, r.url)
        else:
            click.echo(f"key \"{r.params[0]}\", value: {r.result}")


def handle_server_exceptions(func: t.Callable[[t.Any, t.Any], t.Awaitable[None]], /) -> None:
    """Decorator used to handle aiohttp.ServerDisconnectedError and 
    asyncio.TimeoutError. 

    When interacting with the remote storage via the cli, we should catch the
    exceptions coming from the client for better user experience,
    rather than displaying the stack trace.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs) -> None:
        # Handle server disconnect and timeout errors for convenience, 
        # so we don't display the whole stack trace to a command prompt
        try:
            await func(*args, **kwargs)
        except ServerDisconnectedError:
            click.echo(f"Command '{func.__name__} failed, server disconnected.") 
        except asyncio.TimeoutError:
            click.echo(f"Command '{func.__name__}' failed, timeout.")

    return wrapper


@click.group()
def root() -> None:
    """Cli root command."""


@root.command()
@click.argument("args", type=str, nargs=-1)
def echo(args: list[str]) -> None:
    """Invoke echo remote procedural call.
    
    :param echo_string: strings to be passed to echo rpc.
    """
    @handle_server_exceptions
    async def kvs_echo() -> None:
        async with Client() as client:
            _handle_get_result(await asyncio.gather(
                *(asyncio.create_task(client.echo(s)) for s in args)
            ))
    asyncio.run(kvs_echo())


@root.command()
def hello() -> None:
    """Invoke hello remote procedural call. 
    Mainly used to test the connection and doesn't modify storage state"""
    @handle_server_exceptions
    async def kvs_hello() -> None:
        async with Client() as client:
            _handle_get_result(await client.hello())

    asyncio.run(kvs_hello())


@root.command()
@click.argument("index", type=int, nargs=-1)
def fibo(index: list[int]) -> None:
    """Invoke fibo remote procedural call. 

    :param index: list of fibonacci sequence indices to be computed.
    """
    async def cancellable_fibo(client: Client, n: int, /) -> None:
        task = asyncio.create_task(client.fibo(n))
        time_elapsed = 0
        while not task.done():
            time_elapsed += 0.25
            await asyncio.sleep(0.25)
            if time_elapsed >= 10: # seconds elapsed
                task.cancel()
        try:
            _handle_get_result(await task)
        except asyncio.CancelledError:
            click.echo(f"Task fibo({n}) was canceled, elapsed time {time_elapsed}")

    @handle_server_exceptions
    async def kvs_fibo(indices: list[int], /) -> None:
        async with Client() as client:
            await asyncio.gather(
                *(asyncio.create_task(cancellable_fibo(client, n)) for n in indices)
            )

    asyncio.run(kvs_fibo(index))


@root.command()
@click.argument("pairs", type=str, nargs=-1)
def int_put(pairs: list[str]) -> None:
    """Put integer to the remote storage with the following key.
    :param pairs: space separated key-value pairs in a form 'key:value'
    """
    def gen(pairs: list[str], /) -> Generator[tuple[str, np.int32], np.int32, None]:
        for p in pairs:
            k, v = p.split(":", maxsplit=1)
            yield (k, np.array([v]).astype(np.int32)[0])

    @handle_server_exceptions
    async def kvs_int_put() -> None:
        async with Client() as client:
            _handle_put_result(await asyncio.gather(
                    *(asyncio.create_task(client.int_put(p[0], p[1])) for p in gen(pairs))
            ))

    asyncio.run(kvs_int_put())


@root.command()
@click.argument("key", type=str, nargs=-1)
def int_get(key: list[str]) -> None:
    """Get integer from the remote storage with the following key.
    :param key: list of keys to be retrieved.  
    """
    @handle_server_exceptions
    async def kvs_int_get(keys: list[str], /) -> None:
        async with Client() as client:
            _handle_get_result(await asyncio.gather(
                *(asyncio.create_task(client.int_get(k)) for k in keys)
            ))
    asyncio.run(kvs_int_get(key))


@root.command()
@click.argument("key", type=str, nargs=-1)
def int_del(key: list[str]) -> None:
    """Delete integers from the remote storage with the following keys.
    :param key: list of keys to be deleted.
    """
    @handle_server_exceptions
    async def kvs_int_del(keys: list[str], /) -> None:
        async with Client() as client:
            _handle_del_result(await asyncio.gather(
                *(asyncio.create_task(client.int_del(k)) for k in keys)
            ))

    asyncio.run(kvs_int_del(key))


@root.command()
@click.argument("key", type=str, nargs=-1)
def int_incr(key: list[str]) -> None:
    """Increment the value reffered by the following key by one.
    :param key: key of the value to be incremented.
    """
    @handle_server_exceptions
    async def kvs_incr(keys: list[str], /) -> None:
        async with Client() as client:
            _handle_get_result(await asyncio.gather(
                *(asyncio.create_task(client.incr(k)) for k in keys)
            ))
            
    asyncio.run(kvs_incr(key))


@root.command()
@click.argument("key", type=str)
@click.argument("value", type=int)
def int_incr_by(key: str, value: int) -> None:
    """Increment the value reffered by the following key by `value`.
    If `value` is negative, the final value will be decremented.

    :param key: key of the value to be incremented.
    :param value: count to add/subtract.
    """
    @handle_server_exceptions
    async def kvs_incr_by(key: str, value: int, /) -> None:
        async with Client() as client:
            _handle_get_result(await client.incr_by(key, value))
            
    asyncio.run(kvs_incr_by(key, value))


@root.command()
@click.argument("pairs", type=str, nargs=-1)
def float_put(pairs: list[str]) -> None:
    """Put float into the remote storage with the following key.
    :param pairs: space separate key-value pairs in a form 'key:value'
    """
    def gen(pairs: list[str], /) -> Generator[tuple[str, np.float32], np.float32, None]:
        for p in pairs:
            k, v = p.rsplit(":", maxsplit=1)
            yield (k, np.array([v]).astype(np.float32)[0])

    @handle_server_exceptions
    async def kvs_float_put() -> None:
        async with Client() as client:
            _handle_put_result(await asyncio.gather(
                *(asyncio.create_task(client.float_put(p[0], p[1])) for p in gen(pairs))
            ))

    asyncio.run(kvs_float_put())


@root.command()
@click.argument("key", type=str, nargs=-1)
def float_get(key: list[str]) -> None:
    """Get value from the float remote storage with the following key.
    :param key: key holding a value, if exists
    """
    @handle_server_exceptions
    async def kvs_float_get(keys: list[str], /) -> None:
        async with Client() as client:
            _handle_get_result(await asyncio.gather(
                *(asyncio.create_task(client.float_get(k)) for k in keys)
            ))
            
    asyncio.run(kvs_float_get(key))


@root.command()
@click.argument("key", type=str, nargs=-1)
def float_del(key: list[str]) -> None:
    """Delete value from the storage reffered by the following key.

    :param ctx: click context holding service url.
    :param key: list of keys to be deleted.
    """
    @handle_server_exceptions
    async def kvs_float_del(keys: list[str], /) -> None:
        async with Client() as client:
            _handle_del_result(await asyncio.gather(
                *(asyncio.create_task(client.float_del(k)) for k in keys)
            ))
    asyncio.run(kvs_float_del(key))


@root.command
@click.argument("pairs", type=str, nargs=-1)
def str_put(pairs: list[str]) -> None:
    """Put values into a remote string storage.
    :param pairs: space separated key-value pairs.
    """
    # This code has some limitations, if the value string contains ':',
    # they whole string won't be split properly.
    def gen(pairs: list[str], /) -> Generator[tuple[str, str], None, None]:
        for p in pairs:
            k, v = p.rsplit(":", maxsplit=1)
            yield (k, v)
            
    @handle_server_exceptions
    async def kvs_str_put() -> None:
        async with Client() as client:
            _handle_put_result(await asyncio.gather(
                *(asyncio.create_task(client.str_put(p[0], p[1])) for p in gen(pairs))
            ))

    asyncio.run(kvs_str_put())


@root.command
@click.argument("key", type=str, nargs=-1)
def str_get(key: list[str]) -> None:
    """Get value from string remote storage.
    :param key: key holding a value, if exists.
    """
    @handle_server_exceptions
    async def kvs_str_get(keys: list[str], /) -> None:
        async with Client() as client:
            _handle_get_result(await asyncio.gather(
                *(asyncio.create_task(client.str_get(k)) for k in keys)
            ))
            
    asyncio.run(kvs_str_get(key))


@root.command
@click.argument("keys", type=str, nargs=-1)
def str_del(keys: list[str]) -> None:
    """Delete value(s) from a remote string storage.
    :param keys: keys to be deleted.
    """
    @handle_server_exceptions
    async def kvs_str_del() -> None:
        async with Client() as client:
            _handle_del_result(await asyncio.gather(
                *(asyncio.create_task(client.str_del(k)) for k in keys)
            ))

    asyncio.run(kvs_str_del())


@root.command
@click.argument("key", type=str)
@click.argument("pairs", type=str, nargs=-1)
def dict_put(key: str, pairs: list[str]) -> None:
    """Put dictionary into the dict remote storage.

    :param key: key to be inserted.
    :param pairs: colon-separated list of key-value strings.
            Example: "participant_name":"Jacob" 
    """
    @handle_server_exceptions
    async def kvs_dict_put(key: str, value: dict[str, str], /) -> None:
        async with Client() as client:
            _handle_put_result(await client.dict_put(key, value))
    
    # extract key-value pairs and make a dict out of them.
    value = dict(map(lambda p: tuple(p.split("=", maxsplit=1)), pairs))

    asyncio.run(kvs_dict_put(key, value))


@root.command
@click.argument("keys", type=str, nargs=-1)
def dict_get(keys: list[str]) -> None:
    """Get dictionary form the dict remote storage.
    :param key: key(s) holding a dictionary in a remote storage.
    """
    @handle_server_exceptions
    async def kvs_dict_get() -> None:
        async with Client() as client:
            _handle_get_result(await asyncio.gather(
                *(asyncio.create_task(client.dict_get(k)) for k in keys))
            )
    asyncio.run(kvs_dict_get())


@root.command
@click.argument("keys", type=str, nargs=-1)
def dict_del(keys: list[str]) -> None:
    """Delete dictionary form the dict remote storage.
    :param key: key(s) to be deleted.
    """
    @handle_server_exceptions
    async def kvs_dict_del() -> None:
        async with Client() as client:
            _handle_del_result(await asyncio.gather(
                *(asyncio.create_task(client.dict_del(k)) for k in keys)
            ))
            
    asyncio.run(kvs_dict_del())


@root.command
@click.argument("pairs", type=str, nargs=-1)
def uint_put(pairs: list[str]) -> None:
    """
    """
    def gen(pairs: list[str], /) -> Generator[tuple[str, np.uint32], None, None]:
        for p in pairs:
            k, v = p.rsplit(":", maxsplit=1)
            yield (k, np.array([v]).astype(np.uint32)[0])

    @handle_server_exceptions
    async def kvs_uint_put() -> None:
        async with Client() as client:
            _handle_put_result(await asyncio.gather(
                *(asyncio.create_task(client.uint_put(t[0], t[1])) for t in gen(pairs))
            ))

    asyncio.run(kvs_uint_put())


@root.command
@click.argument("keys", type=str, nargs=-1)
def uint_get(keys: list[str]) -> None:
    """
    """
    @handle_server_exceptions
    async def kvs_uint_get() -> None:
        async with Client() as client:
            _handle_get_result(await asyncio.gather(
                *(asyncio.create_task(client.uint_get(key)) for key in keys)
            ))
    
    asyncio.run(kvs_uint_get())



@root.command
@click.argument("keys", type=str, nargs=-1)
def uint_del(keys: list[str]) -> None:
    """
    """
    @handle_server_exceptions
    async def kvs_uint_del() -> None:
        async with Client() as client:
            _handle_del_result(await asyncio.gather(
                *(asyncio.create_task(client.uint_del(key)) for key in keys)
            ))
    
    asyncio.run(kvs_uint_del())