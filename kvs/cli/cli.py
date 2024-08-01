import logging
import asyncio
import typing as t
from functools import wraps

import click
from aiohttp import ServerDisconnectedError

from kvs.client import Client
from kvs.results import (
    StrResult, 
    IntResult, 
    BoolResult, 
    FloatResult, 
    DictResult,
)


_ResultT: t.TypeAlias = t.Union[FloatResult|IntResult|BoolResult|StrResult|DictResult]


def _handle_results(results: list[_ResultT] | _ResultT, /) -> None:
    """Helper function for displaying response results.

    :param results: response returned by the remote storage.
    """
    def echo_result(r: _ResultT, /) -> None:
        if r.error:
            click.echo(f"failed with status: {r.status}, \
                error: {r.error.strip()}, url: {r.url}")
            return
        click.echo(r.result)

    if isinstance(results, list):
        for r in results:
            echo_result(r)
        return

    echo_result(results) # If a single result is given


def _with_handled_server_exceptions(func: t.Callable[[t.Any, t.Any], t.Awaitable[None]], /) -> None:
    """Decorator used to handle aiohttp.ServerDisconnectedError and 
    asyncio.TimeoutError. 

    When interacting with the remote storage via the cli, we should catch the
    exceptions coming from the client for better user experience,
    rather than displaying the stack trace.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs) -> None:
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
@click.argument("string", type=str, nargs=-1)
def echo(string: list[str]) -> None:
    """Invoke echo remote procedural call.
    
    :param echo_string: strings to be passed to echo rpc.
    """
    @_with_handled_server_exceptions
    async def kvs_echo(strings: list[str], /) -> None:
        async with Client() as client:
            _handle_results(await asyncio.gather(
                *(asyncio.create_task(client.echo(s)) for s in strings)
            ))
    asyncio.run(kvs_echo(string))


@root.command()
def hello() -> None:
    """Invoke hello remote procedural call. 
    Mainly used to test the connection and doesn't modify storage state"""
    @_with_handled_server_exceptions
    async def kvs_hello() -> None:
        async with Client() as client:
            _handle_results(await client.hello())

    asyncio.run(kvs_hello())


@root.command()
@click.argument("index", type=int, nargs=-1)
def fibo(index: list[int]) -> None:
    """Invoke fibo remote procedural call. 

    :param index: list of fibonacci sequence indices to be computed.
    """
    # TODO: Use asyncio.wait_for() instead?
    async def cancellable_fibo(client: Client, n: int, /) -> None:
        task = asyncio.create_task(client.fibo(n))
        time_elapsed = 0
        while not task.done():
            time_elapsed += 0.25
            await asyncio.sleep(0.25)
            if time_elapsed >= 10: # seconds elapsed
                task.cancel()
        try:
            _handle_results(await task)
        except asyncio.CancelledError:
            click.echo(f"Task fibo({n}) was canceled, elapsed time {time_elapsed}")

    @_with_handled_server_exceptions
    async def kvs_fibo(indices: list[int], /) -> None:
        async with Client() as client:
            await asyncio.gather(
                *(asyncio.create_task(cancellable_fibo(client, n)) for n in indices)
            )

    asyncio.run(kvs_fibo(index))


@root.command()
@click.argument("key", type=str)
@click.argument("value", type=int)
def int_put(key: str, value: int) -> None:
    """Put integer to the remote storage with the following key.

    :param key: key to be inserted into the storage.
    :param value: a corresponding value.
    """
    @_with_handled_server_exceptions
    async def kvs_int_add(key: str, value: int, /) -> None:
        async with Client() as client:
            _handle_results(await client.int_put_d({key: value}))
    asyncio.run(kvs_int_add(key, value))


@root.command()
@click.argument("key", type=str, nargs=-1)
def int_get(key: list[str]) -> None:
    """Get integer from the remote storage with the following key.
    
    :param key: list of keys to be retrieved.  
    """
    @_with_handled_server_exceptions
    async def kvs_int_get(keys: list[str], /) -> None:
        async with Client() as client:
            _handle_results(await asyncio.gather(
                *(asyncio.create_task(client.int_get(k)) for k in keys)
            ))
    asyncio.run(kvs_int_get(key))


@root.command()
@click.argument("key", type=str, nargs=-1)
def int_del(key: list[str]) -> None:
    """Delete integers from the remote storage with the following keys.

    :param key: list of keys to be deleted.
    """
    @_with_handled_server_exceptions
    async def kvs_int_del(keys: list[str], /) -> None:
        async with Client() as client:
            try: 
                _handle_results(await asyncio.gather(
                    *(asyncio.create_task(client.int_del(k)) for k in keys)
                ))
            except ServerDisconnectedError:
                click.echo(f"Command '{int_del.name} failed, server disconnected")

    asyncio.run(kvs_int_del(key))


@root.command()
@click.argument("key", type=str, nargs=-1)
def int_incr(key: list[str]) -> None:
    """Increment the value reffered by the following key by one.
    
    :param key: key of the value to be incremented.
    """
    @_with_handled_server_exceptions
    async def kvs_incr(keys: list[str], /) -> None:
        async with Client() as client:
            _handle_results(await asyncio.gather(
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
    @_with_handled_server_exceptions
    async def kvs_incr_by(key: str, value: int, /) -> None:
        async with Client() as client:
            _handle_results(await client.incr_by(key, value))
    asyncio.run(kvs_incr_by(key, value))


@root.command()
@click.argument("key", type=str)
@click.argument("value", type=float)
def float_put(key: str, value: float) -> None:
    """Put float into the remote storage with the following key.

    :param key: 
    :param value:
    """
    @_with_handled_server_exceptions
    async def kvs_float_put(key: str, value: int, /) -> None:
        async with Client() as client:
            _handle_results(await client.float_put(key, value))
    asyncio.run(kvs_float_put(key, value))


@root.command()
@click.argument("key", type=str, nargs=-1)
def float_get(key: list[str]) -> None:
    """_summary_

    :param ctx:
    :param key:
    :param value:
    """
    @_with_handled_server_exceptions
    async def kvs_float_get(keys: list[str], /) -> None:
        async with Client() as client:
            _handle_results(await asyncio.gather(
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
    @_with_handled_server_exceptions
    async def kvs_float_del(keys: list[str], /) -> None:
        async with Client() as client:
            _handle_results(await asyncio.gather(
                *(asyncio.create_task(client.float_del(k)) for k in keys)
            ))
    asyncio.run(kvs_float_del(key))


@root.command
@click.argument("key", type=str)
@click.argument("value", type=str)
def str_put(key: str, value: str) -> None:
    """_summary_
    :param ctx:
    """
    @_with_handled_server_exceptions
    async def kvs_str_put(key: str, value: str, /) -> None:
        async with Client() as client:
            _handle_results(await client.str_put(key, value))
    asyncio.run(kvs_str_put(key, value))


@root.command
@click.argument("key", type=str, nargs=-1)
def str_get(key: list[str]) -> None:
    """_summary_

    :param ctx:
    """
    @_with_handled_server_exceptions
    async def kvs_str_get(keys: list[str], /) -> None:
        async with Client() as client:
            _handle_results(await asyncio.gather(
                *(asyncio.create_task(client.str_get(k)) for k in keys)
            ))
    asyncio.run(kvs_str_get(key))


@root.command
@click.argument("key", type=str)
@click.argument("pairs", type=str, nargs=-1)
def dict_put(key: str, pairs: list[str]) -> None:
    """Put dictionary into the dict remote storage.

    :param key: key to be inserted.
    :param pairs: colon-separated list of key-value strings.
            Example: "participant_name":"Jacob" 
    """
    @_with_handled_server_exceptions
    async def kvs_dict_put(key: str, value: dict[str, str], /) -> None:
        async with Client() as client:
            _handle_results(await client.dict_put(key, value))
    
    # extract key-value pairs and make a dict out of them.
    value = dict(map(lambda p: tuple(p.split("=", maxsplit=1)), pairs))

    asyncio.run(kvs_dict_put(key, value))


@root.command
@click.argument("key", type=str, nargs=-1)
def dict_get(key: list[str]) -> None:
    """Get dictionary form the dict remote storage.

    :param key: key(s) holding a dictionary in a remote storage.
    """
    @_with_handled_server_exceptions
    async def kvs_dict_get(keys: list[str], /) -> None:
        async with Client() as client:
            _handle_results(await asyncio.gather(
                *(asyncio.create_task(client.dict_get(k)) for k in keys))
            )
    asyncio.run(kvs_dict_get(key))


@root.command
@click.argument("key", type=str, nargs=-1)
def dict_del(key: list[str]) -> None:
    """Delete dictionary form the dict remote storage.

    :param key: key(s) to be deleted.
    """
    @_with_handled_server_exceptions
    async def kvs_dict_del(keys: list[str], /) -> None:
        async with Client() as client:
            _handle_results(await asyncio.gather(
                *(asyncio.create_task(client.dict_del(k)) for k in keys)
            ))
    asyncio.run(kvs_dict_del(key))
