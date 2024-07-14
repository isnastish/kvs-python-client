import os
import click
import asyncio
import typing as t
from yarl import URL
from .client import KVSClient
from .result import (
    StrResult, 
    IntResult, 
    BoolResult, 
    FloatResult, 
    DictResult,
    BaseResult
)

# NOTE: Database transactions in nutshell
# https://stackoverflow.com/questions/974596/what-is-a-database-transaction
# pgx code samples: https://github.com/jackc/pgx-top-to-bottom

# TODO: Move this to KVS client?
_KVS_SERVICE_URL = os.getenv("KVS_SERVICE_URL", "http://localhost:8080")

# NOTE: If it seems like we have a lot of duplications, each kvs_... function could
# be replaced with this one which would make the understanding a bit harder. 
 
# _KVSCommandT: t.TypeAlias = t.Callable[[KVSClient, str, t.Optional[t.Any]], 
#                                        FloatResult|IntResult|BoolResult|StrResult|DictResult]

# async def _kvs_command(ctx: click.Context, member_func: _KVSCommandT, *args) -> None:
#     """
    
#     :param ctx: click context containing service url.
#     :param func: KVSClient member function, like int_add, int_del etc.
#     :param args: additional arguments forwarded to func.
#               it's a key and an optional value.
#     """
#     async with KVSClient(ctx.obj["service_url"]) as client:
#         res = await member_func(client, *args)
#         if res.error:
#             return
#         click.echo(res.result)
# Usage:
# asyncio.run(_kvs_command(ctx, KVSClient.incr, key))

def _click_echo_if_error(result: BaseResult, /) -> bool:
    """Print an error message to the stdout.

    :param result: 
    :return true if has error, false otherwise.
    """
    has_error: bool = (result.error != None)
    if has_error:
        click.echo(f"failed with status: {result.status}, \
                   error: {result.error.strip()}, url: {result.url}")
    return has_error

@click.group()
@click.option("--service-url", "-u", type=str, default=_KVS_SERVICE_URL, help="KVS service URL.")
@click.pass_context
def root(ctx: click.Context, service_url: str) -> None:
    """Cli root command.

    :param ctx:
    :param service_url:
    """
    ctx.ensure_object(dict)
    ctx.obj["service_url"] = service_url


@root.command()
@click.argument("echo_string", type=str, nargs=-1)
@click.pass_context
def echo(ctx: click.Context, echo_string: list[str]) -> None:
    """Construct kvs client and invoke `echo` remote procedural call.
    
    :param service_url: url to KVS service.
    :param echo_string: string(s) to be echoed back.
    """
    async def multi_echo_call(echo_strings: list[str], /) -> None:
        async with KVSClient(ctx.obj["service_url"]) as client:
            for echo_str in echo_strings:
                res: StrResult = await client.echo(echo_str)
                if _click_echo_if_error(res.base):
                    continue
                click.echo(res.result)

    asyncio.run(multi_echo_call(echo_string))


@root.command()
@click.pass_context
def hello(ctx: click.Context) -> None:
    """

    :param ctx: click context.
    """
    async def hello_call() -> None:
        async with KVSClient(ctx.obj["service_url"]) as client:
            res: StrResult = await client.hello()
            if _click_echo_if_error(res.base):
                return
            click.echo(res.result)

    asyncio.run(hello_call())


@root.command()
@click.argument("index", type=int, nargs=-1)
@click.pass_context
def fibo(ctx: click.Context, index: list[int]) -> None:
    """
    :param ctx: click context.
    :param index: fibonacci sequence index.
    """
    async def invoke_fibo_rpc(indices: list[int], /) -> None:
        async with KVSClient(ctx.obj["service_url"]) as client:
            for idx in indices:
                res: IntResult = await client.fibo(idx)
                if _click_echo_if_error(res.base):
                    continue
                click.echo(res.result)

    asyncio.run(invoke_fibo_rpc(index))


# TODO: Try passing multiple values with type specified as a dict
# NOTE: Click treats negative integers as options 
@root.command()
@click.argument("key", type=str)
@click.argument("value", type=int)
@click.pass_context
def int_add(ctx: click.Context, key: str, value: int) -> None:
    """Command for adding integers to the remote storage.
    For example `int-add "n" 12 "m" -34` will push two integers
    into the remote storage with the corresponding `n` and `m` keys.

    :param key: list of keys.
    :param value: list of corresponding values.
    """
    async def invoke_int_add(key: str, value: int, /) -> None:
        async with KVSClient(ctx.obj["service_url"]) as client:
            res: BoolResult = await client.int_add(key, value)
            if _click_echo_if_error(res.base):
                return
            click.echo(f"status: {res.base.status}")

    asyncio.run(invoke_int_add(key, value))


@root.command()
@click.argument("key", type=str, nargs=-1)
@click.pass_context
def int_get(ctx: click.Context, key: list[str]) -> None:
    """_summary_
    """
    async def invoke_int_get(keys: list[str], /) -> None:
        async with KVSClient(ctx.obj["service_url"]) as client:
            for key in keys:
                res: IntResult = await client.int_get(key)
                if _click_echo_if_error(res.base):
                    continue
                click.echo(f"{key}:{res.result}")

    asyncio.run(invoke_int_get(key))


@root.command()
@click.argument("key", type=str, nargs=-1)
@click.pass_context
def int_del(ctx: click.Context, key: list[str]) -> None:
    """Delete multiple keys from the remote storage.

    :param ctx: click context containing service url.
    :param key: list of keys to be deleted.
    """
    async def kvs_int_del(keys: list[str], /) -> None:
        async with KVSClient(ctx.obj["service_url"]) as client:
            for key in keys:
                res: IntResult = await client.int_del(key)
                if _click_echo_if_error(res.base):
                    continue
                click.echo(f"key: {key}, deleted: {res.result}")

    asyncio.run(kvs_int_del(key))


@root.command()
@click.argument("key", type=str) # Support multiple keys?
@click.pass_context
def incr(ctx: click.Context, key: str) -> None:
    """Increment the value reffered by the following key by one.
    
    :param ctx: click context containing service url.
    :param key: key of the value to be incremented.
    """
    async def kvs_incr(key: str, /) -> None:
        async with KVSClient(ctx.obj["service_url"]) as client:
            res: IntResult = await client.incr(key)
            if _click_echo_if_error(res.base):
                return
            click.echo(res.result)

    asyncio.run(kvs_incr(key))


@root.command()
@click.argument("key", type=str)
@click.argument("value", type=int)
@click.pass_context
def incr_by(ctx: click.Context, key: str, value: int) -> None:
    """Increment the value reffered by the following key by `value`.
    If `value` is negative, the final value will be decremented.
    
    :param ctx: click context containing service url.
    :param key: 
    :param value:
    """
    async def kvs_incr_by(key: str, value: int, /) -> None:
        async with KVSClient(ctx.obj["service_url"]) as client:
            res: IntResult = await client.incr_by(key, value)
            if _click_echo_if_error(res.base):
                return
            click.echo(res.result)
    
    asyncio.run(kvs_incr_by(key, value))


@root.command()
@click.argument("key", type=str)
@click.argument("value", type=float)
@click.pass_context
def float_add(ctx: click.Context, key: str, value: float) -> None:
    """_summary_

    :param ctx:
    :param key:
    :param value:
    """
    async def kvs_float_add(key: str, value: int, /) -> None:
        async with KVSClient(ctx.obj["service_url"]) as client:
            res: FloatResult = await client.float_add(key, value)
            if _click_echo_if_error(res.base):
                return
            click.echo(res.base.status) # Use result instead?

    asyncio.run(kvs_float_add(key, value))


@root.command()
@click.argument("key", type=str, nargs=-1)
@click.pass_context
def float_get(ctx: click.Context, key: list[str]) -> None:
    """_summary_

    :param ctx:
    :param key:
    :param value:
    """
    async def kvs_float_get(keys: list[str], /) -> None:
        async with KVSClient(ctx.obj["service_url"]) as client:
            for key in keys: 
                res: FloatResult = await client.float_get(key)
                if _click_echo_if_error(res.base):
                    return
                click.echo(res.result)

    asyncio.run(kvs_float_get(key))


@root.command()
@click.argument("key", type=str, nargs=-1)
@click.pass_context
def float_del(ctx: click.Context, key: list[str]) -> None:
    """Delete value from the storage reffered by the following key.

    :param ctx: click context holding service url.
    :param key: list of keys to be deleted.
    """
    async def kvs_float_del(keys: list[str], /) -> None:
        async with KVSClient(ctx.obj["service_url"]) as client:
            for key in keys:
                res: BoolResult = await client.float_del(key)
                if _click_echo_if_error(res.base):
                    continue
                click.echo(f"key: {key}, deleted: {res.result}")

    asyncio.run(kvs_float_del(key))

# @root.command()
# @click.argument("key", type=str)
# @click.argument("value" )
# @click.pass_context
# def dict_add(ctx: click.Context, key: str, value: dict[str, str]) -> None:
#     """_summary_

#     :param ctx:
#     :param key:
#     :param value:
#     """
#     async def kvs_dict_add(key: str, value: dict[str, str], /) -> None:
#         async with KVSClient(ctx.obj["service_url"]) as client:
#             res: DictResult = await client.dict_add(key, value)
#             if _click_echo_if_error(res.base):
#                 return
#             click.echo(res.result)

#     asyncio.run(kvs_dict_add(key, value))
