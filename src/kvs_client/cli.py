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

# TODO: Move this to KVS client?
_KVS_SERVICE_URL = os.getenv("KVS_SERVICE_URL", "http://localhost:8080")

_ResultT: t.TypeAlias = t.Union[FloatResult|IntResult|BoolResult|StrResult|DictResult]
_KVSCommandT: t.TypeAlias = t.Callable[[KVSClient, str, t.Optional[t.Any]], _ResultT]


# NOTE: If it's possible, try to avoid using this function, it will make the code less verbose.
async def _kvs_command(ctx: click.Context, query_func: _KVSCommandT, v: dict[str, t.Any]) -> None:
    """
    
    :param ctx: click context containing service url.
    :param func: KVSClient member function, like int_add, int_del etc.
    :param args: additional arguments forwarded to func.
              it's a key and an optional value.
    """
    async with KVSClient(ctx.obj["service_url"]) as client:
        for key in v.keys():
            res = await query_func(client, v[key])
            if res.error:
                click.echo(f"failed with status: {res.status}, \
                   error: {res.error.strip()}, url: {res.url}")
                continue
            click.echo(res.result)


def _handle_results(results: list[_ResultT], /) -> None:
    """Print an error message to the stdout.

    :param results: 
    """
    for r in results:
        if r.error:
            click.echo(f"failed with status: {r.status}, \
                error: {r.error.strip()}, url: {r.url}")
            continue
        # if r.params:
        #     click.echo(f"params: {r.params}, result: {r.result}")
        click.echo(r.result)


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
    async def kvs_echo(echo_strings: list[str], /) -> None:
        async with KVSClient(ctx.obj["service_url"]) as client:
            results: tuple[StrResult] = asyncio.gather(
                *(asyncio.create_task(client.echo(s)) for s in echo_strings)
            )
            _handle_results(results)

    asyncio.run(kvs_echo(echo_string))



@root.command()
@click.pass_context
def hello(ctx: click.Context) -> None:
    """

    :param ctx: click context.
    """
    async def kvs_hello() -> None:
        async with KVSClient(ctx.obj["service_url"]) as client:
            _handle_results([await client.hello()])

    asyncio.run(kvs_hello())


@root.command()
@click.argument("index", type=int, nargs=-1)
@click.pass_context
def fibo(ctx: click.Context, index: list[int]) -> None:
    """
    :param ctx: click context.
    :param index: fibonacci sequence index.
    """
    async def kvs_fibo(indices: list[int], /) -> None:
        async with KVSClient(ctx.obj["service_url"]) as client:
            results: tuple[IntResult] = asyncio.gather(
                *(asyncio.create_task(client.fibo(index)) for index in indices)
            )
            _handle_results(results)

    asyncio.run(kvs_fibo(index))


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
    async def kvs_int_add(key: str, value: int, /) -> None:
        async with KVSClient(ctx.obj["service_url"]) as client:
            _handle_results([await client.int_add(key, value)])

    asyncio.run(kvs_int_add(key, value))


@root.command()
@click.argument("key", type=str, nargs=-1)
@click.pass_context
def int_get(ctx: click.Context, key: list[str]) -> None:
    """

    :param ctx:
    :param key:
    """
    async def kvs_int_get(keys: list[str], /) -> None:
        async with KVSClient(ctx.obj["service_url"]) as client:
            results: tuple[IntResult] = asyncio.gather(
                *(asyncio.create_task(client.int_get(k)) for k in keys)
            )
            _handle_results(results)

    asyncio.run(kvs_int_get(key))


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
            results: tuple[BoolResult] = asyncio.gather(
                *(asyncio.create_task(client.int_del(k)) for k in keys)
            ) 
            _handle_results(results)

    asyncio.run(kvs_int_del(key))


@root.command()
@click.argument("key", type=str, nargs=-1)
@click.pass_context
def incr(ctx: click.Context, key: list[str]) -> None:
    """Increment the value reffered by the following key by one.
    
    :param ctx: click context containing service url.
    :param key: key of the value to be incremented.
    """
    async def kvs_incr(keys: list[str], /) -> None:
        async with KVSClient(ctx.obj["service_url"]) as client:
            results: tuple[IntResult] = asyncio.gather(
                *(asyncio.create_task(client.incr(k)) for k in keys)
            )
            _handle_results(results)

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
            _handle_results([await client.incr_by(key, value)])

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
            _handle_results(await client.float_add(key, value))

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
            results: tuple[FloatResult] = asyncio.gather(
                *(asyncio.create_task(client.float_get(k)) for k in keys)
            )
            _handle_results(results)

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
            results: tuple[BoolResult] = asyncio.gather(
                *(asyncio.create_task(client.float_get(k)) for k in keys)
            )
            _handle_results(results)

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

# NOTE: Adding multiple strings at a time should be established with str_set_add command?
# But the go server doesn't support it currently 
@root.command
@click.argument("key", type=str)
@click.pass_context
def str_add(ctx: click.Context, key: str) -> None:
    """_summary_

    :param ctx:
    """
    async def kvs_str_add(key: str, /) -> None:
        async with KVSClient(ctx.obj["service_url"]) as client:
            _handle_results(await client.str_add(key))

    asyncio.run(kvs_str_add(key))