import click
import asyncio
from yarl import URL
from .client import KVSClient
from .result import (
    StrResult, 
    IntResult, 
    BoolResult, 
    FloatResult, 
    DictResult,
)


def _click_echo_on_failure(status: int, error: str, url: URL, /) -> None:
    """_summary_
    """
    click.echo(f"failed with status: {status}, error: {error}, url: {url}")


@click.group()
@click.option("--service-url", "-u", type=str, required=True, help="KVS service URL.")
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
                if res.error:
                    _click_echo_on_failure(res.status_code, res.error, res.url)
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
            if res.error:
                _click_echo_on_failure(res.status_code, res.error, res.url)
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
                if res.error:
                    _click_echo_on_failure(res.status_code, res.error, res.url)
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
            if res.error:
                _click_echo_on_failure(res.status_code, res.error, res.url)
                return
            click.echo(f"status: {res.status_code}")

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
                if res.error:
                    _click_echo_on_failure(res.status_code, res.error, res.url)
                    continue
                click.echo(f"{key}:{res.result}")

    asyncio.run(invoke_int_get(key))
    

@root.command()
@click.argument("key", type=str, nargs=-1)
@click.pass_context
def int_del() -> None:
    """_summary_
    """