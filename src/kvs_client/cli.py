import click
import asyncio

from .client import (
    kvs_int_add, 
    kvs_int_get,
    kvs_int_del,
    KVS_SERVICE_DEFAULT_URL,
    KVS_DEFAULT_PORT,
) 

@click.group()
def root() -> None:
    pass

@click.command()
@click.option("--url", "-u", default=KVS_SERVICE_DEFAULT_URL, type=str, help="KVS service URL.")
@click.option("--port", "-p", default=KVS_DEFAULT_PORT, type=int, help="Port the kvs service is running on.")
@click.argument("key", type=str)
@click.argument("value", type=int)
def int_add(
    url: str,
    port: int,
    key: str,
    value: int,
) -> None:
    """Command for adding integers to the remote storage.
    For example `int-add "n" 12 "m" -34` will push two integers 
    into the remote storage with the corresponding `n` and `m` keys.

    :param key: list of keys.
    :param value: list of corresponding values.
    """
    asyncio.run(kvs_int_add(url, port, key, value))

@click.command()
# @click.
def int_get():
    """
    """


@click.command()
def int_del():
    click.echo("del integer")


root.add_command(int_add)
root.add_command(int_get)
root.add_command(int_del)