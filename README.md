## Python client for KVS service.
Python client to interact with the key-value storage service. The client relies on `aiohttp` python library for making asynchronous requests to the service, and a `click` tool to provide the same functionality via the cli.

### Set up
Clone the git repository and open it in your favourite editor. Specify an URL to your KVS service inside `.env` file as the following `KVS_SERVICE_URL="http://url-to-your-service"`. Follow this instruction on how to [run the service inside a docker container](https://github.com/isnastish/kvs?tab=readme-ov-file#running-kvs-service-in-a-docker-container). Install all the packages specified in `Pipfile.lock` by running `pipenv sync` and spawn a shell within the virtual environment with `pipenv shell`. Note that [pipenv](https://pipenv.pypa.io/en/latest/) tool is required.

### Using KVSClient
Once everything is set up and the service is running, the following code snippet shows how to test the connection using the `KVSClient` and `echo` rpc.
```py
import asyncio

async def make_call_to_kvs_service(echo_input: list[str], /) -> None:
    """Invoke KVS's echo rpc"""
    async with Client() as client:
        results: list[StrResult] = await asyncio.gather(
            *(asyncio.create_task(client.echo(s)) for s in echo_input)
        )
        for r in results:
            print(r.result)

asyncio.run(make_call_to_kvs_service(["echo", "ello", "HELO", "olloO", "allo"]))
```

> Echo remote procedural call is convenient for testing the connection and doesn't modify the state of the storage. It replaces all the upper case latters with lower case and vise versa, returning back the modified string.

Here is another example for storing key-value pairs in a remote storage.
```py
import asyncio

async def store_dict(key: str, value: dict[str, str], /) -> None:
    """Store dict in a remote storage"""
    async with Client() as client:
        # Put dict into the remote storage
        res: BoolResult = await client.dict_put(key, value)
        if res.error:
            pass # error handling here
            return

        # Get dict from the remote storage
        res: DictResult = await client.dict_get(key)
        if res.error:
            pass # handle error
            return

        print(res.result)

        # Delete dict from the storage
        res: BoolResult = await client.dict_del(key)
        if res.error:
            pass # handle error

asyncio.run(store_dict("my_dict", {"key_1": "value_1", "key_2": "799879", "key_3": "0xffffaa"}))
```

### Using the cli
For interacting with the service from command line I use [click](https://click.palletsprojects.com/en/8.1.x/) python tool. It's the most convenient way for testing the service, as it doesn't require writing any code at all. Assuming the click has been installed, the followinag command would generate the same result as the `echo` rpc: `python -m src.kvs echo ello HELO olloO allo`. For bravity, all the exceptions coming from the client are caught to avoid having big stack traces from executing commands, instead error messages are displayed. Note that the behaviour is different when using `KVSClient` directly.

To store key-value pairs inside the remote storage use the following command `python -m src.kvs dict_put "my_dict" key_1=value_1 key_2=799879 key_3=0xffffaa`. It is equivalent to `client.dict_put(key, value)` mentioned in the code sample above.

## Result types
Each api function call returns a result of a particular type. For delete operations, the result type is `BoolResult`
with `result` field set to true if the specified key was deleted, false otherwise. For get methods, the type varies depending on the api function, for example `str_get` will return a result of type `StrResult`, containing the requested string if a request has succeeded.
All the result types are represented as python dataclasses and have the following shape:
```py
@dataclass
class StrResult(BaseResult):
    """Result for kvs commands returning a string."""
    result: str = field(default="")
```