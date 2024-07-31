import asyncio
import random
import string
import itertools
import base64
from contextlib import AsyncExitStack
from unittest import IsolatedAsyncioTestCase

from src.kvs.client import Client 
from src.kvs.result import (
    BoolResult, 
    IntResult, 
    StrResult, 
    FloatResult,
    DictResult
)

from .testing import (
    start_kvs_docker_container,
    kill_kvs_docker_container, 
)

# If URL is provide, all the tests are executed against the service pointed by url, 
# otherwise against the service run inside a mock docker container.
_KVS_SERVICE_URL = "http://localhost:8080"


class KVSClientTest(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        """Start up a docker container running kvs service"""
        if _KVS_SERVICE_URL:
            cls.service_url = _KVS_SERVICE_URL
        else:
            cls.service_process, cls.service_url = start_kvs_docker_container()

    @classmethod
    def tearDownClass(cls) -> None:
        """Tear down the docker container if it was started without any issues"""
        if not _KVS_SERVICE_URL:
            kill_kvs_docker_container(cls.service_process)

    async def asyncSetUp(self) -> None:
        self.exit_stack = AsyncExitStack()
        self.client = await self.exit_stack.enter_async_context(
            Client(self.service_url)
        )

    async def asyncTearDown(self) -> None:
        await self.exit_stack.aclose()

    async def test_remote_procedural_calls(self) -> None:
        """Test remote procedural calls (echo/hello/fibo)"""
        def echo(s: str) -> str:
            """Convert lowercase leter to uppercase and vice versa"""
            res = map(lambda c: c.lower() if c.isupper() else c.upper(), s)
            return "".join(res)
        
        echo_str: str = "ECHO ECHo ECho Echo echo echO ecHO eCHO ECHO"
        res: StrResult = await self.client.echo(echo_str)
        self.assertEqual(res.error, None)
        self.assertEqual(res.status, 200)
        self.assertEqual(res.result, echo(echo_str))
        
        res = await self.client.hello()
        self.assertEqual(res.error, None)
        self.assertEqual(res.status, 200)
        self.assertTrue(res.result)

        # fibo sequence of the first 10 digits
        expected = [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]
        res: tuple[IntResult] = await asyncio.gather(
            *(asyncio.create_task(self.client.fibo(i)) for i in range(10))
        )
        self.assertEqual(expected, [r.result for r in res])

    async def test_integer_storage(self) -> None:
        """Test remote integer storage"""
        key: str = "number"
        value: int = 999997
        
        res: BoolResult = await self.client.int_put(key, value)
        self.assertEqual(res.error, None)
        self.assertEqual(res.status, 200)

        res: IntResult = await self.client.int_get(key)
        self.assertEqual(res.error, None)
        self.assertEqual(res.status, 200)
        self.assertEqual(res.result, value)

        res: BoolResult = await self.client.int_del(key)
        self.assertEqual(res.error, None)
        self.assertTrue(res.result)
        
        # Make sure that the value doesn't exist
        res: BoolResult = await self.client.int_get(key)
        self.assertEqual(res.status, 404)


    async def test_store_integers_asynchronously(self) -> None:
        """Test make multiple requests to the storage simultaneously"""
        # generate key-value pairs, in total 5040 pairs will be generated, which is a factorial of 7!
        keys = ["".join(i) for i in itertools.permutations("abcdefg", 7)]
        values = [random.randrange(-10000, 10000, 2) for _ in range(0, len(keys))]
        pairs = list(zip(keys, values, strict=True))

        REQUEST_LIMIT = 20

        results: tuple[BoolResult] = await asyncio.gather(
            *(asyncio.create_task(self.client.int_put(p[0], p[1])) for p in pairs[:REQUEST_LIMIT])
        )
        
        results: tuple[IntResult] = await asyncio.gather(
            *(asyncio.create_task(self.client.int_get(p[0])) for p in pairs[:REQUEST_LIMIT]),
        )

        self.assertEqual([r.result for r in results], [p[1] for p in pairs[:REQUEST_LIMIT]])


    async def test_dict_storage(self) -> None:
        """Test remote dict storage"""
        key = "_dict_key_"
        value: dict[str, str] = {
            "_key_1": "./value1/file.bin",
            "_key_2": "value2",
            "_key_3": "-789977+str",
        }
        # put dict into the storage
        res: BoolResult = await self.client.dict_put(key, value)
        self.assertIsNone(res.error)
        # get dict from the storage
        res: DictResult = await self.client.dict_get(key)
        self.assertIsNone(res.error)
        self.assertEqual(res.result, value)
        # delete dict from the storage
        res: BoolResult = await self.client.dict_del(key)
        self.assertIsNone(res.error)
        self.assertTrue(res.result)
        # make sure that the key doesn't exist anymore
        res: BoolResult = await self.client.dict_get(key)
        self.assertEqual(res.status, 404)


    async def test_big_dict(self) -> None:
        """Test put big dict into the remote storage"""
        key = "permutations"
        value = dict(itertools.product(["".join(p) for p in itertools.permutations("abcdef", 6)], 
                          ["".join(p) for p in itertools.permutations("efghij", 6)]))

        res: BoolResult = await self.client.dict_put(key, value)
        self.assertIsNone(res.error)
        # get dict from the storage
        res: DictResult = await self.client.dict_get(key)
        self.assertIsNone(res.error)
        self.assertEqual(res.result, value)
        # clear the storage
        res: BoolResult = await self.client.dict_del(key)
        self.assertTrue(res.result)


    async def test_float_storage(self) -> None:
        """Test remote float storage"""
        key = "_float_key_"
        value = 3.14159

        res: BoolResult = await self.client.float_put(key, value)
        self.assertIsNone(res.error)

        getRes: FloatResult = await self.client.float_get(key)
        self.assertIsNone(res.error)
        self.assertEqual(res.result, value)

        res: BoolResult = await self.client.float_del(key)
        self.assertIsNone(res.error)
        self.assertTrue(res.result)
        # make sure that the key doesn't exist anymore
        res: BoolResult = await self.client.float_get(key)
        self.assertEqual(res.status, 404)


    async def test_str_storage(self) -> None:
        """Test remote str storage"""
        data = b"hello world!@"

        key = "my_secret"
        value = base64.b64encode(data).decode()

        res: BoolResult = await self.client.str_put(key, value)
        self.assertIsNone(res.error)
        
        getRes: StrResult = await self.client.str_get(key)
        self.assertIsNone(getRes.error)
        self.assertEqual(base64.b64decode(getRes.result.encode()), data)

        res = await self.client.str_del(key)
        self.assertTrue(res.result)