import asyncio

from contextlib import AsyncExitStack
from unittest import IsolatedAsyncioTestCase

from src.kvs_client.client import KVSClient 
from src.kvs_client.result import (
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

class KVSClientTest(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        """Start up a docker container running kvs service"""
        cls.service_process, cls.service_url = start_kvs_docker_container()

    @classmethod
    def tearDownClass(cls) -> None:
        """Tear down the docker container if it was started without any issues"""
        kill_kvs_docker_container(cls.service_process)

    async def asyncSetUp(self) -> None:
        self.exit_stack = AsyncExitStack()
        self.kvs_client = await self.exit_stack.enter_async_context(
            KVSClient(self.service_url)
        )

    async def asyncTearDown(self) -> None:
        await self.exit_stack.aclose()

    async def test_remote_procedural_calls(self) -> None:
        """Test remote procedural calls (echo/hello/fibo)"""
        def echo(s: str) -> str:
            """Convert lowercase leter to uppercase and vice-versa"""
            res = map(lambda c: c.lower() if c.isupper() else c.upper(), s)
            return "".join(res)
        
        echo_str: str = "ECHO ECHo ECho Echo echo echO ecHO eCHO ECHO"
        res: StrResult = await self.kvs_client.echo(echo_str)
        self.assertEqual(res.base.error, None)
        self.assertEqual(res.base.status, 200)
        self.assertEqual(res.result, echo(echo_str))
        
        res = await self.kvs_client.hello()
        self.assertEqual(res.base.error, None)
        self.assertEqual(res.base.status, 200)
        self.assertTrue(res.result)

        # fibo sequence of the first 10 digits
        expected = [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]
        res: tuple[IntResult] = await asyncio.gather(
            *(asyncio.create_task(self.kvs_client.fibo(i)) for i in range(10))
        )
        self.assertEqual(expected, [r.result for r in res])


    async def test_store_integers(self) -> None:
        """Test store integers in a remote storage"""
        key: str = "number"
        value: int = 999997
        
        res: BoolResult = await self.kvs_client.int_add(key, value)
        self.assertEqual(res.base.error, None)
        self.assertEqual(res.base.status, 200)
        
        res: IntResult = await self.kvs_client.int_get(key)
        self.assertEqual(res.base.error, None)
        self.assertEqual(res.base.status, 200)
        self.assertEqual(res.result, value)

        res: BoolResult = await self.kvs_client.int_del(key)
        self.assertEqual(res.base.error, None)
        self.assertTrue(res.result) # True if the value was deleted, False otherwise
        
        # Make sure that the value doesn't exist
        res: BoolResult = await self.kvs_client.int_get(key)
        self.assertNotEqual(res.base.error, None)
    
    async def test_store_strings(self) -> None:
        """Test store strings in a remote storage"""