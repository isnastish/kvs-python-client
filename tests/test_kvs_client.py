from http import HTTPStatus
from contextlib import AsyncExitStack
from unittest import IsolatedAsyncioTestCase

from src.kvs_client.client import (
    KVSClient, 
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
    
    # async def test_store_integers(self) -> None:
    #     """Test store integers in a remote storage"""
    #     key: str = "number"
    #     value: int = 999997
        
    #     res: BoolResult = await self.kvs_client.int_add(key)
    #     self.assertEqual(res.error, None)
    #     self.assertEqual(res.status_code, HTTPStatus.OK)
        
    #     res: IntResult = await self.kvs_client.int_get(key)
    #     self.assertEqual(res.error, None)
    #     self.assertEqual(res.status_code, HTTPStatus.OK)
    #     self.assertEqual(res.result, value)

    #     res: BoolResult = await self.kvs_client.int_del(key)
    #     self.assertEqual(res.error, None)
    #     self.assertTrue(res.result) # True if the value was deleted, False otherwise
        
    #     # Make sure that the value doesn't exist
    #     res: BoolResult = await self.kvs_client.int_get(key)
    #     self.assertNotEqual(res.error, None)
    
    # async def test_store_strings(self) -> None:
    #     """Test store strings in a remote storage"""