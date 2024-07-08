from typing import TypeVar, Generic, Callable, ParamSpec, Awaitable, overload
from dataclasses import dataclass
from contextlib import asynccontextmanager
from datetime import datetime
from haskellian import Either, Left, Right, promise as P, asyn_iter as AI
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob.aio import BlobServiceClient
from kv import DBError, InexistentItem, LocatableKV
from kv.serialization import Parse, Dump, default, serializers
from .util import blob_url

T = TypeVar('T')
L = TypeVar('L')
Ps = ParamSpec('Ps')

def azure_safe(coro: Callable[Ps, Awaitable[Either[L, T]]]):
  @P.lift
  async def wrapper(*args: Ps.args, **kwargs: Ps.kwargs) -> Either[L|DBError, T]:
    try:
      return await coro(*args, **kwargs)
    except ResourceNotFoundError as e:
      return Left(InexistentItem(detail=e)) # type: ignore
    except Exception as e:
      return Left(DBError(e))
  return wrapper # type: ignore

@dataclass
class BlobContainerKV(LocatableKV[T], Generic[T]):
  """Key-Value store using a single Azure Blob Container. Keys must be valid blob names"""

  client: Callable[[], BlobServiceClient]
  container: str
  parse: Parse[T] = default[T].parse
  dump: Dump[T] = default[T].dump

  @staticmethod
  def validated(Type: type[T], client: Callable[[], BlobServiceClient], *, container: str) -> 'BlobContainerKV[T]':
    return BlobContainerKV(client, container, **serializers(Type))

  @staticmethod
  def from_conn_str(conn_str: str, container: str, Type: type[T] | None = None) -> 'BlobContainerKV[T]':
    client = lambda: BlobServiceClient.from_connection_string(conn_str)
    if Type:
      return BlobContainerKV.validated(Type, client, container=container)
    else:
      return BlobContainerKV(client, container)

  @asynccontextmanager
  async def container_manager(self):
    async with self.client() as client:
      yield client.get_container_client(self.container)

  @azure_safe
  async def read(self, key: str):
    async with self.container_manager() as client:
      r = await client.download_blob(key)
      data = await r.readall()
      return self.parse(data)

  @azure_safe
  async def insert(self, key: str, value: T):
    async with self.container_manager() as client:
      if not await client.exists():
        await client.create_container()
      data = self.dump(value)
      await client.upload_blob(key, data, overwrite=True)
      return Right(None)

  @azure_safe
  async def has(self, key: str):
    async with self.container_manager() as client:
      if not await client.exists():
        return Right(False)
      return Right(await client.get_blob_client(key).exists())

  @azure_safe
  async def delete(self, key: str):
    async with self.container_manager() as client:
      await client.delete_blob(key)
      return Right(None)
  
  @AI.lift
  async def keys(self):
    try:
      async with self.container_manager() as client:
        if not await client.exists():
          return
        async for name in client.list_blob_names():
          yield Right(name)
    except Exception as e:
      yield Left(DBError(e))

  def url(self, key: str, *, expiry: datetime | None = None) -> str:
    bc = self.client().get_blob_client(self.container, key)
    return blob_url(bc, expiry=expiry)
  
  @azure_safe
  async def clear(self):
    async with self.container_manager() as client:
      await client.delete_container()
      return Right(None)