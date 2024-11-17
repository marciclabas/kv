from typing import TypeVar, Generic, Callable, ParamSpec, Awaitable, overload
from dataclasses import dataclass
from contextlib import asynccontextmanager
from datetime import datetime
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob.aio import BlobServiceClient
from kv import KVError, InexistentItem, LocatableKV
from kv.serialization import Parse, Dump, default, serializers
from .util import blob_url

T = TypeVar('T')
U = TypeVar('U')
L = TypeVar('L')
Ps = ParamSpec('Ps')

def azure_safe(coro: Callable[Ps, Awaitable[T]]):
  async def wrapper(*args: Ps.args, **kwargs: Ps.kwargs) -> T:
    try:
      return await coro(*args, **kwargs)
    except ResourceNotFoundError as e:
      raise InexistentItem from e
    except Exception as e:
      raise KVError from e
  return wrapper # type: ignore

@dataclass
class BlobContainerKV(LocatableKV[T], Generic[T]):
  """Key-Value store using a single Azure Blob Container. Keys must be valid blob names"""

  client: Callable[[], BlobServiceClient]
  container: str
  parse: Parse[T] = default[T].parse
  dump: Dump[T] = default[T].dump

  def __repr__(self):
    return f'BlobContainerKV(account={self.client().account_name}, container={self.container})'

  @staticmethod
  def new(client: Callable[[], BlobServiceClient], type: type[U] | None = None, *, container: str) -> 'BlobContainerKV[U]':
    return (
      BlobContainerKV(client, container, **serializers(type))
      if type and type is not bytes else BlobContainerKV(client, container)
    )

  @staticmethod
  def from_conn_str(conn_str: str, container: str, type: type[U] | None = None) -> 'BlobContainerKV[U]':
    client = lambda: BlobServiceClient.from_connection_string(conn_str)
    return BlobContainerKV.new(client, type, container=container)

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

  @azure_safe
  async def has(self, key: str):
    async with self.container_manager() as client:
      if not await client.exists():
        return False
      return await client.get_blob_client(key).exists()

  @azure_safe
  async def delete(self, key: str):
    async with self.container_manager() as client:
      await client.delete_blob(key)
  
  async def keys(self):
    try:
      async with self.container_manager() as client:
        if not await client.exists():
          return
        async for name in client.list_blob_names():
          yield name
    except Exception as e:
      raise KVError(e) from e

  def url(self, key: str, *, expiry: datetime | None = None) -> str:
    bc = self.client().get_blob_client(self.container, key)
    return blob_url(bc, expiry=expiry)
  
  @azure_safe
  async def clear(self):
    async with self.container_manager() as client:
      await client.delete_container()