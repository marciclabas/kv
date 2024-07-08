from typing import TypeVar, Generic, Callable
from dataclasses import dataclass
from datetime import datetime
from haskellian import asyn_iter as AI, promise as P, either as E
from kv import LocatableKV
from kv.serialization import Parse, Dump, default, serializers
from kv.impl.azure import BlobContainerKV
from azure.storage.blob.aio import BlobServiceClient

T = TypeVar('T')

def default_split(key: str) -> tuple[str, str]:
  parts = key.split('/', 1)
  return (parts[0], parts[1]) if len(parts) > 1 else ('default-container', parts[0])

def default_merge(container: str, blob: str) -> str:
  return f'{container}/{blob}' if container != 'default-container' else blob

@dataclass
class BlobKV(LocatableKV[T], Generic[T]):
  client: Callable[[], BlobServiceClient]
  split_key: Callable[[str], tuple[str, str]] = default_split
  """Split a key into container + blob. Defaults to `{container}/{blob/with/slashes}`"""
  merge_key: Callable[[str, str], str] = default_merge
  """Merge a container and blob into a key. Defaults to `{container}/{blob/with/slashes}`"""
  parse: Parse[T] = default[T].parse
  dump: Dump[T] = default[T].dump

  @staticmethod
  def validated(Type: type[T], client: Callable[[], BlobServiceClient], *, split_key: Callable[[str], tuple[str, str]] = default_split) -> 'BlobKV[T]':
    return BlobKV(client, split_key, **serializers(Type))
  
  @staticmethod
  def from_conn_str(conn_str: str, Type: type[T] | None = None, *, split_key: Callable[[str], tuple[str, str]] = default_split) -> 'BlobKV[T]':
    client = lambda: BlobServiceClient.from_connection_string(conn_str)
    if Type:
      return BlobKV.validated(Type, client, split_key=split_key)
    else:
      return BlobKV(client, split_key)

  def prefix(self, container: str) -> BlobContainerKV:
    return BlobContainerKV(
      client=self.client, container=container,
      parse=self.parse, dump=self.dump
    )

  def delete(self, key: str):
    container, blob = self.split_key(key)
    return self.prefix(container).delete(blob)
  
  def insert(self, key: str, value: T):
    container, blob = self.split_key(key)
    return self.prefix(container).insert(blob, value)
  
  def read(self, key: str):
    container, blob = self.split_key(key)
    return self.prefix(container).read(blob)
  
  async def containers(self):
    async with self.client() as client:
      async for c in client.list_containers():
        yield c.name or ''
  
  async def container_keys(self, container: str):
    async for e in self.prefix(container).keys():
      yield e.fmap(lambda key: self.merge_key(container, key))

  @AI.lift
  async def keys(self):
    async for container in self.containers():
      async for key in self.container_keys(container):
        yield key
  
  @AI.lift
  async def items(self):
    async for container in self.containers():
      async for item in self.prefix(container).items():
        yield item

  @P.lift
  @E.do()
  async def clear(self):
    async for container in self.containers():
      (await self.prefix(container).clear()).unsafe()


  def url(self, key: str, *, expiry: datetime | None = None) -> str:
    container, blob = self.split_key(key)
    return self.prefix(container).url(blob, expiry=expiry)