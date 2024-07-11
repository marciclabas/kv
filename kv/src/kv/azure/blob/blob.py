from typing import TypeVar, Generic, Callable
from dataclasses import dataclass
from datetime import datetime
from haskellian import asyn_iter as AI, promise as P, either as E
from kv import LocatableKV
from kv.serialization import Parse, Dump, default, serializers
from kv.azure import BlobContainerKV
from azure.storage.blob.aio import BlobServiceClient

T = TypeVar('T')
U = TypeVar('U')

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


  def __repr__(self):
    return f'BlobKV(account={self.client().account_name})'

  @staticmethod
  def new(client: Callable[[], BlobServiceClient], type: type[U] | None, *, split_key: Callable[[str], tuple[str, str]] = default_split) -> 'BlobKV[U]':
    return (
      BlobKV(client, split_key, **serializers(type))
      if type else BlobKV(client, split_key)
    )
  
  @staticmethod
  def from_conn_str(conn_str: str, type: type[T] | None = None, *, split_key: Callable[[str], tuple[str, str]] = default_split) -> 'BlobKV[T]':
    client = lambda: BlobServiceClient.from_connection_string(conn_str)
    return BlobKV.new(client, type, split_key=split_key)

  def prefixed(self, prefix: str): # type: ignore
    return BlobContainerKV(
      client=self.client, container=prefix,
      parse=self.parse, dump=self.dump
    )

  def delete(self, key: str):
    container, blob = self.split_key(key)
    return self.prefixed(container).delete(blob)
  
  def insert(self, key: str, value: T):
    container, blob = self.split_key(key)
    return self.prefixed(container).insert(blob, value)
  
  def read(self, key: str):
    container, blob = self.split_key(key)
    return self.prefixed(container).read(blob)
  
  async def containers(self):
    async with self.client() as client:
      async for c in client.list_containers():
        yield c.name or ''
  
  async def container_keys(self, container: str):
    async for e in self.prefixed(container).keys():
      yield e.fmap(lambda key: self.merge_key(container, key))

  @AI.lift
  async def keys(self):
    async for container in self.containers():
      async for key in self.container_keys(container):
        yield key
  
  @AI.lift
  async def items(self):
    async for container in self.containers():
      async for item in self.prefixed(container).items():
        yield item

  @P.lift
  @E.do()
  async def clear(self):
    async for container in self.containers():
      (await self.prefixed(container).clear()).unsafe()


  def url(self, key: str, *, expiry: datetime | None = None) -> str:
    container, blob = self.split_key(key)
    return self.prefixed(container).url(blob, expiry=expiry)