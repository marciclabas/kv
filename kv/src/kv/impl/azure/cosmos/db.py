from typing import TypeVar, Generic, Callable, Any
from dataclasses import dataclass
from azure.cosmos.aio import CosmosClient
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from kv import KV, KVError
from .util import DatabaseMixin, default_split, default_merge, serializers, azure_safe
from .container import CosmosContainerKV

T = TypeVar('T')
U = TypeVar('U')


@dataclass
class CosmosKV(KV[T], DatabaseMixin[T], Generic[T]):
  split_key: Callable[[str], tuple[str, str]] = default_split
  """Split a key into container + partition. Defaults to `{container}/{id/with/slashes}`"""
  merge_key: Callable[[str, str], str] = default_merge
  """Merge a container and partition into a key. Defaults to `{container}/{id/with/slashes}`"""

  def __repr__(self):
    endpoint = self.client().client_connection.url_connection
    return f'CosmosKV(endpoint={endpoint}, database={self.db})'
  
  @staticmethod
  def new(
    client: Callable[[], CosmosClient], type: type[U] | None = None,
    *, db: str, split_key=default_split, merge_key=default_merge
  ):
    return CosmosKV(
      client, db=db, split_key=split_key, merge_key=merge_key,
      **serializers(type or Any)
    )
  
  @staticmethod
  def from_conn_str(
    conn_str: str, type: type[U] | None = None,
    *, db: str, split_key=default_split, merge_key=default_merge
  ):
    client = lambda: CosmosClient.from_connection_string(conn_str)
    return CosmosKV.new(client, type, db=db, split_key=split_key, merge_key=merge_key)
  
  def prefixed(self, prefix: str): # type: ignore
    return CosmosContainerKV(
      self.client, db=self.db, container=prefix,
      parse=self.parse, dump=self.dump
    )
  
  def insert(self, key: str, value: T):
    partition, item = self.split_key(key)
    return self.prefixed(partition).insert(item, value)

  def read(self, key: str):
    partition, item = self.split_key(key)
    return self.prefixed(partition).read(item)

  def has(self, key: str):
    partition, item = self.split_key(key)
    return self.prefixed(partition).has(item)
  
  def delete(self, key: str):
    partition, item = self.split_key(key)
    return self.prefixed(partition).delete(item)
  
  async def keys(self):
    try:
      async with self.database_manager() as dc:
        async for c in dc.list_containers():
          cont = c['id']
          async for k in self.prefixed(cont).keys():
            yield self.merge_key(cont, k)
    except CosmosResourceNotFoundError:
      ...
    except Exception as e:
      raise KVError(e)

  async def items(self):
    try:
      async with self.database_manager() as dc:
        async for c in dc.list_containers():
          cont = c['id']
          async for k, v in self.prefixed(cont).items():
            yield self.merge_key(cont, k), v
    except CosmosResourceNotFoundError:
      ...
    except Exception as e:
      raise KVError(e) from e

  @azure_safe
  async def clear(self):
    try:
      await self.client().delete_database(self.db)
    except CosmosResourceNotFoundError:
      ...