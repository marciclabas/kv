from typing import Callable, TypeVar, Generic, Any
from dataclasses import dataclass
from azure.cosmos.aio import CosmosClient
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from kv import KV, KVError
from .util import ContainerMixin, azure_safe, decode, serializers, default_split, default_merge
from .partition import CosmosPartitionKV

T = TypeVar('T')
U = TypeVar('U')

@dataclass
class CosmosContainerKV(KV[T], ContainerMixin[T], Generic[T]):
  split_key: Callable[[str], tuple[str, str]] = default_split
  """Split a key into container + partition. Defaults to `{container}/{id/with/slashes}`"""
  merge_key: Callable[[str, str], str] = default_merge
  """Merge a container and partition into a key. Defaults to `{container}/{id/with/slashes}`"""

  def __repr__(self):
    endpoint = self.client().client_connection.url_connection
    return f'CosmosContainerKV(endpoint={endpoint}, database={self.db}, container={self.container})'
  
  @staticmethod
  def new(
    client: Callable[[], CosmosClient], type: type[U] | None = None,
    *, db: str, container: str, split_key=default_split, merge_key=default_merge
  ):
    return CosmosContainerKV(
      client, db=db, container=container,
      split_key=split_key, merge_key=merge_key,
      **serializers(type or Any)
    )
  
  @staticmethod
  def from_conn_str(
    conn_str: str, type: type[U] | None = None,
    *, db: str, container: str, split_key=default_split, merge_key=default_merge
  ):
    client = lambda: CosmosClient.from_connection_string(conn_str)
    return CosmosContainerKV.new(client, type, db=db, container=container, split_key=split_key, merge_key=merge_key)
  
  def prefixed(self, prefix: str): # type: ignore
    return CosmosPartitionKV(
      self.client, db=self.db, container=self.container, partition_key=prefix,
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
  
  def key(self, item: dict):
    return self.merge_key(item['partition'], decode(item['id']))
  
  async def keys(self):
    try:
      async with self.container_manager() as cc:
        query = 'SELECT c.id, c.partition FROM c'
        async for item in cc.query_items(query=query):
          yield self.key(item)
    except CosmosResourceNotFoundError:
      ...
    except Exception as e:
      raise KVError(e) from e

  async def items(self):
    try:
      async with self.container_manager() as cc:
        query = 'SELECT c.id, c.partition, c["value"] FROM c'
        async for item in cc.query_items(query=query):
          v = self.parse(item['value'])
          yield self.key(item), v
    except CosmosResourceNotFoundError:
      ...
    except Exception as e:
      raise KVError(e) from e

  @azure_safe
  async def clear(self):
    try:
      async with self.database_manager() as dc:
        await dc.delete_container(self.container)
    except CosmosResourceNotFoundError:
      ...
