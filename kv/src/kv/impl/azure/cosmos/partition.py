from typing import TypeVar, Generic, Callable, Any
from dataclasses import dataclass, replace
from azure.cosmos.aio import CosmosClient
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from kv import KVError, KV
from .util import azure_safe, ContainerMixin, serializers, encode, decode

T = TypeVar('T')
U = TypeVar('U')
L = TypeVar('L')

@dataclass
class CosmosPartitionKV(KV[T], ContainerMixin[T], Generic[T]):
  partition_key: str
  prefix_: str = ''

  def __repr__(self):
    return f'''CosmosPartitionKV(
  endpoint={self.client().client_connection.url_connection},
  database={self.db}, container={self.container}, partition_key={self.partition_key},
  prefix={self.prefix_!r},
)'''

  @staticmethod
  def new(
    client: Callable[[], CosmosClient], type: type[U] | None = None,
    *, db: str, container: str, partition_key: str
  ) -> 'CosmosPartitionKV[U]':
    return CosmosPartitionKV(
      client, db=db, container=container, partition_key=partition_key,
      **serializers(type or Any) # type: ignore
    )

  @staticmethod
  def from_conn_str(
    conn_str: str, type: type[U] | None = None, *,
    db: str, container: str, partition_key: str
  ) -> 'CosmosPartitionKV[U]':
    client = lambda: CosmosClient.from_connection_string(conn_str)
    return CosmosPartitionKV.new(client, type, db=db, container=container, partition_key=partition_key)

  @azure_safe
  async def insert(self, key: str, value: T):
    async with self.container_manager() as cc:
      item = {'id': encode(key), 'key': key, 'partition': self.partition_key, 'value': self.dump(value) }
      try:
        await cc.upsert_item(item)
      except CosmosResourceNotFoundError:
        await self.create()
        await cc.upsert_item(item)

  @azure_safe
  async def read(self, key: str):
    async with self.container_manager() as cc:
      item = await cc.read_item(item=encode(key), partition_key=self.partition_key)
      return self.parse(item['value'])

  @azure_safe
  async def delete(self, key: str):
    async with self.container_manager() as cc:
      await cc.delete_item(item=encode(key), partition_key=self.partition_key)
    
  @azure_safe
  async def has(self, key: str):
    try:
      async with self.container_manager() as cc:
        query = 'SELECT c.id FROM c WHERE c.id = @key'
        params: list[dict] = [{'name': '@key', 'value': encode(key)}]
        async for _ in cc.query_items(query=query, parameters=params, partition_key=self.partition_key):
          return True
    except CosmosResourceNotFoundError:
      ...
    return False

  async def keys(self):
    try:
      async with self.container_manager() as cc:
        query = 'SELECT c["key"] FROM c WHERE STARTSWITH(c["key"], @prefix)'
        params: list[dict] = [{'name': '@prefix', 'value': self.prefix_}]
        async for item in cc.query_items(query=query, parameters=params, partition_key=self.partition_key):
          yield item['key'].removeprefix(self.prefix_)
    except CosmosResourceNotFoundError:
      ...
    except Exception as e:
      raise KVError(e)

  async def items(self):
    try:
      async with self.container_manager() as cc:
        query = 'SELECT c.id, c["value"] FROM c'
        async for item in cc.query_items(query=query, partition_key=self.partition_key):
          yield decode(item['id']), self.parse(item['value'])
    except CosmosResourceNotFoundError:
      ...
    except Exception as e:
      raise KVError(e) from e

  @azure_safe
  async def clear(self):
    try:
      async with self.container_manager() as cc:
        await cc.delete_all_items_by_partition_key(self.partition_key)
    except CosmosResourceNotFoundError:
      ...

  def prefixed(self, prefix: str):
    new_prefix = self.prefix_.rstrip('/') + '/' + prefix.strip('/')
    return replace(self, prefix_=new_prefix.lstrip('/'))