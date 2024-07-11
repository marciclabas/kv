from typing import TypeVar, Generic, Callable, Awaitable, ParamSpec
from contextlib import asynccontextmanager
from dataclasses import dataclass
from azure.cosmos import PartitionKey
from azure.cosmos.aio import CosmosClient
from azure.core.exceptions import ResourceNotFoundError
from haskellian import Either, Left, Right, promise as P, asyn_iter as AI
from kv import DBError, InexistentItem, InvalidData, KV

T = TypeVar('T')
U = TypeVar('U')
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
class CosmosPartitionKV(KV[T], Generic[T]):
  
  client: Callable[[], CosmosClient]
  db: str
  container: str
  partition_key: str
  parse: Callable[[dict|list|str], Either[InvalidData, T]]
  dump: Callable[[T], dict|list|str]

  def __repr__(self):
    return f'''CosmosPartitionKV(
  endpoint={self.client().client_connection.url_connection},
  database={self.db}, container={self.container}, partition_key={self.partition_key}
)'''

  @staticmethod
  def new(
    client: Callable[[], CosmosClient], type: type[U],
    *, db: str, container: str, partition_key: str
  ) -> 'CosmosPartitionKV[U]':
    from pydantic import TypeAdapter
    Type = TypeAdapter(type)
    def parse(x):
      try:
        return Right(Type.validate_python(x))
      except Exception as e:
        return Left(InvalidData(str(e)))
    return CosmosPartitionKV(client, db, container, partition_key, parse, Type.dump_python)

  @staticmethod
  def from_conn_str(
    conn_str: str, type: type[U], *,
    db: str, container: str, partition_key: str
  ) -> 'CosmosPartitionKV[U]':
    client = lambda: CosmosClient.from_connection_string(conn_str)
    return CosmosPartitionKV.new(client, type, db=db, container=container, partition_key=partition_key)

  @asynccontextmanager
  async def container_manager(self):
    async with self.client() as client:
      db = await client.create_database_if_not_exists(self.db)
      yield await db.create_container_if_not_exists(id=self.container, partition_key=PartitionKey(path='/partition'))

  @azure_safe
  async def insert(self, key: str, value: T):
    async with self.container_manager() as cc:
      item = {'id': key, 'partition': self.partition_key, 'value': self.dump(value) }
      await cc.upsert_item(item)
      return Right(None)

  @azure_safe
  async def read(self, key: str):
    async with self.container_manager() as cc:
      item = await cc.read_item(item=key, partition_key=key)
      return self.parse(item['value'])

  @azure_safe
  async def delete(self, key: str):
    async with self.container_manager() as cc:
      await cc.delete_item(item=key, partition_key=key)
      return Right(None)
    
  @azure_safe
  async def has(self, key: str):
    async with self.container_manager() as cc:
      query = 'SELECT c.id FROM c WHERE c.id = @key'
      params: list[dict] = [{'name': '@key', 'value': key}]
      async for _ in cc.query_items(query=query, parameters=params, partition_key=self.partition_key):
        return Right(True)
      return Right(False)

  @AI.lift
  async def keys(self):
    try:
      async with self.container_manager() as cc:
        query = 'SELECT c.id FROM c'
        async for item in cc.query_items(query=query, partition_key=self.partition_key):
          yield Right(item['id'])
    except Exception as e:
      yield Left(DBError(e))

  @AI.lift
  async def items(self):
    try:
      async with self.container_manager() as cc:
        query = 'SELECT c.id, c["value"] FROM c'
        async for item in cc.query_items(query=query, partition_key=self.partition_key):
          e = self.parse(item['value'])
          yield e.fmap(lambda v: (item['id'], v))
    except Exception as e:
      yield Left(DBError(e))

  @azure_safe
  async def clear(self):
    async with self.container_manager() as cc:
      await cc.delete_all_items_by_partition_key(self.partition_key)
    return Right(None)