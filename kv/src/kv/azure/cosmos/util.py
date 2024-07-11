from typing_extensions import TypeVar, Callable, Awaitable, ParamSpec, Generic, TypedDict
from dataclasses import dataclass, KW_ONLY
from contextlib import asynccontextmanager
from urllib.parse import quote, unquote
from haskellian import Either, Left, Right, promise as P
from azure.cosmos import PartitionKey
from azure.cosmos.aio import CosmosClient
from azure.core.exceptions import ResourceNotFoundError
from kv import DBError, InexistentItem, InvalidData

Ps = ParamSpec('Ps')
T = TypeVar('T')
L = TypeVar('L')

def encode(key: str):
  return quote(key, safe='')

def decode(encoded_key: str):
  return unquote(encoded_key)

class Serializers(TypedDict, Generic[T]):
  parse: Callable[[dict|list|str], Either[InvalidData, T]]
  dump: Callable[[T], dict|list|str]

def serializers(type: type[T]) -> Serializers[T]:
  from pydantic import TypeAdapter
  Type = TypeAdapter(type)
  def parse(x):
    try:
      return Right(Type.validate_python(x))
    except Exception as e:
      return Left(InvalidData(str(e)))
  def dump(x):
    return Type.dump_python(x)
  return Serializers(parse=parse, dump=dump)

def default_split(key: str) -> tuple[str, str]:
  parts = key.split('/', 1)
  return (parts[0], parts[1]) if len(parts) > 1 else ('default', parts[0])

def default_merge(a: str, b: str) -> str:
  return f'{a}/{b}' if a != 'default' else b

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
class DatabaseMixin(Generic[T]):
  client: Callable[[], CosmosClient]
  _: KW_ONLY
  db: str
  parse: Callable[[dict|list|str], Either[InvalidData, T]]
  dump: Callable[[T], dict|list|str]

  @asynccontextmanager
  async def database_manager(self):
    async with self.client() as client:
      yield await client.create_database_if_not_exists(self.db)

  async def create(self):
    async with self.client() as client:
      await client.create_database_if_not_exists(self.db)

@dataclass
class ContainerMixin(DatabaseMixin[T], Generic[T]):
  container: str

  @asynccontextmanager
  async def container_manager(self):
    async with self.database_manager() as db:
      yield db.get_container_client(self.container)

  async def create(self):
    async with self.client() as client:
      db = client.get_database_client(self.db)
      try:
        await db.create_container_if_not_exists(self.container, partition_key=PartitionKey(path='/partition'))
      except ResourceNotFoundError:
        await client.create_database_if_not_exists(self.db)
        await db.create_container_if_not_exists(self.container, partition_key=PartitionKey(path='/partition'))

