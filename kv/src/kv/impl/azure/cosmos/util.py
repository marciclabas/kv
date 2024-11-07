from typing_extensions import TypeVar, Callable, Awaitable, ParamSpec, Generic, TypedDict
from dataclasses import dataclass, KW_ONLY
from contextlib import asynccontextmanager
import base64
from azure.cosmos import PartitionKey
from azure.cosmos.aio import CosmosClient
from azure.core.exceptions import ResourceNotFoundError
from kv import KVError, InexistentItem, InvalidData

Ps = ParamSpec('Ps')
T = TypeVar('T')
L = TypeVar('L')

def encode(key: str):
  return base64.urlsafe_b64encode(key.encode()).decode()

def decode(encoded_key: str):
  return base64.urlsafe_b64decode(encoded_key.encode()).decode()

class Serializers(TypedDict, Generic[T]):
  parse: Callable[[dict|list|str], T]
  dump: Callable[[T], dict|list|str]

def serializers(type: type[T]) -> Serializers[T]:
  from pydantic import RootModel, ValidationError
  Type = RootModel[type]
  def parse(x):
    try:
      return Type.model_validate(x).root
    except ValidationError as e:
      raise InvalidData from e
  def dump(x):
    return Type(x).model_dump()
  return Serializers(parse=parse, dump=dump)

def default_split(key: str) -> tuple[str, str]:
  parts = key.split('/', 1)
  return (parts[0], parts[1]) if len(parts) > 1 else ('default', parts[0])

def default_merge(a: str, b: str) -> str:
  return f'{a}/{b}' if a != 'default' else b

def azure_safe(coro: Callable[Ps, Awaitable[T]]):
  async def wrapper(*args: Ps.args, **kwargs: Ps.kwargs) -> T:
    try:
      return await coro(*args, **kwargs)
    except ResourceNotFoundError as e:
      raise InexistentItem from e
    except Exception as e:
      raise KVError(str(e)) from e
  return wrapper

@dataclass
class DatabaseMixin(Generic[T]):
  client: Callable[[], CosmosClient]
  _: KW_ONLY
  db: str
  parse: Callable[[dict|list|str], T]
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

