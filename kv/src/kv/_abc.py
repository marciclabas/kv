from typing_extensions import TypeVar, Generic, AsyncIterable, Literal, Any, TYPE_CHECKING
from abc import ABC, abstractmethod
if TYPE_CHECKING:
  from datetime import datetime
from haskellian import Either, Promise, AsyncIter, \
  either as E, promise as P, asyn_iter as AI
from dataclasses import dataclass

class StrMixin:
  def __str__(self) -> str:
    return self.__repr__()

@dataclass(eq=False)
class InexistentItem(StrMixin, BaseException):
  key: str | None = None
  detail: Any | None = None
  reason: Literal['inexistent-item'] = 'inexistent-item'

@dataclass(eq=False)
class DBError(StrMixin, BaseException):
  detail: Any = None
  reason: Literal['db-error'] = 'db-error'

@dataclass(eq=False)
class InvalidData(StrMixin, BaseException):
  detail: Any = None
  reason: Literal['invalid-data'] = 'invalid-data'

ReadError = DBError | InvalidData | InexistentItem

T = TypeVar('T')
U = TypeVar('U')

class KV(ABC, Generic[T]):
  """Async, exception-free key-value store ABC"""
  
  @staticmethod
  def of(conn_str: str, type: type[U] | None = None) -> 'KV[U]':
    """Create a KV from a connection string. Supports:
    - `file://<path>`: `FilesystemKV`
    - `sql+<protocol>://<conn_str>;Table=<table>`: `SQLKV`
    - `azure+blob://<conn_str>`: `BlobKV`
    - `azure+blob+container://<conn_str>;Container=<container_name>`: `BlobContainerKV`
    - `https://<endpoint>` (or `http://<endpoint>`): `ClientKV`
    - `https://<endpoint>;Token=<token>` (or `http://<endpoint>;Token=<token>`): `ClientKV`
    - `redis://...`, `rediss://...`, `redis+unix://...`: `RedisKV`
    """
    ...
    from .conn_strings import parse
    return parse(conn_str, type)
  
  @abstractmethod
  def insert(self, key: str, value: T) -> Promise[Either[DBError, None]]:
    """Insert entry: `self[key] = value`"""

  @abstractmethod
  def read(self, key: str) -> Promise[Either[ReadError, T]]:
    """Read item with key `key`. Returns `Left[InexistentItem]` if the item does not exist."""

  @abstractmethod
  def delete(self, key: str) -> Promise[Either[DBError | InexistentItem, None]]:
    """Delete item with key `key`. Returns `Left[InexistentItem]` if the item does not exist."""

  @AI.lift
  async def items(self) -> AsyncIterable[Either[DBError | InvalidData, tuple[str, T]]]:
    """Iterate over all items in the `KV`"""
    @E.do[DBError|InvalidData]()
    async def fetch_one(key: Either[DBError, str]):
      k = key.unsafe()
      return k, (await self.read(k)).unsafe()
    async for e in self.keys():
      yield (await fetch_one(e))

  @P.lift
  @E.do[DBError]()
  async def has(self, key: str):
    """Does the `KV` have `key`?"""
    async for k in self.keys().map(E.unsafe):
      if k == key:
        return True
    return False
  
  @abstractmethod
  def keys(self) -> AsyncIter[Either[DBError, str]]:
    """Read all keys in the `KV`"""

  @AI.lift
  async def values(self) -> AsyncIterable[Either[DBError|InvalidData, T]]:
    """Iterate over all values in the `KV`"""
    async for e in self.items():
      yield e.fmap(lambda it: it[1])

  @P.lift
  @E.do[ReadError]()
  async def copy(self, key: str, to: 'KV[T]', to_key: str):
    """Copy `self[key]` to `to[to_key]`"""
    value = (await self.read(key)).unsafe()
    return (await to.insert(to_key, value)).unsafe()

  @P.lift
  @E.do[ReadError]()
  async def move(self, key: str, to: 'KV[T]', to_key: str):
    """Move `self[key]` to `to[to_key]`"""
    (await self.copy(key, to, to_key)).unsafe()
    (await self.delete(key)).unsafe()

  @E.do[DBError]()
  async def clear(self):
    """Delete all entries"""
    async for key in self.keys().map(E.unsafe):
      (await self.delete(key)).unsafe()

  def prefixed(self, prefix: str) -> 'KV[T]':
    """Create a `KV` with all keys prefixed with `prefix`"""
    from .prefix import PrefixedKV
    return PrefixedKV(prefix, self)


class LocatableKV(KV[T], Generic[T]):
  @abstractmethod
  def url(self, key: str, /, *, expiry: 'datetime | None' = None) -> str:
    ...