from typing_extensions import TypeVar, Generic, AsyncIterable, Literal, Any, TYPE_CHECKING, Self
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
    """
    Create a KV (Key-Value) store instance from a connection string.

    The function supports various schemes to connect to different types of key-value stores, 
    and allows for additional parameters to be specified via the query string.

    Supported schemes:
    - `file://<path>`: FilesystemKV
    - `sqlite://<path>?table=<table>`: SQLiteKV (uses sqlite3)
    - `sql+<protocol>://<conn_str>?table=<table>`: SQLKV (uses SQLAlchemy)
    - `azure+blob://<conn_str>`: BlobKV
    - `azure+blob://<conn_str>?container=<container_name>`: BlobContainerKV
    - `azure+cosmos://<conn_str>?db=<db>`: CosmosKV
    - `azure+cosmos://<conn_str>?db=<db>&container=<container>`: CosmosContainerKV
    - `azure+cosmos://<conn_str>?db=<db>&container=<container>&partition=<partition>`: CosmosPartitionKV
    - `http://<endpoint>?token=<bearer>` or `https://<endpoint>?token=<bearer>`: ClientKV
    - `redis://<url>`, `rediss://<url>`, `redis+unix://<url>`: RedisKV

    Examples:
    >>> kv = KV.of('file://path/to/base?prefix=hello/')
    >>> kv = KV.of('sqlite://path/to/db.sqlite?table=mytable')
    >>> kv = KV.of('http://example.com?token=secret&prefix=hello-')
    """
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

  def prefixed(self, prefix: str, /) -> 'Self':
    """Create a `KV` with all keys prefixed with `prefix`, without nesting."""
    from .prefix import PrefixedKV
    return PrefixedKV(prefix.rstrip('/') + '/', self) # type: ignore
  
  def prefix(self, prefix: str, /) -> 'Self':
    """Nested prefix. Supports slashes in `prefix`.
    >>> kv.prefix('a/b')
    is equivalent to
    >>> kv.prefixed('a').prefixed('b')
    """
    kv = self
    for p in prefix.strip('/').split('/'):
      kv = kv.prefixed(p)
    return kv

  def served(self, base_url: str) -> 'LocatableKV[T]':
    """Create a `LocatableKV` assuming `self` is being served at `base_url`
    
    Example:
    ```
    import uvicorn
    from kv import KV, ServerKV
    
    host = ...
    port = ...
    kv = KV.of('file://data').served(f'http://{host}:{port}')
    kv.url('hello') # f'http://{host}:{port}/read?key=hello'
    api = ServerKV(kv)
    uvicorn.run(api, host=host, port=port)
    ```
    """
    return Served(base_url, self)

class LocatableKV(KV[T], Generic[T]):
  @abstractmethod
  def url(self, key: str, /, *, expiry: 'datetime | None' = None) -> str:
    ...


@dataclass
class Served(LocatableKV[T], Generic[T]):
  base_url: str
  kv: KV[T]
  prefix_: str = ''

  def url(self, key: str, /, *, expiry: 'datetime | None' = None) -> str:
    from urllib.parse import quote
    return f"{self.base_url.rstrip('/')}/read?key={quote(key)}&prefix={quote(self.prefix_)}"
  
  def prefixed(self, prefix: str):
    return Served(self.base_url, self.kv, self.prefix_ + '/' + prefix) # type: ignore
  
  def insert(self, key, value):
    return self.kv.prefix(self.prefix_).insert(key, value)
  
  def read(self, key):
    return self.kv.prefix(self.prefix_).read(key)
  
  def delete(self, key):
    return self.kv.prefix(self.prefix_).delete(key)
  
  def keys(self):
    return self.kv.prefix(self.prefix_).keys()
  
  def items(self):
    return self.kv.prefix(self.prefix_).items()
  
  def values(self):
    return self.kv.prefix(self.prefix_).values()
  
  def has(self, key):
    return self.kv.prefix(self.prefix_).has(key)
  
  def copy(self, key, to, to_key):
    return self.kv.prefix(self.prefix_).copy(key, to, to_key)
  
  def move(self, key, to, to_key):
    return self.kv.prefix(self.prefix_).move(key, to, to_key)
  
  def clear(self):
    return self.kv.prefix(self.prefix_).clear()
  