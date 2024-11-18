from typing_extensions import TypeVar, Generic, AsyncIterable, Literal, Any, TYPE_CHECKING, Self
from abc import ABC, abstractmethod
if TYPE_CHECKING:
  from datetime import datetime
from dataclasses import dataclass

class StrMixin:
  def __str__(self) -> str:
    return self.__repr__()

@dataclass
class KVError(StrMixin, BaseException):
  detail: Any = None
  
@dataclass
class InexistentItem(KVError):
  key: str | None = None
  detail: Any | None = None
  reason: Literal['inexistent-item'] = 'inexistent-item'

@dataclass
class InvalidData(KVError):
  detail: Any = None
  reason: Literal['invalid-data'] = 'invalid-data'

T = TypeVar('T')
U = TypeVar('U')

class KV(ABC, Generic[T]):
  """Async, exception-free key-value store ABC"""
  
  @staticmethod
  def of(conn_str: str, type: type[U]) -> 'KV[U]':
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
  async def insert(self, key: str, value: T):
    """Insert entry: `self[key] = value`"""

  @abstractmethod
  async def read(self, key: str) -> T:
    """Read item with key `key`. Raises `InexistentItem` if the item does not exist."""

  async def safe_read(self, key: str) -> T | None:
    """Read item with key `key`. Returns `None` if the item does not exist."""
    try:
      return await self.read(key)
    except InexistentItem:
      ...

  @abstractmethod
  async def delete(self, key: str):
    """Delete item with key `key`. Returns `Left[InexistentItem]` if the item does not exist."""

  async def items(self) -> AsyncIterable[tuple[str, T]]:
    """Iterate over all items in the `KV`"""
    async for key in self.keys():
      yield key, await self.read(key)

  async def has(self, key: str) -> bool:
    """Does the `KV` have `key`?"""
    async for k in self.keys():
      if k == key:
        return True
    return False
  
  @abstractmethod
  def keys(self) -> AsyncIterable[str]:
    """Read all keys in the `KV`"""

  async def values(self) -> AsyncIterable[T]:
    """Iterate over all values in the `KV`"""
    async for _, val in self.items():
      yield val

  async def copy(self, key: str, to: 'KV[T]', to_key: str):
    """Copy `self[key]` to `to[to_key]`"""
    val = await self.read(key)
    await to.insert(to_key, val)

  async def move(self, key: str, to: 'KV[T]', to_key: str):
    """Move `self[key]` to `to[to_key]`"""
    await self.copy(key, to, to_key)
    await self.delete(key)

  async def rename(self, key: str, new_key: str):
    """Rename `key` to `new_key`"""
    await self.move(key, self, new_key)

  async def copy_all(self, to: 'KV[T]', *, max_concurrent: int = 16):
    import asyncio
    sem = asyncio.Semaphore(max_concurrent)
    async def copy_one(key):
      async with sem:
        await self.copy(key, to, key)
    await asyncio.gather(*[copy_one(key) async for key in self.keys()])

  async def move_all(self, to: 'KV[T]', *, max_concurrent: int = 16):
    await self.copy_all(to, max_concurrent=max_concurrent)
    await self.clear()

  async def clear(self):
    """Delete all entries"""
    async for key in self.keys():
      await self.delete(key)

  def prefixed(self, prefix: str, /) -> 'Self':
    """Create a `KV` with all keys prefixed with `prefix`, without nesting."""
    from .prefix import PrefixedKV
    return PrefixedKV(prefix, self) # type: ignore
  
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

  def served(self, base_url: str, *, secret: str | None = None) -> 'LocatableKV[T]':
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
    from kv import Served
    return Served(base_url, self, secret=secret)

class LocatableKV(KV[T], Generic[T]):
  @abstractmethod
  def url(self, key: str, /, *, expiry: 'datetime | None' = None) -> str:
    ...

