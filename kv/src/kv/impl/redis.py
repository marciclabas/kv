from typing_extensions import Generic, TypeVar, Callable, overload, ParamSpec, Awaitable, AsyncIterable
from dataclasses import dataclass
import redis.asyncio as redis
from kv import KV, KVError, InexistentItem
from kv.serialization import Parse, Dump, default, serializers

T = TypeVar('T')
L = TypeVar('L')
Ps = ParamSpec('Ps')

def redis_safe(f: Callable[Ps, Awaitable[T]]):
  async def wrapper(*args: Ps.args, **kwargs: Ps.kwargs) -> T:
    try:
      return await f(*args, **kwargs)
    except redis.RedisError as e:
      raise KVError(str(e)) from e
  return wrapper

def ensure_str(s: str | bytes) -> str:
  return s.decode() if isinstance(s, bytes) else s # type: ignore

@dataclass
class RedisKV(KV[T], Generic[T]):
  """Redis-based `KV` implementation"""
  client: redis.Redis
  parse: Parse[T] = default[T].parse
  dump: Dump[T] = default[T].dump

  def __repr__(self):
    return f'RedisKV({self.client!r})'

  @staticmethod
  @overload
  def from_url(url: str, *, parse: Parse[T] = default[T].parse, dump: Dump[T] = default[T].dump) -> 'RedisKV[T]':
    ...
  @staticmethod
  @overload
  def from_url(url: str, type: type[T] | None = None) -> 'RedisKV[T]':
    ...
  @staticmethod
  def from_url(url: str, type = None, parse = default[T].parse, dump = default[T].dump) -> 'RedisKV[T]':
    client = redis.Redis.from_url(url)
    return RedisKV(client, parse, dump) if type is None else RedisKV(client, **serializers(type))

  @redis_safe
  async def insert(self, key: str, value: T):
    await self.client.set(key, self.dump(value))
  
  @redis_safe
  async def read(self, key: str) -> T:
    if (val := await self.client.get(key)) is None:
      raise InexistentItem(key)
    else:
      return self.parse(val)
  
  @redis_safe
  async def delete(self, key: str):
    if (await self.client.delete(key)) == 0:
      raise InexistentItem(key)
  
  async def keys(self) -> AsyncIterable[str]:
    try:
      keys = await self.client.keys()
      for key in keys:
        yield ensure_str(key)
    except redis.RedisError as e:
      raise KVError(str(e)) from e

  @redis_safe
  async def clear(self):
    await self.client.flushdb()
  
  def __del__(self):
    import asyncio
    async def cleanup():
      await self.client.close()
    try:
      asyncio.create_task(cleanup())
    except RuntimeError:
      loop = asyncio.new_event_loop()
      asyncio.set_event_loop(loop)
      loop.run_until_complete(cleanup())
      loop.close()