from typing_extensions import Generic, TypeVar, Callable, overload, ParamSpec, Awaitable, AsyncIterable
from dataclasses import dataclass
import redis.asyncio as redis
from haskellian import Left, Right, Either, promise as P, asyn_iter as AI
from kv import KV, DBError, InexistentItem, ReadError
from kv.serialization import Parse, Dump, default, serializers

T = TypeVar('T')
L = TypeVar('L')
Ps = ParamSpec('Ps')

def redis_safe(f: Callable[Ps, Awaitable[Either[L, T]]]):
  async def wrapper(*args: Ps.args, **kwargs: Ps.kwargs) -> Either[DBError|L, T]:
    try:
      return await f(*args, **kwargs)
    except redis.RedisError as e:
      return Left(DBError(str(e)))
  return wrapper

def ensure_str(s: str | bytes) -> str:
  return s.decode() if isinstance(s, bytes) else s # type: ignore

@dataclass
class RedisKV(KV[T], Generic[T]):
  """Redis-based `KV` implementation"""
  client: redis.Redis
  parse: Parse[T] = default[T].parse
  dump: Dump[T] = default[T].dump

  @staticmethod
  @overload
  def from_url(url: str, *, parse: Parse[T] = default[T].parse, dump: Dump[T] = default[T].dump) -> 'RedisKV[T]':
    ...
  @staticmethod
  @overload
  def from_url(url: str, type: type[T]) -> 'RedisKV[T]':
    ...
  @staticmethod
  def from_url(url: str, type = None, parse = default[T].parse, dump = default[T].dump) -> 'RedisKV[T]':
    client = redis.Redis.from_url(url)
    return RedisKV(client, parse, dump) if type is None else RedisKV(client, **serializers(type))

  @P.lift
  @redis_safe
  async def insert(self, key: str, value: T):
    return Right(await self.client.set(key, self.dump(value)))
  
  @P.lift
  @redis_safe
  async def read(self, key: str) -> Either[ReadError, T]:
    if (val := await self.client.get(key)) is None:
      return Left(InexistentItem(key))
    else:
      return self.parse(val)
  
  @P.lift
  @redis_safe
  async def delete(self, key: str) -> Either[DBError|InexistentItem, None]:
    if (await self.client.delete(key)) == 0:
      return Left(InexistentItem(key))
    else:
      return Right(None)
  
  @AI.lift
  async def keys(self) -> AsyncIterable[Either[DBError, str]]:
    try:
      keys = await self.client.keys()
      for key in keys:
        yield Right(ensure_str(key))
    except redis.RedisError as e:
      yield Left(DBError(str(e)))

  @P.lift
  @redis_safe
  async def clear(self):
    return Right(await self.client.flushdb())
  
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