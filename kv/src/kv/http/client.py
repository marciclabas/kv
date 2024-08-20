from datetime import datetime
from typing_extensions import TypeVar, Generic, Any, Literal, AsyncIterable, Sequence, overload
from dataclasses import dataclass
from urllib.parse import quote
from pydantic import TypeAdapter, ValidationError
from haskellian import Either, Left, Right, promise as P, asyn_iter as AI
from kv import LocatableKV, ReadError, DBError
from .req import Request, request as default_request, bound_request
from ..serialization import Parse, Dump, default, serializers

T = TypeVar('T')
U = TypeVar('U')
ErrType = TypeAdapter(ReadError)
SeqType = TypeAdapter(Sequence[Either[DBError, str]])

def validate_left(raw_json: bytes, status: int) -> Left[DBError, Any]:
  try:
    return Left(ErrType.validate_json(raw_json))
  except ValidationError:
    return Left(DBError(f'Unexpected status code: {status}. Content: "{raw_json.decode()}"'))
  
def validate_seq(raw_json: bytes):
  try:
    return SeqType.validate_json(raw_json)
  except Exception as e:
    return [Left(DBError(e))]

@dataclass
class ClientKV(LocatableKV[T], Generic[T]):
  """HTTP-based client `KV` implementation"""
  endpoint: str
  parse: Parse[T] = default[T].parse
  dump: Dump[T] = default[T].dump
  request: Request = default_request
  prefix_: str = ''

  @classmethod
  @overload
  def new(cls, endpoint: str, *, token: str | None = None) -> 'ClientKV[bytes]':
    ...
  @classmethod
  @overload
  def new(cls, endpoint: str, type: type[U] | None = None, *, token: str | None = None) -> 'ClientKV[U]':
    ...
  @classmethod
  def new(cls, endpoint: str, type: type[U] | None = None, *, token: str | None = None):
    req = bound_request(headers={'Authorization': f'Bearer {token}'}) if token else default_request
    return (
      ClientKV(endpoint, **serializers(type), request=req)
      if type else ClientKV(endpoint, request=req)
    )
  
  def __repr__(self):
    return f'ClientKV({self.endpoint})'
  
  async def _req(
    self, method: Literal['GET', 'POST', 'DELETE'], path: str, *,
    data: bytes | str | None = None, query: dict = {}
  ):
    try:
      r = await self.request(method, f"{self.endpoint.rstrip('/')}/{path.lstrip('/')}", data=data, params=query)
      return Right(r.content) if r.status_code == 200 else validate_left(r.content, r.status_code)
    except Exception as e:
      return Left(DBError(e))

  @P.lift
  async def read(self, key: str):
    r = await self._req('GET', 'read', query={'prefix': self.prefix_, 'key': key})
    return r.bind(self.parse)
  
  @P.lift
  async def insert(self, key: str, value: T):
    query = {'prefix': self.prefix_, 'key': key}
    r = await self._req('POST', 'insert', data=self.dump(value), query=query)
    return r.fmap(lambda _: None)
    
  @P.lift
  async def delete(self, key: str):
    r = await self._req('DELETE', 'delete', query={'prefix': self.prefix_, 'key': key})
    return r.fmap(lambda _: None)
  
  @P.lift
  async def clear(self):
    r = await self._req('DELETE', 'clear', query={'prefix': self.prefix_})
    return r.fmap(lambda _: None)

  @AI.lift
  async def keys(self) -> AsyncIterable[Either[DBError, str]]:
    r = await self._req('GET', 'keys', query={'prefix': self.prefix_})
    keys = r.fmap(validate_seq)  
    if keys.tag == 'left':
      yield Left(keys.value)
    else:
      for key in keys.value:
        yield key

  def url(self, key: str, /, *, expiry: datetime | None = None) -> str:
    return f"{self.endpoint.rstrip('/')}/read?key={quote(key)}&prefix={quote(self.prefix_)}"
  
  def prefixed(self, prefix: str):
    return ClientKV(self.endpoint, self.parse, self.dump, self.request, self.prefix_ + prefix)