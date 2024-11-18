from typing_extensions import TypeVar, Generic, Literal, AsyncIterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from urllib.parse import quote
import jwt
import httpx
from kv import KV, LocatableKV, KVError, InexistentItem
from ...serialization import Parse, Dump, default, serializers

T = TypeVar('T')
U = TypeVar('U', default=bytes)

def sign_token(secret: str, expiry: datetime | None = None) -> str:
  payload = {} if expiry is None else {'exp': expiry.timestamp()}
  return jwt.encode(payload, secret, algorithm='HS256')

@dataclass
class ClientKV(LocatableKV[T], Generic[T]):
  """HTTP-based client `KV` implementation"""
  endpoint: str
  parse: Parse[T] = default[T].parse
  dump: Dump[T] = default[T].dump
  secret: str | None = None
  prefix_: str = ''

  @classmethod
  def new(cls, endpoint: str, type: type[U], *, secret: str | None = None) -> 'ClientKV[U]':
    return (
      ClientKV(endpoint, **serializers(type), secret=secret)
      if type is not bytes else ClientKV(endpoint, secret=secret)
    )
    
  def __repr__(self):
    return f'ClientKV({self.endpoint}, prefix={self.prefix_})'
  
  async def _req(self, method: Literal['GET', 'POST', 'DELETE'], path: str, *, data: bytes | str | None = None):
    async with httpx.AsyncClient() as client:
      endpoint = f'{self.endpoint.rstrip("/")}/{path.lstrip("/")}'
      params = {}
      if self.prefix_:
        params['prefix'] = self.prefix_
      if self.secret:
        params['token'] = sign_token(self.secret, datetime.now() + timedelta(minutes=5))
      return await client.request(method, endpoint, data=data, params=params) # type: ignore
  
  async def read(self, key: str) -> T:
    r = await self._req('GET', f'/item/{quote(key)}')
    if r.status_code == 404:
      raise InexistentItem(key)
    if r.status_code != 200:
      raise KVError(r.text)
    return self.parse(r.content)
  
  async def insert(self, key: str, value: T):
    r = await self._req('POST', f'/item/{quote(key)}', data=self.dump(value))
    if r.status_code != 200:
      raise KVError(r.text)
    
  async def delete(self, key: str):
    r = await self._req('DELETE', f'/item/{quote(key)}')
    if r.status_code == 404:
      raise InexistentItem(key)
    if r.status_code != 200:
      raise KVError(r.text)
    
  async def has(self, key: str) -> bool:
    r = await self._req('GET', f'/item/{quote(key)}/has')
    return r.json()
  
  async def keys(self) -> AsyncIterable[str]:
    r = await self._req('GET', '/keys')
    if r.status_code != 200:
      raise KVError(r.text)
    for key in r.json():
      yield key

  async def clear(self):
    r = await self._req('DELETE', '/')
    if r.status_code != 200:
      raise KVError(r.text)
  
  def url(self, key: str, /, *, expiry: datetime | None = None) -> str:
    url = f"{self.endpoint.rstrip('/')}/item/{quote(key)}?"
    if self.prefix_:
      url += f"prefix={quote(self.prefix_)}&"
    if self.secret:
      url += f"token={quote(sign_token(self.secret, expiry))}"
    return url
  
  def prefixed(self, prefix: str):
    new_prefix = self.prefix_ + '/' + prefix if self.prefix_ else prefix
    return ClientKV(endpoint=self.endpoint, parse=self.parse, dump=self.dump, secret=self.secret, prefix_=new_prefix)
  

@dataclass
class Served(LocatableKV[T], Generic[T]):
  base_url: str
  kv: KV[T]
  prefix_: str = ''
  secret: str | None = None

  def url(self, key: str, /, *, expiry: datetime | None = None) -> str:
    url = f"{self.base_url.rstrip('/')}/item/{quote(key)}?"
    if self.prefix_:
      url += f"prefix={quote(self.prefix_)}&"
    if self.secret:
      url += f"token={quote(sign_token(self.secret, expiry))}"
    return url
  
  def prefixed(self, prefix: str):
    new_prefix = self.prefix_ + '/' + prefix if self.prefix_ else prefix
    return Served(self.base_url, self.kv, new_prefix)
  
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