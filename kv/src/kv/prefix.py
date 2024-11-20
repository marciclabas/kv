from typing import TypeVar, Generic
from dataclasses import dataclass, replace
from kv import KV, LocatableKV, KVError

T = TypeVar('T')

@dataclass
class PrefixedKV(KV[T], Generic[T]):
  prefix_: str
  kv: KV[T]

  def insert(self, key: str, value: T):
    return self.kv.insert(self.prefix_ + key, value)
  
  def read(self, key: str):
    return self.kv.read(self.prefix_ + key)
  
  def delete(self, key: str):
    return self.kv.delete(self.prefix_ + key)
  
  def has(self, key: str):
    return self.kv.has(self.prefix_ + key)
  
  def prefixed(self, prefix: str):
    new_prefix = self.prefix_.rstrip('/') + '/' + prefix.strip('/')
    return replace(self, prefix_=new_prefix.lstrip('/'))
  
  async def keys(self):
    async for key in self.kv.keys():
      if key.startswith(self.prefix_):
        yield key.removeprefix(self.prefix_)

  def url(self, key: str, /, *, expiry=None):
    if not isinstance(self.kv, LocatableKV):
      raise KVError('This KV is not locatable')
    return self.kv.url(self.prefix_ + key, expiry=expiry)
