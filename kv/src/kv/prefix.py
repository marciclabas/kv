from typing import TypeVar, Generic
from dataclasses import dataclass
from haskellian import asyn_iter as AI, Right
from kv import KV

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
  
  @AI.lift
  async def keys(self):
    async for e in self.kv.keys():
      if e.tag == 'right' and e.value.startswith(self.prefix_):
        yield Right(e.value.removeprefix(self.prefix_))

  def url(self, key: str, /, *, expiry=None):
    return self.kv.url(self.prefix_ + key, expiry=expiry) # type: ignore
