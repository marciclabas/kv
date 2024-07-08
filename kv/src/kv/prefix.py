from typing import TypeVar, Generic
from dataclasses import dataclass
from haskellian import asyn_iter as AI, Right
from kv import KV

T = TypeVar('T')

@dataclass
class PrefixedKV(KV[T], Generic[T]):
  prefix: str
  kv: KV[T]

  def insert(self, key: str, value: T):
    return self.kv.insert(self.prefix + key, value)
  
  def read(self, key: str):
    return self.kv.read(self.prefix + key)
  
  def delete(self, key: str):
    return self.kv.delete(self.prefix + key)
  
  def has(self, key: str):
    return self.kv.has(self.prefix + key)
  
  @AI.lift
  async def keys(self):
    async for e in self.kv.keys():
      if e.tag == 'right' and e.value.startswith(self.prefix):
        yield Right(e.value.removeprefix(self.prefix))
