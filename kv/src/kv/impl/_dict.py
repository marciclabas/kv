from typing_extensions import TypeVar, Generic
from dataclasses import dataclass, field
from kv import KV, InexistentItem

T = TypeVar('T')

@dataclass
class DictKV(KV[T], Generic[T]):
  """In-memory `KV` implementation over a built-in dict"""
  xs: dict[str, T] = field(default_factory=dict)

  async def insert(self, key: str, value: T):
    self.xs[key] = value

  async def read(self, key: str):
    if key in self.xs:
      return self.xs[key]
    else:
      raise InexistentItem(key)
  
  async def delete(self, key: str):
    if key in self.xs:
      del self.xs[key]
    else:
      raise InexistentItem(key)
    
  async def keys(self):
    for key in self.xs.keys():
      yield key

  async def items(self):
    for key, value in self.xs.items():
      yield key, value

  async def clear(self):
    self.xs.clear()