from typing_extensions import TypeVar, Generic
from dataclasses import dataclass, field
from haskellian import Left, Right, promise as P, asyn_iter as AI
from kv import KV, InexistentItem

T = TypeVar('T')

@dataclass
class DictKV(KV[T], Generic[T]):
  """In-memory `KV` implementation over a built-in dict"""
  xs: dict[str, T] = field(default_factory=dict)

  @P.lift
  async def insert(self, key: str, value: T):
    self.xs[key] = value
    return Right(None)

  @P.lift
  async def read(self, key: str):
    return Right(self.xs[key]) if key in self.xs else Left(InexistentItem(key))
  
  @P.lift
  async def delete(self, key: str):
    if key in self.xs:
      del self.xs[key]
      return Right(None)
    else:
      return Left(InexistentItem(key))
    
  @AI.lift
  async def keys(self):
    for key in self.xs.keys():
      yield Right(key)

  @AI.lift
  async def items(self):
    for key, value in self.xs.items():
      yield Right((key, value))

  @P.lift
  async def clear(self):
    self.xs.clear()
    return Right(None)