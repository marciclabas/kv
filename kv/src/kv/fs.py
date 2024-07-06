from typing_extensions import TypeVar, Generic, Callable, ParamSpec, Awaitable
from dataclasses import dataclass
from functools import wraps
import os
from haskellian import Left, Right, Either, \
  promise as P, asyn_iter as AI, either as E
import fs
from kv import KV, InvalidData, DBError, InexistentItem

T = TypeVar('T')
L = TypeVar('L')
Ps = ParamSpec('Ps')

def parse_err(err: OSError) -> DBError | InexistentItem:
  match err:
    case FileNotFoundError():
      return InexistentItem(detail=str(err))
    case OSError():
      return DBError(str(err))

@dataclass
class FilesystemKV(KV[T], Generic[T]):
  """Filesystem-based `KV` implementation"""

  base_path: str
  extension: str = ''
  parse: Callable[[bytes], Either[InvalidData, T]] = lambda x: x # type: ignore
  dump: Callable[[T], bytes] = lambda x: x # type: ignore

  def __post_init__(self):
    os.makedirs(self.base_path, exist_ok=True)

  def __repr__(self):
    return f'FilesystemKV({self.base_path!r}, extension={self.extension!r})'
  
  def path(self, key: str):
    return os.path.join(self.base_path, key + self.extension)
  
  def key(self, path: str):
    return path.removesuffix(self.extension)
  
  @P.lift
  async def insert(self, key: str, value: T): # type: ignore
    return fs.write(self.path(key), self.dump(value)).mapl(parse_err)

  @P.lift
  async def read(self, key: str): # type: ignore
    return fs.read(self.path(key)).mapl(parse_err).bind(self.parse)
    
  @P.lift
  async def delete(self, key: str):
    return fs.delete(self.path(key)).mapl(parse_err)
  
  @P.lift
  async def has(self, key: str):
    return Right(os.path.exists(self.path(key)))
  
  @AI.lift
  async def keys(self):
    for key in fs.filenames(self.base_path).map(self.key):
      yield Right(key)

  @P.lift
  async def copy(self, key: str, to: 'KV[T]', to_key: str):
    if not isinstance(to, FilesystemKV):
      return await super().copy(key, to, to_key)
    return fs.copy(self.path(key), to.path(to_key)).mapl(parse_err)
  
  @P.lift
  async def move(self, key: str, to: 'KV[T]', to_key: str):
    if not isinstance(to, FilesystemKV):
      return await super().move(key, to, to_key)
    return fs.move(self.path(key), to.path(to_key)).mapl(parse_err)
  
  @P.lift
  async def clear(self):
    from shutil import rmtree
    try:
      rmtree(self.base_path)
      os.makedirs(self.base_path)
      return Right(None)
    except OSError as e:
      return Left(DBError(str(e)))