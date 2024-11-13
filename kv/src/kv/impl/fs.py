from typing_extensions import TypeVar, Generic, ParamSpec, overload, Iterable, Callable, Coroutine
from functools import wraps
from dataclasses import dataclass
import os
from kv import KV, KVError, InexistentItem
from kv.serialization import Parse, Dump, default, serializers

T = TypeVar('T')
U = TypeVar('U')
L = TypeVar('L')
Ps = ParamSpec('Ps')

def ensure_path(file: str):
  """Creates the path to `file`'s folder if it didn't exist
  - E.g. `ensure('path/to/file.txt')` will create `'path/to'` if needed
  """
  dir = os.path.dirname(file)
  if dir != '':
    os.makedirs(dir, exist_ok=True)

def rec_paths(base_path: str) -> Iterable[str]:
  """Returns all files inside `base_path`, recursively, relative to `base_path`"""
  for root, _, files in os.walk(base_path):
    for file in files:
      path = os.path.join(root, file)
      yield os.path.relpath(path, start=base_path)

def wrap_exceptions(f: Callable[Ps, Coroutine[None, None, T]]) -> Callable[Ps, Coroutine[None, None, T]]:
  @wraps(f)
  async def wrapper(*args: Ps.args, **kwargs: Ps.kwargs) -> T:
    try:
      return await f(*args, **kwargs)
    except FileNotFoundError as e:
      raise InexistentItem(str(e)) from e
    except OSError as e:
      raise KVError(str(e)) from e
  return wrapper

@dataclass
class FilesystemKV(KV[T], Generic[T]):
  """Filesystem-based `KV` implementation"""

  base_path: str
  extension: str = ''
  parse: Parse[T] = default[T].parse
  dump: Dump[T] = default[T].dump

  @classmethod
  @overload
  def new(cls, base_path: str) -> 'FilesystemKV[bytes]':
    ...
  @classmethod
  @overload
  def new(cls, base_path: str, type: type[U] | None = None) -> 'FilesystemKV[U]':
    ...
  @classmethod
  def new(cls, base_path: str, type: type[T] | None = None):
    if type and type is not bytes:
      return FilesystemKV(base_path, extension='.json', **serializers(type))
    else:
      return FilesystemKV(base_path)

  def __post_init__(self):
    os.makedirs(self.base_path, exist_ok=True)

  def __repr__(self):
    return f'FilesystemKV(base_path={self.base_path!r}, extension={self.extension!r})'
  
  def path(self, key: str):
    return os.path.join(self.base_path, key + self.extension)
  
  def key(self, path: str):
    return path.removesuffix(self.extension)
  
  @wrap_exceptions
  async def insert(self, key: str, value: T):
    ensure_path(self.path(key))
    with open(self.path(key), 'wb') as f:
      f.write(self.dump(value))

  @wrap_exceptions
  async def read(self, key: str):
    with open(self.path(key), 'rb') as f:
      return self.parse(f.read())
    
  @wrap_exceptions
  async def delete(self, key: str):
    os.remove(self.path(key))
    try: # clean up empty directories
      os.removedirs(os.path.dirname(self.path(key)))
    except:
      ...
  
  async def has(self, key: str):
    return os.path.exists(self.path(key))
  
  async def keys(self):
    for name in rec_paths(self.base_path):
      yield self.key(name)

  async def copy(self, key: str, to: 'KV[T]', to_key: str):
    if not isinstance(to, FilesystemKV):
      return await super().copy(key, to, to_key)
    import shutil
    ensure_path(to.path(to_key))
    shutil.copy(self.path(key), to.path(to_key))
  
  async def move(self, key: str, to: 'KV[T]', to_key: str):
    if not isinstance(to, FilesystemKV):
      return await super().move(key, to, to_key)
    import shutil
    ensure_path(to.path(to_key))
    shutil.move(self.path(key), to.path(to_key))

  @wrap_exceptions
  async def clear(self):
    import shutil
    shutil.rmtree(self.base_path)
    os.makedirs(self.base_path)
    
  def prefixed(self, prefix: str) -> 'FilesystemKV[T]':
    new_base = os.path.join(self.base_path, prefix)
    return FilesystemKV(new_base, self.extension, self.parse, self.dump)