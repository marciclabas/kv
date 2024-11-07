from typing_extensions import TypeVar, Callable, Generic, TypedDict
from kv import InvalidData

T = TypeVar('T')

Parse = Callable[[bytes], T]
Dump = Callable[[T], bytes]

class Serializers(TypedDict, Generic[T]):
  parse: Parse[T]
  dump: Dump[T]

class default(Generic[T]):
  """Default, dummy serializers"""
  @staticmethod
  def parse(data: bytes) -> T:
    return data # type: ignore
  @staticmethod
  def dump(value: T) -> bytes:
    return value # type: ignore
  
  serializers = Serializers(parse=parse, dump=dump)
  

def serializers(type: type[T]) -> Serializers[T]:
  """Get default serializers for a type"""
  from pydantic import RootModel, ValidationError
  Root = RootModel[type]

  def parse(data: bytes):
    try:
      return Root.model_validate_json(data).root
    except ValidationError as e:
      raise InvalidData(str(e)) from e
    
  def dump(value: T):
    return Root(value).model_dump_json(exclude_none=True).encode()
  
  return Serializers(parse=parse, dump=dump)