from typing_extensions import TypeVar, Callable, Generic, TypedDict
from haskellian import Either, Left, Right
from kv import InvalidData

T = TypeVar('T')

Parse = Callable[[bytes], Either[InvalidData, T]]
Dump = Callable[[T], str|bytes]

class Serializers(TypedDict, Generic[T]):
  parse: Parse[T]
  dump: Dump[T]

class default(Generic[T]):
  """Default, dummy serializers"""
  @staticmethod
  def parse(data: bytes) -> Either[InvalidData, T]:
    return Right(data) # type: ignore
  @staticmethod
  def dump(value: T) -> str|bytes:
    return value # type: ignore
  
  serializers = Serializers(parse=parse, dump=dump)
  

def serializers(type: type[T]) -> Serializers[T]:
  """Get default serializers for a type"""
  from pydantic import TypeAdapter, ValidationError
  Adapter = TypeAdapter(type)

  def parse(data: bytes):
    try:
      return Right(Adapter.validate_json(data))
    except ValidationError as e:
      return Left(InvalidData(str(e)))
    
  def dump(value: T):
    return Adapter.dump_json(value, exclude_none=True, exclude_defaults=True, by_alias=True)
  
  return Serializers(parse=parse, dump=dump)