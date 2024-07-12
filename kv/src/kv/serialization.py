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
  from pydantic import RootModel, ValidationError
  Root = RootModel[type]

  def parse(data: bytes):
    try:
      return Right(Root.model_validate_json(data).root)
    except ValidationError as e:
      return Left(InvalidData(str(e)))
    
  def dump(value: T):
    return Root(value).model_dump_json(exclude_none=True)
  
  return Serializers(parse=parse, dump=dump)