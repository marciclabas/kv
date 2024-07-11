from typing_extensions import TypeVar, overload
from urllib.parse import urlparse, parse_qs, unquote
from pydantic import BaseModel
from kv import KV

T = TypeVar('T')

def parse_type(type: str):
  if type == 'dict':
    return dict
  if type == 'list':
    return list
  if type == 'set':
    return set
  if type == 'str':
    return str
  if type == 'int':
    return int
  if type == 'float':
    return float
  if type == 'bool':
    return bool
  if type == 'bytes':
    return None
  raise ValueError(f'Invalid type: {type}')

class Params(BaseModel):
  prefix: str | None = None
  type: str | None = None

class HTTPParams(Params):
  token: str | None = None

class AzureBlobParams(Params):
  container: str | None = None

class CosmosParams(Params):
  db: str
  container: str | None = None
  partition: str | None = None

class SQLParams(Params):
  table: str

@overload
def parse(conn_str: str) -> KV[bytes]:
  ...
@overload
def parse(conn_str: str, type: type[T] | None = None) -> KV[T]:
  ...
def parse(conn_str: str, type: type[T] | None = None): # type: ignore
  parsed_url = urlparse(conn_str) # 'file://path/to/base?prefix=hello'
  scheme = parsed_url.scheme # 'file'
  netloc = parsed_url.netloc # 'path'
  path = unquote(parsed_url.path) # '/to/base'
  endpoint = netloc + path # 'path/to/base'
  query = parse_qs(parsed_url.query) # { 'prefix': ['hello'] }
  query = { k: v[0] for k, v in query.items() }

  params = Params(**query)
  type = type or params.type and parse_type(params.type) # type: ignore

  if scheme in ('http', 'https'):
    params = HTTPParams(**query)
    from kv.http import ClientKV
    url = f'{scheme}://{endpoint}'
    kv = ClientKV.new(url, type, token=params.token)

  elif scheme == 'azure+blob':
    params = AzureBlobParams(**query)
    from kv.azure import BlobKV, BlobContainerKV
    if params.container:
      kv = BlobContainerKV.from_conn_str(endpoint, params.container, type)
    else:
      kv = BlobKV.from_conn_str(endpoint, type)

  elif scheme == 'azure+cosmos':
    params = CosmosParams(**query)
    from kv.azure import CosmosKV, CosmosContainerKV, CosmosPartitionKV
    if params.container and params.partition:
      kv = CosmosPartitionKV.from_conn_str(endpoint, type, db=params.db, container=params.container, partition_key=params.partition)
    elif params.container:
      kv = CosmosContainerKV.from_conn_str(endpoint, type, db=params.db, container=params.container)
    else:
      kv = CosmosKV.from_conn_str(endpoint, type, db=params.db)

  elif scheme == 'sqlite':
    params = SQLParams(**query)
    from kv import SQLiteKV
    kv = SQLiteKV.at(endpoint, type, table=params.table)

  elif scheme.startswith('sql+'):
    params = SQLParams(**query)
    from kv import SQLKV
    proto = scheme.removeprefix('sql+')
    url = f'{proto}://{endpoint}'
    kv = SQLKV.new(url, type, table=params.table)

  elif scheme == 'file':
    from kv import FilesystemKV
    kv = FilesystemKV.new(endpoint, type)

  elif scheme.startswith('redis'):
    if scheme.startswith('redis+'):
      scheme = scheme.removeprefix('redis+')
    url = f'{scheme}://{endpoint}'
    from kv import RedisKV
    kv = RedisKV.from_url(url, type)

  else:
    raise ValueError(f'Unknown scheme: {scheme}')
  
  return kv.prefixed(params.prefix) if params.prefix else kv