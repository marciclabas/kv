from typing import NamedTuple, TypeVar
from kv import KV
from kv.http import req

T = TypeVar('T')

class ParsedSQL(NamedTuple):
  conn_str: str
  table: str

def parse_sql(conn_str: str) -> ParsedSQL | None:
  import re
  sql_regex = re.compile(r'^sql\+.+://')
  match = sql_regex.match(conn_str)
  if match:
    pattern = re.compile(r"^sql\+")
    sql_str = pattern.sub("", conn_str)
    proto, url = sql_str.split("://")
    parts = url.rsplit(';', maxsplit=1)
    if len(parts) != 2 or not parts[1].lower().startswith("table="):
      raise ValueError(f"Missing table in connection string: '{conn_str[:16]}...'. Expected 'sql+<sql protocol>://<URL>;Table=<table>'")
    table = parts[1].split('=')[1]
    return ParsedSQL(conn_str=f'{proto}://{parts[0]}', table=table)
  
def parse(conn_str: str, type: type[T] | None = None) -> KV[T]:
    """Create a KV from a connection string. Supports:
    - `file://<path>`: `FilesystemKV`
    - `sqlite://<path>`: `SQLiteKV` (uses `sqlite3`)
    - `sql+<protocol>://<conn_str>;Table=<table>`: `SQLKV` (uses `sqlalchemy`)
    - `azure+blob://<conn_str>`: `BlobKV`
    - `azure+blob+container://<conn_str>;Container=<container_name>`: `BlobContainerKV`
    - `https://<endpoint>` (or `http://<endpoint>`): `ClientKV`
    - `https://<endpoint>;Token=<bearer>` (or `http://<endpoint>;Token=<bearer>`): `ClientKV`
    - `redis://...`, `rediss://...`, `redis+unix://...`: `RedisKV`
    """
    if conn_str.startswith('file://'):
        from kv import FilesystemKV
        _, path = conn_str.split('://', maxsplit=1)
        return FilesystemKV(path) if type is None else FilesystemKV.validated(type, path)
    
    if conn_str.startswith('redis'):
      if conn_str.startswith('redis+unix://'):
        conn_str = conn_str.removeprefix('redis+')
      from kv import RedisKV
      return RedisKV.from_url(conn_str, type)

    if conn_str.startswith("azure+blob://"):
        from kv.impl.azure import BlobKV
        _, conn_str = conn_str.split('://', maxsplit=1)
        return BlobKV.from_conn_str(conn_str, type)
    
    if conn_str.startswith("azure+blob+container://"):
        parts = conn_str.split('://')[1].rsplit(';', maxsplit=1)
        if len(parts) != 2 or not parts[1].lower().startswith("container="):
          raise ValueError("Invalid connection string. Expected 'azure+blob+container://<conn_str>;Container=<container_name>'")
        from kv.impl.azure import BlobContainerKV
        blob_conn_str = parts[0]
        container = parts[1].split('=')[1]
        return BlobContainerKV.from_conn_str(blob_conn_str, container, type)
    
    if conn_str.startswith("http://") or conn_str.startswith("https://"):
        from kv.http import ClientKV, bound_request, request
        if len(parts := conn_str.split(';')) == 2:
          conn_str = parts[0]
          token = parts[1].split('=')[1]
          req = bound_request(headers={'Authorization': f'Bearer {token}'})
        else:
          req = request
        if type:
          return ClientKV.validated(type, conn_str, request=req)
        else:
          return ClientKV(conn_str, request=req)
        
    if conn_str.startswith('sqlite://'):
      from kv.impl.sqlite import SQLiteKV
      _, rest = conn_str.split('://', maxsplit=1)
      if len(parts := rest.split(';')) == 2:
        path = parts[0]
        table = parts[1].split('=')[1]
        return SQLiteKV.at(path, type or bytes, table=table)
      else:
        path = parts[0]
        return SQLiteKV.at(path, type or bytes)

    if (parsed_sql := parse_sql(conn_str)) is not None:
        from sqlalchemy import create_engine
        from kv import SQLKV
        engine = lambda: create_engine(parsed_sql.conn_str)
        return SQLKV(type or bytes, engine, table=parsed_sql.table) # type: ignore
    
    raise ValueError(f"Invalid connection string: '{conn_str[:8]}...'. Expected 'file://<path>', 'sql+<protocol>://<conn_str>;Table=<table>', 'azure+blob://<conn_str>', or 'azure+blob+container://<conn_str>;Container=<container_name>'")