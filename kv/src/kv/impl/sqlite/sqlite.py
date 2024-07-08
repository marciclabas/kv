from typing_extensions import TypeVar, Generic
from dataclasses import dataclass
from haskellian import Left, Right, Either, either as E, promise as P, asyn_iter as AI
from kv import KV, InexistentItem, DBError
from kv.serialization import Parse, Dump, default, serializers
import sqlite3
import os
from . import queries

T = TypeVar('T')
M = TypeVar('M')

@dataclass
class SQLiteKV(KV[T], Generic[T]):

  conn: sqlite3.Connection
  db_path: str
  table: str = 'kv'
  parse: Parse[T] = default[T].parse
  dump: Dump[T] = default[T].dump
  dtype: str = 'TEXT'
  batch_size: int = 256

  @staticmethod
  def at(db_path: str, Type: type[T], *, table: str = 'kv') -> 'SQLiteKV[T]':
    dir = os.path.dirname(db_path)
    if dir != '':
      os.makedirs(dir, exist_ok=True)
    if Type is bytes:
      return SQLiteKV(sqlite3.connect(db_path), db_path, table, dtype='BLOB', **default[T].serializers)
    elif Type is str:
      return SQLiteKV(sqlite3.connect(db_path), db_path, table, dtype='TEXT', **default[T].serializers)
    else:
      return SQLiteKV(sqlite3.connect(db_path), db_path, table, dtype='JSON', **serializers(Type))

  def __post_init__(self):
    self.conn.execute(*queries.create(self.table, self.dtype))

  def execute(self, query: queries.Query) -> Either[DBError, sqlite3.Cursor]:
    """Safely execute `query` on `self.conn`"""
    try:
      cur = self.conn.execute(*query)
      self.conn.commit()
      return Right(cur)
    except sqlite3.Error as err:
      return Left(DBError(str(err)))

  @P.lift
  @E.do()
  async def insert(self, key: str, value: T):
    data = self.dump(value)
    string = data.decode() if isinstance(data, bytes) else data
    self.execute(queries.upsert(key, string, table=self.table)).unsafe()
  
  @P.lift
  @E.do()
  async def has(self, key: str):
    cur = self.execute(queries.read(key, table=self.table)).unsafe()
    return cur.fetchone() is not None
    
  @P.lift
  @E.do()
  async def read(self, key: str):
    cur = self.execute(queries.read(key, table=self.table)).unsafe()
    if (data := cur.fetchone()) is None:
      return Left(InexistentItem(key)).unsafe()
    else:
      return self.parse(data[0]).unsafe()

  @P.lift
  @E.do()
  async def delete(self, key: str):
    cur = self.execute(queries.delete(key, table=self.table)).unsafe()
    if cur.rowcount == 0:
      return Left(InexistentItem(key)).unsafe()
  
  @AI.lift
  async def keys(self):
    cur = self.execute(queries.keys(self.table)).unsafe()
    for [key] in cur.fetchall():
      yield Right(key)

  @AI.lift
  async def items(self):
    match self.execute(queries.items(self.table)):
      case Right(cur):
        while (batch := cur.fetchmany(self.batch_size)) != []:
          for k, v in batch:
            yield self.parse(v).fmap(lambda v: (k, v))
      case Left(err):
        yield Left(err)
  
  @P.lift
  async def clear(self):
    return self.execute(queries.clear(self.table))