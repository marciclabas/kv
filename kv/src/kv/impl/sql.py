from typing_extensions import AsyncIterable, TypeVar, Generic, Any, overload
from dataclasses import dataclass, replace
from pydantic import RootModel
from sqlalchemy import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.exc import DatabaseError
from sqlalchemy.types import BLOB, String
from sqlalchemy.dialects.postgresql.types import BYTEA
from sqltypes import ValidatedJSON
from kv import KV, KVError, InexistentItem

T = TypeVar('T')
U = TypeVar('U')

@dataclass
class SQLKV(KV[T], Generic[T]):
  """`KV` implementation over sqlalchemy"""

  Type: type[T]
  engine: Engine
  table: str
  prefix_: str = ''

  def __post_init__(self):

    class Base(DeclarativeBase):
      ...

    if self.Type is bytes:
      self.dump = lambda x: x
      self.parse = lambda x: x
      class Table(Base): # type: ignore
        __tablename__ = self.table
        key: Mapped[str] = mapped_column(primary_key=True)
        value: Mapped[bytes] = mapped_column(type_=BYTEA if self.engine.dialect.name == 'postgresql' else BLOB)

    elif self.Type is str:
      self.dump = lambda x: x
      self.parse = lambda x: x
      class Table(Base): # type: ignore
        __tablename__ = self.table
        key: Mapped[str] = mapped_column(primary_key=True)
        value: Mapped[str] = mapped_column(type_=String)
      
    else:
      self.Type = self.Type or Any
      Root = RootModel[self.Type]
      self.dump = lambda x: Root(x)
      self.parse = lambda x: x.root
      class Table(Base):
        __tablename__ = self.table
        key: Mapped[str] = mapped_column(primary_key=True)
        value: Mapped[RootModel[self.Type]] = mapped_column(type_=ValidatedJSON(Root)) # type: ignore

    self.Table = Table
    self.Base = Base
    Base.metadata.create_all(self.engine)


  @overload
  @classmethod
  def new(cls, conn_str: str, *, table: str) -> 'SQLKV[bytes]':
    ...
  @overload
  @classmethod
  def new(cls, conn_str: str, type: type[U] | None = None, *, table: str) -> 'SQLKV[U]':
    ...
  @classmethod
  def new(cls, conn_str: str, type: type[U] | None = None, *, table: str): # type: ignore
    from sqlalchemy import create_engine
    engine = create_engine(conn_str)
    return cls(type or bytes, engine, table=table) # type: ignore

  async def delete(self, key: str):
    key = self.prefix_ + key
    from sqlmodel import Session, select
    try:
      with Session(self.engine) as session:
        stmt = select(self.Table).where(self.Table.key == key)
        row = session.exec(stmt).first()
        if row is None:
          raise InexistentItem(key)
        session.delete(row)
        session.commit()
    except DatabaseError as e:
      raise KVError(e) from e

  async def read(self, key: str) -> T:
    from sqlmodel import Session, select
    key = self.prefix_ + key
    try:
      with Session(self.engine) as session:
        stmt = select(self.Table).where(self.Table.key == key)
        row = session.exec(stmt).first()
        if row is None:
          raise InexistentItem(key)
        return self.parse(row.value)
    except DatabaseError as e:
      raise KVError(e) from e

  async def insert(self, key: str, value: T):
    key = self.prefix_ + key
    from sqlmodel import Session, select
    try:
      with Session(self.engine) as session:
        stmt = select(self.Table).where(self.Table.key == key)
        row = session.exec(stmt).first()
        if row is not None:
          session.delete(row)
        session.add(self.Table(key=key, value=self.dump(value)))
        session.commit()
    except DatabaseError as e:
      raise KVError(e) from e

  async def keys(self) -> AsyncIterable[str]:
    from sqlmodel import Session, select
    try:
      with Session(self.engine) as session:
        stmt = select(self.Table.key).where(self.Table.key.like(self.prefix_ + '%'))
        for key in session.exec(stmt).all():
          yield key
    except DatabaseError as e:
      raise KVError(e) from e

  async def items(self, batch_size: int | None = None) -> AsyncIterable[tuple[str, T]]:
    from sqlmodel import Session, select
    try:
      with Session(self.engine) as session:
        result = session.exec(select(self.Table).where(self.Table.key.like(self.prefix_ + '%')))
        while (batch := result.fetchmany(batch_size)) != []:
          for row in batch:
            yield row.key, self.parse(row.value)
    except DatabaseError as e:
      raise KVError(e) from e


  async def clear(self):
    from sqlmodel import Session, delete
    try:
      with Session(self.engine) as session:
        session.execute(delete(self.Table))
        session.commit()
    except DatabaseError as e:
      raise KVError(e) from e
    
  
  def prefixed(self, prefix: str):
    new_prefix = self.prefix_.rstrip('/') + '/' + prefix.strip('/')
    return replace(self, prefix_=new_prefix.lstrip('/'))