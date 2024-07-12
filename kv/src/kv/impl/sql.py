from typing_extensions import AsyncIterable, TypeVar, Generic, Any, overload
from pydantic import RootModel
from haskellian import Either, Left, Right, promise as P, asyn_iter as AI
from sqlalchemy import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.exc import DatabaseError
from sqlalchemy.types import BLOB, String
from sqltypes import ValidatedJSON
from kv import KV, DBError, InexistentItem, InvalidData

T = TypeVar('T')
U = TypeVar('U')

class SQLKV(KV[T], Generic[T]):
  """`KV` implementation over sqlalchemy"""

  def __init__(self, Type: type[T], engine: Engine, *, table: str):
    self.engine = engine

    class Base(DeclarativeBase):
      ...

    if Type is bytes:
      self.dump = lambda x: x
      self.parse = lambda x: x
      class Table(Base): # type: ignore
        __tablename__ = table
        key: Mapped[str] = mapped_column(primary_key=True)
        value: Mapped[bytes] = mapped_column(type_=BLOB)

    elif Type is str:
      self.dump = lambda x: x
      self.parse = lambda x: x
      class Table(Base): # type: ignore
        __tablename__ = table
        key: Mapped[str] = mapped_column(primary_key=True)
        value: Mapped[str] = mapped_column(type_=String)
      
    else:
      Type = Type or Any
      Root = RootModel[Type]
      self.dump = lambda x: Root(x)
      self.parse = lambda x: x.root
      class Table(Base):
        __tablename__ = table
        key: Mapped[str] = mapped_column(primary_key=True)
        value: Mapped[RootModel[Type]] = mapped_column(type_=ValidatedJSON(Root))

    self.Table = Table
    Base.metadata.create_all(engine)

  def __repr__(self):
    return f'SQLKV(engine={self.engine!r}, table={self.Table.__tablename__!r}, type={self.Table.value.type!r})'

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
    return cls(type or bytes, engine, table=table)

  @P.lift
  async def delete(self, key: str) -> Either[DBError | InexistentItem, None]:
    from sqlmodel import Session, select
    try:
      with Session(self.engine) as session:
        stmt = select(self.Table).where(self.Table.key == key)
        row = session.exec(stmt).first()
        if row is None:
          return Left(InexistentItem(key))
        session.delete(row)
        session.commit()
        return Right(None)
    except DatabaseError as e:
      return Left(DBError(e))

  @P.lift
  async def read(self, key: str) -> Either[DBError | InvalidData | InexistentItem, T]:
    from sqlmodel import Session, select
    try:
      with Session(self.engine) as session:
        stmt = select(self.Table).where(self.Table.key == key)
        row = session.exec(stmt).first()
        if row is None:
          return Left(InexistentItem(key))
        return Right(self.parse(row.value))
    except DatabaseError as e:
      return Left(DBError(e))

  @P.lift
  async def insert(self, key: str, value: T) -> Either[DBError, None]:
    from sqlmodel import Session, select
    try:
      with Session(self.engine) as session:
        stmt = select(self.Table).where(self.Table.key == key)
        row = session.exec(stmt).first()
        if row is not None:
          session.delete(row)
        session.add(self.Table(key=key, value=self.dump(value)))
        session.commit()
        return Right(None)
    except DatabaseError as e:
      return Left(DBError(e))

  @AI.lift
  async def keys(self) -> AsyncIterable[Either[DBError, str]]:
    from sqlmodel import Session, select
    try:
      with Session(self.engine) as session:
        stmt = select(self.Table.key)
        for key in session.exec(stmt).all():
          yield Right(key)
    except DatabaseError as e:
      yield Left(DBError(e))

  @AI.lift
  async def items(self, batch_size: int | None = None) -> AsyncIterable[Either[DBError | InvalidData, tuple[str, T]]]:
    from sqlmodel import Session, select
    try:
      with Session(self.engine) as session:
        result = session.exec(select(self.Table))
        while (batch := result.fetchmany(batch_size)) != []:
          for row in batch:
            yield Right((row.key, self.parse(row.value)))
    except DatabaseError as e:
      yield Left(DBError(e))


  @P.lift
  async def clear(self):
    from sqlmodel import Session, delete
    try:
      with Session(self.engine) as session:
        session.execute(delete(self.Table))
        session.commit()
        return Right(None)
    except DatabaseError as e:
      return Left(DBError(e))