from typing_extensions import AsyncIterable, TypeVar, Generic, Callable
from pydantic import RootModel
from haskellian import Either, Left, Right, promise as P, asyn_iter as AI
from sqlalchemy import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.exc import DatabaseError
from sqltypes import PydanticModel
from kv import KV, DBError, InexistentItem, InvalidData

T = TypeVar('T')

class SQLKV(KV[T], Generic[T]):
  """`KV` implementation over sqlalchemy"""

  def __init__(self, Type: type[T], engine: Callable[[], Engine], table: str = 'kv'):
    self.Type = RootModel[Type]
    self.engine = engine

    class Base(DeclarativeBase):
      ...

    class Table(Base):
      __tablename__ = table
      key: Mapped[str] = mapped_column(primary_key=True)
      value: Mapped[RootModel[Type]] = mapped_column(type_=PydanticModel(self.Type))

    self.Table = Table
    Base.metadata.create_all(engine())

  @P.lift
  async def delete(self, key: str) -> Either[DBError | InexistentItem, None]:
    from sqlmodel import Session, select
    try:
      with Session(self.engine()) as session:
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
      with Session(self.engine()) as session:
        stmt = select(self.Table).where(self.Table.key == key)
        row = session.exec(stmt).first()
        if row is None:
          return Left(InexistentItem(key))
        return Right(row.value.root)
    except DatabaseError as e:
      return Left(DBError(e))

  @P.lift
  async def insert(self, key: str, value: T) -> Either[DBError, None]:
    from sqlmodel import Session, select
    try:
      with Session(self.engine()) as session:
        stmt = select(self.Table).where(self.Table.key == key)
        row = session.exec(stmt).first()
        if row is not None:
          session.delete(row)
        session.add(self.Table(key=key, value=self.Type(value)))
        session.commit()
        return Right(None)
    except DatabaseError as e:
      return Left(DBError(e))

  @AI.lift
  async def keys(self) -> AsyncIterable[Either[DBError, str]]:
    from sqlmodel import Session, select
    try:
      with Session(self.engine()) as session:
        stmt = select(self.Table.key)
        for key in session.exec(stmt):
          yield Right(key)
    except DatabaseError as e:
      yield Left(DBError(e))

  @AI.lift
  async def items(self, batch_size: int | None = None) -> AsyncIterable[Either[DBError | InvalidData, tuple[str, T]]]:
    from sqlmodel import Session, select
    try:
      with Session(self.engine()) as session:
        result = session.exec(select(self.Table))
        while (batch := result.fetchmany(batch_size)) != []:
          for row in batch:
            yield Right((row.key, row.value.root))
    except DatabaseError as e:
      yield Left(DBError(e))


  @P.lift
  async def clear(self):
    from sqlmodel import Session, delete
    try:
      with Session(self.engine()) as session:
        session.execute(delete(self.Table))
        session.commit()
        return Right(None)
    except DatabaseError as e:
      return Left(DBError(e))