from typing import TypeVar, Any, ParamSpec, Callable, Awaitable, overload
from fastapi import FastAPI, Response, status as st, Request
from fastapi.responses import JSONResponse
from pydantic import TypeAdapter
from haskellian import either as E, Either, Left, Right, kwargs as kw
from kv import KV, ReadError, InvalidData, DBError
from ..serialization import Parse, Dump, default, serializers

T = TypeVar('T')

def _api(
  kv: KV[T], *, token: str | None = None,
  parse: Parse[T] = default[T].parse,
  dump: Dump[T] = default[T].dump,
):

  app = FastAPI(generate_unique_id_function=lambda r: r.name)

  if token:
    @app.middleware('http')
    async def check_token(req: Request, call_next):
      if req.headers.get('Authorization') != f'Bearer {token}':
        return Response(status_code=st.HTTP_401_UNAUTHORIZED)
      return await call_next(req)

  @app.post('/insert')
  async def insert(key: str, req: Request, res: Response) -> Either[DBError|InvalidData, None]:
    body = await req.body()
    match parse(body):
      case Right(val):
        if (r := await kv.insert(key, val)).tag == 'left':
          res.status_code = 500
        return r
      case Left(e):
        res.status_code = st.HTTP_422_UNPROCESSABLE_ENTITY
        return Left(e)

  @app.get('/read')
  async def read(key: str):
    e = await kv.read(key)
    if e.tag == 'right':
      return Response(content=dump(e.value))
    else:
      content = TypeAdapter(ReadError).dump_json(e.value)
      status = 404 if e.value.reason == 'inexistent-item' else 500
      return Response(content, status_code=status)
  
  @app.get('/has')
  async def has(key: str, res: Response) -> Either[DBError, bool]:
    r = await kv.has(key)
    if r.tag == 'left':
      res.status_code = 500
    return r
  
  @app.get('/keys')
  async def keys():
    return await kv.keys().map(lambda e: e.mapl(DBError)).sync()
  
  @app.delete('/delete')
  async def delete(key: str, res: Response):
    e = await kv.delete(key)
    if e.tag == 'left':
      res.status_code = 500
  

  @app.delete('/clear')
  async def clear(res: Response):
    e = await kv.clear()
    if e.tag == 'left':
      res.status_code = 500
  
  return app


@overload
def ServerKV(
  kv: KV[T], type: None = None, *, token: str | None = None,
  parse: Parse[T] = default[T].parse,
  dump: Dump[T] = default[T].dump
) -> FastAPI:
  ...

@overload
def ServerKV(kv: KV[T], type: type[T], *, token: str | None = None) -> FastAPI:
  ...

def ServerKV(kv, type = None, *, parse = Right, dump = lambda x: x, token = None): # type: ignore
  if type is None:
    return _api(kv, parse=parse, dump=dump, token=token)
  else:
    return _api(kv, **serializers(type), token=token)