from typing import TypeVar, Any, ParamSpec, Callable, Awaitable, overload
from functools import wraps
import inspect
from fastapi import FastAPI, Response, status as st, Request
from pydantic import TypeAdapter
from haskellian import either as E, Either, Right, kwargs as kw
from kv import KV, ReadError, InvalidData, DBError

A = TypeVar('A')
Ps = ParamSpec('Ps')

def status(e: Either[ReadError, Any]):
  if e.tag == 'right':
    return st.HTTP_200_OK
  elif e.value.reason == 'db-error' or e.value.reason == 'invalid-data':
    return st.HTTP_500_INTERNAL_SERVER_ERROR
  else:
    return st.HTTP_404_NOT_FOUND

def with_status(func: Callable[Ps, Awaitable[Either[ReadError, A]]]):
  @wraps(func)
  async def wrapper(response: Response, *args: Ps.args, **kwargs: Ps.kwargs) -> ReadError | A:
    e = await func(*args, **kwargs)
    response.status_code = status(e)
    return e.value
  wrapper.__signature__ = kw.add_kw(inspect.signature(wrapper), 'response', Response) # type: ignore
  return wrapper

def _api(
  kv: KV[A], *,
  parse: Callable[[bytes], Either[InvalidData, A]] = Right, # type: ignore
  dump: Callable[[A], bytes|str] = lambda x: x, # type: ignore
  token: str | None = None
):

  app = FastAPI(generate_unique_id_function=lambda r: r.name)

  if token:
    @app.middleware('http')
    async def check_token(req: Request, call_next):
      if req.headers.get('Authorization') != f'Bearer {token}':
        return Response(status_code=st.HTTP_401_UNAUTHORIZED)
      return await call_next(req)

  @app.post('/insert')
  @with_status
  @E.do[ReadError]()
  async def insert(key: str, req: Request):
    body = await req.body()
    val = parse(body).unsafe()
    return (await kv.insert(key, val)).unsafe()
  
  @app.get('/read')
  async def read(key: str):
    e = await kv.read(key)
    if e.tag == 'right':
      return Response(content=dump(e.value))
    else:
      content = TypeAdapter(ReadError).dump_json(e.value)
      return Response(content, status_code=status(e))
  
  @app.get('/has')
  @with_status
  async def has(key: str):
    return await kv.has(key)
  
  @app.get('/keys')
  @with_status
  async def keys():
    return E.sequence(await kv.keys().sync()).mapl(DBError)
  
  @app.delete('/delete')
  @with_status
  async def delete(key: str):
    return await kv.delete(key)
  

  @app.delete('/clear')
  @with_status
  async def clear():
    return await kv.clear()
  
  return app


@overload
def api(
  kv: KV[A], *,
  parse: Callable[[bytes], Either[InvalidData, A]] = Right, # type: ignore
  dump: Callable[[A], bytes|str] = lambda x: x, # type: ignore
  token: str | None = None
) -> FastAPI:
  ...

@overload
def api(kv: KV[A], type: type[A], *, token: str | None = None) -> FastAPI:
  ...

def api(kv, type = None, *, parse = Right, dump = lambda x: x, token = None): # type: ignore
  if type is not None:
    from pydantic import RootModel
    Model = RootModel[type]
    parse = lambda b: E.validate_json(b, Model).fmap(lambda x: x.root).mapl(InvalidData)
    dump = lambda x: Model(x).model_dump_json(exclude_none=True, exclude_defaults=True)
  return _api(kv, parse=parse, dump=dump, token=token)