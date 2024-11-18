from typing import TypeVar
from datetime import datetime
from pydantic import TypeAdapter
import jwt
from fastapi import FastAPI, Response, Request, HTTPException
from kv import KV, InexistentItem

T = TypeVar('T')

def verify_token(*, token: str, secret: str, now: datetime | None = None) -> bool:
  now = now or datetime.now()
  try:
    exp = jwt.decode(token, secret, algorithms=['HS256'], options={'verify_exp': False}).get('exp')
    return exp is None or now < datetime.fromtimestamp(exp)
  except jwt.PyJWTError:
    return False

def ServerKV(kv: KV[T], *, type: type[T], secret: str | None = None):

  app = FastAPI(generate_unique_id_function=lambda r: r.name)

  if type is not bytes:
    adapter = TypeAdapter(type)
    parse = adapter.validate_json
    dump = adapter.dump_json
    media_type = 'application/json'
  else:
    parse = lambda x: x
    dump = lambda x: x
    media_type = 'application/octet-stream'

  if secret:
    @app.middleware('http')
    async def check_token(req: Request, call_next):
      token = req.query_params.get('token')
      if token and verify_token(token=token, secret=secret):
        return await call_next(req)
      else:
        return Response(status_code=401, content='Unauthorized')
      
  def _kv(prefix: str):
    return prefix and kv.prefixed(prefix) or kv
  
  @app.post('/item/{key:path}')
  async def insert(key: str, *, req: Request, prefix: str = ''):
    value = parse(await req.body())
    await _kv(prefix).insert(key, value)

  @app.get('/item/{key:path}')
  async def read(key: str, *, prefix: str = ''):
    try:
      item = await _kv(prefix).read(key)
      return Response(content=dump(item), media_type=media_type)
    except InexistentItem:
      raise HTTPException(status_code=404, detail=f'Inexistent Item "{key}"')
  
  @app.get('/item/{key:path}/has')
  async def has(key: str, *, res: Response, prefix: str = '') -> bool:
    has = await _kv(prefix).has(key)
    res.status_code = 200 if has else 404
    return has
  
  @app.delete('/item/{key:path}')
  async def delete(key: str, *, prefix: str = ''):
    try:
      await _kv(prefix).delete(key)
    except InexistentItem:
      return Response(status_code=404, content=f'Inexistent Item "{key}"')

  @app.get('/keys')
  async def keys(prefix: str = ''):
    return [key async for key in _kv(prefix).keys()]
  
  @app.delete('/')
  async def clear(prefix: str = ''):
    print(f'Deleting at prefix: {prefix}')
    await _kv(prefix).clear()

  return app