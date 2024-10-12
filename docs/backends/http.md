# HTTP Client/Server KV

You can serve any `KV` using `ServerKV`, and access it using `ClientKV`.

## Example

### Server

```python
import uvicorn
from kv import KV, ServerKV

kv = KV.of('file://path/to/folder')
api = ServerKV(kv) # FastAPI

uvicorn.run(api, host='0.0.0.0', port=8000)
```

### Client

```python
from kv import KV

kv = KV.of('http://localhost:8000')
```

## Authentication

### Built-in Token Mechanism

Simplest is to use this built-in mechanism for token-based authentication:

#### Server

```python
import uvicorn
from kv import KV, ServerKV

kv = KV.of('file://path/to/folder')
api = ServerKV(kv, token='supersecret') # FastAPI

uvicorn.run(api, host='0.0.0.0', port=8000)
```

#### Client

```python
from kv import KV

kv = KV.of('http://localhost:8000?token=supersecret')
```


### Custom Authentication

There's no authentication by default. You can easily use any mechanism you want.

#### Server

Use `fastapi`'s middleware:

```python
import uvicorn
from kv import KV, ServerKV

kv = KV.of('file://path/to/folder')
api = ServerKV(kv, token='supersecret') # FastAPI

@api.middleware('http')
async def auth(request, call_next):
  token = request.query_params.get('token')
  if token != 'supersecret':
    return JSONResponse(status_code=401, content={'error': 'Unauthorized'})
  return await call_next(request)


uvicorn.run(api, host='0.0.0.0', port=8000)
```

#### Client

Pass a custom request function:

```python
from typing import Literal, Mapping
from kv.http import ClientKV, Request

async def my_request(
  self, method: Literal['GET', 'POST', 'DELETE'], url: str, /, *,
  data: bytes | str | None = None, params: Mapping[str, str] = {}
) -> Request:
  ...

kv = ClientKV('http://localhost:8000', request=my_request)
```
