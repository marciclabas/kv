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

Or you can use the CLI:

```bash
kv serve 'file://path/to/folder'
```

### Client

```python
from kv import KV

kv = KV.of('http://localhost:8000')
```

## Authentication

#### Server

```python
import uvicorn
from kv import KV, ServerKV

kv = KV.of('file://path/to/folder')
api = ServerKV(kv, secret='supersecret')

uvicorn.run(api, host='0.0.0.0', port=8000)
```

#### Client

```python
from datetime import datetime, timedelta
from kv import KV

kv = KV.of('http://localhost:8000?secret=supersecret')

kv.url('key', expiry=datetime.now() + timedelta(minutes=2)) # http://localhost:8000/item/key?token=<JWT>
```