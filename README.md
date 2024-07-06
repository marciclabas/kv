# KV

> Async, exception-free key-value store ABC. Implementations over SQLAlchemy, the filesystem, Azure Blob, and more

```bash
pip install python-kv
```

## Usage

```python
from kv import KV

kv = KV.of('sqlite://path/to/db.sqlite') # easiest way to switch backends: connection strings

await kv.insert('key', 'value') # Left[DBError] | Right[None]
await kv.read('key') # Left[ReadError] | Right['value']
await kv.delete('key') # Left[DBError] | Right[None]
[k async for k in kv.keys()] # list[Left[ReadError] | Right[T]]
[it async for it in kv.items()] # list[Left[ReadError] | Right[tuple[str, T]]]
await kv.clear() # Left[DBError] | Right[None]
# and a few more
```

## Serialization & Validation
  
```python
from dataclasses import dataclass

@dataclass
class MySerializableType:
  a: int
  b: str

kv = KV.of('sql+sqlite://...', MySerializableType)
await kv.insert('key', MySerializableType(1, '2')) # Left[ValidationError] | Right[None]
# etc.
```

## CLI
  
```bash
kv serve CONN_STR --token "shhhhh" --port 8080 --type dict
```


## TODO
- ABC: OK
- Dict: OK
- Filesystem: OK
- SQL: OK
- HTTP: OK
- `kv serve` CLI: OK
- `BlobKV` with prefix
- Deptrecate sqlite in favor of SQLAlchemy

## Maybe, Ideas
- Redis
- `kv read, insert, etc.` CLI