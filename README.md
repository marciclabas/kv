# KV

`KV` is an async key-value store interface for Python. It provides a simple API to store serializable objects.

```bash
pip install python-kv
```

`KV` supports multiple backends, including the filesystem, SQLite, Redis, Azure Blob, and many more.

```python
from kv import KV

kv = KV.of('sql+sqlite:///path/to/db.sqlite', type=dict) 

await kv.insert('hello', {'world': 42})
await kv.read('hello') # {'world': 42}
await kv.delete('hello')
async for k, v in kv.items():
  ...
await kv.clear()
```

[**Read the docs!**](https://marciclabas.github.io/kv/)