# Getting Started

The easiest way to use `KV` is via URLs:

```python
from kv import KV

kv = KV.of('sqlite://path/to/db.sqlite?table=kv', type=dict)
```

### CRUD

A `KV` has the CRUD methods you'd expect:

| Method | Example |
|--------|---------|
| `insert` | `await kv.insert('user1', {'name': 'John', 'email': '...'}) ` |
| `read` | `await kv.read('user1') # dict` |
| `has` | `await kv.has('user1') # bool` |
| `keys` | `kv.keys() # AsyncIterable[str]` |
| `values` | `kv.values() # AsyncIterable[dict]` |
| `items` | `kv.items() # AsyncIterable[tuple[str, dict]]` |
| `clear` | `await kv.clear()` |


### Cross-KV Operations

You can also copy and move data between `KV`s:

| Method | Example |
|--------|---------|
| `copy` | `await kv.copy('user1', other_kv, to_key='other-user1')` |
| `move` | `await kv.move('user1', other_kv, to_key='other-user1')` |
