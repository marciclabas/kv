# SQLite KV

The `SQLiteKV` stores items in a SQLite table with columns `(key, value)`

## Example

```python
from kv import KV, SQLiteKV
kv = KV.of('sqlite://path/to.db?table=kv', type=dict) # -> SQLiteKV

await kv.insert('key1', {'value': 1})
await kv.insert('key2', {'value': 2})
```

This will yield:

**Table `kv`**

| KEY (TEXT) | VALUE (JSON) |
|------------|--------------|
| `key1`       | `{'value': 1}` |
| `key2`       | `{'value': 2}` |

## Datatypes
- `bytes`: `BLOB` SQLite type
- `str`: `TEXT` SQLite type
- `dict|list|dataclass|etc`: `JSON` SQLite type, validated using [`pydantic`](https://docs.pydantic.dev/latest/)