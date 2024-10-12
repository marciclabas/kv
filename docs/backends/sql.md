# SQL KV

The `SQLKV` stores items in a SQLite table with columns `(key, value)`. It uses [`sqlalchemy`](https://www.sqlalchemy.org/) to connect to the database.

## Example

```python
from kv import KV, SQLKV
kv = KV.of('sql+postgresql://user:passowrd@mydatabase.com/db?table=kv') # -> SQLKV
kv = KV.of('sql+sqlite:///path/to.db?table=kv') # -> SQLKV
# any backend supported by sqlalchemy
kv = KV.of('sql+<sqlalchemy connection string>?table=kv') # -> SQLKV

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
- `bytes`: `BLOB` SQLAlchemy type
- `str`: `String` SQLAlchemy type
- `dict|list|dataclass|etc`: `JSON` SQLAlchemy type, validated using [`pydantic`](https://docs.pydantic.dev/latest/)