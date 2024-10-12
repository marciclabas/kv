# Filesystem KV

The `FilesystemKV` stores items as files in a (nested) directory.

## Example

```python
from kv import KV, FilesystemKV
kv = KV.of('file://path/to/folder', type=dict) # -> FilesystemKV

await kv.insert('key1', {'value': 1})
await kv.insert('nested/key2', {'value': 2})
await kv.insert('key3', {'value': 3})
```

This will yield:
```
path/to/folder/
  key1.json
  nested/
    key2.json
  key3.json
```

## File Extensions
By default, the extension is determined by the data type:

```python
KV.of('file://path/to/folder', type=dict) # -> .json
KV.of('file://path/to/folder', type=str) # -> .txt
KV.of('file://path/to/folder', type=bytes) # -> (no extension)
```

Or you can specify it manually:

```python
KV.of('file://path/to/folder?extension=.jpg') # -> .jpg
```

## Datatypes
- `str|bytes`: written as-is
- `dict|list|dataclass|etc`: validated as JSON using [`pydantic`](https://docs.pydantic.dev/latest/)

## Prefixing
The filesystem backend supports prefixing by creating nested directories.

All of these are equivalent:

```python
KV.of('file://users').prefix('user1').prefix('nested')
KV.of('file://users').prefix('user1/nested')
KV.of('file://users/user1').prefix('nested')
KV.of('file://users/user1/nested')
```