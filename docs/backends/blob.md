# Azure Blob KV

The `BlobKV` stores items as blobs in an Azure Blob Storage service. You can also use `BlobContainerKV` to store items in a specific container.

## Example

```python
from kv import KV, BlobKV, BlobContainerKV
blob = KV.of('azure+blob://<connection string>', type=dict)
# BlobKV

container = KV.of('azure+blob://<connection string>?container=user1')
# BlobContainerKV

await blob.insert('key1', {'value': 1})
await container.insert('nested/key2', {'value': 2})
await container.insert('key3', {'value': 3})
```

This will yield:
```
default-container/
  key1

user1/
  nested/
    key2
  key3
```

## Datatypes
- `str|bytes`: written as-is
- `dict|list|dataclass|etc`: validated as JSON using [`pydantic`](https://docs.pydantic.dev/latest/)

## Prefixing

Prefixing a `BlobKV` a single level yields a `BlobContainerKV`. Further nesting falls back to prefixing blob names.

```python
blob = KV.of('azure+blob://<connection string>') # -> BlobKV

container = blob.prefix('user1') # -> BlobContainerKV
# equivalent to:
container = KV.of('azure+blob://<connection string>?container=user1')
```