# Azure Cosmos KV

The `CosmosKV` stores items as blobs in an Azure Cosmos DB. You can also use:
- `CosmosContainerKV` to store items in a specific container
- `CosmosPartitionKV` to store items in a specific partition (within a container)

## Example

```python
from kv import KV, CosmosKV, CosmosContainerKV, CosmosPartitionKV

cosmos = KV.of('azure+cosmos://<connection string>', type=dict)
# CosmosKV

container = KV.of('azure+cosmos://<connection string>?container=user1')
# CosmosContainerKV

partition = KV.of('azure+cosmos://<connection string>?container=user1&partition=partition1')
# CosmosPartitionKV

await cosmos.insert('key1', {'value': 1})
await container.insert('key2', {'value': 2})
await partition.insert('key3', {'value': 3})
```

This will yield:
```
default/
  default/
    key1

user1/
  default/
    key2
  partition1/
    key3
```

## Datatypes

All types are serialized as JSON using [`pydantic`](https://docs.pydantic.dev/latest/). `CosmosKV` **doesn't support binary data.**


## Prefixing

Prefixing a `CosmosKV` a single level yields a `CosmosContainerKV`. Prefixing two levels yields a `CosmosPartitionKV`. Further nesting falls back to prefixing blob names.

These single-level examples are equivalent:

```python
KV.of('azure+cosmos://<connection string>').prefix('container')
KV.of('azure+cosmos://<connection string>?container=container')
```

And so are these double-level examples:
```python
KV.of('azure+cosmos://<connection string>').prefix('container').prefix('partition')
KV.of('azure+cosmos://<connection string>').prefix('container/partition')
KV.of('azure+cosmos://<connection string>?container=container').prefix('partition')
KV.of('azure+cosmos://<connection string>?container=container&partition=partition')
```
