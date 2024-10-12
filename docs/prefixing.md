# Prefixing

Prefixing is a very simple but powerful feature of `KV`. It allows you to namespace keys and search by prefix.

## Example

Let's say we're storing documents per user. We can prefix each's documents by their ID, so that:

- No conflicts occur between users
- We can quickly query all documents for a user

### Namespacing

```python
from kv import KV

kv = KV.of('file://users', type=dict)
kv1 = kv.prefix('user1')
kv2 = kv.prefix('user2')

await kv1.insert('settings', {'theme': 'dark'})
await kv2.insert('settings', {'theme': 'light'})
```

This will yield:
```
users/
  user1/
    settings.json
  user2/
    settings.json
```

### Prefix Querying

Say we want to get all documents for `user1`:

```python
user1_keys = [key async for key in kv.prefix('user1').keys()] # ['settings']
```

### Nested Prefixing

You can imagine a complexer scenario with nested namespacing. No problem at all!

```python
kv.prefix('user1/images/cats')
```

## Backend Support

Prefixing has a default implementation using string prefixes. Specific backends may provide more efficient implementations (like the filesystem).

To check out the details, go to the [Supported Backends](supported-backends.md) page.