# Supported Backends

Multiple backends are supported by default. And you can always roll out your own!
  

### Installation

To install specific backends, install the corresponding extras. For example:

```bash
pip install python-kv[fs,redis]
```

### Schemas

To access a specific backend, simplest is to use URLs. For example:
  
```python 
from kv import KV
kv = KV.of('file://path/to/folder')
```

### Supported Backends
| Backend | Example URL  | Installation |
|---------|-------- | ------------ |
| [Filesystem](backends/filesystem.md) | `file://path/to/folder` | No deps |
| [SQL](backends/sql.md) (`sqlalchemy`) | `sql+postgresql://user:pass@host/db` | `python-kv[sql]` |
| Redis | `redis://localhost:6379/0` | `python-kv[redis]` |
| [Azure Blob Storage](backends/blob.md) | `azure+blob://<connection_string>` | `python-kv[blob]` |
| [Azure Cosmos DB](backends/cosmos.md) | `azure+cosmos://<connection_string>` | `python-kv[cosmos]` |
| [HTTP Client](backends/http.md) | `http://example.com/kv` | `python-kv[client]` |

Next up, a powerful mechanism: [prefixing](prefixing.md)