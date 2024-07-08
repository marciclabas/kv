from typing_extensions import NamedTuple as _NamedTuple

class Query(_NamedTuple):
  """Query `q` to be run via `conn.execute(*q)`"""
  query: str
  params: list

def quote(unsafe: str) -> str:
  """Safely quote user input"""
  unquoted = unsafe.replace('"', '')
  return f'"{unquoted}"'

def create(table: str, dtype: str) -> Query:
  """Create a key-value table"""
  query = f'''
  CREATE TABLE IF NOT EXISTS {quote(table)} (
      KEY TEXT PRIMARY KEY,
      VALUE {quote(dtype)}
    )
  '''
  return Query(query, [])

def clear(table: str) -> Query:
  query = f'DELETE FROM {quote(table)}'
  return Query(query, [])

def insert(key: str, value, table: str) -> Query:
  query = f'INSERT INTO {quote(table)} (KEY, VALUE) VALUES (?, ?)'
  return Query(query, [key, value])

def update(key: str, value, table: str) -> Query:
  query = f'UPDATE {quote(table)} SET VALUE = ? WHERE KEY = ?'
  return Query(query, [value, key])

def read(key: str, table: str) -> Query:
  query = f'SELECT VALUE FROM {quote(table)} WHERE KEY = ?'
  return Query(query, [key])

def has(key: str, table: str) -> Query:
  query = f'SELECT KEY FROM {quote(table)} WHERE KEY = ?'
  return Query(query, [key])

def keys(table: str) -> Query:
  query = f'SELECT KEY FROM {quote(table)}'
  return Query(query, [])

def items(table: str) -> Query:
  query = f'SELECT KEY, VALUE FROM {quote(table)}'
  return Query(query, [])

def delete(key: str, table: str) -> Query:
  query = f'DELETE FROM {quote(table)} WHERE KEY = ?'
  return Query(query, [key])

def upsert(key: str, value, table: str) -> Query:
  query = f'''
    INSERT INTO {quote(table)} (KEY, VALUE) VALUES (?1, ?2)
    ON CONFLICT(KEY) DO UPDATE SET VALUE=?2
  '''
  return Query(query, [key, value])