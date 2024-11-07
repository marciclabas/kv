from typing import TypeVar, Mapping
from kv import KV, InexistentItem

T = TypeVar('T')

async def test_str(kv: KV[str]):
  return await test(kv, {'a': '1', 'b': '2', 'c': '3'})

async def test_int(kv: KV[int]):
  return await test(kv, {'a': 1, 'b': 2, 'c': 3})

async def test_float(kv: KV[float]):
  return await test(kv, {'a': 1.0, 'b': 2.0, 'c': 3.0})

async def test_bool(kv: KV[bool]):
  return await test(kv, {'a': True, 'b': False})

async def test_bytes(kv: KV[bytes]):
  return await test(kv, {'a': b'1', 'b': b'2', 'c': b'3'})

async def test_dict(kv: KV[dict]):
  return await test(kv, {'a': {'a': 1}, 'b': {'b': 2}, 'c': {'c': 3}})

async def test_list(kv: KV[list]):
  return await test(kv, {'a': [1], 'b': [2], 'c': [3]})

async def test(kv: KV[T], items: Mapping[str, T]):
  keys = [key async for key in kv.keys()]
  if keys != []:
    raise ValueError('KV must be empty for testing')
  
  errors = []

  for k, v in items.items():
    await kv.insert(k, v)

  for k, v in items.items():
    if (r := await kv.read(k)) != v:
      errors.append(f'Point read error. Expected: {v} Got: {r}')

  keys = [key async for key in kv.keys()]
  if set(keys) != set(items.keys()):
    errors.append(f'Keys error. Expected: {set(items.keys())} Got: {set(keys)}')

  for k in items.keys():
    await kv.delete(k)
    try:
      r = (await kv.read(k))
    except InexistentItem:
      ...
    else:
      errors.append(f'Point delete error. Expected error. Got: {r}')

  for k, v in items.items():
    await kv.insert(k, v)

  await kv.clear()
  keys = [key async for key in kv.keys()]
  if keys != []:
    errors.append(f'Clear error. Expected: [] Got: {keys}')

  return errors