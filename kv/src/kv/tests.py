from typing import TypeVar, Mapping
from haskellian import either as E, Left
from dslog import Logger
from kv import KV

T = TypeVar('T')

async def test_str(kv: KV[str], logger: Logger = Logger.click().prefix('[TEST]')):
  return await test(kv, {'a': '1', 'b': '2', 'c': '3'}, logger=logger)

async def test_int(kv: KV[int], logger: Logger = Logger.click().prefix('[TEST]')):
  return await test(kv, {'a': 1, 'b': 2, 'c': 3}, logger=logger)

async def test_float(kv: KV[float], logger: Logger = Logger.click().prefix('[TEST]')):
  return await test(kv, {'a': 1.0, 'b': 2.0, 'c': 3.0}, logger=logger)

async def test_bool(kv: KV[bool], logger: Logger = Logger.click().prefix('[TEST]')):
  return await test(kv, {'a': True, 'b': False}, logger=logger)

async def test_bytes(kv: KV[bytes], logger: Logger = Logger.click().prefix('[TEST]')):
  return await test(kv, {'a': b'1', 'b': b'2', 'c': b'3'}, logger=logger)

async def test_dict(kv: KV[dict], logger: Logger = Logger.click().prefix('[TEST]')):
  return await test(kv, {'a': {'a': 1}, 'b': {'b': 2}, 'c': {'c': 3}}, logger=logger)

async def test_list(kv: KV[list], logger: Logger = Logger.click().prefix('[TEST]')):
  return await test(kv, {'a': [1], 'b': [2], 'c': [3]}, logger=logger)

@E.do()
async def test(kv: KV[T], items: Mapping[str, T], logger: Logger = Logger.click().prefix('[TEST]')):
  assert (await kv.keys().map(E.unsafe).sync()) == [], 'KV must be empty for testing'
  ok = True

  logger('Inserting items...')
  for k, v in items.items():
    (await kv.insert(k, v)).unsafe()
  logger('Inserted items OK')

  logger('Testing point read...')
  for k, v in items.items():
    if (r := (await kv.read(k)).unsafe()) != v:
      logger('Point read error. Expected:', v, 'Got:', r, level='ERROR')
      ok = False
  logger('Point read OK')

  logger('Testing keys...')
  keys = await kv.keys().map(E.unsafe).sync()
  if set(keys) != set(items.keys()):
    logger('Keys error. Expected:', set(items.keys()), 'Got:', set(keys), level='ERROR')
    ok = False
  logger('Keys OK')

  logger('Testing point delete...')
  for k in items.keys():
    (await kv.delete(k)).unsafe()
    r = (await kv.read(k))
    if r.tag != 'left' or r.value.reason != 'inexistent-item':
      logger('Point delete error. Expected: Left("inexistent-item") Got:', r, level='ERROR')
      ok = False
  logger('Point delete OK')

  logger('Reinserting items...', level='DEBUG')
  for k, v in items.items():
    (await kv.insert(k, v)).unsafe()

  logger('Testing clear...')
  (await kv.clear()).unsafe()
  keys = await kv.keys().map(E.unsafe).sync()
  if keys != []:
    logger('Clear error. Expected: [] Got:', keys, level='ERROR')
    ok = False
  logger('Clear OK')

  if not ok:
    Left('Some tests failed').unsafe()