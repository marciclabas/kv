import os
import typer
from haskellian import either as E

app = typer.Typer()

@app.callback()
def callback(debug: bool = typer.Option(False, '--debug', help='Enable debug mode')):
  if debug:
    import debugpy
    debugpy.listen(5678)
    print('Waiting for debugger attach...')
    debugpy.wait_for_client()

@app.command()
def test(conn_str: str, verbose: bool = typer.Option(False, '-v', '--verbose')):
  """Performs some basic tests on `KV.of(conn_str)`"""
  import asyncio
  from dslog import Logger
  from kv import KV, SQLiteKV
  from kv.tests import test_bool, test_bytes, test_dict, test_float, test_int, test_list, test_str
  logger = Logger.click().prefix('[TEST]')
  async def tests():
    results = []
    for dtype, test in [(bool, test_bool), (bytes, test_bytes), (dict, test_dict), (float, test_float), (int, test_int), (list, test_list), (str, test_str)]:
      logger(f'Testing KV[{dtype.__name__}]...')
      kv = KV.of(conn_str, type=dtype)
      e = await test(kv, logger=logger if verbose else Logger.empty())
      if e.tag == 'left':
        logger(f'--> ERROR:', e.value, level='ERROR')
      else:
        logger(f'--> OK')
      results.append(e)
      if isinstance(kv, SQLiteKV):
        os.remove(kv.db_path)
    return results

  results = asyncio.run(tests())
  if E.sequence(results).tag == 'left':
    raise typer.Exit(1)

def parse_type(type: str):
  if type == 'dict':
    return dict
  if type == 'list':
    return list
  if type == 'set':
    return set
  if type == 'str':
    return str
  if type == 'int':
    return int
  if type == 'float':
    return float
  if type == 'bool':
    return bool
  if type == 'bytes':
    return None
  raise ValueError(f'Invalid type: {type}')

@app.command()
def serve(
  conn_str: str = typer.Argument(..., help='KV connection string'),
  token: str = typer.Option('', '--token', help='Bearer token for authorization'),
  host: str = typer.Option('0.0.0.0', '--host'),
  port: int = typer.Option(8000, '-p', '--port'),
  type: str = typer.Option('bytes', '--type', help='Datatype. Supports: dict, list, set, str, int, float, bool, bytes (default)'),
):
  from kv import http, KV
  import uvicorn

  t = parse_type(type)
  print('Starting API with type:', type)
  kv = KV.of(conn_str, type=t)
  app = http.ServerKV(kv, type=t, token=token or None)

  uvicorn.run(app, host=host, port=port)