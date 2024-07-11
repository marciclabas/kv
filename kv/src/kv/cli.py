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

@app.command()
def serve(
  conn_str: str = typer.Argument(..., help='KV connection string'),
  token: str = typer.Option('', '--token', help='Bearer token for authorization'),
  host: str = typer.Option('0.0.0.0', '--host'),
  port: int = typer.Option(8000, '-p', '--port'),
  type: str = typer.Option('bytes', '--type', help='Datatype. Supports: dict, list, set, str, int, float, bool, bytes (default)'),
):
  from kv import http, KV, parse_type
  import uvicorn

  t = parse_type(type)
  print('Starting API with type:', type)
  kv = KV.of(conn_str, type=t)
  app = http.ServerKV(kv, type=t, token=token or None)

  uvicorn.run(app, host=host, port=port)

@app.command()
def copy(
  input: str = typer.Option(..., '-i', '--input', help='Input KV connection string'),
  output: str = typer.Option(..., '-o', '--output', help='Output KV connection string'),
  verbose: bool = typer.Option(False, '-v', '--verbose'),
  type: str = typer.Option('bytes', '--type', help='Datatype. Supports: dict, list, set, str, int, float, bool, bytes (default)'),
):
  from kv import KV, parse_type
  t = parse_type(type)
  import asyncio

  async def copy():
    inp = KV.of(input, t)
    out = KV.of(output, t)
    if verbose:
      print('Fetching keys...')
    keys = await inp.keys().map(E.unsafe).sync()
    if verbose:
      print(f'Copying {len(keys)} items...')
    for i, key in enumerate(keys):
      if verbose:
        print(f'\r[{i+1}/{len(keys)}]: {key}', end='', flush=True)
      (await inp.copy(key, out, key)).unsafe()
    if verbose:
      print()

  asyncio.run(copy())