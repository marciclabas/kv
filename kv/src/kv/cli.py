import typer

app = typer.Typer()

@app.callback()
def callback(debug: bool = typer.Option(False, '--debug', help='Enable debug mode')):
  if debug:
    import debugpy
    debugpy.listen(5678)
    print('Waiting for debugger attach...')
    debugpy.wait_for_client()

@app.command()
def test(conn_str: str):
  """Performs some basic tests on `KV.of(conn_str)`"""
  import asyncio
  from dslog import Logger
  from kv import KV, SQLKV
  from kv.tests import test_bool, test_bytes, test_dict, test_float, test_int, test_list, test_str
  logger = Logger.click().prefix('[TEST]')
  async def tests():
    results = []
    for dtype, test in [(bool, test_bool), (bytes, test_bytes), (dict, test_dict), (float, test_float), (int, test_int), (list, test_list), (str, test_str)]:
      logger(f'Testing KV[{dtype.__name__}]...')
      kv = KV.of(conn_str, type=dtype)
      errors = await test(kv)
      if not errors:
        logger(f'--> OK')
      else:
        logger(f'--> {len(errors)} errors', level='ERROR')
        for e in errors:
          logger(e, level='ERROR')
      results.append(not errors)
      
      if isinstance(kv, SQLKV):
        kv.Base.metadata.drop_all(kv.engine)
        
    return results

  results = asyncio.run(tests())
  if any(not r for r in results):
    raise typer.Exit(1)

@app.command()
def serve(
  conn_str: str = typer.Argument(..., help='KV connection string'),
  secret: str = typer.Option('', '--secret', help='Secret token for authorization'),
  host: str = typer.Option('0.0.0.0', '--host'),
  port: int = typer.Option(8000, '-p', '--port'),
  type: str = typer.Option('any', '--type', help='Datatype. Supports: dict, list, set, str, int, float, bool, bytes (default)'),
):
  from kv import ServerKV, KV, parse_type
  import uvicorn

  t = parse_type(type)
  print('Starting API with type:', type)
  kv = KV.of(conn_str, type=t)
  app = ServerKV(kv, secret=secret or None, type=t)

  uvicorn.run(app, host=host, port=port)

# @app.command()
# def copy(
#   input: str = typer.Option(..., '-i', '--input', help='Input KV connection string'),
#   output: str = typer.Option(..., '-o', '--output', help='Output KV connection string'),
#   verbose: bool = typer.Option(False, '-v', '--verbose'),
#   type: str = typer.Option('bytes', '--type', help='Datatype. Supports: dict, list, set, str, int, float, bool, bytes (default)'),
# ):
#   from kv import KV, parse_type
#   t = parse_type(type)
#   import asyncio

#   async def copy():
#     inp = KV.of(input, t)
#     out = KV.of(output, t)

#   asyncio.run(copy())