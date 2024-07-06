import typer

app = typer.Typer()

@app.callback()
def callback():
  ...

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
    return bytes
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

  kv = KV.of(conn_str)
  app = http.api(kv, type=parse_type(type), token=token or None)

  uvicorn.run(app, host=host, port=port)