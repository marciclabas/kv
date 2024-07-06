from kv import http, KV
import uvicorn

kv = KV.of('sql+sqlite:///db.sqlite;Table=kv')
app = http.api(kv, dict, token='shhh')

if __name__ == '__main__':
  uvicorn.run(app, host='0.0.0.0')