mod kv

set dotenv-load

VENV := ".venv"
BIN := ".venv/bin"
PYTHON := ".venv/bin/python"

help:
  @just --list

init:
  rm -drf {{VENV}} || :
  python3.11 -m venv {{VENV}}
  {{PYTHON}} -m pip install --upgrade pip
  {{PYTHON}} -m pip install -r requirements.txt

@test-fs:
  rm -drf test || :
  echo "Running filesystem test"
  mkdir -p test
  {{BIN}}/kv test "file://test/fs"  \
    && echo "OK" \
    || echo "ERROR"

@test-sql:
  rm -drf test || :
  echo "Running sql+sqlite test"
  mkdir -p test
  {{BIN}}/kv test "sql+sqlite:///test/db.sqlite;Table=kv" \
    && echo "OK" \
    || echo "ERROR"

@test-sqlite:
  rm -drf test || :
  echo "Running sqlite test"
  mkdir -p test
  {{BIN}}/kv test "sqlite://test/db.sqlite;Table=kv" \
    && echo "OK" \
    || echo "ERROR"

@test-http:
  rm -drf test || :
  echo "Running HTTP over sqlite test"
  mkdir -p test
  {{BIN}}/kv serve "sql+sqlite:///test/db.sqlite;Table=kv" --type str --port 8627 >> test.log 2>&1 &
  sleep 1
  {{BIN}}/kv test "http://localhost:8627"  \
    && echo "OK" \
    || echo "ERROR"
  pkill kv >> test.log 2>&1

@test-redis:
  echo "Running Redis test"
  docker run -d --name redis-kv-test -p 7271:6379 redis/redis-stack-server:latest >> test.log 2>&1
  sleep 1
  {{BIN}}/kv test "redis://localhost:7271"  \
    && echo "OK" \
    || echo "ERROR"
  docker rm -f redis-kv-test >> test.log 2>&1

@test-postgres:
  echo "Running Postgres test"
  docker rm -f postgres-kv-test > /dev/null 2>&1
  docker run -d --name postgres-kv-test -p 5221:5432 \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_USER=postgres \
    -e POSTGRESS_DB=postgres \
    postgres:latest >> test.log 2>&1
  sleep 1
  {{BIN}}/kv test "sql+postgresql+psycopg2://postgres:postgres@localhost:5221/postgres;Table=kv" \
    && echo "OK" \
    || echo "ERROR"
  docker rm -f postgres-kv-test >> test.log 2>&1

@test-azure:
  echo "Running Azure Blob test"
  {{BIN}}/kv test "azure+blob://$BLOB_CONN_STR" \
    && echo "OK" \
    || echo "ERROR"

@test-azure-container:
  echo "Running Azure Blob test"
  {{BIN}}/kv test "azure+blob+container://$BLOB_CONN_STR;Container=randomname" \
    && echo "OK" \
    || echo "ERROR"

@clean:
  rm -drf test || :
  rm test.log || :

@test: clean test-fs test-sqlite test-http test-redis test-postgres test-azure-container
  rm -drf test || :
  echo "Run cat test.log to see full logs"