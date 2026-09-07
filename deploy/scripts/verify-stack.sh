#!/bin/sh
set -eu

BASE_URL=${BASE_URL:-http://127.0.0.1:8080}

for path in /db-check /login-page /admin-login; do
  code=$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
    --max-time 10 "$BASE_URL$path")
  [ "$code" = "200" ] || { echo "$path returned HTTP $code" >&2; exit 1; }
  echo "$path HTTP 200"
done

echo "STACK_SMOKE_OK"
