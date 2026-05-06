#!/usr/bin/env bash
set -euo pipefail

cargo build -p coral-cli --locked
TARGET_DIR="$(cargo metadata --format-version 1 --locked --no-deps | python3 -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')"
CORAL_BIN="$TARGET_DIR/debug/coral"

SMOKE_ROOT="$(mktemp -d)"
export CORAL_CONFIG_DIR="$SMOKE_ROOT/config"

cleanup() {
  if [[ -n "${HTTP_PID:-}" ]]; then
    kill "$HTTP_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

mkdir -p "$SMOKE_ROOT/jsonl-data"
cat > "$SMOKE_ROOT/jsonl-data/events.jsonl" <<'EOF'
{"id":1,"category":"alpha","nullable_text":"one","active":true}
{"id":2,"category":"beta","nullable_text":null,"active":false}
{"id":3,"category":"alpha","nullable_text":"three","active":true}
{"id":4,"category":"gamma","active":false}
{"id":5,"category":"beta","nullable_text":"five","active":true}
EOF

cat > "$SMOKE_ROOT/local-stats-jsonl.yaml" <<EOF
name: local_stats_jsonl
version: 0.1.0
dsl_version: 3
backend: jsonl
tables:
  - name: events
    description: Local stats smoke events
    source:
      location: file://$SMOKE_ROOT/jsonl-data/
      glob: "**/*.jsonl"
    columns:
      - name: id
        type: Int64
      - name: category
        type: Utf8
      - name: nullable_text
        type: Utf8
        nullable: true
      - name: active
        type: Boolean
EOF

"$CORAL_BIN" source lint "$SMOKE_ROOT/local-stats-jsonl.yaml"
"$CORAL_BIN" source add --file "$SMOKE_ROOT/local-stats-jsonl.yaml"

"$CORAL_BIN" sql --format json "
  SELECT column_name, null_fraction, approx_distinct_count, stats_sample_count, stats_precision
  FROM coral.columns
  WHERE schema_name = 'local_stats_jsonl' AND table_name = 'events'
  ORDER BY ordinal_position
" > "$SMOKE_ROOT/jsonl-before.json"

"$CORAL_BIN" sql --format json "
  SELECT id, category, nullable_text, active
  FROM local_stats_jsonl.events
  ORDER BY id
" > "$SMOKE_ROOT/jsonl-query.json"

"$CORAL_BIN" sql --format json "
  SELECT column_name, null_fraction, approx_distinct_count, stats_sample_count, stats_precision
  FROM coral.columns
  WHERE schema_name = 'local_stats_jsonl' AND table_name = 'events'
  ORDER BY ordinal_position
" > "$SMOKE_ROOT/jsonl-after.json"

python3 - "$SMOKE_ROOT/jsonl-before.json" "$SMOKE_ROOT/jsonl-after.json" <<'PY'
import json
import math
import sys

before = json.load(open(sys.argv[1]))
after = json.load(open(sys.argv[2]))

assert before, before
assert all(row["stats_sample_count"] is None for row in before), before
by_name = {row["column_name"]: row for row in after}
assert by_name["nullable_text"]["stats_sample_count"] == 5, by_name["nullable_text"]
assert math.isclose(by_name["nullable_text"]["null_fraction"], 0.4), by_name["nullable_text"]
assert by_name["nullable_text"]["stats_precision"] == "observed_sample", by_name["nullable_text"]
assert by_name["category"]["approx_distinct_count"] == 3, by_name["category"]
PY

cat > "$SMOKE_ROOT/http_server.py" <<'PY'
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

ROWS = [
    {"id": "m1", "status": "open", "body": "first"},
    {"id": "m2", "status": "closed", "body": None},
    {"id": "m3", "status": "open", "body": "third"},
]

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path != "/messages":
            self.send_response(404)
            self.end_headers()
            return
        params = parse_qs(parsed.query)
        status = params.get("status", [None])[0]
        rows = [row for row in ROWS if row["status"] == status] if status else ROWS
        body = json.dumps({"data": rows}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        return

HTTPServer(("127.0.0.1", 8765), Handler).serve_forever()
PY

python3 "$SMOKE_ROOT/http_server.py" &
HTTP_PID=$!
sleep 1

cat > "$SMOKE_ROOT/local-stats-http.yaml" <<'EOF'
name: local_stats_http
version: 0.1.0
dsl_version: 3
backend: http
inputs:
  API_BASE:
    kind: variable
    default: http://127.0.0.1:8765
base_url: "{{input.API_BASE}}"
tables:
  - name: messages
    description: Local HTTP stats smoke messages
    filters:
      - name: status
    request:
      method: GET
      path: /messages
      query:
        - name: status
          from: filter
          key: status
    response:
      rows_path: ["data"]
    columns:
      - name: id
        type: Utf8
      - name: status
        type: Utf8
      - name: body
        type: Utf8
        nullable: true
EOF

"$CORAL_BIN" source lint "$SMOKE_ROOT/local-stats-http.yaml"
"$CORAL_BIN" source add --file "$SMOKE_ROOT/local-stats-http.yaml"

"$CORAL_BIN" sql --format json "
  SELECT id, body
  FROM local_stats_http.messages
  WHERE status = 'open'
  ORDER BY id
" > "$SMOKE_ROOT/http-filtered-query.json"

"$CORAL_BIN" sql --format json "
  SELECT column_name, stats_sample_count
  FROM coral.columns
  WHERE schema_name = 'local_stats_http' AND table_name = 'messages'
  ORDER BY ordinal_position
" > "$SMOKE_ROOT/http-after-filtered.json"

python3 - "$SMOKE_ROOT/http-filtered-query.json" "$SMOKE_ROOT/http-after-filtered.json" <<'PY'
import json
import sys

query_rows = json.load(open(sys.argv[1]))
stats_rows = json.load(open(sys.argv[2]))
assert [row["id"] for row in query_rows] == ["m1", "m3"], query_rows
assert stats_rows
assert all(row["stats_sample_count"] is None for row in stats_rows), stats_rows
PY

"$CORAL_BIN" sql --format json "
  SELECT id, status, body
  FROM local_stats_http.messages
  ORDER BY id
" > "$SMOKE_ROOT/http-unfiltered-query.json"

"$CORAL_BIN" sql --format json "
  SELECT column_name, stats_sample_count, stats_precision
  FROM coral.columns
  WHERE schema_name = 'local_stats_http' AND table_name = 'messages'
  ORDER BY ordinal_position
" > "$SMOKE_ROOT/http-after-unfiltered.json"

python3 - "$SMOKE_ROOT/http-after-unfiltered.json" <<'PY'
import json
import sys

rows = json.load(open(sys.argv[1]))
assert rows
assert any(row["stats_sample_count"] == 3 for row in rows), rows
assert all(row["stats_precision"] in (None, "observed_sample") for row in rows), rows
PY

cargo run -p xtask -- write-stats-parquet-fixture --output-dir "$SMOKE_ROOT/parquet-data"

cat > "$SMOKE_ROOT/local-stats-parquet.yaml" <<EOF
name: local_stats_parquet
version: 0.1.0
dsl_version: 3
backend: parquet
tables:
  - name: metrics
    description: Local stats smoke metrics
    source:
      location: file://$SMOKE_ROOT/parquet-data/
      glob: "**/*.parquet"
    columns: []
EOF

"$CORAL_BIN" source lint "$SMOKE_ROOT/local-stats-parquet.yaml"
"$CORAL_BIN" source add --file "$SMOKE_ROOT/local-stats-parquet.yaml"

"$CORAL_BIN" sql --format json "
  SELECT *
  FROM local_stats_parquet.metrics
  ORDER BY id
" > "$SMOKE_ROOT/parquet-query.json"

"$CORAL_BIN" sql --format json "
  SELECT column_name, null_fraction, stats_sample_count, stats_precision
  FROM coral.columns
  WHERE schema_name = 'local_stats_parquet' AND table_name = 'metrics'
  ORDER BY ordinal_position
" > "$SMOKE_ROOT/parquet-after.json"

python3 - "$SMOKE_ROOT/parquet-after.json" <<'PY'
import json
import sys

rows = json.load(open(sys.argv[1]))
assert rows
assert any(row["stats_sample_count"] is not None for row in rows), rows
assert all(row["stats_precision"] in (None, "observed_sample", "exact") for row in rows), rows
PY

echo "column statistics smoke passed: $SMOKE_ROOT"
