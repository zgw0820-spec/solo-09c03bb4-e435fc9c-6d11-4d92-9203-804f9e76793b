#!/usr/bin/env bash
# 直接调用 /join 的示例。
# 用法：
#   ./examples/call-api.sh
#   BASE_URL=http://your-host:8000 ./examples/call-api.sh
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# -N 关闭 curl 输出缓冲，逐行看到 NDJSON；-F 以 multipart 上传两份 UTF-8 CSV
curl -sS -N \
  -F "requests=@${DIR}/requests.csv;type=text/csv" \
  -F "attempts=@${DIR}/attempts.csv;type=text/csv" \
  "${BASE_URL}/join"
