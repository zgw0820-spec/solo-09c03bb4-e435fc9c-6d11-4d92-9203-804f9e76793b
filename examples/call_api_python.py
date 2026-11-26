#!/usr/bin/env python3
"""用 requests 流式读取 NDJSON 的可直接调用示例。

依赖：pip install requests
用法：
    python examples/call_api_python.py
    BASE_URL=http://127.0.0.1:8000 python examples/call_api_python.py
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import requests

BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
HERE = Path(__file__).resolve().parent

with (HERE / "requests.csv").open("rb") as req_f, (
    HERE / "attempts.csv"
).open("rb") as att_f:
    resp = requests.post(
        f"{BASE_URL}/join",
        files={
            "requests": ("requests.csv", req_f, "text/csv"),
            "attempts": ("attempts.csv", att_f, "text/csv"),
        },
        stream=True,  # 关键：不要等整个响应体，逐行读 NDJSON
        timeout=(10, 300),
    )
    resp.raise_for_status()
    for raw_line in resp.iter_lines(chunk_size=None):
        if not raw_line:
            continue
        record = json.loads(raw_line)  # 一行 = 一个 请求×尝试 pair
        print(
            record["request"]["request_id"],
            "->",
            None if record["attempt"] is None else record["attempt"]["upstream"],
        )
