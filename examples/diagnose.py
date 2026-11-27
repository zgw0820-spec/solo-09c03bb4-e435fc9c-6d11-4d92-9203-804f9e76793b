#!/usr/bin/env python3
"""对照 /join 的两类变慢：建索引慢 vs 正文写不动。

用法（服务需已启动，默认 http://127.0.0.1:8000）：

    python3 examples/diagnose.py            # 两个场景都跑
    python3 examples/diagnose.py index      # 只跑“建索引慢”
    python3 examples/diagnose.py body       # 只跑“正文写慢”
    BASE_URL=http://otherhost:8000 python3 examples/diagnose.py

场景设计（只用标准库，临时夹具跑完即删）：

* index：attempts 有 40 万行、requests 只有 1 行。输出只有 1 行，
  工作量几乎全在给尝试日志建 SQLite 索引。
* body ：attempts 只有表头，requests 有 300 行、每行带 32 KiB 填充字段（约 9.6 MiB 正文，
  才能打满内核发送缓冲、逼出背压）。
  索引几乎瞬间完成、逐行生成几乎不花时间；客户端读到一小段后故意停顿
  数秒不读，正文 send 堵在 TCP 背压上，body_write 的作业秒随之增长。

脚本在客户端作业进行中每 0.5s 调一次 ``GET /diagnostics``，把
``index`` 与 ``body_write`` 的在途数 / 累计作业秒并排打印；最后给出
“这段作业的时间主要花在哪个阶段”的判断。
"""

from __future__ import annotations

import json
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

BASE_URL = "http://127.0.0.1:8000"
POLL_SECONDS = 0.5

INDEX_ATTEMPT_ROWS = 400_000
BODY_REQUEST_ROWS = 300
BODY_PADDING = "x" * 32768
# 慢读客户端：累计读这么多字节就停顿这么久，逼出 send 背压
BODY_STALL_EVERY_BYTES = 1_048_576
BODY_STALL_SECONDS = 2.0


def fetch_diagnostics() -> dict | None:
    try:
        with urllib.request.urlopen(
            f"{BASE_URL}/diagnostics", timeout=3
        ) as resp:
            return json.load(resp)
    except (urllib.error.URLError, TimeoutError):
        return None


def post_join(requests_path: Path, attempts_path: Path, on_chunk) -> None:
    """multipart 上传并消费 NDJSON；on_chunk(bytes) 控制读取快慢。"""
    boundary = "----csvjoin-diagnose-boundary"
    head = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="requests"; '
        'filename="requests.csv"\r\n'
        "Content-Type: text/csv\r\n\r\n"
    ).encode()
    mid = (
        f"\r\n--{boundary}\r\n"
        'Content-Disposition: form-data; name="attempts"; '
        'filename="attempts.csv"\r\n'
        "Content-Type: text/csv\r\n\r\n"
    ).encode()
    tail = f"\r\n--{boundary}--\r\n".encode()

    body = head + requests_path.read_bytes() + mid
    body += attempts_path.read_bytes() + tail

    req = urllib.request.Request(
        f"{BASE_URL}/join",
        data=body,
        method="POST",
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        },
    )
    with urllib.request.urlopen(req, timeout=300) as resp:
        on_chunk(resp)


def generate_fixtures(root: Path) -> tuple[Path, Path, Path, Path]:
    index_req = root / "index_requests.csv"
    index_att = root / "index_attempts.csv"
    index_req.write_text("request_id,path\nr0,/hot\n", encoding="utf-8")
    with index_att.open("w", encoding="utf-8") as f:
        f.write("request_id,n,payload\n")
        for i in range(INDEX_ATTEMPT_ROWS):
            f.write(f"r{i},{i},padding-{i % 97}\n")

    body_req = root / "body_requests.csv"
    body_att = root / "body_attempts.csv"
    with body_req.open("w", encoding="utf-8") as f:
        f.write("request_id,path,pad\n")
        for i in range(BODY_REQUEST_ROWS):
            # 每行约 32 KiB，单条 NDJSON 大，少量行就能让 send 明显受阻
            f.write(f"nr{i},/no-attempt,{BODY_PADDING}\n")
    body_att.write_text("request_id,n\n", encoding="utf-8")
    return index_req, index_att, body_req, body_att


def run_scenario(name: str, req: Path, att: Path, slow_reader: bool) -> None:
    diag_before = fetch_diagnostics()
    t0 = time.time()
    stop = threading.Event()
    samples: list[tuple[float, dict]] = []

    def poll() -> None:
        while not stop.is_set():
            snap = fetch_diagnostics()
            if snap is not None:
                samples.append((time.time() - t0, snap))
            stop.wait(POLL_SECONDS)

    def consume(resp) -> None:
        if not slow_reader:
            while resp.read(256 * 1024):
                pass
            return
        # 读一小段后长时间不读：服务器 send 会堵在背压上，body_write 在途
        got = 0
        while True:
            chunk = resp.read(64 * 1024)
            if not chunk:
                return
            got += len(chunk)
            if got >= BODY_STALL_EVERY_BYTES:
                time.sleep(BODY_STALL_SECONDS)
                got = 0

    poller = threading.Thread(target=poll, daemon=True)
    poller.start()
    try:
        post_join(req, att, consume)
    finally:
        stop.set()
        poller.join(timeout=5)

    diag_after = fetch_diagnostics() or {}
    duration = time.time() - t0

    def phase(snap: dict, key: str) -> tuple[int, float]:
        p = snap.get("phases", {}).get(key, {})
        return int(p.get("in_flight", 0)), float(p.get("job_seconds_total", 0.0))

    def base(snap: dict | None, key: str) -> float:
        return phase(snap, key)[1] if snap else 0.0

    d_index = base(diag_after, "index") - base(diag_before, "index")
    d_body = base(diag_after, "body_write") - base(diag_before, "body_write")

    print(f"\n=== 场景 {name}：墙钟 {duration:.1f}s ===")
    print(f"{'t(s)':>5}  {'index 在途':>10} {'index 作业秒':>12}"
          f"  {'body 在途':>9} {'body 作业秒':>11}")
    for t, snap in samples:
        ii, isec = phase(snap, "index")
        bi, bsec = phase(snap, "body_write")
        print(f"{t:5.1f}  {ii:>10} {isec:>12.2f}  {bi:>9} {bsec:>11.2f}")

    print(f"本次作业累计：index={d_index:.2f}s  body_write={d_body:.2f}s")
    if d_index > d_body * 2 and d_index > 0.3:
        print("=> 主要时间在【建索引】：尝试日志大，整理 SQLite 索引是瓶颈。")
    elif d_body > d_index * 2 and d_body > 0.3:
        print("=> 主要时间在【正文写入】：客户端/网络读得慢，写 NDJSON 被背压拖住。")
    else:
        print("=> 两个阶段耗时接近，或作业太短不足以判断（可调大行数/延长慢读）。")


def main(argv: list[str]) -> int:
    global BASE_URL
    import os

    BASE_URL = os.environ.get("BASE_URL", BASE_URL)
    modes = argv[1:] or ["index", "body"]
    if any(m not in {"index", "body"} for m in modes):
        print(__doc__)
        return 2
    if fetch_diagnostics() is None:
        print(f"无法访问 {BASE_URL}/diagnostics，请先启动服务。")
        return 1

    with tempfile.TemporaryDirectory(prefix="csvjoin-diagnose-") as tmp:
        root = Path(tmp)
        index_req, index_att, body_req, body_att = generate_fixtures(root)
        if "index" in modes:
            run_scenario("index（大 attempts、输出仅 1 行）",
                         index_req, index_att, slow_reader=False)
        if "body" in modes:
            run_scenario("body（无尝试、300 行大正文、客户端慢读）",
                         body_req, body_att, slow_reader=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
