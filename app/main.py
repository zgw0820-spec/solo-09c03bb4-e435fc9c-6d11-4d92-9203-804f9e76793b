"""网关请求日志 × 上游尝试日志的低内存左联接 API。

启动：
    uvicorn app.main:app --host 0.0.0.0 --port 8000

接口：
    POST /join            （同义路径 /api/join、/v1/join）
        multipart/form-data 上传两份 UTF-8 CSV：
            requests  —— 请求日志（规范字段名，兼容 request/request_log/...）
            attempts  —— 上游尝试日志（兼容 attempt/attempt_log/...）
        也支持 application/json：
            {"requests": "<CSV 文本>", "attempts": "<CSV 文本>"}

    GET  /healthz         健康检查
    GET  /                用法说明

输出：application/x-ndjson，每行一条记录：
    {"request": { ...请求行... }, "attempt": { ...尝试行... } | null}
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import BinaryIO

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from starlette.datastructures import UploadFile

from .join_core import (
    CsvFormatError,
    find_join_column,
    left_join_ndjson,
    read_header,
)

logging.basicConfig(
    level=os.environ.get("CSVJOIN_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("csvjoin.api")

# 本次作业临时 SQLite 索引所在根目录；不设置则用系统临时目录。
TMP_ROOT = os.environ.get("CSVJOIN_TMPDIR") or None
if TMP_ROOT:
    Path(TMP_ROOT).mkdir(parents=True, exist_ok=True)

REQUEST_ALIASES = {
    "requests",
    "request",
    "request_log",
    "requestlog",
    "requests_csv",
    "request_file",
    "requests_file",
    "req",
}
ATTEMPT_ALIASES = {
    "attempts",
    "attempt",
    "attempt_log",
    "attemptlog",
    "attempts_csv",
    "attempt_file",
    "attempts_file",
    "upstream_attempts",
    "upstream_attempts_csv",
    "upstream_attempt",
    "upstream",
    "att",
}

app = FastAPI(
    title="CSV Request/Attempt Left Join",
    version="1.0.0",
    description=(
        "低内存流式左联接：按请求文件行序返回 NDJSON，"
        "同 request_id 的每个 请求行×尝试行 pair 输出一行，"
        "无匹配尝试时 attempt=null。"
    ),
)

# 方便浏览器内 /demo 或任意内网页面直接调用
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


def _canonical_field(name: str) -> str:
    """把 requestLog / request-log_csv 之类的字段名归一化成下划线小写，
    并剥掉常见的 _csv/_file/_data/_log 后缀，扩大可直接对接的命名范围。"""
    raw = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(name))
    key = "".join("_" if ch in "- " else ch for ch in raw.lower())
    while True:
        for suffix in ("_logs", "_log", "_csv", "_file", "_data"):
            if key.endswith(suffix) and len(key) > len(suffix):
                key = key[: -len(suffix)]
                break
        else:
            return key


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


_DEMO_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>请求 × 上游尝试 左联接演示</title>
<style>
 body{font:14px/1.5 system-ui,sans-serif;margin:24px;max-width:980px}
 .row{display:flex;gap:16px;flex-wrap:wrap;margin:12px 0}
 label{display:flex;flex-direction:column;gap:4px}
 pre{background:#0f172a;color:#d1fae5;padding:12px;border-radius:8px;
     height:420px;overflow:auto;white-space:pre-wrap;word-break:break-all}
 button{padding:8px 18px;border:0;border-radius:6px;background:#2563eb;color:#fff;cursor:pointer}
 button.secondary{background:#b91c1c}
 code{background:#eef2ff;padding:1px 4px;border-radius:4px}
</style>
</head>
<body>
<h2>网关请求日志 × 上游尝试日志 — 低内存左联接</h2>
<p>选择两份带 <code>request_id</code> 的 UTF-8 CSV，点击“开始联接”。
输出为逐行到达的 NDJSON；点“停止（模拟客户端断连）”后服务器会立即停止处理并清理临时索引。</p>
<div class="row">
 <label>请求日志 requests <input type="file" id="req" accept=".csv,text/csv"></label>
 <label>尝试日志 attempts <input type="file" id="att" accept=".csv,text/csv"></label>
</div>
<div class="row">
 <button id="go">开始联接</button>
 <button id="stop" class="secondary" disabled>停止（断连）</button>
 <span id="stat"></span>
</div>
<pre id="out"></pre>
<script>
let controller = null;
const out = document.getElementById('out');
const stat = document.getElementById('stat');
document.getElementById('go').onclick = async () => {
  const req = document.getElementById('req').files[0];
  const att = document.getElementById('att').files[0];
  if (!req || !att) { alert('请先选择两份 CSV'); return; }
  const fd = new FormData();
  fd.append('requests', req); fd.append('attempts', att);
  controller = new AbortController();
  document.getElementById('stop').disabled = false;
  out.textContent = ''; let n = 0; const t0 = performance.now();
  try {
    const resp = await fetch('/join', {method:'POST', body:fd, signal:controller.signal});
    if (!resp.ok) { out.textContent = 'HTTP ' + resp.status + '\\n' + await resp.text(); return; }
    const reader = resp.body.getReader();
    const dec = new TextDecoder(); let buf = '';
    while (true) {
      const {value, done} = await reader.read();
      if (done) break;
      buf += dec.decode(value, {stream:true});
      let i;
      while ((i = buf.indexOf('\\n')) >= 0) {
        const line = buf.slice(0, i); buf = buf.slice(i+1);
        if (line) { n++; out.textContent += line + '\\n'; out.scrollTop = out.scrollHeight; }
      }
      stat.textContent = n + ' 行, ' + ((performance.now()-t0)/1000).toFixed(1) + 's';
    }
    stat.textContent = '完成：' + n + ' 行, ' + ((performance.now()-t0)/1000).toFixed(1) + 's';
  } catch (e) {
    stat.textContent = '已中断：' + e.name + '，服务器应已停止并清理临时文件';
  } finally {
    document.getElementById('stop').disabled = true; controller = null;
  }
};
document.getElementById('stop').onclick = () => controller && controller.abort();
</script>
</body></html>
"""


@app.get("/demo", response_class=HTMLResponse)
async def demo() -> str:
    return _DEMO_HTML


@app.get("/")
async def root() -> JSONResponse:
    return JSONResponse(
        {
            "service": "csv-request-attempt-left-join",
            "endpoint": "POST /join",
            "content_types": [
                "multipart/form-data (fields: requests, attempts)",
                "application/json ({\"requests\": csv_text, \"attempts\": csv_text})",
            ],
            "output": "application/x-ndjson; charset=utf-8",
            "record": {"request": "object", "attempt": "object|null"},
            "example_curl": (
                'curl -N -F "requests=@requests.csv" '
                '-F "attempts=@attempts.csv" http://127.0.0.1:8000/join'
            ),
        }
    )


def _pick_named(
    files: list[tuple[str, UploadFile]], aliases: set[str]
) -> UploadFile | None:
    for name, upload in files:
        if _canonical_field(name) in aliases:
            return upload
    return None


def resolve_multipart_streams(
    form_items: list[tuple[str, object]],
) -> tuple[BinaryIO, BinaryIO]:
    """从 multipart 表单解析 (requests 流, attempts 流)。

    支持：
    - 文件字段（UploadFile，规范名 requests/attempts 及常见别名）；
    - 同名字段直接给 CSV 文本；
    - 名字都不认识但恰好有两个文件时，按 multipart 顺序
      第一个当 requests、第二个当 attempts。
    """
    files: list[tuple[str, UploadFile]] = [
        (name, value)
        for name, value in form_items
        if isinstance(value, UploadFile)
    ]

    req_up = _pick_named(files, REQUEST_ALIASES)
    att_up = _pick_named(files, ATTEMPT_ALIASES)
    all_uploads = [value for _, value in files]

    if req_up is None and att_up is None and len(all_uploads) == 2:
        req_up, att_up = all_uploads[0], all_uploads[1]
    else:
        if req_up is None:
            others = [u for u in all_uploads if u is not att_up]
            if len(others) == 1:
                req_up = others[0]
        if att_up is None:
            others = [u for u in all_uploads if u is not req_up]
            if len(others) == 1:
                att_up = others[0]

    # 文件没匹配到时，允许同名字段是普通文本（CSV 字符串）
    req_stream: BinaryIO | None = req_up.file if req_up is not None else None
    att_stream: BinaryIO | None = att_up.file if att_up is not None else None
    if req_stream is None:
        for name, value in form_items:
            if _canonical_field(name) in REQUEST_ALIASES and isinstance(
                value, str
            ):
                req_stream = io.BytesIO(value.encode("utf-8"))
                break
    if att_stream is None:
        for name, value in form_items:
            if _canonical_field(name) in ATTEMPT_ALIASES and isinstance(
                value, str
            ):
                att_stream = io.BytesIO(value.encode("utf-8"))
                break

    if req_stream is None or att_stream is None:
        received = [
            f"{name}:{type(value).__name__}" for name, value in form_items
        ]
        raise HTTPException(
            status_code=400,
            detail=(
                "需要两份 CSV：requests（请求日志）和 attempts"
                f"（上游尝试日志）。收到的表单字段：{received}"
            ),
        )
    return req_stream, att_stream


def _validate_headers(
    requests_binary: BinaryIO, attempts_binary: BinaryIO
) -> None:
    """在响应头发出之前完成表头校验，以便能返回 4xx JSON。"""
    try:
        req_header = read_header(requests_binary)
        att_header = read_header(attempts_binary)
    except CsvFormatError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    missing: list[str] = []
    if find_join_column(req_header) < 0:
        missing.append("requests")
    if find_join_column(att_header) < 0:
        missing.append("attempts")
    if missing:
        raise HTTPException(
            status_code=400,
            detail=(
                f"{', '.join(missing)} CSV 中找不到 request_id 列；"
                f"requests 表头={req_header!r}, attempts 表头={att_header!r}"
            ),
        )


async def _resolve_json_body(
    request: Request,
) -> tuple[BinaryIO, BinaryIO]:
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(
            status_code=400, detail="请求体不是合法 JSON"
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=400,
            detail="JSON 体必须是对象，包含 requests 和 attempts 两个 CSV 字符串",
        )

    def find_text(aliases: set[str]) -> str | None:
        for key, value in payload.items():
            if _canonical_field(key) in aliases and isinstance(value, str):
                return value
        return None

    req_text = find_text(REQUEST_ALIASES)
    att_text = find_text(ATTEMPT_ALIASES)
    if req_text is None or att_text is None:
        raise HTTPException(
            status_code=400,
            detail="JSON 体必须包含 requests 和 attempts 两个 CSV 文本字段",
        )
    return (
        io.BytesIO(req_text.encode("utf-8")),
        io.BytesIO(att_text.encode("utf-8")),
    )


@app.post("/join")
@app.post("/api/join")
@app.post("/v1/join")
@app.post("/api/v1/join")
async def join_logs(request: Request) -> StreamingResponse:
    content_type = request.headers.get("content-type", "").lower()

    if (
        "multipart/form-data" in content_type
        or "application/x-www-form-urlencoded" in content_type
    ):
        # python-multipart 把大文件自动 spool 到磁盘（默认 >1MiB），不占大内存
        form = await request.form()
        requests_binary, attempts_binary = resolve_multipart_streams(
            form.multi_items()
        )
    elif "application/json" in content_type:
        requests_binary, attempts_binary = await _resolve_json_body(request)
    else:
        raise HTTPException(
            status_code=415,
            detail=(
                "请使用 multipart/form-data 上传 requests、attempts 两份 CSV，"
                "或用 application/json / x-www-form-urlencoded 提交 CSV 文本"
            ),
        )

    # 提前校验：此时尚未发送 200 响应头，错误可以正常返回 4xx
    _validate_headers(requests_binary, attempts_binary)

    job_dir = Path(
        tempfile.mkdtemp(prefix="csvjoin-", dir=TMP_ROOT)
    )
    logger.info("job %s start (tmp=%s)", job_dir.name, job_dir)

    async def managed_body():
        try:
            async for line in left_join_ndjson(
                requests_binary,
                attempts_binary,
                job_dir,
                is_disconnected=request.is_disconnected,
            ):
                yield line
        except asyncio.CancelledError:
            # 真实客户端断连时 uvicorn/Starlette 会取消该任务
            logger.info(
                "job %s cancelled because client disconnected", job_dir.name
            )
            raise
        except Exception:
            logger.exception("job %s failed", job_dir.name)
            raise
        finally:
            # 无论正常结束、校验/处理异常还是客户端断连取消，都清理本次目录
            shutil.rmtree(job_dir, ignore_errors=True)
            logger.info(
                "job %s temp cleaned (exists=%s)",
                job_dir.name,
                job_dir.exists(),
            )

    return StreamingResponse(
        managed_body(),
        media_type="application/x-ndjson; charset=utf-8",
        headers={
            "Cache-Control": "no-store",
            "X-Accel-Buffering": "no",  # 禁 nginx 缓冲，保证逐行可达
        },
    )
