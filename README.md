# 请求日志 × 上游尝试日志 — 低内存左联接 API

网关值班排障用：把**请求日志**和**上游尝试日志**两份带 `request_id` 的 UTF-8 CSV
做左联接，按**请求文件的行序**以 **NDJSON 流**返回。同一个 `request_id` 有 N 条尝试，
就输出 N 条 `请求行 × 尝试行` 记录，尝试按**尝试文件原行序**排列；没有匹配尝试的请求
也输出一行，`attempt` 为 `null`；尝试日志里的孤儿 `request_id` 不输出。

```json lines
{"request": {"request_id": "r1", "path": "/a"}, "attempt": {"request_id": "r1", "n": "9"}}
{"request": {"request_id": "r2", "path": "/b"}, "attempt": {"request_id": "r2", "n": "1"}}
{"request": {"request_id": "r2", "path": "/b"}, "attempt": {"request_id": "r2", "n": "2"}}
{"request": {"request_id": "r3", "path": "/c"}, "attempt": null}
```

---

## 1. 用 Docker Compose 启动

```bash
docker compose up --build
# 健康检查： curl http://127.0.0.1:8000/healthz
```

服务监听容器内 `8000`，并映射到宿主机 <http://127.0.0.1:8000>。
镜像以非 root 用户运行，自带 HEALTHCHECK。

本地直接用 Python 跑（Python 3.11+）：

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 2. 可直接调用的 API 示例

仓库自带 `examples/requests.csv`、`examples/attempts.csv`，在服务启动后：

```bash
# 方式一：封装好的脚本
./examples/call-api.sh

# 方式二：原始 curl（-N 逐行输出，立即看到 NDJSON）
curl -N -sS \
  -F "requests=@examples/requests.csv;type=text/csv" \
  -F "attempts=@examples/attempts.csv;type=text/csv" \
  http://127.0.0.1:8000/join

# 期望结果（可直接 diff）
./examples/call-api.sh | diff - examples/expected-output.ndjson
```

Python（requests 流式逐行读取，见 `examples/call_api_python.py`）：

```python
import json, requests
with open("examples/requests.csv","rb") as f1, open("examples/attempts.csv","rb") as f2:
    r = requests.post("http://127.0.0.1:8000/join",
                      files={"requests": ("requests.csv", f1),
                             "attempts": ("attempts.csv", f2)},
                      stream=True)
    for line in r.iter_lines():          # 不等整份结果，逐行处理
        if line:
            print(json.loads(line))
```

浏览器打开 <http://127.0.0.1:8000/demo> 可选择两份 CSV 直接试，
页面上的“停止（断连）”按钮可验证客户端断开后服务立即停工清理。

### 接口约定

| 项 | 内容 |
| --- | --- |
| 路径 | `POST /join`（同义：`/api/join`、`/v1/join`） |
| 入参 | `multipart/form-data` 两个文件字段：`requests`、`attempts` |
| 兼容 | 常见别名 `request(s)_log/file/csv`、`attempt(s)_log/file/csv`、`upstream_attempts`、`req`/`att` 等；两个字段名都不认识但恰好两个文件时按表单顺序识别；也接受同名字段直接给 CSV 文本、`application/json`（`{"requests":"<CSV>","attempts":"<CSV>"}`）、`x-www-form-urlencoded` |
| 表头 | 两份 CSV 都必须含 `request_id` 列（大小写不敏感兜底；兼容 UTF-8 BOM） |
| 成功响应 | `200`，`Content-Type: application/x-ndjson; charset=utf-8`，附加 `X-Accel-Buffering: no` |
| 记录结构 | `{"request": {列: 值}, "attempt": {列: 值} \| null}`，CSV 值一律按字符串保留 |
| 客户端错误 | 缺文件 / 缺 `request_id` 列 / 空 CSV → `400`；不支持的 Content-Type → `415`，均为 JSON 错误 |
| 健康检查 | `GET /healthz` → `{"status":"ok"}` |

## 3. 内存、断连、清理是怎么保证的

核心代码在 `app/join_core.py`，HTTP/生命周期在 `app/main.py`。

1. **尝试日志只扫一次，不按请求重扫。**
   第一阶段把 attempts 顺序读一遍，按固定批次（默认 1000 行，
   `CSVJOIN_BATCH_SIZE` 可调）写入临时目录里的 SQLite：

   ```sql
   CREATE TABLE attempt_index (
     request_id TEXT NOT NULL,
     seq        INTEGER NOT NULL,   -- 尝试在原文件中的记录序号
     payload    TEXT NOT NULL,      -- 该行的 JSON 对象
     PRIMARY KEY (request_id, seq)
   ) WITHOUT ROWID;
   ```

   复合主键本身就是 `(request_id, seq)` 聚簇磁盘索引。实测查询计划：
   `SEARCH attempt_index USING PRIMARY KEY (request_id=?)`。
   每个 request 走一次索引查询，**不会为每条请求重扫尝试文件**。

2. **算子内存不随输入 / 匹配行数增长。**
   - Python 侧只有固定大小的插入批次，用完即 `clear()`；
   - SQLite 页缓存用 `PRAGMA cache_size=-8192` 封在约 8 MiB，
     `journal_mode=OFF`、`synchronous=OFF`（一次性可丢弃库）；
   - 取匹配尝试始终 `cursor.fetchone()` 逐行拉，**不用 `fetchall()`**，
     所以某个 `request_id` 命中几百万条也不会攒在内存；
   - NDJSON 一条 pair `yield` 一行，Starlette 逐块发送并带背压，
     算子内不保存整份结果。
   - 上传本身由 `python-multipart`/Starlette 按 1 MiB 阈值 spool 到磁盘。

3. **顺序。** 请求 CSV 逐行驱动；同一 `request_id` 的尝试
   `ORDER BY seq ASC`（主键天然有序），即尝试文件原行序。

4. **客户端断开就停。** 双保险：
   - 每个插入批次之间、每个请求行 / 每 256 条输出之间调用
     `await request.is_disconnected()` 探针并 `await asyncio.sleep(0)`
     让出事件循环，发现断开立即停止；
   - uvicorn(ASGI 2.3) + Starlette 会在连接断开时直接取消流式任务，
     取消异常在批次 / `yield` 边界落地。出数阶段每个 `yield` 本身就是
     取消点和背压点。

5. **无论正常还是异常都清理本次临时文件。**
   每次请求用 `tempfile.mkdtemp(prefix="csvjoin-")` 建独立作业目录
   （可用 `CSVJOIN_TMPDIR` 指定根目录），SQLite 文件就在其中。
   流式生成器最外层 `finally: shutil.rmtree(job_dir, ignore_errors=True)`，
   正常结束、处理异常、客户端断连取消三条路径都执行；CSV 包装器关闭时
   连带关闭 `UploadFile.file`，其 spool 临时文件也会立即删除。
   每个作业一个目录，并发请求互不干扰。

## 4. 试跑验证结果（交付前对照改动执行）

### 4.1 自动化测试：27/27 通过

```bash
pip install -r requirements-dev.txt
PYTHONPATH=. python3 -m pytest
# 27 passed
```

覆盖：左联接顺序 / 一对多 / `attempt=null` / 孤儿尝试不输出 /
空 attempts / 重复请求行 / 空行与中文 UTF-8 / 缺 `request_id` 报错 /
索引阶段断连早停 / `aclose()` 取消清理 / 查询计划走主键 /
multipart 多别名字段 / 任意两文件兜底 / JSON / urlencoded / 400 / 415 /
正常与校验失败后的临时目录清理 / 仓库示例文件端到端。

内存恒定性用 `tracemalloc` 做了 2 万 vs 20 万匹配行的对照
（`tests/test_join_core.py::test_memory_does_not_grow_with_match_count`）：

```text
tracemalloc peak: 20k=0.32 MiB   200k=0.26 MiB
```

10 倍匹配行数，峰值分配没有增长（断言增量 < 8 MiB、绝对值 < 30 MiB）。

### 4.2 真实 uvicorn + curl 现场：14/14 通过

```bash
PYTHONPATH=/tmp/pylibs python3 scripts/live_verify.py
```

该脚本会拉起真实 uvicorn，生成 300 万全匹配尝试 + 500 万混合尝试，
用真实 curl 制造中途断连并采样 `/proc/<pid>/status`：

```text
[PASS] A examples 顺序与 null
[PASS] A 正常结束无临时目录残留
[PASS] B 出数中断连：已收到数据后停止
[PASS] B 出数中断连：临时目录已清理
[PASS] C 索引阶段确实开始
[PASS] C 断连后停止继续索引
[PASS] C 断连停止被日志记录
       日志实证: job csvjoin-vlgvlyt1 cancelled because client disconnected
                 job csvjoin-vlgvlyt1 temp cleaned (exists=False)
[PASS] C 索引中断连：临时目录已清理
[PASS] D 大匹配量输出条数正确            (3,000,000 / 3,000,000)
       RSS before=58.2MiB max=59.3MiB growth=1.1MiB samples=1217
[PASS] D 服务器峰值 RSS < 150MiB
[PASS] D RSS 相对增长 < 60MiB
[PASS] D 正常大作业后临时目录清理
[PASS] E 两个并发作业均得到 6 行
[PASS] E 并发作业后无临时目录残留
14/14 passed
```

## 5. 目录结构

```text
app/
  main.py          # FastAPI 路由、入参兼容、StreamingResponse、作业目录清理
  join_core.py     # 一次性 SQLite 磁盘索引 + 逐行左联接算子（不依赖 Web 层）
examples/
  requests.csv / attempts.csv
  expected-output.ndjson
  call-api.sh / call_api_python.py
tests/             # pytest 算子级 + HTTP 级用例
scripts/
  live_verify.py   # 真实服务器断连 / 清理 / RSS 验证（标准库 + curl）
Dockerfile
docker-compose.yml
requirements.txt / requirements-dev.txt
```

## 6. 还有哪些没做 / 已知边界（如实说明）

- **本沙箱没有 Docker / daemon**，因此 `docker compose up --build`
  没能在我这边实际执行；已用 PyYAML 校验 compose 结构，并用等价的
  本地 uvicorn（Python 3.11 + 锁定版本依赖）跑完上述全部关键场景。
  Dockerfile 仅做静态编写，镜像构建本身未在本机验证。
- **内存保证针对“行数”维度**：固定批次、逐行取数、逐行输出。
  若单个 CSV 字段本身大到反常（例如一行里塞几百 MiB），这一行仍需进内存，
  即内存复杂度为 `O(batch_size + max_row_size)`，而不是严格 `O(1)`。
- CSV 按标准库 `csv` 解析，支持引号内换行；重名列以后值覆盖前值，
  空表头用 `_column_n` 兜底；尾部缺列补 `null`，超出表头的列忽略。
- 没有加鉴权 / TLS / 多副本编排：服务定位为内网值班工具，
  如需公网暴露请放在网关或反向代理之后并加鉴权。
- 单 uvicorn worker；并发作业各自独立目录与 SQLite 文件，互不共享状态。
  水平扩展可直接加副本。
- 处理中途（200 响应头已发出后）若出现磁盘满、CSV 非法字节等错误，
  无法再改成 HTTP 4xx，当前策略是记录异常、清理临时文件并中止流，
  由调用方按“NDJSON 是否完整解析”判断失败；没有额外发错误事件行，
  以免污染正常记录 schema。
- 没有做按 `request_id` 过滤 / 时间范围裁剪等额外查询参数，
  严格按两份文件全量左联接实现。
