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
镜像以非 root 用户运行，自带 HEALTHCHECK。诊断分段长度可用环境变量
`CSVJOIN_DIAG_SEGMENT_SECONDS`（默认 60）调整，`docker-compose.yml` 已带上。

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
| 诊断 | `GET /diagnostics` → 建索引 / 正文写入两阶段的在途数与累计作业秒，见第 4 节 |

## 3. 诊断：到底是建索引慢，还是正文写不动

`GET /diagnostics` 把 `/join` 的两段工作分开报，返回示例：

```json
{
  "process": {"uptime_seconds": 812.4, "segment_seconds": 60.0,
              "current_segment": 13, "previous_segment": 12},
  "phases": {
    "index": {"in_flight": 1, "job_seconds_total": 271.6,
              "current_segment_seconds": 23.1, "previous_segment_seconds": 60.0},
    "body_write": {"in_flight": 3, "job_seconds_total": 512.0,
                   "current_segment_seconds": 18.4, "previous_segment_seconds": 44.7}
  },
  "totals": {"in_flight": 4, "job_seconds_total": 783.6,
             "current_segment_seconds": 41.5, "previous_segment_seconds": 104.7}
}
```

口径（实现见 `app/metrics.py`）：

| 项 | 含义 |
| --- | --- |
| `index` | 为 attempts 日志建磁盘 SQLite 索引的在途作业数与累计耗时 |
| `body_write` | 每次**非空** NDJSON 正文 chunk 的 `send` 从发起到返回/中止的耗时；两次 send 之间“生成下一行”的时间**不算** |
| `in_flight` | 当前处于该阶段的作业数。作业断开或失败后立即不再算在途，但已耗时间照算 |
| `job_seconds_total` | 自进程启动以来该阶段的**累计作业秒**：每个作业的在途时长相加，两笔作业并行 1 秒记 2 秒 |
| `current_segment_seconds` | 本段（默认 60 秒，`CSVJOIN_DIAG_SEGMENT_SECONDS` 可调）内的作业秒 |
| `previous_segment_seconds` | 上一段的作业秒；跨段作业按实际耗时拆到各段 |
| 未结束阶段 | 按“到查询时刻为止”计入 |

统计**不保存逐行或逐作业历史**：每个阶段只维护在途计数、累计值和最近
两个段桶，进入/离开/查询时按 `在途数 × Δt` 积分。因此查询是常数时间，
不会遍历在途作业，内存也不随作业数或运行时长增长。
分段起点是进程启动（模块导入）时刻。

### 对照示例：区分两种变慢

`examples/diagnose.py` 只用标准库，在作业进行中每 0.5s 采一次
`/diagnostics`，并排打印两阶段耗时：

```bash
# 服务启动后
python3 examples/diagnose.py index   # 大 attempts、输出仅 1 行
python3 examples/diagnose.py body    # 无尝试、大行慢读客户端
python3 examples/diagnose.py         # 两个都跑
```

实测输出（Python 3.11 + uvicorn，分段 5s）：

```text
=== 场景 index（大 attempts、输出仅 1 行）：墙钟 4.1s ===
 t(s)    index 在途    index 作业秒    body 在途    body 作业秒
  0.5           1         0.36          0        0.00
  1.5           1         1.38          0        0.00
  2.5           1         2.40          0        0.00
  3.6           1         3.41          0        0.00
  4.1           0         3.91          0        0.00
本次作业累计：index=3.91s  body_write=0.00s
=> 主要时间在【建索引】：尝试日志大，整理 SQLite 索引是瓶颈。

=== 场景 body（无尝试、300 行大正文、客户端慢读）：墙钟 18.1s ===
 t(s)    index 在途    index 作业秒    body 在途    body 作业秒
  0.5           0         3.91          1        0.42
  4.0           0         3.91          1        3.90
  8.0           0         3.91          1        7.88
 12.0           0         3.91          1       11.87
 12.5           0         3.91          0       11.90
本次作业累计：index=0.00s  body_write=11.90s
=> 主要时间在【正文写入】：客户端/网络读得慢，写 NDJSON 被背压拖住。
```

值班判断方法：`index` 高是尝试日志大、索引整理慢；`body_write` 高是
客户端/网络消费慢。两者都高时按段内数值比较；`job_seconds_total` 按
并行实数相加，多作业并发不会被墙钟稀释。

## 4. 内存、断连、清理是怎么保证的

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

## 5. 试跑验证结果（交付前对照改动执行）

### 5.1 自动化测试：40/40 通过

```bash
pip install -r requirements-dev.txt
PYTHONPATH=. python3 -m pytest
# 40 passed
```

覆盖：左联接顺序 / 一对多 / `attempt=null` / 孤儿尝试不输出 /
空 attempts / 重复请求行 / 空行与中文 UTF-8 / 缺 `request_id` 报错 /
索引阶段断连早停 / `aclose()` 取消清理 / 查询计划走主键 /
multipart 多别名字段 / 任意两文件兜底 / JSON / urlencoded / 400 / 415 /
正常与校验失败后的临时目录清理 / 仓库示例文件端到端；
以及诊断统计（`tests/test_diagnostics.py` 13 例）：
并行作业秒相加 / 跨段拆分 / 只留本段与上一段 / 未结束阶段算到查询时刻 /
失败与取消后不再在途而耗时保留 / 正文计时只包非空 send 不计生成时间 /
索引阶段进出在途 / `/diagnostics` 结构与真实作业接线。

内存恒定性用 `tracemalloc` 做了 2 万 vs 20 万匹配行的对照
（`tests/test_join_core.py::test_memory_does_not_grow_with_match_count`）：

```text
tracemalloc peak: 20k=0.32 MiB   200k=0.26 MiB
```

10 倍匹配行数，峰值分配没有增长（断言增量 < 8 MiB、绝对值 < 30 MiB）。

### 5.2 真实 uvicorn 现场：25/25 通过

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
[PASS] F /diagnostics 结构与本进程分段
[PASS] F1 建索引场景：诊断中 index 在途 >= 1
[PASS] F1 建索引场景：index 作业秒显著增长、body 几乎不动
       index=6.78s body=0.00s 运行中index=6.72s 运行中body=0.00s
[PASS] F1 建索引场景：结束后 index 不再在途
[PASS] F2 慢写场景：诊断中 body_write 在途 >= 1
[PASS] F2 慢写场景：客户端停读期间 body 作业秒持续增长
       2.5s 窗口内 body +2.50s in_flight=1 index +0.00s
[PASS] F2 慢写场景：body 增长明显大于 index
       body=3.97s index=0.00s
[PASS] F2 慢写场景：结束后 body 不再在途
[PASS] F3 返回本段/上一段且 totals 自洽
       totals={'in_flight': 0, 'job_seconds_total': 18.271,
               'current_segment_seconds': 1.063, 'previous_segment_seconds': 2.0}
[PASS] F4 并行作业秒按实数相加
       in_flight=2 growth=4.00s / wall=2.00s 双作业 body +9.91s
[PASS] F 诊断场景后无临时目录残留
25/25 passed
```

F 组用 `CSVJOIN_DIAG_SEGMENT_SECONDS=2` 缩小分段；`index` 场景喂 60 万行
attempts 并观察其累计；`body_write` 场景让标准库客户端读 512KiB 后停读
4 秒，制造真实 TCP 背压；F4 同时跑两个这样的客户端，证明并行作业秒
在 2 秒墙钟内累计约 4 秒（并行一秒算两秒）。

## 6. 目录结构

```text
app/
  main.py          # FastAPI 路由、入参兼容、StreamingResponse、作业目录清理
  join_core.py     # 一次性 SQLite 磁盘索引 + 逐行左联接算子（不依赖 Web 层）
  metrics.py       # 两阶段在途数 / 累计作业秒的 O(1) 聚合与分段快照
examples/
  requests.csv / attempts.csv
  expected-output.ndjson
  call-api.sh / call_api_python.py
  diagnose.py      # 对照“建索引慢 / 正文写慢”的诊断示例
tests/             # pytest 算子级 + HTTP 级 + 诊断统计用例
scripts/
  live_verify.py   # 真实服务器断连 / 清理 / RSS / 诊断 API 验证
Dockerfile
docker-compose.yml
requirements.txt / requirements-dev.txt
```

## 7. 还有哪些没做 / 已知边界（如实说明）

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
- **诊断口径的边界**：`body_write` 计的是“应用层 `send` 阻塞时长”。
  ASGI 2.3 的 `send` 在传输层缓冲未满时会立即返回（真正的背压通过
  uvicorn 的 flow-control `drain` 体现），因此对“客户端读得很慢但内核对
  缓冲尚未打满”的连接，正文斜率可能低于主观变慢程度；一旦缓冲打满、
  `send` 真的被堵住，耗时会如实累计（F2 场景实测 2.5s 窗口 +2.50s）。
  这是在不改协议的前提下可观测到的最大口径，未额外插桩 socket 层。
- 诊断是进程级聚合，不区分请求 / 租户；不保留逐作业或逐行历史，
  因此无法事后回放某个具体作业在哪个阶段慢，只能看当前与最近两段。
- 多 worker 部署时每个进程各自计时，需要按副本汇总。
- 统计只覆盖 `/join` 的两段；上传解析、响应头等开销不计入任何阶段。
