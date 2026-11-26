"""低内存左联接算子（不依赖 Web 框架，便于单测）。

内存约束的实现方式：

1. 尝试日志只顺序扫描 **一次**，逐批写入磁盘 SQLite 表
   ``attempt_index``（复合主键 ``(request_id, seq)``，本身就是
   按 request_id 组织、seq 有序的磁盘索引）。不会为每条请求重扫文件。
2. Python 侧只保留固定大小的插入批次（``batch_size``），SQLite 页缓存
   用 ``cache_size`` 封顶，内存不随输入行数增长。
3. 请求文件逐行读取；每个 request_id 走主键索引取尝试，
   用 ``fetchone`` 一次取一行，**不使用 fetchall**，
   因此内存不随某个 request_id 的匹配行数增长。
4. NDJSON 一条 pair 产出一行、立即 yield，不在算子内攒整份结果。

断连处理：每个插入批次之间、每个请求行 / 每若干条输出之间都会调用
``is_disconnected`` 探针并让出事件循环；真实 ASGI 服务器在连接断开时
还会直接取消正在运行的生成器。两条路径都会进入 finally 关闭连接，
临时目录由外层（main.managed_join_body）统一删除。
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import sqlite3
from pathlib import Path
from typing import Awaitable, BinaryIO, Callable, AsyncIterator

logger = logging.getLogger("csvjoin")

JOIN_KEY = "request_id"
DEFAULT_BATCH_SIZE = int(os.environ.get("CSVJOIN_BATCH_SIZE", "1000"))
# 每处理多少行打一条进度日志（用于观察“断连后确实停下”）
PROGRESS_EVERY = int(os.environ.get("CSVJOIN_PROGRESS_EVERY", "100000"))
# 出数阶段每隔多少条记录探测一次客户端断连
OUTPUT_PROBE_EVERY = 256

DisconnectCheck = Callable[[], Awaitable[bool]]


class CsvFormatError(Exception):
    """CSV 缺少必要结构（空文件 / 缺 request_id 列 / 编码错误）。"""


def normalise_header(name: object) -> str:
    if name is None:
        return ""
    # strip 去掉首尾空白；再去掉可能残留的 UTF-8 BOM
    return str(name).strip().lstrip(chr(0xFEFF))


def find_join_column(header: list[str]) -> int:
    """精确查找 request_id 列；找不到再做大小写不敏感兜底。"""
    for i, col in enumerate(header):
        if col == JOIN_KEY:
            return i
    target = JOIN_KEY.lower()
    for i, col in enumerate(header):
        if col.lower() == target:
            return i
    return -1


def read_header(binary: BinaryIO) -> list[str]:
    """读取并校验表头，读完把二进制流复位到 0，不关闭调用方的流。"""
    binary.seek(0)
    text = io.TextIOWrapper(binary, encoding="utf-8-sig", newline="")
    try:
        reader = csv.reader(text)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise CsvFormatError("CSV 为空，缺少表头行") from exc
        except UnicodeDecodeError as exc:
            raise CsvFormatError(f"CSV 不是合法的 UTF-8 文本: {exc}") from exc
    finally:
        # detach 而不是 close：不能关掉调用方的 UploadFile.file
        try:
            text.detach()
        except Exception:  # pragma: no cover - 防御性
            pass
    binary.seek(0)
    return [normalise_header(h) for h in header]


def row_to_object(header: list[str], row: list[str]) -> dict[str, object]:
    """按表头把一行 CSV 转成 JSON 对象。

    - 缺失的尾部列补 null；
    - 表头里的空列名用 ``_column_<位置>`` 兜底；
    - 多余的列忽略（标准 CSV 不会出现）；
    - 重名列以后出现的为准。
    """
    obj: dict[str, object] = {}
    for i, key in enumerate(header):
        name = key if key else f"_column_{i + 1}"
        obj[name] = row[i] if i < len(row) else None
    return obj


def connect_index(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    # 索引库是一次性的磁盘临时产物：关掉回滚日志 / 同步刷盘，
    # 并用负数 cache_size 把页缓存封顶在 ~8MiB。
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=FILE")
    conn.execute("PRAGMA cache_size=-8192")
    conn.execute("PRAGMA locking_mode=EXCLUSIVE")
    # WITHOUT ROWID + 复合主键 = 数据直接按 (request_id, seq) 聚簇，
    # 同时服务于按 request_id 定位和按原文件行序排序。
    conn.execute(
        """
        CREATE TABLE attempt_index (
            request_id TEXT NOT NULL,
            seq        INTEGER NOT NULL,
            payload    TEXT NOT NULL,
            PRIMARY KEY (request_id, seq)
        ) WITHOUT ROWID
        """
    )
    return conn


async def _client_gone(check: DisconnectCheck | None) -> bool:
    if check is None:
        return False
    try:
        return bool(await check())
    except Exception:
        # 注意：不能捕 BaseException —— CancelledError / GeneratorExit
        # 必须继续向上传播以触发取消与 finally 清理。
        logger.debug("disconnect probe raised", exc_info=True)
        return False


def _dumps(record: dict[str, object]) -> str:
    return json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"


async def left_join_ndjson(
    requests_binary: BinaryIO,
    attempts_binary: BinaryIO,
    job_dir: Path | str,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    is_disconnected: DisconnectCheck | None = None,
) -> AsyncIterator[str]:
    """执行左联接，逐行 yield NDJSON 字符串（每行以 \\n 结尾）。

    生成器结束 / 被取消时保证关闭 SQLite 连接和 CSV 文本包装器
    （关闭包装器会连带关闭传入的二进制流，例如 UploadFile.file）。
    临时目录本身由调用方负责删除。
    """
    job_dir = Path(job_dir)
    db_path = job_dir / "attempts.sqlite3"
    conn: sqlite3.Connection | None = None
    attempts_text: io.TextIOWrapper | None = None
    requests_text: io.TextIOWrapper | None = None

    if batch_size <= 0:
        batch_size = DEFAULT_BATCH_SIZE

    try:
        if await _client_gone(is_disconnected):
            logger.info("client already gone before indexing, job=%s", job_dir.name)
            return

        # ------------------------------------------------------------------
        # 阶段 1：尝试日志全量扫描一次 -> 磁盘 SQLite 索引（固定批次提交）
        # ------------------------------------------------------------------
        conn = connect_index(db_path)
        attempts_text = io.TextIOWrapper(
            attempts_binary, encoding="utf-8-sig", newline=""
        )
        attempts_reader = csv.reader(attempts_text)
        try:
            raw_attempts_header = next(attempts_reader)
        except StopIteration as exc:
            raise CsvFormatError("attempts CSV 为空，缺少表头行") from exc
        except UnicodeDecodeError as exc:
            raise CsvFormatError(f"attempts CSV 不是合法 UTF-8: {exc}") from exc

        attempts_header = [normalise_header(h) for h in raw_attempts_header]
        attempts_rid_idx = find_join_column(attempts_header)
        if attempts_rid_idx < 0:
            raise CsvFormatError("attempts CSV 缺少 request_id 列")

        batch: list[tuple[str, int, str]] = []
        seq = 0
        indexed = 0
        last_progress_mark = 0

        for row in attempts_reader:
            if not row:  # 跳过纯空行（csv.reader 对空行返回 []）
                continue
            seq += 1
            raw_rid = (
                row[attempts_rid_idx]
                if attempts_rid_idx < len(row)
                else ""
            )
            attempt_obj = row_to_object(attempts_header, row)
            batch.append(
                (
                    raw_rid if raw_rid is not None else "",
                    seq,
                    json.dumps(
                        attempt_obj, ensure_ascii=False, separators=(",", ":")
                    ),
                )
            )

            if len(batch) >= batch_size:
                inserted = len(batch)
                conn.executemany(
                    "INSERT INTO attempt_index(request_id, seq, payload)"
                    " VALUES (?, ?, ?)",
                    batch,
                )
                conn.commit()
                indexed += inserted
                batch.clear()

                if (
                    PROGRESS_EVERY
                    and indexed // PROGRESS_EVERY > last_progress_mark
                ):
                    last_progress_mark = indexed // PROGRESS_EVERY
                    logger.info(
                        "job=%s indexed %d attempt rows", job_dir.name, indexed
                    )

                # 断连探针 + 事件循环让出点：真实断连的取消也在这里落地
                if await _client_gone(is_disconnected):
                    logger.info(
                        "job=%s client gone during indexing at row %d, stopping",
                        job_dir.name,
                        indexed,
                    )
                    return
                await asyncio.sleep(0)

        if batch:
            conn.executemany(
                "INSERT INTO attempt_index(request_id, seq, payload)"
                " VALUES (?, ?, ?)",
                batch,
            )
            conn.commit()
            indexed += len(batch)
            batch.clear()

        logger.info(
            "job=%s attempt index ready: %d rows", job_dir.name, indexed
        )

        # ------------------------------------------------------------------
        # 阶段 2：按请求文件行序逐行做左联接，逐 pair 输出
        # ------------------------------------------------------------------
        requests_text = io.TextIOWrapper(
            requests_binary, encoding="utf-8-sig", newline=""
        )
        requests_reader = csv.reader(requests_text)
        try:
            raw_requests_header = next(requests_reader)
        except StopIteration as exc:
            raise CsvFormatError("requests CSV 为空，缺少表头行") from exc
        except UnicodeDecodeError as exc:
            raise CsvFormatError(f"requests CSV 不是合法 UTF-8: {exc}") from exc

        requests_header = [normalise_header(h) for h in raw_requests_header]
        requests_rid_idx = find_join_column(requests_header)
        if requests_rid_idx < 0:
            raise CsvFormatError("requests CSV 缺少 request_id 列")

        emitted = 0
        for row in requests_reader:
            if not row:
                continue

            raw_rid = (
                row[requests_rid_idx]
                if requests_rid_idx < len(row)
                else ""
            )
            request_obj = row_to_object(requests_header, row)
            rid_key = raw_rid if raw_rid is not None else ""

            # 主键 (request_id, seq) 天然给出该 request_id 下按 seq 的顺序，
            # 显式 ORDER BY 让语义不依赖实现细节。
            cursor = conn.execute(
                "SELECT payload FROM attempt_index "
                "WHERE request_id = ? ORDER BY seq ASC",
                (rid_key,),
            )

            payload_row = cursor.fetchone()
            if payload_row is None:
                # 没有匹配尝试：仍然输出请求行，attempt 记为 null
                yield _dumps({"request": request_obj, "attempt": None})
                emitted += 1
                continue

            # 有匹配：每个“请求行 × 尝试行”pair 一行；
            # 始终 fetchone，绝不 fetchall，匹配行数再大也不攒内存。
            while payload_row is not None:
                attempt_obj = json.loads(payload_row[0])
                yield _dumps(
                    {"request": request_obj, "attempt": attempt_obj}
                )
                emitted += 1

                if emitted % OUTPUT_PROBE_EVERY == 0:
                    if await _client_gone(is_disconnected):
                        logger.info(
                            "job=%s client gone during output at line %d, stopping",
                            job_dir.name,
                            emitted,
                        )
                        return
                    # yield 本身已是取消点；这里无需再 sleep

                payload_row = cursor.fetchone()

        logger.info(
            "job=%s output complete: %d NDJSON records",
            job_dir.name,
            emitted,
        )

    finally:
        # 同步清理，保证取消 / 异常路径也执行；临时目录由外层 rmtree。
        if conn is not None:
            try:
                conn.close()
            except Exception:  # pragma: no cover
                logger.debug("sqlite close failed", exc_info=True)
        for wrapper in (attempts_text, requests_text):
            if wrapper is not None:
                try:
                    wrapper.close()  # 连带关闭 UploadFile.file / 测试文件流
                except Exception:  # pragma: no cover
                    logger.debug("csv wrapper close failed", exc_info=True)
