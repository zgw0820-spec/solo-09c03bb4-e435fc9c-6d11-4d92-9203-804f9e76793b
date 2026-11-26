from __future__ import annotations

import asyncio
import json
import tracemalloc
from pathlib import Path

import pytest

from app.join_core import (
    CsvFormatError,
    connect_index,
    left_join_ndjson,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLES = REPO_ROOT / "examples"


def write_csv(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def run_join(requests_csv: Path, attempts_csv: Path, tmp_path: Path, **kwargs):
    async def _collect():
        lines: list[str] = []
        with requests_csv.open("rb") as rf, attempts_csv.open("rb") as af:
            async for line in left_join_ndjson(rf, af, tmp_path, **kwargs):
                lines.append(line)
        return lines

    return asyncio.run(_collect())


def parse(lines: list[str]):
    return [json.loads(line) for line in lines]


REQUESTS_CSV = """request_id,path,status
r1,/a,200
r2,/b,500
r3,/c,200
"""

# 刻意打乱：r2 的三次尝试在文件中不连续，验证按“尝试原文件行序”输出
ATTEMPTS_CSV = """request_id,attempt_no,upstream
r2,1,u-a
r1,1,u-x
r2,2,u-b
r2,3,u-a
r9,1,ghost
"""


def test_left_join_order_pairs_and_null(tmp_path):
    job = tmp_path / "job"
    job.mkdir()
    req = write_csv(tmp_path / "req.csv", REQUESTS_CSV)
    att = write_csv(tmp_path / "att.csv", ATTEMPTS_CSV)

    records = parse(run_join(req, att, job))

    # 请求文件行序：r1 -> r2 -> r3
    assert [r["request"]["request_id"] for r in records] == [
        "r1",
        "r2",
        "r2",
        "r2",
        "r3",
    ]
    # r2 的尝试严格按尝试文件中的行序 1,2,3
    r2_attempts = [
        r["attempt"]["attempt_no"]
        for r in records
        if r["request"]["request_id"] == "r2"
    ]
    assert r2_attempts == ["1", "2", "3"]

    # 无匹配的 r3 输出一行且 attempt=None
    r3 = [r for r in records if r["request"]["request_id"] == "r3"]
    assert len(r3) == 1
    assert r3[0]["attempt"] is None
    assert r3[0]["request"]["path"] == "/c"

    # 每个 pair 的结构恰好是 request / attempt 两个键
    assert all(set(r.keys()) == {"request", "attempt"} for r in records)
    # 孤儿尝试 r9 不输出（左联接以请求为驱动）
    assert all(r["request"]["request_id"] != "r9" for r in records)
    assert len(records) == 5


def test_shipped_examples(tmp_path):
    job = tmp_path / "job"
    job.mkdir()
    lines = run_join(EXAMPLES / "requests.csv", EXAMPLES / "attempts.csv", job)
    records = parse(lines)
    ids = [r["request"]["request_id"] for r in records]
    assert ids == [
        "req-1001",
        "req-1002",
        "req-1002",
        "req-1002",
        "req-1003",
        "req-1004",
    ]
    req_1002_upstreams = [
        r["attempt"]["upstream"]
        for r in records
        if r["request"]["request_id"] == "req-1002"
    ]
    assert req_1002_upstreams == ["payments-a", "payments-b", "payments-a"]
    assert [r for r in records if r["request"]["request_id"] == "req-1003"][0][
        "attempt"
    ] is None
    # 每行都是合法 NDJSON 且恰好一个换行结尾
    assert all(line.endswith("\n") for line in lines)
    assert all("\n" not in line[:-1] for line in lines)


def test_empty_attempts_header_only(tmp_path):
    job = tmp_path / "job"
    job.mkdir()
    req = write_csv(tmp_path / "req.csv", REQUESTS_CSV)
    att = write_csv(tmp_path / "att.csv", "request_id,attempt_no\n")

    records = parse(run_join(req, att, job))
    assert len(records) == 3
    assert all(r["attempt"] is None for r in records)
    assert [r["request"]["request_id"] for r in records] == ["r1", "r2", "r3"]


def test_duplicate_request_lines_each_emit_pairs(tmp_path):
    job = tmp_path / "job"
    job.mkdir()
    req = write_csv(
        tmp_path / "req.csv",
        "request_id,v\nr1,a\nr1,b\n",
    )
    att = write_csv(tmp_path / "att.csv", "request_id,n\nr1,1\nr1,2\n")
    records = parse(run_join(req, att, job))
    # 两条同号请求行，各自 × 两条尝试 = 4 行，请求行内容保持各自原值
    assert len(records) == 4
    assert [r["request"]["v"] for r in records] == ["a", "a", "b", "b"]
    assert [r["attempt"]["n"] for r in records] == ["1", "2", "1", "2"]


def test_blank_lines_and_utf8(tmp_path):
    job = tmp_path / "job"
    job.mkdir()
    req = write_csv(
        tmp_path / "req.csv",
        "request_id,msg\nr1,你好\n\nr2,世界\n",
    )
    att = write_csv(
        tmp_path / "att.csv",
        "request_id,note\nr1,尝试一\n",
    )
    records = parse(run_join(req, att, job))
    assert [r["request"]["request_id"] for r in records] == ["r1", "r2"]
    assert records[0]["attempt"]["note"] == "尝试一"
    assert records[0]["request"]["msg"] == "你好"
    assert records[1]["attempt"] is None


def test_missing_request_id_column_raises(tmp_path):
    job = tmp_path / "job"
    job.mkdir()
    req = write_csv(tmp_path / "req.csv", "id,x\nr1,1\n")
    att = write_csv(tmp_path / "att.csv", "request_id,n\nr1,1\n")
    with pytest.raises(CsvFormatError):
        run_join(req, att, job)


def test_disconnect_during_indexing_stops_promptly(tmp_path):
    job = tmp_path / "job"
    job.mkdir()
    req = write_csv(tmp_path / "req.csv", REQUESTS_CSV)
    # 2000 条尝试，batch_size=1，探针在第 5 个批次后要求停止
    att_lines = ["request_id,n"] + [f"r{i % 3},{i}" for i in range(2000)]
    att = write_csv(tmp_path / "att.csv", "\n".join(att_lines) + "\n")

    calls = 0

    async def gone():
        nonlocal calls
        calls += 1
        return calls >= 5

    async def scenario():
        out = []
        with req.open("rb") as rf, att.open("rb") as af:
            async for line in left_join_ndjson(
                rf, af, job, batch_size=1, is_disconnected=gone
            ):
                out.append(line)
        return out

    out = asyncio.run(scenario())
    assert out == []  # 还没进入出数阶段
    # 探针调用次数是个很小的常数，而不是 2000（证明早早停下）
    assert calls <= 8


def test_aclose_runs_finalizer_without_error(tmp_path):
    job = tmp_path / "job"
    job.mkdir()
    req = write_csv(tmp_path / "req.csv", REQUESTS_CSV)
    att = write_csv(tmp_path / "att.csv", ATTEMPTS_CSV)

    async def scenario():
        gen = left_join_ndjson(req.open("rb"), att.open("rb"), job)
        first = await gen.__anext__()
        assert json.loads(first)["request"]["request_id"] == "r1"
        # 模拟客户端断连时 ASGI 框架对生成器执行 aclose
        await gen.aclose()

    asyncio.run(scenario())
    # SQLite 连接已关闭：可以删除/打开数据库文件而不挂起
    assert (job / "attempts.sqlite3").exists()


def test_query_plan_uses_primary_key_index(tmp_path):
    db = tmp_path / "plan.sqlite3"
    conn = connect_index(db)
    conn.executemany(
        "INSERT INTO attempt_index VALUES (?,?,?)",
        [("r1", 2, "{}"), ("r1", 1, "{}"), ("r2", 1, "{}")],
    )
    conn.commit()
    plan = "\n".join(
        str(row[-1])
        for row in conn.execute(
            "EXPLAIN QUERY PLAN SELECT payload FROM attempt_index "
            "WHERE request_id=? ORDER BY seq",
            ("r1",),
        )
    )
    print("\nQUERY PLAN:", plan)
    conn.close()
    assert "attempt_index" in plan
    assert "PRIMARY KEY" in plan.upper() or "USING INDEX" in plan.upper()


def _write_scaled_fixture(base: Path, n_attempts: int):
    req = base / f"req_{n_attempts}.csv"
    att = base / f"att_{n_attempts}.csv"
    req.write_text("request_id,p\nshot,/hot\n", encoding="utf-8")
    with att.open("w", encoding="utf-8") as f:
        f.write("request_id,n,payload\n")
        # 全部匹配同一个 request_id：匹配行数 = n_attempts
        for i in range(n_attempts):
            f.write(f"shot,{i},padding-value-{i % 97}\n")
    return req, att


def _count_join_lines(req: Path, att: Path, job: Path) -> int:
    async def _run():
        count = 0
        with req.open("rb") as rf, att.open("rb") as af:
            async for _line in left_join_ndjson(rf, af, job):
                count += 1  # 只计数，绝不保存记录
        return count

    return asyncio.run(_run())


def test_memory_does_not_grow_with_match_count(tmp_path):
    """匹配行数 x10，算子的 Python 峰值分配不应相应增长。"""
    results = {}
    for n in (20_000, 200_000):
        req, att = _write_scaled_fixture(tmp_path, n)
        job = tmp_path / f"job_{n}"
        job.mkdir()

        tracemalloc.start()
        try:
            count = _count_join_lines(req, att, job)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()

        assert count == n
        results[n] = peak

    small_peak_mb = results[20_000] / 1024 / 1024
    large_peak_mb = results[200_000] / 1024 / 1024
    # 打印出来便于在验证报告中引用实测数字
    print(f"\ntracemalloc peak: 20k={small_peak_mb:.2f}MiB "
          f"200k={large_peak_mb:.2f}MiB")
    # 10 倍匹配行数，峰值增长小于 8MiB（批次 + 单行 + 缓冲的量级）
    assert large_peak_mb < small_peak_mb + 8.0
    assert large_peak_mb < 30.0
