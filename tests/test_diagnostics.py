from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import join_core, main
from app.join_core import CsvFormatError, left_join_ndjson
from app.main import _TimedJoinResponse, app
from app.metrics import JoinPhaseMetrics


class FakeClock:
    """可控单调时钟，让耗时断言与真实调度抖动解耦。"""

    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture()
def clock():
    return FakeClock()


@pytest.fixture()
def metrics(clock):
    return JoinPhaseMetrics(segment_seconds=10.0, clock=clock)


# ---------------------------------------------------------------------------
# 聚合口径：并行相加 / 在途 / 跨段拆分 / 结束后照算
# ---------------------------------------------------------------------------


def test_parallel_jobs_sum_job_seconds(metrics, clock):
    metrics.index.enter()
    metrics.index.enter()
    clock.advance(1.0)

    snap = metrics.snapshot()
    index = snap["phases"]["index"]
    assert index["in_flight"] == 2
    # 两笔作业并行一秒 = 两作业秒，而不是墙钟 1 秒
    assert index["job_seconds_total"] == 2.0
    assert index["current_segment_seconds"] == 2.0
    assert index["previous_segment_seconds"] == 0.0

    metrics.index.exit()
    snap = metrics.snapshot()
    assert snap["phases"]["index"]["in_flight"] == 1
    assert snap["phases"]["index"]["job_seconds_total"] == 2.0


def test_segment_split_across_boundary(metrics, clock):
    # 第 0 段 [0,10)；t=5 进入，t=15 查询：本段 5..10=5s，下一段 10..15=5s
    clock.advance(5.0)
    metrics.index.enter()
    clock.advance(10.0)

    snap = metrics.snapshot()
    index = snap["phases"]["index"]
    assert snap["process"]["current_segment"] == 1
    assert snap["process"]["previous_segment"] == 0
    assert index["previous_segment_seconds"] == 5.0
    assert index["current_segment_seconds"] == 5.0
    assert index["job_seconds_total"] == 10.0
    assert index["in_flight"] == 1


def test_long_running_job_keeps_only_two_segment_buckets(metrics, clock):
    # t=5 进入，直到 t=125（第 12 段）仍在途
    clock.advance(5.0)
    metrics.index.enter()
    clock.advance(120.0)

    snap = metrics.snapshot()
    index = snap["phases"]["index"]
    # 跨 12 段按实际耗时拆开：0..10 的 5s 早已超出“上一段”，被丢弃
    assert index["job_seconds_total"] == 120.0
    assert index["previous_segment_seconds"] == 10.0  # 11 段整段
    assert index["current_segment_seconds"] == 5.0  # 12 段 120..125
    # 不保存逐段历史，更不保存逐作业历史
    assert len(metrics.index._buckets) <= 2


def test_pending_phase_counts_to_query_time_without_double_count(metrics, clock):
    metrics.body_write.enter()
    clock.advance(3.0)
    snap = metrics.snapshot()
    assert snap["phases"]["body_write"]["in_flight"] == 1
    assert snap["phases"]["body_write"]["job_seconds_total"] == 3.0

    clock.advance(2.0)
    snap = metrics.snapshot()
    assert snap["phases"]["body_write"]["job_seconds_total"] == 5.0

    # 作业结束后不再算在途，但已耗时间保留，后续查询不重复累计
    metrics.body_write.exit()
    clock.advance(100.0)
    snap = metrics.snapshot()
    assert snap["phases"]["body_write"]["in_flight"] == 0
    assert snap["phases"]["body_write"]["job_seconds_total"] == 5.0


def test_failure_keeps_elapsed_and_drops_in_flight(metrics, clock):
    with pytest.raises(RuntimeError):
        with metrics.index.track():
            clock.advance(1.5)
            raise RuntimeError("boom")
    snap = metrics.snapshot()
    assert snap["phases"]["index"]["in_flight"] == 0
    assert snap["phases"]["index"]["job_seconds_total"] == 1.5


def test_phases_reported_separately(metrics, clock):
    metrics.index.enter()
    metrics.body_write.enter()
    metrics.body_write.enter()
    clock.advance(2.0)

    snap = metrics.snapshot()
    assert snap["phases"]["index"]["job_seconds_total"] == 2.0
    assert snap["phases"]["body_write"]["job_seconds_total"] == 4.0
    assert snap["totals"]["in_flight"] == 3
    assert snap["totals"]["job_seconds_total"] == 6.0
    assert snap["totals"]["current_segment_seconds"] == 6.0


# ---------------------------------------------------------------------------
# 正文写入计时：只包非空 send，生成下一行的时间不计
# ---------------------------------------------------------------------------


def _run_stream_response(metrics, clock, body_items, send_hook):
    async def body():
        for item in body_items:
            # “生成下一行”的时间必须不计入 body_write
            clock.advance(0.5)
            yield item

    sent: list[dict] = []

    async def send(message):
        sent.append(message)
        await send_hook(message)

    async def scenario():
        resp = _TimedJoinResponse(body(), metrics=metrics)
        await resp.stream_response(send)
        return sent

    return asyncio.run(scenario())


def test_body_write_times_only_non_empty_sends(metrics, clock):
    async def slow_send(message):
        if message["type"] == "http.response.body" and message["body"]:
            clock.advance(2.0)

    sent = _run_stream_response(
        metrics, clock, ['{"i":0}\n', b"", '{"i":1}\n', '{"i":2}\n'], slow_send
    )
    snap = metrics.snapshot()
    body = snap["phases"]["body_write"]
    # 3 次非空 send × 2s；空 chunk 与 4 次生成各 0.5s 都不计入
    assert body["job_seconds_total"] == 6.0
    assert body["in_flight"] == 0
    assert snap["phases"]["index"]["job_seconds_total"] == 0.0
    assert sent[-1]["more_body"] is False


def test_body_write_in_flight_while_send_blocks(metrics, clock):
    started = asyncio.Event()
    release = asyncio.Event()

    async def body():
        yield '{"only":1}\n'

    async def blocking_send(message):
        if message["type"] == "http.response.body" and message["body"]:
            clock.advance(1.0)
            started.set()
            await release.wait()

    async def scenario():
        resp = _TimedJoinResponse(body(), metrics=metrics)
        task = asyncio.create_task(resp.stream_response(blocking_send))
        await started.wait()
        mid = metrics.snapshot()["phases"]["body_write"]
        release.set()
        await task
        final = metrics.snapshot()["phases"]["body_write"]
        return mid, final

    mid, final = asyncio.run(scenario())
    assert mid["in_flight"] == 1
    assert mid["job_seconds_total"] == 1.0
    assert final["in_flight"] == 0
    assert final["job_seconds_total"] == 1.0


def test_body_write_cancelled_send_drops_in_flight_keeps_time(metrics, clock):
    started = asyncio.Event()

    async def body():
        yield '{"only":1}\n'

    async def stuck_send(message):
        if message["type"] == "http.response.body" and message["body"]:
            clock.advance(1.25)
            started.set()
            await asyncio.sleep(3600)

    async def scenario():
        resp = _TimedJoinResponse(body(), metrics=metrics)
        task = asyncio.create_task(resp.stream_response(stuck_send))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    snap = metrics.snapshot()
    assert snap["phases"]["body_write"]["in_flight"] == 0
    assert snap["phases"]["body_write"]["job_seconds_total"] == 1.25


# ---------------------------------------------------------------------------
# 算子接入：索引阶段在途 / 断连后收口
# ---------------------------------------------------------------------------


def test_index_phase_in_flight_during_indexing(tmp_path):
    clock = FakeClock()
    metrics = JoinPhaseMetrics(segment_seconds=60.0, clock=clock)
    req = tmp_path / "req.csv"
    req.write_text("request_id,path\nr1,/a\n", encoding="utf-8")
    att = tmp_path / "att.csv"
    att.write_text(
        "request_id,n\n" + "".join(f"r{i % 3},{i}\n" for i in range(200)),
        encoding="utf-8",
    )
    job = tmp_path / "job"
    job.mkdir()

    seen: list[tuple[int, float]] = []
    calls = 0

    async def probe():
        nonlocal calls
        calls += 1
        # 索引期间：阶段在途恰好 1，且查询是常数时间快照
        snap = metrics.snapshot()["phases"]["index"]
        seen.append((snap["in_flight"], snap["job_seconds_total"]))
        clock.advance(0.5)
        return calls >= 5

    async def scenario():
        with req.open("rb") as rf, att.open("rb") as af:
            async for _line in left_join_ndjson(
                rf,
                af,
                job,
                batch_size=1,
                is_disconnected=probe,
                metrics=metrics,
            ):
                pytest.fail("断连发生在索引阶段，不应进入出数")

    asyncio.run(scenario())
    # 第 1 次探针是建索引之前的“客户端已走”预检，此时尚未进入索引阶段；
    # 之后每次探针都看到索引阶段在途恰好 1，作业秒随虚拟时钟累计。
    assert seen[0][0] == 0
    assert all(snap[0] == 1 for snap in seen[1:])
    assert [snap[1] for snap in seen[1:]] == [0.0, 0.5, 1.0, 1.5]
    snap = metrics.snapshot()["phases"]["index"]
    assert snap["in_flight"] == 0
    assert snap["job_seconds_total"] == 2.0


def test_index_phase_exit_on_format_error(tmp_path):
    metrics = JoinPhaseMetrics(segment_seconds=60.0, clock=FakeClock())
    req = tmp_path / "req.csv"
    req.write_text("request_id,path\nr1,/a\n", encoding="utf-8")
    att = tmp_path / "att.csv"
    att.write_text("id,n\nr1,1\n", encoding="utf-8")
    job = tmp_path / "job"
    job.mkdir()

    async def scenario():
        with req.open("rb") as rf, att.open("rb") as af:
            async for _line in left_join_ndjson(rf, af, job, metrics=metrics):
                pass

    with pytest.raises(CsvFormatError):
        asyncio.run(scenario())
    snap = metrics.snapshot()["phases"]["index"]
    assert snap["in_flight"] == 0


# ---------------------------------------------------------------------------
# HTTP 层接线
# ---------------------------------------------------------------------------


def test_diagnostics_endpoint_shape():
    with TestClient(app) as client:
        resp = client.get("/diagnostics")
    assert resp.status_code == 200
    payload = resp.json()
    assert set(payload["phases"]) == {"index", "body_write"}
    for phase in payload["phases"].values():
        assert set(phase) == {
            "in_flight",
            "job_seconds_total",
            "current_segment_seconds",
            "previous_segment_seconds",
        }
    assert payload["process"]["segment_seconds"] > 0
    assert dict(payload["totals"]).keys() == {
        "in_flight",
        "job_seconds_total",
        "current_segment_seconds",
        "previous_segment_seconds",
    }
    assert resp.headers.get("cache-control") == "no-store"


def test_diagnostics_reports_real_join_activity(monkeypatch):
    fresh = JoinPhaseMetrics(segment_seconds=60.0)
    monkeypatch.setattr(main, "METRICS", fresh)
    monkeypatch.setattr(join_core, "METRICS", fresh)

    n = 20_000
    req_csv = "request_id,path\nhot,/x\n"
    att_csv = "request_id,n\n" + "".join(f"hot,{i}\n" for i in range(n))

    with TestClient(app) as client:
        before = client.get("/diagnostics").json()
        resp = client.post(
            "/join",
            json={"requests": req_csv, "attempts": att_csv},
        )
        assert resp.status_code == 200, resp.text
        assert len(resp.text.splitlines()) == n
        after = client.get("/diagnostics").json()

    assert before["totals"]["job_seconds_total"] == 0.0
    index = after["phases"]["index"]
    body = after["phases"]["body_write"]
    # 索引阶段确实被计时，且结束后不再在途
    assert index["job_seconds_total"] > 0.0
    assert index["in_flight"] == 0
    # 正文写入阶段也被计时（20k 次非空 send）
    assert body["job_seconds_total"] > 0.0
    assert body["in_flight"] == 0
    assert after["totals"]["job_seconds_total"] == pytest.approx(
        index["job_seconds_total"] + body["job_seconds_total"], abs=1e-3
    )
