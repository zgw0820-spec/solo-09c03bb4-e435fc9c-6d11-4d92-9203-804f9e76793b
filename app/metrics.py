"""``/join`` 两个阶段的 O(1) 聚合统计（只做计数，不保存作业历史）。

值班排障需要回答一个问题：导出变慢，是 **给尝试日志建 SQLite 索引**
拖住了，还是 **NDJSON 正文写不动**（客户端/网络背压）？

计量口径：

* 阶段 ``index``：为 attempts 日志建磁盘 SQLite 索引的时间。
* 阶段 ``body_write``：每次 **非空** NDJSON 正文 ``send`` 从发起到返回
  （或抛错/被取消）的时间；两次 send 之间生成下一行的时间不算。
* 累计作业秒（job-seconds）：对每个作业的在途时长求和，而不是墙钟时间。
  两个作业并行 1 秒记 2 秒。
* 时间轴从本进程启动（模块导入）起，按 ``segment_seconds``（默认 60）
  切段。查询返回 **本段** 与 **上一段** 的作业秒；跨段作业按实际耗时
  拆分到各段。
* 尚未结束的阶段按“到查询时刻为止”计入；查询本身不遍历在途作业，
  只读常数个聚合数。

实现上每个阶段只保留：``in_flight``、上次积分时刻 ``_last_ts``、
累计值 ``_total`` 和最近两个段桶。作业进入/离开时先按
``in_flight × Δt`` 积分，因此无需逐作业计时器；查询时再做一次积分即可
把在途作业算到当前时刻。
"""

from __future__ import annotations

import os
import threading
import time
from contextlib import contextmanager
from typing import Callable, Iterator

DEFAULT_SEGMENT_SECONDS = 60.0
PHASE_INDEX = "index"
PHASE_BODY_WRITE = "body_write"


def _env_segment_seconds() -> float:
    raw = os.environ.get("CSVJOIN_DIAG_SEGMENT_SECONDS", "")
    if not raw:
        return DEFAULT_SEGMENT_SECONDS
    try:
        value = float(raw)
    except ValueError:
        return DEFAULT_SEGMENT_SECONDS
    return value if value > 0 else DEFAULT_SEGMENT_SECONDS


class PhaseTracker:
    """单个阶段的在途数 + 累计作业秒。

    无逐作业记录：只保留在途数量、最近一次积分时刻、累计值和
    “本段 / 上一段”两个聚合桶。
    """

    __slots__ = (
        "name",
        "_metrics",
        "_in_flight",
        "_last_ts",
        "_total",
        "_buckets",
    )

    def __init__(self, name: str, metrics: "JoinPhaseMetrics", now: float) -> None:
        self.name = name
        self._metrics = metrics
        self._in_flight = 0
        self._last_ts = now
        self._total = 0.0
        self._buckets: dict[int, float] = {}

    @property
    def in_flight(self) -> int:
        return self._in_flight

    @contextmanager
    def track(self) -> Iterator[None]:
        """标记一个作业处于本阶段。

        用 ``with`` 包住可能被取消/抛错的 await；finally 保证离开时
        在途数立即下降，但已经累计的耗时保留（作业断开或失败后照算）。
        """
        self.enter()
        try:
            yield
        finally:
            self.exit()

    def enter(self) -> None:
        with self._metrics._lock:
            self._enter(self._metrics._clock())

    def exit(self) -> None:
        with self._metrics._lock:
            self._exit(self._metrics._clock())

    # -- 以下方法调用时须持有 metrics._lock ------------------------------

    def _enter(self, now: float) -> None:
        self._accrue(now)
        self._in_flight += 1

    def _exit(self, now: float) -> None:
        self._accrue(now)
        if self._in_flight > 0:
            self._in_flight -= 1

    def _segment_of(self, ts: float) -> int:
        return int((ts - self._metrics._epoch) // self._metrics.segment_seconds)

    def _accrue(self, now: float) -> None:
        """把 ``[last_ts, now]`` 间 ``in_flight × dt`` 积分进累计值和段桶。"""
        last = self._last_ts
        if now < last:  # 单调时钟理论上不会回退；防御性夹住
            now = last
        n = self._in_flight
        if now > last and n:
            seg_seconds = self._metrics.segment_seconds
            epoch = self._metrics._epoch
            delta = now - last
            self._total += delta * n

            seg_last = self._segment_of(last)
            seg_now = self._segment_of(now)
            if seg_now == seg_last:
                self._buckets[seg_last] = (
                    self._buckets.get(seg_last, 0.0) + delta * n
                )
            else:
                # 上一次积分点到上一段末尾的残余
                end_last = epoch + (seg_last + 1) * seg_seconds
                self._buckets[seg_last] = (
                    self._buckets.get(seg_last, 0.0)
                    + (end_last - last) * n
                )
                if seg_now - 1 > seg_last:
                    # 中间整段对本接口无意义，只保留“上一段”用于返回
                    self._buckets[seg_now - 1] = (
                        self._buckets.get(seg_now - 1, 0.0)
                        + seg_seconds * n
                    )
                start_now = epoch + seg_now * seg_seconds
                self._buckets[seg_now] = (
                    self._buckets.get(seg_now, 0.0)
                    + (now - start_now) * n
                )
        self._last_ts = now

        # 只保留本段和上一段，不随运行时间增长（也不是逐作业历史）
        cutoff = self._segment_of(now) - 1
        for key in [k for k in self._buckets if k < cutoff]:
            del self._buckets[key]

    def _snapshot_locked(self, now: float) -> dict[str, object]:
        self._accrue(now)
        seg_now = self._segment_of(now)
        current = self._buckets.get(seg_now, 0.0)
        previous = self._buckets.get(seg_now - 1, 0.0) if seg_now > 0 else 0.0
        return {
            "in_flight": self._in_flight,
            "job_seconds_total": round(self._total, 3),
            "current_segment_seconds": round(current, 3),
            "previous_segment_seconds": round(previous, 3),
        }


class JoinPhaseMetrics:
    """两个阶段的共享时间轴与查询入口。"""

    def __init__(
        self,
        *,
        segment_seconds: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._clock = clock
        self._epoch = clock()
        self.segment_seconds = float(
            segment_seconds
            if segment_seconds is not None
            else _env_segment_seconds()
        )
        self._lock = threading.RLock()
        self.index = PhaseTracker(PHASE_INDEX, self, self._epoch)
        self.body_write = PhaseTracker(PHASE_BODY_WRITE, self, self._epoch)

    def snapshot(self) -> dict[str, object]:
        """常数时间快照；在途阶段按查询时刻结算。"""
        now = self._clock()
        with self._lock:
            index = self.index._snapshot_locked(now)
            body = self.body_write._snapshot_locked(now)
            seg_now = int((now - self._epoch) // self.segment_seconds)
            return {
                "process": {
                    "uptime_seconds": round(now - self._epoch, 3),
                    "segment_seconds": self.segment_seconds,
                    "current_segment": seg_now,
                    "previous_segment": seg_now - 1 if seg_now > 0 else None,
                },
                "phases": {
                    PHASE_INDEX: index,
                    PHASE_BODY_WRITE: body,
                },
                "totals": {
                    "in_flight": int(index["in_flight"]) + int(body["in_flight"]),
                    "job_seconds_total": round(
                        float(index["job_seconds_total"])
                        + float(body["job_seconds_total"]),
                        3,
                    ),
                    "current_segment_seconds": round(
                        float(index["current_segment_seconds"])
                        + float(body["current_segment_seconds"]),
                        3,
                    ),
                    "previous_segment_seconds": round(
                        float(index["previous_segment_seconds"])
                        + float(body["previous_segment_seconds"]),
                        3,
                    ),
                },
            }


# 进程级单例；uvicorn 导入 app.main 时创建，分段起点即进程启动时刻。
METRICS = JoinPhaseMetrics()
