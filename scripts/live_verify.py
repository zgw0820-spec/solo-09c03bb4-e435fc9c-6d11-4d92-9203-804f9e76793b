#!/usr/bin/env python3
"""真实服务器 + curl 的关键场景验证（仅用标准库；需要 PATH 中有 curl）。

验证项：
  A. 示例数据左联接顺序 / null 正确
  B. 出数阶段客户端断开：处理停止且本次临时目录被清理
  C. 整理（索引）尝试日志阶段客户端断开：尽快停止、有日志、临时目录清理
  D. 300 万匹配行单请求：结果条数正确，服务器 RSS 不随匹配行数膨胀
  E. 两个并发作业：各自结果正确，结束后临时目录全部清理

运行：
  PYTHONPATH=/tmp/pylibs python3 scripts/live_verify.py
可选环境：
  VERIFY_DIR=/tmp/csvjoin-verify   夹具 / 临时索引 / 日志根目录
  PORT=8011
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
VERIFY_DIR = Path(os.environ.get("VERIFY_DIR", "/tmp/csvjoin-verify"))
DATA = VERIFY_DIR / "data"
TMPROOT = VERIFY_DIR / "tmp"
OUT = VERIFY_DIR / "out"
PORT = int(os.environ.get("PORT", "8011"))
BASE = f"http://127.0.0.1:{PORT}"

results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")


def sh(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def generate_fixtures() -> None:
    DATA.mkdir(parents=True, exist_ok=True)
    # 全挂在同一个 request_id 下的大量尝试（验证匹配行数不涨内存）
    hot_req = DATA / "hot_requests.csv"
    hot_att = DATA / "hot_attempts.csv"
    n_hot = int(os.environ.get("HOT_ROWS", "3000000"))
    hot_req.write_text("request_id,path\nshot,/hot-path\n", encoding="utf-8")
    print(f"generating {n_hot} hot attempts ...")
    with hot_att.open("w", encoding="utf-8") as f:
        f.write("request_id,n,payload\n")
        for i in range(n_hot):
            f.write(f"shot,{i},padding-value-{i % 997}\n")

    # 混合 request_id 的更大尝试表（让“索引阶段”持续足够久）
    mix_req = DATA / "mix_requests.csv"
    mix_att = DATA / "mix_attempts.csv"
    n_mix = int(os.environ.get("MIX_ROWS", "5000000"))
    mix_req.write_text("request_id,path\nr0,/zero\nr7,/seven\n", encoding="utf-8")
    print(f"generating {n_mix} mixed attempts ...")
    with mix_att.open("w", encoding="utf-8") as f:
        f.write("request_id,n\n")
        for i in range(n_mix):
            f.write(f"r{i % 5000},{i}\n")


def start_server() -> subprocess.Popen:
    TMPROOT.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)
    log = (VERIFY_DIR / "server.log").open("wb")
    env = os.environ.copy()
    env["CSVJOIN_TMPDIR"] = str(TMPROOT)
    env["CSVJOIN_PROGRESS_EVERY"] = "100000"
    env["PYTHONUNBUFFERED"] = "1"
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(PORT),
            "--no-access-log",
        ],
        cwd=str(REPO),
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    # 等健康检查
    deadline = time.time() + 40
    last_err = ""
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early code={proc.returncode}")
        try:
            with urllib.request.urlopen(f"{BASE}/healthz", timeout=2) as r:
                if r.status == 200:
                    print("server is healthy")
                    return proc
        except Exception as exc:  # noqa: BLE001
            last_err = str(exc)
            time.sleep(0.4)
    raise RuntimeError(f"server not healthy: {last_err}")


def leftover_jobs() -> list[Path]:
    return list(TMPROOT.glob("csvjoin-*"))


def wait_no_leftovers(timeout: float = 12.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not leftover_jobs():
            return True
        time.sleep(0.2)
    return not leftover_jobs()


def rss_kb(pid: int) -> int:
    try:
        text = Path(f"/proc/{pid}/status").read_text()
        for line in text.splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except Exception:  # noqa: BLE001
        pass
    return -1


def scenario_examples() -> None:
    out_file = OUT / "examples.ndjson"
    with out_file.open("wb") as f:
        cp = subprocess.run(
            [
                "curl",
                "-sS",
                "-F",
                f"requests=@{REPO / 'examples/requests.csv'};type=text/csv",
                "-F",
                f"attempts=@{REPO / 'examples/attempts.csv'};type=text/csv",
                f"{BASE}/join",
            ],
            stdout=f,
            stderr=subprocess.PIPE,
        )
    if cp.returncode != 0:
        check("A examples curl", False, cp.stderr.decode()[:300])
        return
    rows = [json.loads(line) for line in out_file.read_text().splitlines() if line]
    ids = [r["request"]["request_id"] for r in rows]
    ok = ids == [
        "req-1001",
        "req-1002",
        "req-1002",
        "req-1002",
        "req-1003",
        "req-1004",
    ]
    ups = [
        r["attempt"]["upstream"]
        for r in rows
        if r["request"]["request_id"] == "req-1002"
    ]
    check(
        "A examples 顺序与 null",
        ok and ups == ["payments-a", "payments-b", "payments-a"]
        and rows[4]["attempt"] is None,
        f"ids={ids}",
    )
    check("A 正常结束无临时目录残留", wait_no_leftovers())


def scenario_output_disconnect() -> None:
    proc = subprocess.Popen(
        [
            "curl",
            "-sS",
            "-N",
            "-F",
            f"requests=@{DATA / 'hot_requests.csv'}",
            "-F",
            f"attempts=@{DATA / 'hot_attempts.csv'}",
            f"{BASE}/join",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        bufsize=1,
    )
    try:
        first = proc.stdout.readline()  # type: ignore[union-attr]
        second = proc.stdout.readline()  # type: ignore[union-attr]
        ok_lines = first.startswith("{") and second.startswith("{")
        parsed = json.loads(first)
        ok_id = parsed["request"]["request_id"] == "shot"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
    cleaned = wait_no_leftovers()
    check(
        "B 出数中断连：已收到数据后停止",
        ok_lines and ok_id,
        f"first_line_prefix={first[:60]!r}",
    )
    check("B 出数中断连：临时目录已清理", cleaned, f"left={leftover_jobs()}")


def scenario_index_disconnect(server: subprocess.Popen) -> None:
    log_path = VERIFY_DIR / "server.log"
    size_before_start = log_path.stat().st_size

    curl = subprocess.Popen(
        [
            "curl",
            "-sS",
            "-F",
            f"requests=@{DATA / 'mix_requests.csv'}",
            "-F",
            f"attempts=@{DATA / 'mix_attempts.csv'}",
            f"{BASE}/join",
            "-o",
            "/dev/null",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # 等服务器日志明确出现“索引进度”，证明已进入整理尝试日志阶段
    reached = False
    deadline = time.time() + 90
    last_size = size_before_start
    while time.time() < deadline:
        text = log_path.read_text(errors="replace")
        tail = text[size_before_start:]
        if "indexed 100000 attempt rows" in tail:
            reached = True
            break
        time.sleep(0.5)
        last_size = log_path.stat().st_size
    if not reached:
        curl.kill()
        check("C 索引阶段到达(100k 进度)", False, "超时未观察到索引进度")
        return

    lines_before = log_path.read_text(errors="replace").count("\n")
    curl.terminate()
    try:
        curl.wait(timeout=5)
    except subprocess.TimeoutExpired:
        curl.kill()

    cleaned = wait_no_leftovers(timeout=15)

    # 给服务器 2.5s，确认取消后没有继续刷索引进度
    time.sleep(2.5)
    log_after = log_path.read_text(errors="replace")
    new_lines = log_after.splitlines()[lines_before:]
    continued = any("indexed" in ln and "attempt rows" in ln for ln in new_lines)
    stop_logged = any(
        ("client gone during indexing" in ln)
        or ("cancelled because client disconnected" in ln)
        for ln in new_lines
    ) or any(
        ("client gone during indexing" in ln)
        or ("cancelled because client disconnected" in ln)
        for ln in log_after.splitlines()[-15:]
    )
    check("C 索引阶段确实开始", reached)
    check("C 断连后停止继续索引", not continued, f"new_log_lines={new_lines[-6:]}")
    check("C 断连停止被日志记录", stop_logged)
    check("C 索引中断连：临时目录已清理", cleaned, f"left={leftover_jobs()}")


def scenario_rss(server: subprocess.Popen) -> None:
    out_file = OUT / "hot_full.ndjson"
    rss_before = rss_kb(server.pid)
    curl = subprocess.Popen(
        [
            "curl",
            "-sS",
            "-F",
            f"requests=@{DATA / 'hot_requests.csv'}",
            "-F",
            f"attempts=@{DATA / 'hot_attempts.csv'}",
            f"{BASE}/join",
            "-o",
            str(out_file),
        ]
    )
    max_rss = rss_before
    samples = 0
    while curl.poll() is None:
        max_rss = max(max_rss, rss_kb(server.pid))
        samples += 1
        time.sleep(0.1)
    rc = curl.wait()
    line_count = 0
    if out_file.exists():
        wc = sh(["wc", "-l", str(out_file)])
        try:
            line_count = int(wc.stdout.split()[0])
        except Exception:  # noqa: BLE001
            pass
    expected = int(os.environ.get("HOT_ROWS", "3000000"))
    check(
        "D 大匹配量输出条数正确",
        rc == 0 and line_count == expected,
        f"rc={rc} lines={line_count} expected={expected}",
    )
    growth_mb = (max_rss - max(rss_before, 0)) / 1024
    max_mb = max_rss / 1024
    print(
        f"    RSS before={rss_before / 1024:.1f}MiB "
        f"max={max_mb:.1f}MiB growth={growth_mb:.1f}MiB samples={samples}"
    )
    # 300 万匹配行；若用 fetchall/攒结果，RSS 会涨数百 MiB 以上。
    # 阈值放宽到 150MiB 绝对值，避免环境抖动误报。
    check("D 服务器峰值 RSS < 150MiB", 0 < max_rss < 150 * 1024, f"max={max_mb:.1f}MiB")
    check("D RSS 相对增长 < 60MiB", growth_mb < 60, f"growth={growth_mb:.1f}MiB")
    check("D 正常大作业后临时目录清理", wait_no_leftovers())


def scenario_concurrent() -> None:
    procs = []
    outs = []
    for i in range(2):
        out_file = OUT / f"concurrent_{i}.ndjson"
        outs.append(out_file)
        f = out_file.open("wb")
        p = subprocess.Popen(
            [
                "curl",
                "-sS",
                "-F",
                f"requests=@{REPO / 'examples/requests.csv'}",
                "-F",
                f"attempts=@{REPO / 'examples/attempts.csv'}",
                f"{BASE}/join",
            ],
            stdout=f,
            stderr=subprocess.DEVNULL,
        )
        procs.append((p, f))
    ok = True
    for p, f in procs:
        p.wait(timeout=30)
        f.close()
    for out_file in outs:
        rows = [json.loads(line) for line in out_file.read_text().splitlines() if line]
        if len(rows) != 6:
            ok = False
    check("E 两个并发作业均得到 6 行", ok)
    check("E 并发作业后无临时目录残留", wait_no_leftovers())


def main() -> int:
    if VERIFY_DIR.exists():
        shutil.rmtree(VERIFY_DIR, ignore_errors=True)
    server = None
    try:
        generate_fixtures()
        server = start_server()
        scenario_examples()
        scenario_output_disconnect()
        scenario_index_disconnect(server)
        scenario_rss(server)
        scenario_concurrent()
    finally:
        if server is not None:
            server.send_signal(signal.SIGTERM)
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
        # 验证结束后清掉整个验证目录（含数 GB 夹具/输出）
        shutil.rmtree(VERIFY_DIR, ignore_errors=True)

    failed = [name for name, ok, _ in results if not ok]
    print("\n================ SUMMARY ================")
    for name, ok, detail in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}")
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
