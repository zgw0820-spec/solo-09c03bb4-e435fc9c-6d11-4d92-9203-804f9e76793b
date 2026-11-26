from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import main
from app.main import app

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"

REQ_CSV = (
    "request_id,path\n"
    "r1,/a\n"
    "r2,/b\n"
    "r3,/c\n"
).encode()
ATT_CSV = (
    "request_id,n\n"
    "r2,1\n"
    "r1,9\n"
    "r2,2\n"
).encode()


@pytest.fixture()
def client():
    with TestClient(app) as c:
        yield c


def _files(req_name="requests", att_name="attempts", other_names=None):
    names = other_names or (req_name, att_name)
    return {
        names[0]: ("requests.csv", REQ_CSV, "text/csv"),
        names[1]: ("attempts.csv", ATT_CSV, "text/csv"),
    }


def test_health(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


@pytest.mark.parametrize("path", ["/join", "/api/join", "/v1/join"])
def test_multipart_happy_path_all_paths(client, path):
    resp = client.post(path, files=_files())
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"].startswith("application/x-ndjson")
    rows = [json.loads(line) for line in resp.text.splitlines() if line]
    assert [r["request"]["request_id"] for r in rows] == [
        "r1",
        "r2",
        "r2",
        "r3",
    ]
    assert [
        r["attempt"]["n"] for r in rows if r["attempt"] is not None
    ] == ["9", "1", "2"]
    assert rows[-1]["attempt"] is None


def test_alias_field_names(client):
    resp = client.post(
        "/join", files=_files(other_names=("request_log", "attempt_log"))
    )
    assert resp.status_code == 200, resp.text
    assert len(resp.text.splitlines()) == 4


def test_suffixed_alias_field_names(client):
    resp = client.post(
        "/join", files=_files(other_names=("req_csv", "upstream_attempts_csv"))
    )
    assert resp.status_code == 200, resp.text
    assert len(resp.text.splitlines()) == 4


def test_demo_page_and_cors(client):
    page = client.get("/demo")
    assert page.status_code == 200
    assert "text/html" in page.headers["content-type"]
    cors = client.get("/healthz", headers={"origin": "http://example"})
    assert cors.headers.get("access-control-allow-origin") == "*"


def test_fallback_arbitrary_two_files(client):
    resp = client.post("/join", files=_files(other_names=("aaa", "bbb")))
    assert resp.status_code == 200, resp.text
    assert len(resp.text.splitlines()) == 4


def test_json_body(client):
    resp = client.post(
        "/join",
        json={
            "requests": REQ_CSV.decode(),
            "attempts": ATT_CSV.decode(),
        },
    )
    assert resp.status_code == 200, resp.text
    rows = [json.loads(line) for line in resp.text.splitlines() if line]
    assert len(rows) == 4
    assert rows[0]["attempt"]["n"] == "9"


def test_urlencoded_csv_text(client):
    resp = client.post(
        "/join",
        data={
            "requests": REQ_CSV.decode(),
            "attempts": ATT_CSV.decode(),
        },
    )
    assert resp.status_code == 200, resp.text
    assert len(resp.text.splitlines()) == 4


def test_missing_file_returns_400(client):
    resp = client.post(
        "/join",
        files={"requests": ("r.csv", REQ_CSV, "text/csv")},
    )
    assert resp.status_code == 400


def test_unknown_single_file_returns_400(client):
    resp = client.post(
        "/join",
        files={"weird": ("r.csv", REQ_CSV, "text/csv")},
    )
    assert resp.status_code == 400


def test_missing_request_id_column_returns_400(client):
    bad_req = b"id,path\nr1,/a\n"
    resp = client.post(
        "/join",
        files={
            "requests": ("r.csv", bad_req, "text/csv"),
            "attempts": ("a.csv", ATT_CSV, "text/csv"),
        },
    )
    assert resp.status_code == 400
    assert "request_id" in resp.text


def test_unsupported_content_type_415(client):
    resp = client.post("/join", content="hello", headers={"content-type": "text/plain"})
    assert resp.status_code == 415


def test_temp_dir_cleaned_after_normal_response(client, tmp_path, monkeypatch):
    monkeypatch.setattr(main, "TMP_ROOT", str(tmp_path))
    resp = client.post("/join", files=_files())
    assert resp.status_code == 200
    leftovers = list(tmp_path.glob("csvjoin-*"))
    assert leftovers == []


def test_temp_dir_cleaned_after_validation_error(client, tmp_path, monkeypatch):
    monkeypatch.setattr(main, "TMP_ROOT", str(tmp_path))
    bad_req = b"id,path\nr1,/a\n"
    resp = client.post(
        "/join",
        files={
            "requests": ("r.csv", bad_req, "text/csv"),
            "attempts": ("a.csv", ATT_CSV, "text/csv"),
        },
    )
    assert resp.status_code == 400
    # 校验失败发生在建目录之前；根目录下不应有任何 csvjoin-*
    assert list(tmp_path.glob("csvjoin-*")) == []


def test_real_example_files_end_to_end(client):
    with (EXAMPLES / "requests.csv").open("rb") as rf, (
        EXAMPLES / "attempts.csv"
    ).open("rb") as af:
        resp = client.post(
            "/join",
            files={
                "requests": ("requests.csv", rf, "text/csv"),
                "attempts": ("attempts.csv", af, "text/csv"),
            },
        )
    assert resp.status_code == 200, resp.text
    rows = [json.loads(line) for line in resp.text.splitlines() if line]
    assert [r["request"]["request_id"] for r in rows] == [
        "req-1001",
        "req-1002",
        "req-1002",
        "req-1002",
        "req-1003",
        "req-1004",
    ]
