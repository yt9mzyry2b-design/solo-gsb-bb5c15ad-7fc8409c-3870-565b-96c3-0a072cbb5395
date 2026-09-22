import os
import tempfile

import pytest

# 在导入 app 之前指向临时数据库
_tmpdir = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(_tmpdir, "test.db")

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402


@pytest.fixture()
def client():
    with TestClient(app) as c:
        yield c


def pkg(device, seq, key, value=1.0):
    return {
        "device_id": device,
        "seq": seq,
        "idempotency_key": key,
        "measurements": [{"metric": "temperature", "value": value, "unit": "C"}],
    }


def test_in_order_commit(client):
    r = client.post("/api/v1/packages", json=pkg("dev-a", 1, "a-1"))
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "committed"
    assert body["committed_seqs"] == [1]
    assert body["committed_upto"] == 1


def test_out_of_order_buffer_and_drain(client):
    # 首包自举：seq 10 到达即提交
    assert client.post("/api/v1/packages", json=pkg("dev-b", 10, "b-10")).json()["committed_upto"] == 10
    # 乱序：12 先到，进入缓冲
    r = client.post("/api/v1/packages", json=pkg("dev-b", 12, "b-12")).json()
    assert r["status"] == "buffered"
    assert r["committed_upto"] == 10
    # 缺口查询
    gaps = client.get("/api/v1/devices/dev-b/gaps").json()
    assert gaps["gaps"] == [11]
    assert gaps["buffered_seqs"] == [12]
    assert gaps["next_expected"] == 11
    # 补齐缺口 11 -> 11、12 连续提交
    r = client.post("/api/v1/packages", json=pkg("dev-b", 11, "b-11")).json()
    assert r["status"] == "committed"
    assert r["committed_seqs"] == [11, 12]
    assert r["committed_upto"] == 12
    # 缺口清空
    gaps = client.get("/api/v1/devices/dev-b/gaps").json()
    assert gaps["gaps"] == [] and gaps["buffered_seqs"] == []


def test_idempotent_replay(client):
    p = pkg("dev-c", 1, "c-1")
    first = client.post("/api/v1/packages", json=p).json()
    assert first["status"] == "committed"
    replay = client.post("/api/v1/packages", json=p).json()
    assert replay["status"] == "duplicate"
    assert replay["duplicate_reason"] == "idempotent_replay"
    # 重复记录可查
    dups = client.get("/api/v1/devices/dev-c/duplicates").json()
    assert dups["count"] == 1
    assert dups["records"][0]["reason"] == "idempotent_replay"


def test_duplicate_already_committed_and_buffered(client):
    client.post("/api/v1/packages", json=pkg("dev-d", 1, "d-1"))
    client.post("/api/v1/packages", json=pkg("dev-d", 3, "d-3"))  # 缓冲
    # 已提交序号，换个新键再发 -> already_committed
    r = client.post("/api/v1/packages", json=pkg("dev-d", 1, "d-1-new")).json()
    assert r["status"] == "duplicate" and r["duplicate_reason"] == "already_committed"
    # 缓冲区中序号，换个新键再发 -> already_buffered
    r = client.post("/api/v1/packages", json=pkg("dev-d", 3, "d-3-new")).json()
    assert r["status"] == "duplicate" and r["duplicate_reason"] == "already_buffered"
    dups = client.get("/api/v1/devices/dev-d/duplicates").json()
    assert dups["count"] == 2


def test_same_key_different_payload_conflict(client):
    client.post("/api/v1/packages", json=pkg("dev-e", 1, "e-1"))
    bad = pkg("dev-e", 2, "e-1")  # 同键不同内容
    r = client.post("/api/v1/packages", json=bad)
    assert r.status_code == 409


def test_missing_idempotency_key(client):
    p = pkg("dev-f", 1, "f-1")
    del p["idempotency_key"]
    assert client.post("/api/v1/packages", json=p).status_code == 422
    # 用请求头提供幂等键则成功
    r = client.post("/api/v1/packages", json=p, headers={"Idempotency-Key": "f-1"})
    assert r.status_code == 200


def test_committed_query_and_devices_list(client):
    for i in range(1, 6):
        client.post("/api/v1/packages", json=pkg("dev-g", i, f"g-{i}"))
    r = client.get("/api/v1/devices/dev-g/committed").json()
    assert r["committed_upto"] == 5
    assert r["seqs"] == [1, 2, 3, 4, 5]
    r = client.get("/api/v1/devices/dev-g/committed", params={"from_seq": 2, "to_seq": 4}).json()
    assert r["seqs"] == [2, 3, 4]
    devices = client.get("/api/v1/devices").json()
    g = [d for d in devices if d["device_id"] == "dev-g"][0]
    assert g["committed_count"] == 5 and g["next_expected"] == 6


def test_unknown_device_404(client):
    assert client.get("/api/v1/devices/nope/gaps").status_code == 404
    assert client.get("/api/v1/devices/nope/committed").status_code == 404
    assert client.get("/api/v1/devices/nope/duplicates").status_code == 404


# ---------------------------------------------------------------------------
# 按事件时间（sent_at）的测量聚合 /metrics/rollup
# ---------------------------------------------------------------------------

def tpkg(device, seq, key, sent_at, value, metric="temperature"):
    return {
        "device_id": device,
        "seq": seq,
        "idempotency_key": key,
        "sent_at": sent_at,
        "measurements": [{"metric": metric, "value": value, "unit": "C"}],
    }


ROLLUP_PARAMS = {
    "metric": "temperature",
    "from": "2026-09-22T08:00:00Z",
    "to": "2026-09-22T08:02:00Z",
    "window_seconds": 60,
    "allowed_lateness": 0,
}


def _rollup(client, device, **overrides):
    params = {**ROLLUP_PARAMS, **overrides}
    return client.get(f"/api/v1/devices/{device}/metrics/rollup", params=params)


def test_rollup_windows_count_min_max_mean_and_half_open_bounds(client):
    d = "roll-a"
    # 三个点落在 [08:00,08:01)，一个点恰在 08:01（归入下一窗），08:02 被 to 排除
    client.post("/api/v1/packages", json=tpkg(d, 1, f"{d}-1", "2026-09-22T08:00:00Z", 10.0))
    client.post("/api/v1/packages", json=tpkg(d, 2, f"{d}-2", "2026-09-22T08:00:30Z", 20.0))
    client.post("/api/v1/packages", json=tpkg(d, 3, f"{d}-3", "2026-09-22T08:00:45Z", 3.0))
    client.post("/api/v1/packages", json=tpkg(d, 4, f"{d}-4", "2026-09-22T08:01:00Z", 7.0))
    client.post("/api/v1/packages", json=tpkg(d, 5, f"{d}-5", "2026-09-22T08:02:00Z", 99.0))

    body = _rollup(client, d).json()
    assert [w["window_start"] for w in body["windows"]] == [
        "2026-09-22T08:00:00Z",
        "2026-09-22T08:01:00Z",
    ]
    w0 = body["windows"][0]
    assert w0["window_end"] == "2026-09-22T08:01:00Z"
    assert w0["count"] == 3
    assert w0["min"] == 3.0 and w0["max"] == 20.0
    assert w0["mean"] == pytest.approx(11.0)
    w1 = body["windows"][1]
    assert w1["count"] == 1 and w1["min"] == w1["max"] == w1["mean"] == 7.0
    assert body["late_count"] == 0
    assert body["watermark"] == "2026-09-22T08:02:00Z"
    # 查询参数回显
    assert body["from"] == "2026-09-22T08:00:00Z"
    assert body["to"] == "2026-09-22T08:02:00Z"
    assert body["window_seconds"] == 60 and body["allowed_lateness"] == 0


def test_rollup_late_sample_counted_but_never_rewrites_closed_window(client):
    d = "roll-late"
    # 1) 08:00 的样本准时到达（此时水位 08:00，窗口尚未关闭）
    client.post("/api/v1/packages", json=tpkg(d, 1, f"{d}-1", "2026-09-22T08:00:00Z", 10.0))
    # 2) 08:05 到达 -> 水位推进到 08:05，[08:00,08:01) 窗口已关闭
    client.post("/api/v1/packages", json=tpkg(d, 2, f"{d}-2", "2026-09-22T08:05:00Z", 50.0))
    # 3) 迟到样本 sent_at=08:00:30：所属窗口早已关闭 -> late_count，不改写
    client.post("/api/v1/packages", json=tpkg(d, 3, f"{d}-3", "2026-09-22T08:00:30Z", 999.0))

    body = _rollup(client, d, to="2026-09-22T08:06:00Z").json()
    w0 = body["windows"][0]
    assert w0["window_start"] == "2026-09-22T08:00:00Z"
    assert w0["count"] == 1
    assert w0["min"] == w0["max"] == w0["mean"] == 10.0  # 999 未被接纳
    assert body["late_count"] == 1
    assert body["watermark"] == "2026-09-22T08:05:00Z"
    # 迟到样本所在窗口未凭空产生额外结果（该窗口本就存在，结果保持关闭时快照）
    assert [w["window_start"] for w in body["windows"]] == [
        "2026-09-22T08:00:00Z",
        "2026-09-22T08:05:00Z",
    ]


def test_rollup_late_sample_into_never_emitted_window_counts_only(client):
    d = "roll-late2"
    # 先推进水位到 08:05，再迟到一个 08:01 的样本：该窗口此前为空、现已关闭，
    # 不输出窗口，只计 late_count。
    client.post("/api/v1/packages", json=tpkg(d, 1, f"{d}-1", "2026-09-22T08:05:00Z", 1.0))
    client.post("/api/v1/packages", json=tpkg(d, 2, f"{d}-2", "2026-09-22T08:01:30Z", 2.0))

    body = _rollup(client, d, to="2026-09-22T08:06:00Z").json()
    assert body["late_count"] == 1
    assert [w["window_start"] for w in body["windows"]] == ["2026-09-22T08:05:00Z"]


def test_rollup_allowed_lateness_keeps_window_open(client):
    d = "roll-lateness"
    # 同 test_rollup_late_sample... 但 allowed_lateness=300：
    # 08:05 到达时水位仅 08:00，08:00:30 样本到达时水位 08:00:30 仍 < 08:01，窗口未关
    client.post("/api/v1/packages", json=tpkg(d, 1, f"{d}-1", "2026-09-22T08:00:00Z", 10.0))
    client.post("/api/v1/packages", json=tpkg(d, 2, f"{d}-2", "2026-09-22T08:05:00Z", 50.0))
    client.post("/api/v1/packages", json=tpkg(d, 3, f"{d}-3", "2026-09-22T08:00:30Z", 20.0))

    body = _rollup(client, d, to="2026-09-22T08:06:00Z", allowed_lateness=300).json()
    w0 = body["windows"][0]
    assert w0["count"] == 2
    assert w0["min"] == 10.0 and w0["max"] == 20.0
    assert w0["mean"] == pytest.approx(15.0)
    assert body["late_count"] == 0
    assert body["watermark"] == "2026-09-22T08:00:00Z"


def test_rollup_only_considers_committed_packages(client):
    d = "roll-buffer"
    # 首包自举 seq 10 立即提交；seq 12 因缺口 11 留在缓冲区，不得进入聚合
    client.post("/api/v1/packages", json=tpkg(d, 10, f"{d}-10", "2026-09-22T08:00:00Z", 10.0))
    client.post("/api/v1/packages", json=tpkg(d, 12, f"{d}-12", "2026-09-22T08:00:30Z", 20.0))

    body = _rollup(client, d).json()
    assert body["late_count"] == 0
    assert [w["window_start"] for w in body["windows"]] == ["2026-09-22T08:00:00Z"]
    assert body["windows"][0]["count"] == 1

    # 补齐 seq 11 后 seq 12 才提交并参与聚合
    client.post("/api/v1/packages", json=tpkg(d, 11, f"{d}-11", "2026-09-22T08:05:00Z", 50.0))
    body = _rollup(client, d, to="2026-09-22T08:06:00Z").json()
    # 提交顺序 seq=10(08:00) -> 11(08:05) 排水 12(08:00:30)：
    # seq 12 的窗口在水位 08:05 时已关闭 -> 迟到，不改写
    w0 = body["windows"][0]
    assert w0["count"] == 1 and w0["mean"] == 10.0
    assert body["late_count"] == 1


def test_rollup_metric_filter(client):
    d = "roll-metric"
    p = tpkg(d, 1, f"{d}-1", "2026-09-22T08:00:00Z", 10.0, metric="temperature")
    p["measurements"].append({"metric": "ph", "value": 7.0})
    client.post("/api/v1/packages", json=p)

    body = _rollup(client, d, metric="ph").json()
    assert body["metric"] == "ph"
    assert body["windows"][0]["count"] == 1
    assert body["windows"][0]["mean"] == 7.0
    # 无匹配指标的窗口不会被该指标查询计入
    assert _rollup(client, d, metric="nope").json()["windows"] == []


def test_rollup_windows_stably_sorted_across_arrival_order(client):
    d = "roll-sort"
    # 乱序到达但未触发窗口关闭（allowed_lateness 足够大），结果仍按窗口起点升序
    times_values = [
        ("2026-09-22T08:03:00Z", 30.0),
        ("2026-09-22T08:01:00Z", 10.0),
        ("2026-09-22T08:04:30Z", 45.0),
        ("2026-09-22T08:02:15Z", 22.5),
        ("2026-09-22T08:00:05Z", 0.5),
    ]
    for seq, (ts, value) in enumerate(times_values, start=1):
        client.post("/api/v1/packages", json=tpkg(d, seq, f"{d}-{seq}", ts, value))

    body = _rollup(client, d, to="2026-09-22T08:05:00Z", allowed_lateness=3600).json()
    starts = [w["window_start"] for w in body["windows"]]
    assert starts == [
        "2026-09-22T08:00:00Z",
        "2026-09-22T08:01:00Z",
        "2026-09-22T08:02:00Z",
        "2026-09-22T08:03:00Z",
        "2026-09-22T08:04:00Z",
    ]
    assert body["late_count"] == 0
    assert [w["mean"] for w in body["windows"]] == [0.5, 10.0, 22.5, 30.0, 45.0]


def test_rollup_validation(client):
    # 未知设备 -> 404
    assert _rollup(client, "nope").status_code == 404

    d = "roll-validate"
    client.post("/api/v1/packages", json=tpkg(d, 1, f"{d}-1", "2026-09-22T08:00:00Z", 1.0))

    # 缺少必填参数 -> 422
    r = client.get(
        f"/api/v1/devices/{d}/metrics/rollup",
        params={"metric": "temperature", "from": "2026-09-22T08:00:00Z"},
    )
    assert r.status_code == 422
    # window_seconds 必须 >= 1
    assert _rollup(client, d, window_seconds=0).status_code == 422
    # allowed_lateness 不可为负
    assert _rollup(client, d, allowed_lateness=-1).status_code == 422
    # from >= to -> 422
    assert _rollup(
        client, d, **{"from": "2026-09-22T08:02:00Z", "to": "2026-09-22T08:00:00Z"}
    ).status_code == 422
    # 非法时间戳 -> 422
    assert _rollup(client, d, **{"from": "not-a-date"}).status_code == 422


def test_rollup_watermark_null_without_sent_at(client):
    d = "roll-nots"
    p = tpkg(d, 1, f"{d}-1", "2026-09-22T08:00:00Z", 1.0)
    del p["sent_at"]
    client.post("/api/v1/packages", json=p)

    body = _rollup(client, d).json()
    assert body["windows"] == []
    assert body["late_count"] == 0
    assert body["watermark"] is None
