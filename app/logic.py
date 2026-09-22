"""核心摄入逻辑：幂等去重、乱序缓冲、缺口补齐后按序提交。

提交规则（按设备独立维护）：
- 每台设备记录 committed_upto = 已连续提交的最大序号；
- 设备首个到达的包用于自举：committed_upto 初始化为 seq - 1，即首包立即提交；
- 之后到达的包：seq <= committed_upto 视为重复；已在缓冲区视为重复；
- 否则放入缓冲区，随后从 committed_upto + 1 起连续"排水"提交。
"""

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from .schemas import IngestResponse, PackageIn, RollupResponse, RollupWindow


class IdempotencyConflict(Exception):
    """同一幂等键携带了不同的载荷。"""


def _canonical_payload(pkg: PackageIn) -> str:
    return json.dumps(
        {
            "device_id": pkg.device_id,
            "seq": pkg.seq,
            "sent_at": pkg.sent_at.isoformat() if pkg.sent_at else None,
            "measurements": [m.model_dump(mode="json") for m in pkg.measurements],
        },
        sort_keys=True,
        ensure_ascii=False,
    )


def _payload_hash(pkg: PackageIn) -> str:
    return hashlib.sha256(_canonical_payload(pkg).encode("utf-8")).hexdigest()


def _record_duplicate(
    conn: sqlite3.Connection,
    device_id: str,
    seq: Optional[int],
    key: Optional[str],
    reason: str,
) -> None:
    conn.execute(
        "INSERT INTO duplicates (device_id, seq, idempotency_key, reason) VALUES (?,?,?,?)",
        (device_id, seq, key, reason),
    )


def _store_key(
    conn: sqlite3.Connection,
    key: str,
    pkg: PackageIn,
    payload_hash: str,
    response: IngestResponse,
) -> None:
    conn.execute(
        "INSERT INTO idempotency_keys (key, device_id, seq, payload_hash, response_json)"
        " VALUES (?,?,?,?,?)",
        (key, pkg.device_id, pkg.seq, payload_hash, response.model_dump_json()),
    )


def ingest_package(conn: sqlite3.Connection, pkg: PackageIn, key: str) -> IngestResponse:
    """在单个事务内完成一次摄入。调用方需持有 ingest_lock。"""
    payload_hash = _payload_hash(pkg)

    # 1) 幂等键重放：直接返回首次处理的结果
    row = conn.execute(
        "SELECT payload_hash, response_json FROM idempotency_keys WHERE key = ?", (key,)
    ).fetchone()
    if row is not None:
        if row["payload_hash"] != payload_hash:
            raise IdempotencyConflict(
                f"idempotency key {key!r} 已被不同载荷使用"
            )
        _record_duplicate(conn, pkg.device_id, pkg.seq, key, "idempotent_replay")
        stored = IngestResponse.model_validate_json(row["response_json"])
        return stored.model_copy(update={"status": "duplicate", "duplicate_reason": "idempotent_replay"})

    # 2) 取设备状态；不存在则以首包自举
    dev = conn.execute(
        "SELECT committed_upto FROM devices WHERE device_id = ?", (pkg.device_id,)
    ).fetchone()
    if dev is None:
        committed_upto = pkg.seq - 1
        conn.execute(
            "INSERT INTO devices (device_id, committed_upto) VALUES (?,?)",
            (pkg.device_id, committed_upto),
        )
    else:
        committed_upto = dev["committed_upto"]

    # 3) 重复包判定
    duplicate_reason: Optional[str] = None
    if pkg.seq <= committed_upto:
        duplicate_reason = "already_committed"
    else:
        in_buffer = conn.execute(
            "SELECT 1 FROM buffer WHERE device_id = ? AND seq = ?",
            (pkg.device_id, pkg.seq),
        ).fetchone()
        if in_buffer is not None:
            duplicate_reason = "already_buffered"

    if duplicate_reason is not None:
        _record_duplicate(conn, pkg.device_id, pkg.seq, key, duplicate_reason)
        response = IngestResponse(
            status="duplicate",
            device_id=pkg.device_id,
            seq=pkg.seq,
            committed_upto=committed_upto,
            committed_seqs=[],
            duplicate_reason=duplicate_reason,
        )
        _store_key(conn, key, pkg, payload_hash, response)
        return response

    # 4) 入缓冲区，随后从下一个期望序号起连续排水提交
    payload_json = _canonical_payload(pkg)
    conn.execute(
        "INSERT INTO buffer (device_id, seq, payload_json) VALUES (?,?,?)",
        (pkg.device_id, pkg.seq, payload_json),
    )

    committed_seqs = []
    cur = committed_upto + 1
    while True:
        row = conn.execute(
            "SELECT payload_json FROM buffer WHERE device_id = ? AND seq = ?",
            (pkg.device_id, cur),
        ).fetchone()
        if row is None:
            break
        conn.execute(
            "INSERT INTO committed (device_id, seq, payload_json) VALUES (?,?,?)",
            (pkg.device_id, cur, row["payload_json"]),
        )
        conn.execute(
            "DELETE FROM buffer WHERE device_id = ? AND seq = ?", (pkg.device_id, cur)
        )
        committed_seqs.append(cur)
        cur += 1

    committed_upto = cur - 1
    conn.execute(
        "UPDATE devices SET committed_upto = ? WHERE device_id = ?",
        (committed_upto, pkg.device_id),
    )

    response = IngestResponse(
        status="committed" if pkg.seq in committed_seqs else "buffered",
        device_id=pkg.device_id,
        seq=pkg.seq,
        committed_upto=committed_upto,
        committed_seqs=committed_seqs,
    )
    _store_key(conn, key, pkg, payload_hash, response)
    return response


def _as_utc(dt: datetime) -> datetime:
    """naive 时间视为 UTC；带时区的时间统一换算到 UTC。"""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_sent_at(value: Optional[str]) -> Optional[datetime]:
    """解析 canonical payload 中 isoformat 写出的 sent_at（兼容 Z 后缀）。"""
    if not value:
        return None
    dt = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    return _as_utc(dt)


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _window_index(event_time: datetime, window_seconds: int) -> int:
    """epoch 对齐窗口下标：floor(event_time / window)，整数微秒精度运算。"""
    delta = event_time - _EPOCH
    epoch_seconds = delta.days * 86400 + delta.seconds
    return epoch_seconds // window_seconds


def rollup_metrics(
    conn: sqlite3.Connection,
    device_id: str,
    metric: str,
    from_dt: datetime,
    to_dt: datetime,
    window_seconds: int,
    allowed_lateness: int,
) -> RollupResponse:
    """按 sent_at 事件时间对已提交测量做半开窗口聚合。

    - 窗口为 epoch 对齐的半开区间 [window_start, window_end)；
    - 水位 watermark = 该设备已提交包的最大 sent_at − allowed_lateness；
    - 已提交数据按提交顺序（seq 升序，即对外可见的到达顺序）回放：
      某样本到达前，若 watermark 已达到其所属窗口的 window_end，则该窗口
      已经"关闭"，样本计入 late_count 但绝不改写该窗口的结果；
    - 仅统计 metric 匹配、sent_at 落在查询区间 [from, to) 内的测量；
      仅返回至少含一个准时样本的窗口，按 window_start 稳定升序。
    """
    from_dt = _as_utc(from_dt)
    to_dt = _as_utc(to_dt)
    window = timedelta(seconds=window_seconds)
    lateness = timedelta(seconds=allowed_lateness)

    rows = conn.execute(
        "SELECT seq, payload_json FROM committed WHERE device_id = ? ORDER BY seq",
        (device_id,),
    ).fetchall()

    # 窗口聚合状态；一旦样本到达时窗口已关闭，状态即被冻结、不再变动
    accum: Dict[int, Dict[str, float]] = {}
    late_count = 0
    max_seen: Optional[datetime] = None

    for row in rows:
        payload = json.loads(row["payload_json"])
        event_time = _parse_sent_at(payload.get("sent_at"))
        if event_time is None:
            # 没有事件时间的包无法参与事件时间聚合
            continue
        # 最大 sent_at 基于设备收到的全部已提交数据，与查询区间 / 指标无关
        max_seen = event_time if max_seen is None or event_time > max_seen else max_seen
        if event_time < from_dt or event_time >= to_dt:
            continue
        for measurement in payload.get("measurements", []):
            if measurement.get("metric") != metric:
                continue
            idx = _window_index(event_time, window_seconds)
            window_end = _EPOCH + (idx + 1) * window
            # 先判定：以到达前水位衡量窗口是否已关闭，再据此接纳或计迟到
            if window_end <= max_seen - lateness:
                late_count += 1
                continue
            value = float(measurement["value"])
            acc = accum.get(idx)
            if acc is None:
                accum[idx] = {"count": 1.0, "min": value, "max": value, "sum": value}
            else:
                acc["count"] += 1
                acc["sum"] += value
                if value < acc["min"]:
                    acc["min"] = value
                if value > acc["max"]:
                    acc["max"] = value

    windows: List[RollupWindow] = []
    for idx in sorted(accum):
        acc = accum[idx]
        start = _EPOCH + idx * window
        windows.append(
            RollupWindow(
                window_start=start,
                window_end=start + window,
                count=int(acc["count"]),
                min=acc["min"],
                max=acc["max"],
                mean=acc["sum"] / acc["count"],
            )
        )

    return RollupResponse(
        device_id=device_id,
        metric=metric,
        **{"from": from_dt},
        to=to_dt,
        window_seconds=window_seconds,
        allowed_lateness=allowed_lateness,
        watermark=(max_seen - lateness) if max_seen is not None else None,
        late_count=late_count,
        windows=windows,
    )
