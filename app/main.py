"""FastAPI 路由与应用入口。"""

from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, Header, HTTPException, Query

from . import db
from .logic import IdempotencyConflict, ingest_package
from .schemas import (
    CommittedResponse,
    DeviceState,
    DuplicatesResponse,
    DuplicateRecord,
    GapsResponse,
    IngestResponse,
    PackageIn,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()  # 容器/进程启动时自动建库建表
    yield


app = FastAPI(
    title="Lab Instrument Ingest API",
    version="1.0.0",
    description="实验室仪器批量测量包接收服务：幂等去重、乱序缓冲、按序提交。",
    lifespan=lifespan,
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/api/v1/packages", response_model=IngestResponse, status_code=200)
def post_package(
    pkg: PackageIn,
    idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
):
    key = pkg.idempotency_key or idempotency_key
    if not key:
        raise HTTPException(
            status_code=422,
            detail="缺少幂等键：请在请求体 idempotency_key 或 Idempotency-Key 请求头中提供",
        )
    with db.ingest_lock, db.get_conn() as conn:
        try:
            return ingest_package(conn, pkg, key)
        except IdempotencyConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/api/v1/devices", response_model=List[DeviceState])
def list_devices():
    with db.get_conn() as conn:
        rows = conn.execute(
            """
            SELECT d.device_id,
                   d.committed_upto,
                   (SELECT COUNT(*) FROM buffer    b WHERE b.device_id = d.device_id) AS buffered_count,
                   (SELECT COUNT(*) FROM committed c WHERE c.device_id = d.device_id) AS committed_count,
                   (SELECT COUNT(*) FROM duplicates p WHERE p.device_id = d.device_id) AS duplicate_count
            FROM devices d ORDER BY d.device_id
            """
        ).fetchall()
    return [
        DeviceState(
            device_id=r["device_id"],
            committed_upto=r["committed_upto"],
            next_expected=r["committed_upto"] + 1,
            buffered_count=r["buffered_count"],
            committed_count=r["committed_count"],
            duplicate_count=r["duplicate_count"],
        )
        for r in rows
    ]


@app.get("/api/v1/devices/{device_id}/gaps", response_model=GapsResponse)
def get_gaps(device_id: str):
    with db.get_conn() as conn:
        dev = conn.execute(
            "SELECT committed_upto FROM devices WHERE device_id = ?", (device_id,)
        ).fetchone()
        if dev is None:
            raise HTTPException(status_code=404, detail=f"未知设备: {device_id}")
        committed_upto = dev["committed_upto"]
        buffered = [
            r["seq"]
            for r in conn.execute(
                "SELECT seq FROM buffer WHERE device_id = ? ORDER BY seq", (device_id,)
            ).fetchall()
        ]
    gaps: List[int] = []
    if buffered:
        have = set(buffered)
        gaps = [s for s in range(committed_upto + 1, max(buffered) + 1) if s not in have]
    return GapsResponse(
        device_id=device_id,
        committed_upto=committed_upto,
        next_expected=committed_upto + 1,
        buffered_seqs=buffered,
        gaps=gaps,
    )


@app.get("/api/v1/devices/{device_id}/committed", response_model=CommittedResponse)
def get_committed(
    device_id: str,
    from_seq: Optional[int] = Query(default=None, ge=1),
    to_seq: Optional[int] = Query(default=None, ge=1),
    limit: int = Query(default=1000, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
):
    with db.get_conn() as conn:
        dev = conn.execute(
            "SELECT committed_upto FROM devices WHERE device_id = ?", (device_id,)
        ).fetchone()
        if dev is None:
            raise HTTPException(status_code=404, detail=f"未知设备: {device_id}")
        where, params = "device_id = ?", [device_id]
        if from_seq is not None:
            where += " AND seq >= ?"
            params.append(from_seq)
        if to_seq is not None:
            where += " AND seq <= ?"
            params.append(to_seq)
        total = conn.execute(
            f"SELECT COUNT(*) AS n FROM committed WHERE {where}", params
        ).fetchone()["n"]
        seqs = [
            r["seq"]
            for r in conn.execute(
                f"SELECT seq FROM committed WHERE {where} ORDER BY seq LIMIT ? OFFSET ?",
                (*params, limit, offset),
            ).fetchall()
        ]
    return CommittedResponse(
        device_id=device_id,
        committed_upto=dev["committed_upto"],
        count=total,
        seqs=seqs,
    )


@app.get("/api/v1/devices/{device_id}/duplicates", response_model=DuplicatesResponse)
def get_duplicates(
    device_id: str,
    reason: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
):
    with db.get_conn() as conn:
        dev = conn.execute(
            "SELECT 1 FROM devices WHERE device_id = ?", (device_id,)
        ).fetchone()
        if dev is None:
            raise HTTPException(status_code=404, detail=f"未知设备: {device_id}")
        where, params = "device_id = ?", [device_id]
        if reason:
            where += " AND reason = ?"
            params.append(reason)
        total = conn.execute(
            f"SELECT COUNT(*) AS n FROM duplicates WHERE {where}", params
        ).fetchone()["n"]
        rows = conn.execute(
            f"SELECT * FROM duplicates WHERE {where} ORDER BY id LIMIT ? OFFSET ?",
            (*params, limit, offset),
        ).fetchall()
    return DuplicatesResponse(
        device_id=device_id,
        count=total,
        records=[DuplicateRecord(**dict(r)) for r in rows],
    )
