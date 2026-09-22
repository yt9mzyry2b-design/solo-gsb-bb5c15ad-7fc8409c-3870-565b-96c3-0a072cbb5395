"""请求 / 响应的数据模型。"""

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class Measurement(BaseModel):
    metric: str = Field(min_length=1, description="指标名，如 temperature")
    value: float
    unit: Optional[str] = None
    ts: Optional[datetime] = None


class PackageIn(BaseModel):
    """仪器上报的一个批量测量包。"""

    device_id: str = Field(min_length=1)
    seq: int = Field(ge=1, description="包序号，按设备单调递增、连续")
    idempotency_key: Optional[str] = Field(
        default=None, description="幂等键；也可用 Idempotency-Key 请求头传入"
    )
    sent_at: Optional[datetime] = None
    measurements: List[Measurement] = Field(min_length=1)


class IngestResponse(BaseModel):
    status: Literal["committed", "buffered", "duplicate"]
    device_id: str
    seq: int
    committed_upto: int
    committed_seqs: List[int] = Field(
        default_factory=list, description="本次请求触发提交的全部序号（含排水）"
    )
    duplicate_reason: Optional[str] = None


class DeviceState(BaseModel):
    device_id: str
    committed_upto: int
    next_expected: int
    buffered_count: int
    committed_count: int
    duplicate_count: int


class GapsResponse(BaseModel):
    device_id: str
    committed_upto: int
    next_expected: int
    buffered_seqs: List[int]
    gaps: List[int] = Field(description="缓冲区最大序号之前缺失的序号")


class CommittedResponse(BaseModel):
    device_id: str
    committed_upto: int
    count: int
    seqs: List[int]


class DuplicateRecord(BaseModel):
    id: int
    device_id: str
    seq: Optional[int]
    idempotency_key: Optional[str]
    reason: str
    received_at: str


class DuplicatesResponse(BaseModel):
    device_id: str
    count: int
    records: List[DuplicateRecord]


class RollupWindow(BaseModel):
    """单个半开窗口 [start, end) 的聚合结果；空窗口 count=0 且统计值为 null。"""

    start: datetime
    end: datetime
    count: int
    min: Optional[float] = None
    max: Optional[float] = None
    mean: Optional[float] = None


class RollupResponse(BaseModel):
    """按事件时间（sent_at）的窗口聚合结果。"""

    model_config = {"populate_by_name": True}

    device_id: str
    metric: str
    from_: datetime = Field(alias="from", description="聚合范围起点（含）")
    to: datetime = Field(description="聚合范围终点（不含）")
    window_seconds: float
    allowed_lateness: float
    watermark: Optional[datetime] = Field(
        default=None,
        description="设备已提交数据的最大 sent_at 减 allowed_lateness；无数据时为 null",
    )
    late_count: int = Field(
        description="范围内 sent_at 早于水位的迟到样本数（不改写已关闭窗口）"
    )
    windows: List[RollupWindow] = Field(description="按 start 升序的全部窗口")
