"""请求 / 响应的数据模型。"""

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


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
    """单个半开滚动窗口 [window_start, window_end) 的聚合结果。"""

    window_start: datetime
    window_end: datetime
    count: int
    min: float
    max: float
    mean: float


class RollupResponse(BaseModel):
    """按 sent_at 事件时间对已提交测量的窗口聚合。

    已关闭窗口（watermark 越过其 window_end 时关闭）的结果不再被迟到样本改写；
    迟到样本计入 late_count 但不参与任何窗口聚合。
    """

    device_id: str
    metric: str
    # 回显查询区间；from 是 Python 关键字，字段名用 from_ 并序列化为 "from"
    model_config = ConfigDict(populate_by_name=True)
    from_: datetime = Field(alias="from", serialization_alias="from")
    to: datetime
    window_seconds: int
    allowed_lateness: int
    watermark: Optional[datetime] = Field(
        default=None, description="该设备已提交数据的最大 sent_at 减 allowed_lateness；无时间戳样本时为 null"
    )
    late_count: int = Field(description="落入已关闭窗口的迟到样本数（不参与聚合）")
    windows: List[RollupWindow] = Field(description="按 window_start 稳定升序排列的窗口结果")
