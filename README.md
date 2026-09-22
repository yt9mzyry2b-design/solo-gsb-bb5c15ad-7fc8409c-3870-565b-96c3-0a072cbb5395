# 实验室仪器数据接收 API

基于 **FastAPI + SQLite** 的仪器批量测量包接收服务。仪器按设备上报带序号的测量包，服务提供：

- **幂等去重**：每个包携带幂等键，重试 / 重发不会产生重复数据；
- **乱序缓冲**：序号不连续的包先暂存，缺口补齐后**按序连续提交**；
- **事件时间聚合**：按 `sent_at` 将已提交测量分入半开滚动窗口，返回 `count`/`min`/`max`/`mean`，并基于水位线（watermark）隔离迟到样本；
- **可观测接口**：查询每台设备的缺口、已提交序号与重复包记录。

## 快速开始

### Docker Compose（推荐，一条命令启动）

```bash
docker compose up --build
```

无需任何手工初始化：

1. **依赖初始化**：镜像构建时由 Dockerfile 执行 `pip install -r requirements.txt`；容器入口 `docker-entrypoint.sh` 启动前会再次执行同一命令，已满足时为快速 no-op；
2. **建库建表**：FastAPI 启动钩子（lifespan）自动调用 `init_db()` 创建 SQLite 数据库文件与全部表结构（幂等，可重复执行），数据库文件持久化在命名卷 `lab-data`（容器内 `/data/lab.db`）。

服务地址：http://localhost:8000 ，交互式文档：http://localhost:8000/docs

### 本地开发

```bash
# 依赖初始化：安装运行期 + 开发期依赖（含 pytest、httpx）
pip install -r requirements-dev.txt
# 启动服务；应用启动钩子自动创建 SQLite 数据库与表结构（默认 ./lab.db，可用 DB_PATH 覆盖）
uvicorn app.main:app --reload
# 运行测试（测试自动使用临时数据库，互不污染）
pytest tests/ -v
```

## 数据模型

仪器上报的测量包：

```json
{
  "device_id": "ph-meter-01",
  "seq": 12,
  "idempotency_key": "ph-meter-01-2026-09-22-0012",
  "sent_at": "2026-09-22T08:00:00Z",
  "measurements": [
    {"metric": "ph", "value": 7.01, "unit": "pH", "ts": "2026-09-22T07:59:58Z"},
    {"metric": "temperature", "value": 21.4, "unit": "C"}
  ]
}
```

- `seq`：包序号，**按设备**从 1 起单调递增、连续；
- `idempotency_key`：幂等键（也可通过 `Idempotency-Key` 请求头传入，二者至少提供一个）。

## 提交规则

每台设备独立维护 `committed_upto`（已连续提交的最大序号）：

1. **首包自举**：设备第一个到达的包立即提交，`committed_upto` 初始化为 `seq - 1`；
2. 之后到达的包：
   - `seq <= committed_upto` → 重复（`already_committed`）；
   - 已在缓冲区 → 重复（`already_buffered`）；
   - 否则放入缓冲区，并从 `committed_upto + 1` 起连续"排水"提交；
3. 同一幂等键重放 → 返回首次处理结果（`idempotent_replay`）；
4. 同一幂等键携带**不同载荷** → `409 Conflict`。

所有摄入操作在单事务内完成，保证并发下状态一致。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 |
| POST | `/api/v1/packages` | 上报测量包（幂等、乱序缓冲、按序提交） |
| GET | `/api/v1/devices` | 设备列表及状态汇总 |
| GET | `/api/v1/devices/{id}/gaps` | 缺口：缓冲区序号与缺失序号 |
| GET | `/api/v1/devices/{id}/committed` | 已提交序号（支持 `from_seq`/`to_seq`/`limit`/`offset`） |
| GET | `/api/v1/devices/{id}/duplicates` | 重复包记录（可按 `reason` 过滤） |
| GET | `/api/v1/devices/{id}/metrics/rollup` | 按 `sent_at` 事件时间的窗口聚合（count/min/max/mean + 水位线迟到统计） |

## 事件时间窗口聚合

```
GET /api/v1/devices/{device_id}/metrics/rollup
    ?metric=temperature
    &from=2026-09-22T08:00:00Z
    &to=2026-09-22T09:00:00Z
    &window_seconds=60
    &allowed_lateness=30
```

| 参数 | 必填 | 说明 |
|---|---|---|
| `metric` | 是 | 指标名，只聚合该指标的测量 |
| `from` / `to` | 是 | 查询区间，**半开** `[from, to)`；ISO 8601 时间，无时区时按 UTC 处理 |
| `window_seconds` | 是 | 滚动窗口长度（秒，≥1），窗口按 UNIX epoch 对齐：`[floor(sent_at/w), +w)` |
| `allowed_lateness` | 是 | 允许迟到秒数（≥0） |

**水位线与迟到处理**：

- 水位线 `watermark = max(已提交包的 sent_at) − allowed_lateness`（按设备、取全部已提交数据的最大 `sent_at`）；
- 已提交数据按提交顺序（seq 升序，即数据对外可见的到达顺序）回放：某样本到达时，若水位线已达到其所属窗口的右端点（`window_end <= watermark`），该窗口即已**关闭**；
- 落入已关闭窗口的样本计入 `late_count`，但**绝不改写**该窗口的结果（`count`/`min`/`max`/`mean` 保持关闭时的快照）；
- 仅包含至少一个准时样本的窗口，结果按 `window_start` **稳定升序**返回；
- 仍在缓冲区（缺口未补齐）的包不参与聚合；`sent_at` 缺失的包跳过（无时间戳样本时 `watermark` 为 `null`）。

> 注：聚合以**包**的 `sent_at` 作为包内全部测量的事件时间（测量内可选的 `ts` 不参与）。

响应示例：

```json
{
  "device_id": "ph-meter-01",
  "metric": "temperature",
  "from": "2026-09-22T08:00:00Z",
  "to": "2026-09-22T08:06:00Z",
  "window_seconds": 60,
  "allowed_lateness": 0,
  "watermark": "2026-09-22T08:05:00Z",
  "late_count": 1,
  "windows": [
    {"window_start": "2026-09-22T08:00:00Z", "window_end": "2026-09-22T08:01:00Z",
     "count": 2, "min": 10.0, "max": 20.0, "mean": 15.0},
    {"window_start": "2026-09-22T08:05:00Z", "window_end": "2026-09-22T08:06:00Z",
     "count": 1, "min": 50.0, "max": 50.0, "mean": 50.0}
  ]
}
```

### 示例

上报乱序包（seq 2 先到，进入缓冲）：

```bash
curl -X POST localhost:8000/api/v1/packages -H 'Content-Type: application/json' -d '{
  "device_id": "ph-meter-01", "seq": 2, "idempotency_key": "k-2",
  "measurements": [{"metric": "ph", "value": 7.01}]
}'
# => {"status":"buffered","committed_upto":0,"committed_seqs":[],...}
```

查询缺口：

```bash
curl localhost:8000/api/v1/devices/ph-meter-01/gaps
# => {"committed_upto":0,"next_expected":1,"buffered_seqs":[2],"gaps":[1]}
```

补齐缺口后按序提交：

```bash
curl -X POST localhost:8000/api/v1/packages -H 'Content-Type: application/json' -d '{
  "device_id": "ph-meter-01", "seq": 1, "idempotency_key": "k-1",
  "measurements": [{"metric": "ph", "value": 6.98}]
}'
# => {"status":"committed","committed_seqs":[1,2],"committed_upto":2,...}
```

重复重放与查询：

```bash
curl -X POST localhost:8000/api/v1/packages -H 'Content-Type: application/json' -d '<同上请求体>'
# => {"status":"duplicate","duplicate_reason":"idempotent_replay",...}

curl localhost:8000/api/v1/devices/ph-meter-01/committed    # 已提交序号
curl localhost:8000/api/v1/devices/ph-meter-01/duplicates   # 重复包记录
```

## 项目结构

```
app/
  main.py     # FastAPI 路由
  logic.py    # 摄入核心：幂等判定、缓冲、排水提交
  db.py       # SQLite 连接与建表（启动时自动执行）
  schemas.py  # Pydantic 模型
tests/test_api.py
Dockerfile / docker-entrypoint.sh / docker-compose.yml
requirements.txt / requirements-dev.txt
```

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `DB_PATH` | 容器内 `/data/lab.db`，本地 `./lab.db` | SQLite 数据库文件路径 |
| `PORT` | `8000` | 服务监听端口（entrypoint 使用） |
