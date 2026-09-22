# 实验室仪器数据接收 API

基于 **FastAPI + SQLite** 的仪器批量测量包接收服务。仪器按设备上报带序号的测量包，服务提供：

- **幂等去重**：每个包携带幂等键，重试 / 重发不会产生重复数据；
- **乱序缓冲**：序号不连续的包先暂存，缺口补齐后**按序连续提交**；
- **可观测接口**：查询每台设备的缺口、已提交序号与重复包记录。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
```

容器启动时自动安装依赖（`docker-entrypoint.sh`），应用启动钩子自动创建 SQLite 数据库与表结构。数据库文件持久化在命名卷 `lab-data`（容器内 `/data/lab.db`）。

服务地址：http://localhost:8000 ，交互式文档：http://localhost:8000/docs

### 本地开发

```bash
pip install -r requirements-dev.txt
uvicorn app.main:app --reload     # 数据库默认创建在 ./lab.db，可用 DB_PATH 覆盖
pytest tests/                     # 运行测试
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
