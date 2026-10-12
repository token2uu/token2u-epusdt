# 公共节点优先与付费回退

Solana、TRON 和 Aptos 的自动 HTTP 扫描优先使用普通节点。每次请求最多尝试
三个节点；有可用付费兜底时，先尝试至多两个公共节点，为付费兜底保留第三次
机会；未配置可用付费兜底时仍可尝试第三个公共 RPC。普通节点正常
返回时不向付费节点分流；后续请求重新选择节点，恢复后自动回到公共节点。
同一组内先选健康节点，再按权重排序，权重不跨越公共/付费分组。

## 节点配置

继续使用受保护的管理接口 `/admin/api/v1/rpc-nodes`。`purpose` 支持：

| 值 | 自动扫描行为 |
| --- | --- |
| `general` / `both` | 普通优先组；`both` 也参与人工补单 |
| `paid_fallback` | 公共节点失败、不可用或冷却时自动兜底，也可用于人工补单 |
| `manual_verify` | 仅供人工补单，不参与自动扫描或定时探测 |

已有节点不会按域名、URL 是否带 key 等特征自动分类，也不会自动修改用途。
部署时必须明确将现有 Ankr 等付费节点调整到 `paid_fallback`，公共节点保持
`general`，否则旧付费节点仍属于普通组。PATCH 示例（ID 替换为实际节点 ID）：

```http
PATCH /admin/api/v1/rpc-nodes/123
Content-Type: application/json
Authorization: Bearer <admin-token>

{"purpose":"paid_fallback"}
```

管理页面的新增/编辑节点、节点用途筛选和列表标签均支持“付费回退”。
选择后保存为 `paid_fallback`，再次编辑时保留该用途。旧界面曾将未知用途显示
为“通用”，因此不能用旧列表标签判断后端是否完成切换。
`type=indexer` 仍通过管理 API 配置。

本仓库只有已构建的前端静态资源。这次界面兼容修改由
`python3 scripts/update-rpc-purpose-ui.py` 生成，包含六种现有语言的文字及
Service Worker 缓存更新；脚本重复执行不会再修改文件，遇到不兼容的新构建会
明确失败。后续同步前端时，应优先在前端源仓库加入上述功能后重新构建，并
保留本项目收银台的既有定制。

Aptos 需要两类备用：`type=http` 的 Fullnode REST RPC 用于核验交易；
`type=indexer` 的兼容 GraphQL Indexer 用于发现地址入账。普通 Ankr Aptos
REST URL 不能直接作为 GraphQL 备用。可在数据库创建独立 Indexer 池，或在
未配置数据库池时用 `aptos_indexer_fallback_url` / `aptos_indexer_fallback_api_key`
指定一个兼容付费服务。没有提供备用地址时不会臆造或自动购买服务。

## 故障与恢复

429 按 `Retry-After` 冷却，缺省 30 秒；网络错误、5xx 以及 Indexer 数据无效
也在当前请求切换并短暂冷却。冷却期间不向节点发送请求，即使全部节点都在
冷却，也不会绕过限制。已在途请求的成功不会清除端点的 429 冷却。
Solana `-32020` 仍按单独查询退避，不会把该节点对所有钱包永久禁用。
Solana 收款交易在接受节点响应前完成指令及余额解析；HTTP 200 中缺失必要
字段也会切换备用节点。交易响应的首个签名必须与请求一致，签名分页结果须通过
结构校验；收款交易缺少有效 `blockTime` 时会尝试备用节点，全部失败则保留游标，
不创建永久异常收款记录，等待时间数据恢复后重试。
已配置 SPL 资产的 RPC 精度也必须与配置一致；不一致时先回退其他节点，
全部失败则保留游标，不将该响应写成永久 `asset_mismatch` 记录。
相同 URL 的不同 API key 分别记录 429 冷却状态。

高度检查只比较实际链/Indexer 处理器高度，不以“钱包没有新收款”判断故障。
后发请求的高度倒退或持续 120 秒不前进视为异常。各请求在通过本地限流、实际
发出前记录时间；与更高高度观测重叠的旧请求可返回较低的快照，但不会降低已记录
的最高高度，也不会刷新停滞计时。Aptos 分页仍须覆盖该轮要求的高度，不能借此
接受覆盖范围不足的结果。TRON 的区块读取、Aptos Indexer 的
主网/处理器/分页结果、Aptos 按版本读取交易都会校验响应后再接受。
全部候选失败时游标保持不变，恢复后补扫，收款继续使用既有持久化去重机制。
TRON 区块缺少交易列表时，只有零交易根才能证明它是空区块；非零交易根
对应缺失/空列表、无效交易结果或不完整收款字段都会触发备用节点。
还会核对区块头与区块 ID、每笔交易原始 protobuf 与交易 ID，并将完整交易
（含签名和执行结果）的 Merkle 根与区块头比较；付款地址、金额和合约输入也须
匹配原始 protobuf。因此，非空列表中漏掉一笔交易同样会触发回退。
TRON 响应会拒绝重复 JSON 键（包括转义后的重复键），入账与校验使用同一套
区分大小写的字段读取规则，避免未被校验的同名变体覆盖金额或交易列表。

TRON 不再固定停留在付费备用节点，每个区块/高度请求都会重新检查公共优先组。
健康检查对这三个网络读取真实接口，公共节点每 60 秒一次，付费节点每 15 分钟
一次，均最多等待 5 秒；人工补单专用节点不自动探测。处于冷却的节点不探测。
被低频健康检查标记异常的付费兜底，在没有冷却时仍可按需试用，避免等 15 分钟
才恢复必要的付款核验。其他网络的既有 TCP 探测保持原状。

## 预算与监控

```dotenv
rpc_public_requests_per_second=2
rpc_paid_requests_per_second=5
rpc_public_timeout_seconds=5
rpc_paid_timeout_seconds=10
rpc_paid_requests_per_minute=120
rpc_stale_head_seconds=120
rpc_health_public_interval_seconds=60
rpc_health_paid_interval_seconds=900
```

请求速率按端点和凭据共享，包含探测和重试；Solana 还遵守原全进程 RPC 上限，
Aptos Indexer 还遵守全进程每秒 2 次限制，健康探测与正常查询共用这些预算。
端点预算与协议总预算在实际发送前统一检查、同时扣减；等待和取消的请求不预占
任一预算，避免低频端点排队后压缩总请求间隔。
探测因任何本地限流或冷却未能发出时保留原健康状态和检查时间，下次调度再试；主动
健康检查接口会返回暂缓错误。一个付费请求可能消耗多个供应商
credit，因此 `rpc_paid_requests_per_minute` 是请求量软预算，并非精确费用封顶。
达到预算时输出日志，Solana/Aptos 推迟冷地址扫描，待支付及近期活跃地址、付款
核验和 TRON 区块补扫仍可继续使用付费兜底。首次扫描的 Solana 地址仍会初始化。

管理接口 `/admin/api/v1/dashboard/rpc-stats` 的各链数据增加 `routing`：

- `public_requests`、`paid_requests`：实际发出的 HTTP 请求数，按配置的用途分组。
- `failed_requests`：网络或 HTTP 错误次数，不包含 HTTP 200 内的业务数据错误。
- `fallback_reasons`：备用请求的原因计数，如 `rate_limited`、`history_unavailable`、
  `server_error`、`stale_head`、`local_capacity`；公共备用也计入。本地容量不足可尝试
  其他节点，但不会累计该节点失败次数或将其冷却；容量恢复后继续公共优先。
- `paid_requests_this_minute`、`paid_budget_exceeded`：本进程当前一分钟窗口的预算情况。
- `last_scan_success_at`、`scan_stalled_seconds`：成功扫描时间及距成功扫描的秒数，
  不是区块确认时延。冷地址有意降频时也应结合调度间隔解释。

计数及冷却均在进程内保存，重启会重置；付款记录和扫描游标仍持久化。
各副本或共享凭据的其他应用须分别分配请求预算。界面展示这些新字段需要前端
消费新增 API 数据；可先用 SSE 接口和应用日志核对公共/付费调用量及回退原因。
