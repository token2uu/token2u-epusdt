# Aptos 按收款地址发现入账

默认使用 `address_events` 模式：从 Aptos Indexer 的 `fungible_asset_activities`
按已启用收款地址查询成功的 FA Deposit 事件，过滤 gas 费用，按
`transaction_version + event_index` 升序分页。只对命中已启用币种的正金额事件，
通过现有 RPC 节点池读取对应交易，复核事件、完整交易内容并调用原有付款处理。
RPC 核验保留现有节点选择、限流和主备切换规则，不再持续读取全链
`/v1/transactions`。地址事件模式按经 RPC 核验的 Deposit 入账，不要求同笔交易
存在 Withdraw，因此铸币等单边入账也不会阻塞扫描。实付结算、异常到账和
事件去重规则沿用原有处理。

## 配置

```dotenv
aptos_scan_mode=address_events
aptos_indexer_url=https://api.mainnet.aptoslabs.com/v1/graphql
aptos_indexer_processor=fungible_asset_processor
aptos_indexer_api_key=
aptos_indexer_fallback_url=
aptos_indexer_fallback_api_key=
```

未配置上述项时也使用这些默认值。索引请求全进程限速每秒 2 次，公共端点默认
5 秒超时、付费端点默认 10 秒超时，
非空 API key 以 `Authorization: Bearer ...` 发送。需要更高额度时可配置自己的
兼容 Aptos GraphQL 索引端点和凭据；不要把真实密钥提交到仓库。
轮询间隔沿用 Aptos 链配置，没有启用钱包或币种时不请求索引服务。

索引器与 Fullnode RPC 是不同接口，需单独配置。公共索引器不可用、429、
GraphQL 部分失败、数据缺失或处理器高度停滞时，在同一请求中尝试已配置的
备用索引器。每次请求最多尝试三个节点；有可用付费备用时，为其保留最后一次
机会，否则可以继续尝试第三个公共节点。查询地址、
版本上界和事件游标保持一致。所有候选失败或 RPC 核验失败时，保留失败地址
的进度，本轮其他地址仍可处理；
下一轮错误重试间隔至少 30 秒。不会自动退回全链扫描。官方公共端点的
可用性和额度需要持续观察；索引器延迟会延后付款发现。

主备可以使用相同 URL、不同 API key，例如官方匿名访问和带密钥访问。
只有 URL 和 API key 均相同才去重，各凭据分别记录限流冷却。
RPC 即使返回 HTTP 200，也必须通过完整交易字段及入账事件的地址、币种、
金额和事件下标核验；不完整或不匹配的响应会在当前请求中尝试备用 RPC，
所有候选均失败则保留游标，不产生入账记录。

也可以在 `rpc_nodes` 中创建 `network=aptos, type=indexer` 的独立节点池，
公共节点设置 `purpose=general`，付费节点设置 `purpose=paid_fallback`。
一旦数据库存在 Indexer 配置，节点启用/禁用以数据库为准，不会在全部禁用时
偷偷回退环境变量。索引器的 API key 仍使用 Bearer 认证。
部署及软预算说明见 [公共节点优先与付费回退](rpc-fallback.md)。

## 游标与恢复

- 索引器必须返回主网 chain ID 1 和指定 FA 处理器的已完成版本；从该版本按
  现有确认数计算本轮上界，不以节点最高版本越过索引器实际处理进度。
- 每个地址持久化独立游标 `aptos:deposits:<规范地址>`，包含版本和事件下标。
  每页最多 100 条、每轮每地址最多 10 页；同一交易跨页也不会跳过后续事件。
  非空页均按最后一条事件继续翻页，即使服务端返回条数少于请求值；只有成功
  取得空页才推进到本轮上界。达到每轮页数上限后保留事件游标，下轮接续。
- 首次切换时，各地址从旧 `aptos:ledger` 游标接续。没有旧游标时，在任何
  Indexer 请求前保存初始时间边界：默认回看订单有效期加 5 分钟；若有更早
  创建的待支付订单，则回看至该订单创建时间。查询同时按地址和
  `transaction_timestamp` 过滤，随后沿用版本/事件游标分页，不扫描全链历史。
  初次获取高度失败、进程重启或预算暂停都不移动已保存的时间边界。
  已存在的地址游标继续使用，新增钱包如需更早历史须另行核对补扫范围。
- 只有整页检查及付款处理成功才推进进度；失败、进程重启或索引器落后均不会
  跳过失败区间或倒退已保存游标。重复处理由既有到账记录去重。

显式设置 `aptos_scan_mode=full_ledger` 并重启可使用原全链扫描实现作为回滚。
旧 `aptos:ledger` 不随地址扫描推进，回滚会从旧进度补扫，可能再次大量消耗
RPC 额度。正常部署不需要删除或手动修改任何游标。

观察启动日志 `scanner starting mode=address_events`、
`address scan incomplete; retaining cursors`、付款记录和供应商的请求统计。
全链列表调用应停止，RPC 调用主要随实际候选入账交易数量增长，而非全链交易量。

索引字段与接入方式参考 [Aptos 官方 Indexer 文档](https://aptos.guide/build/indexer/indexer-api)。
