# TRON / Aptos / Solana USDT 端到端验收

日期：2026-10-13。商家仓库固定为 `/Users/hyd/GolandProjects/token2u-new-api`，`dev` / `44ddf0545` 加本轮日志修复；套餐订单不走 USDT，不在范围内。

## 测试边界

本组测试贯通两端真实后端业务代码：商家 `RequestEpay` → HTTP 签名建单 → 网关真实路由 → 可选 HTTP 切链 → 实际链扫描器 → RPC 返回解析和付款识别 → 订单及付款回执事务 → 实际回调 worker → 商家 `EpayNotify` → 商家余额、订单状态 → 同步返回。

外部依赖是本地 HTTP 模拟节点和临时 SQLite / Redis。故障恢复阶段清除测试进程内的 RPC 冷却状态以立即重试；因此验证的是持久化状态和重试正确性，不是实际退避等待时长。没有提交真实链上交易，没有连接生产数据库，没有部署服务。它比先前直接注入 `ObservedPayment` 的四场景联调多覆盖了扫描、协议解析、持久化游标和回调结果保存，但仍不是主网验收。

- TRON：真实 block scanner，含 protobuf 交易、交易根及 block ID 校验的合成 TRC20 `transfer` 区块；两确认高度边界。
- Aptos：真实 address-events scanner、GraphQL Indexer HTTP 查询及 REST `/v1/transactions/by_version` 二次校验，FA Deposit / owner / metadata / amount 解析；两确认 version 边界。测试配置的 FA 为六位精度模拟资产，不证明主网 USDT metadata 配置正确。
- Solana：真实 owner / ATA 签名扫描、`getTransaction` 和 SPL `transferChecked` 解析，使用 USDT mint 配置。确认延迟用模拟节点在 `finalized` 查询下延迟发布交易表示；不模拟共识或重组。

## 统一场景矩阵

以下 19 个场景分别运行于三条链，共 57 个用例。每个成功场景核对网关主订单、唯一 settled receipt、商家原订单号、实付金额、原金额和最终余额；并重复发送四个并发 SUCCESS 及一个迟到 CLOSED，确保不重复充值、不回退成功状态。

| 场景 | 必须满足的结果 |
| --- | --- |
| 足额 5 | 商家实收 5，只入账一次 |
| 少付 3 | 商家实收 3，原金额仍为 5 |
| 超付 7 | 商家实收 7，原金额仍为 5 |
| HTTP 切链到被测网络 | 获胜子单支付，主商家单入账；返回保留主订单身份 |
| 自然过期后发现截止前付款 | 先 CLOSED，后 SUCCESS，恢复原商家单 |
| 截止后付款 | 不充值，保留异常付款回执 |
| 人工关闭后发现付款 | 不自动复活、不充值；保留异常。人工关闭按现有语义不发 CLOSED，商家仍 pending |
| 错误代币后正确付款 | 错币不充值，后续正确 USDT 能入账 |
| 错误收款人后正确付款 | 非目标地址不充值，不影响后续正确付款 |
| 失败交易后正确付款 | 失败交易不充值；Aptos 额外覆盖 Indexer 宣称成功而 RPC 返回失败的不一致 |
| RPC 503 后恢复 | 不充值、不跳过游标；恢复后入账 |
| RPC 内容不完整后恢复 | 不接受残缺付款证据，不前移游标；恢复后入账 |
| 确认不足后达到确认条件 | 未确认阶段不入账，确认后入账 |
| 商家余额更新失败 | 商家事务回滚，网关不确认回调；重试后只入账一次 |
| 商家已入账但 ACK 丢失 | 商家余额已变但网关保留重试；再次回调不重复充值 |
| 结算后扫描进程重启重放 | 子进程从两份持久化数据库和旧游标重新读取节点；游标追平，付款回执不增加 |
| 两个扫描轮次并发 | 同一付款只产生一次结算；随后并发回调也不重复入账 |
| 网关付款回执写入失败 | 结算回滚，订单未成功，游标不跳过；恢复存储后可重试 |
| 收款地址重新分配 | 已支付地址给第二张订单；新进程重扫旧款不能支付新单，新款只支付新单 |

`restart-replay` 是真实扫描子进程重启，仍不是完整应用在任意指令处崩溃：本组没有声称覆盖网关 HTTP、后台守护线程、商家缓存及两端数据库一起掉电的组合故障。

## 运行方法与结果

```sh
GOCACHE=/private/tmp/epusdt-review-go-cache EPUSDT_E2E_RACE=1 \
  bash integration/new-api/run.sh /Users/hyd/GolandProjects/token2u-new-api
```

脚本通过 Go overlay 加入测试入口和商家测试桥，两端均从当前工作区编译。`EPUSDT_E2E_RACE=1` 同时为商家子进程和网关启用 race。可用 `EPUSDT_E2E_RUN` 筛选某一场景。普通生产构建不会包含这些入口。

最终结果：三链 **57 / 57 通过**（TRON、Aptos、Solana 各 19），原有两端 **4 / 4 通过**。两端同时启用 `-race`，最终日志无竞态报告。三链矩阵耗时 185.05 秒。

补充验证：网关 `go test ./... -count=1` 的 19 个有测试包全部通过，15 个包无测试；商家 `go test -race ./logger ./controller ./model -run 'TestConcurrentLoggingAcrossRotation|TestEpay|Test.*Referral|Test.*WalletCredit' -count=1` 三包通过；两仓库 `git diff --check` 通过。

本机原始证据（临时目录，不纳入提交）：

- `/private/tmp/epusdt-network-e2e-race-final.log`：57 + 4 场景最终通过。
- `/private/tmp/epusdt-network-e2e-race.log`：修复前商家日志竞态复现。
- `/private/tmp/epusdt-network-default-full.log`：网关默认全量回归。
- `/private/tmp/epusdt-merchant-logger-payment-race.log`：商家日志及支付专项。
- `/private/tmp/epusdt-merchant-current-full.log`：当前商家分支六项非支付失败。

首轮测试夹具修正包括 Aptos `/v1` 路径、人工关闭不主动通知商家的既有语义，以及 TRON 合成交易 timestamp 保证不同付款拥有不同交易哈希；这些不计作业务 bug。实际业务修复只有下述 R6。

## 本轮发现并修复

两端同时启用 race 后，并发回调在商家 `logger.logHelper` 的 `logCount++` 上复现数据竞争。同类检查发现 `setupLogWorking` 轮转标志也未同步。

在正确商家仓库将两者改为原子变量，使用 CAS 保证只有一个请求调度轮转，并由该异步任务负责释放标志。影响范围是公共日志计数和轮转调度；未改变支付金额、结算事务或回调签名。补充了跨轮转阈值的并发日志回归，日志和支付专项 race 已通过。问题纳入统一清单 R6。

当前商家版本的 `go test ./controller ./model -count=1` 仍有此前同名的六项非支付失败（五项错误展示、一项模型生成配置迁移），不能标记商家全量通过。

## 未覆盖及上线边界

- 主网实际转账、真实节点历史完整性与限流、真实 USDT 合约 / metadata、实际部署参数尚未验证。
- TRON 自动扫描沿用直接 `transfer` 解析；任意合约内部转账、批量合约分发及 `transferFrom` 不能据本结果宣称支持。这是既有收款范围限制，真实付款来源需核对。
- 本组 Solana E2E 使用 `transferChecked`；CPI、其他 SPL 指令、多事件、分页及其他节点故障由已有专项测试覆盖，不能将专项测试等同于这些形态也逐一完成了商家 E2E。
- 本组 Aptos 使用 FA Deposit 模型；未将每种历史资产 / 存储形态、所有分页边界都贯通到商家。已有 Indexer / scanner 专项回归提供补充证据。
- 无付款自动过期的完整等待时间、人工补单 HTTP 全流程、浏览器、登录鉴权、真实生产数据库、部署级故障切换和多实例未纳入本组验收。
- 历史漏单、旧错误 ACK 和已保存的异常仍需对账；商家分支此前 6 项非支付测试失败不由这组测试解决。

本组通过只能支持“列出的三链后端场景在隔离依赖下通过”，不能解释为“所有支付场景覆盖”或“可以直接开放生产充值”。
