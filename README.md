# IoT 设备接入网关（本地模拟）

用本机内存连接模拟大量设备的上报与下发，覆盖设备反复断连重连、消息重复投递、
离线堆积、**网关进程随时被强杀/崩溃后的重启恢复**、**凭据平滑轮换与换新身份**
等场景，并保证数据状态可预期。核心代码在 `gateway/` 包：

| 文件 | 职责 |
|---|---|
| `gateway/types.go` | 消息模型与可区分原因的错误码 |
| `gateway/session.go` | 会话建立、保持、恢复、凭据轮换/换新身份与事件重放 |
| `gateway/delivery.go` | 确认窗口、配额、顺序、去重及其快照/重放 |
| `gateway/offline.go` | 离线暂存、淘汰策略、快照装载与事件重放 |
| `gateway/durable.go` | 每设备 WAL（只追加事件日志）+ 原子快照压缩 |
| `gateway/gateway.go` | 编排层：连接、下发、拉取、确认、恢复（`Recover`） |

## 持久化与崩溃恢复模型

配置 `StateDir` 后，所有**状态变更先落盘（fsync）再改内存**，采用每设备独立的
事件溯源（event sourcing）存储：

- `*.wal`：每台设备一条只追加事件日志（connect/sub/rotate/reset/assign/ack/
  enqueue/drain/quota），事件在设备内连续编号；
- `*.snap`：每累计 `CompactEvery` 条事件做一次全量快照（临时文件 + fsync + rename），
  随后截断 WAL。

**任意时刻被强杀或崩溃后的保证：**

- **已确认的消息绝不会再投递**：ack 事件先落盘，"已消费"去重集随快照/事件恢复，
  哪怕跨越多次重启；对它重复 Ack 仍返回 `DUPLICATE_ACK`。
- **在途消息按原序补传，一条不少**：未确认消息连同分配到的 `Seq` 一起持久化，
  重启后设备重连时按 Seq 升序重传。
- **离线积压不多不少**：enqueue/drain 均为事件，恢复后队列内容与崩溃前一致；
  补投顺序与入队顺序一致（先重传在途，再补投离线）。
- **同一条消息从下发到消费，跨重启只真正消费一次**。
- **恢复可反复执行、可从"恢复到一半又被中断"接着恢复**：`Recover()` =
  "装载快照 + 只重放快照点之后的事件"，每类事件的重放都幂等；
  压缩在"快照已就位、WAL 截断前"崩溃也安全（快照点之前的残留事件按序号跳过）；
  崩溃时写了一半的末尾记录（撕裂记录）从未提交，扫描时安全截断丢弃。
- **恢复后所有设备一律处于离线状态**：连接是瞬时状态、不持久化，设备必须重新
  `Connect` 才会在线并触发重传/补投——因此补投永远发生在活连接上。

`Recover()` 返回 `RecoverStats`，恢复结果可观测：成功设备数、被容量挡住的设备、
恢复失败被隔离的设备、重放事件数、恢复的在途/离线/已消费记录数
（`ConsumedRetained` 保留了多少、`ConsumedEvicted` 恢复时因超出容量挡掉了多少）、
损坏记录数。

## 并发掉线/重连的会话语义与投递保证

掉线、重连、云端下发、设备拉取/确认可能在同一台设备上同时发生。网关用
**网关级互斥锁把一台设备的"在线状态判断"与"投递/摘取动作"做成原子操作**，
`Connect`（含重传+补投）、`Disconnect`、`Publish`、`Receive`、`Ack` 之间串行可见：

- **在线判断与实际永远一致**：`Publish` 的"在线走进窗口 / 离线进暂存"判断与
  `Disconnect` 的置离线互斥，不会出现"已经掉线却还按在线路子下发、消息卡住"，
  也不会出现"刚重连却第一次拉取被当成离线"。
- **掉线时待取队列的归宿**：`Disconnect` 作废该连接周期内已进入待取队列、但
  设备还没拉走的投递。这些消息的 `assign` 早已落盘并占用在途窗口，因此
  **重连时由重传按序补回，一条不少**；掉线期间新消息照常进离线暂存。
- **重连即接管**：`Connect` 的"置在线 + 重传在途 + 补投离线"在同一临界区内完成，
  恢复开始前先作废旧连接残留批次，所以并发的重复连接不会产生重复批次；
  同一连接周期内每条在途/暂存消息只投递一次。
- **设备拉取的原子性**：`Receive` 的在线校验与摘取待取队列在同一把锁内完成，
  要么在"仍在线"判定下取走本连接周期的投递，要么在"已离线"时得到
  `NOT_CONNECTED` 且一条都取不走。
- **投递语义仍为至少一次 + 去重**：设备"已拉走但未确认"时掉线，重连会重传
  （Attempt 递增），由消息 ID 去重保证恰好消费一次；顺序仍由单设备单调 `Seq` 保证，
  先重传在途、再补投离线，批内按 Seq 升序。
- **按设备隔离**：所有状态（会话、窗口、待确认、暂存、去重集、WAL、快照）按
  设备分开；单台设备被隔离或其操作失败，只会挡回该设备，其他设备并发照常，
  也不会让整个网关起不来。

## 已消费去重记录的容量边界与淘汰规则

反复下发确认后，"已消费消息 ID 去重集"不再无限增长：

- 配置项 `Config.ConsumedCap`（单设备保留上限）：
  - **未配置（0，默认）**：取默认上限 `1024`；
  - **正数 N**：每台设备至多保留 N 条，超出即淘汰；
  - **负数**：不设上限（仅建议短生命周期或测试场景）。
- **淘汰规则确定可解释**：超限时淘汰 `Seq` 最小（最早消费）的记录；Seq 相同
  （不会发生）时按消息 ID 字典序兜底。被淘汰的只是**远超在途窗口与离线积压范围的
  历史幂等标记**——系统自身需要重传的消息一定仍记录在在途/暂存中，不受影响。
- **容量边界的语义后果**：对一条已被淘汰出去重集的旧消息再次 `Ack`，会得到
  `UNKNOWN_MESSAGE` 而不是 `DUPLICATE_ACK`（该消息早已确认、不会重投，丢失的只是
  容量之外的历史幂等标记）。要保留更长的重复确认识别窗口，调大 `ConsumedCap`，
  建议不小于"确认窗口 + 最大离线积压"规模。
- **可观测**：运行期可用 `ConsumedLen(dev)`（当前保留数）、
  `ConsumedEvicted(dev)`（累计淘汰数）查看；累计淘汰数随快照持久化、跨重启接续。
- **恢复时同样受容量约束**：快照装载与事件重放过程中超出当前 `ConsumedCap` 的
  记录被确定性淘汰，数量计入 `RecoverStats.ConsumedEvicted`，保留数量计入
  `ConsumedRetained`；恢复后触发一次压缩即可把淘汰结果固化，之后重启不再重复计数。

## 凭据平滑轮换 vs 换新身份

凭据变化不再被隐式解释为"换人"，必须显式选择，API 与持久化语义都清楚区分：

| 操作 | 语义 | 订阅 | 在途未确认 | 离线积压 | 已消费去重记录 | Epoch |
|---|---|---|---|---|---|---|
| `RotateCredential(dev, newCred)` | **同一台设备换密钥** | 保留 | 保留，继续重传 | 保留，继续补投 | 保留 | 不变 |
| `ResetIdentity(dev, newCred)` | **以全新身份接入** | 清空 | 作废 | 作废 | 作废 | +1 |
| `Connect(dev, cred)` 凭据与记录不符 | 拒绝并返回 `CREDENTIAL_MISMATCH`，提示二选一 | — | — | — | — | — |

- 平滑轮换后设备用新凭据接入，状态原样接上；用旧凭据接入会被拒绝。
- 换新身份的"作废"本身持久化并立即压缩快照：**即使网关随后重启，新身份也收不到
  旧会话的任何消息**；旧的已消费记录同样清空（对旧消息 Ack 返回 `UNKNOWN_MESSAGE`
  而非 `DUPLICATE_ACK`）。设备损坏被隔离后，也只能通过 `ResetIdentity` 以新身份恢复。

### 与旧版行为的兼容范围

- 旧版"`Connect` 携带不同凭据即视为新身份、清空旧会话"的隐式行为已移除：
  现在这样调用会得到 `CREDENTIAL_MISMATCH`。迁移方式：确属换新身份的调用点改用
  `ResetIdentity`；属于换密钥的调用点改用 `RotateCredential`。
- 凭据不变的重连、正常断开、主动清理、窗口/配额/暂存等行为保持不变。
- 不配置 `StateDir` 时仍为纯内存模式；旧的 `SnapshotPath` + `Snapshot()`/`Restore()`
  离线暂存全量快照接口保留可用（兼容已有用法），新系统推荐直接使用 `StateDir` + `Recover()`。

## 多设备隔离与容量边界

- 会话、投递窗口、配额、离线队列、WAL、快照全部按设备隔离。
- **个别设备状态损坏不会拖垮其他设备，也不会让网关起不来**：快照损坏或 WAL 中部
  损坏/序号缺号（无法保证"不丢"）的设备被标记隔离（`DEVICE_QUARANTINED`），
  其收发被拒绝；其他设备照常恢复。尾部撕裂记录可自愈，不算失败。
- `MaxDevices` 给出明确的设备数容量边界：恢复时按设备 ID 排序，超出部分不恢复并
  计入 `DevicesSkippedLimit`；运行期为新设备落盘同样受此边界约束（`DEVICE_LIMIT`）。
- 离线暂存仍按设备容量封顶，满时确定性淘汰最旧消息（`OfflineEvicted` 可观测）。

## 会话语义

- **建立**：`Connect(deviceID, credential)` 首次接入创建会话，代次（Epoch）从 1 开始。
- **保持与恢复**：凭据不变的重连（含网关重启后）保留全部订阅；重连时自动重传未确认
  消息、补投离线暂存。
- **正常断开 vs 主动清理**：`Disconnect(deviceID, graceful)` 仅标记离线，订阅与未确认
  消息保留；`ClearSession(deviceID)` 使订阅作废、代次递增。
- **身份相关**：见上一节"凭据平滑轮换 vs 换新身份"。

## 投递保证

- **恰好消费一次**：消息 ID 去重；重复 Ack 返回 `DUPLICATE_ACK` 且无副作用；
  已确认消息不会因重传或重启再次被消费。去重集有明确容量上限（`ConsumedCap`），
  被淘汰出容量边界的历史记录的重复 Ack 返回 `UNKNOWN_MESSAGE`，详见下节
  "已消费去重记录的容量边界与淘汰规则"。
- **顺序稳定**：单设备维度分配单调递增的 `Seq`，投递（含重传、补投）按 Seq 顺序进行。
- **可区分的拒绝原因**（`gateway.Error.Code`）：

  | 错误码 | 含义 |
  |---|---|
  | `WINDOW_FULL` | 未确认消息超出确认窗口 |
  | `QUOTA_EXCEEDED` | 超出单设备周期配额（`ResetQuota` 模拟周期刷新） |
  | `DUPLICATE_ACK` | 重复确认 / 重复下发 |
  | `UNKNOWN_MESSAGE` | 确认不存在的消息 |
  | `NOT_CONNECTED` | 离线设备拉取消息 |
  | `SESSION_EXPIRED` | 会话代次失效（换新身份或被清理） |
  | `CREDENTIAL_MISMATCH` | Connect 凭据与记录不符，需显式轮换或换身份 |
  | `DEVICE_LIMIT` | 超出恢复容量边界（设备数上限） |
  | `DEVICE_QUARANTINED` | 设备持久状态损坏被隔离 |

## 验证方法

```bash
go build ./...                                   # 编译
go vet ./...                                     # 静态检查
go test ./... -v                                 # 运行单测（-v 可看到各业务场景的覆盖日志）
go test -race -count=3 ./...                     # 竞态检测 + 重复执行（并发用例在此口径下通过）
go test -race -run 'TestConcurrent_' -v ./...    # 只跑并发抖动/隔离用例（-race 口径）
go test -race -run 'TestConsumedCap' -v ./...    # 只跑去重容量上限与淘汰用例
```

并发用例里的设备采用微秒级周期轮询（而非忙等自旋），既充分交错调度、又贴近
真实设备行为；`-race` 下整套用例约十几秒。日志中每个用例都打印了覆盖的业务
场景与最终结果（"✓"行为该场景的收敛断言）。

单测按业务分组，日志可直接看出覆盖范围与各自结果：

- **跨重启不重不丢**：`TestRecovery_AckedNeverRedeliveredAcrossRestart`（已确认跨多轮
  重启零重投、在途补传）、`TestRecovery_OfflineBacklogAcrossRestart`（离线积压跨重启
  有序不丢）、`TestRecovery_IdempotentRepeatedRecover`（Recover 反复执行幂等）、
  `TestRecovery_MixedInflightAndOfflineOrderAcrossRestarts`（在途+离线混合顺序）、
  `TestRecovery_ResumeInterruptedMidway`（补投反复被崩溃打断后多轮恢复，恰好一次）、
  `TestRecovery_CompactCrashBeforeTruncation`（快照就位/截断前崩溃的混合 WAL 恢复）。
- **凭据轮换 vs 换新身份**：`TestCredential_RotateKeepsEverythingAcrossRestart`
  （轮换后订阅/在途/积压/已消费记录全部接续）、
  `TestCredential_ResetIdentityWipesEverythingAcrossRestart`
  （换新身份旧痕迹跨重启彻底不可见）、`TestCredential_RotateThenResetBoundaries`。
- **并发掉线/重连与下发/拉取**：
  `TestConcurrent_SessionFlapWithPublishAndReceive`（单设备反复掉线重连与下发、
  拉取、确认同时跑：在线判断一致、消息不丢不卡不乱序、恰好消费一次）、
  `TestConcurrent_MultiDeviceFlapNoCrossTalk`（多台设备同时抖动/下发/确认，
  连接状态、待确认、积压、已消费记录互不串味）、
  `TestConcurrent_QuarantinedDeviceDoesNotSinkGateway`（对被隔离设备的并发操作
  全部只挡它自己，其他设备并发收发照常，网关不被拖垮）。
- **去重记录容量边界**：`TestConsumedCap_RuntimeEvictOldest`（运行期超上限按最旧
  确定性淘汰、计数可观测、在途不受影响）、`TestConsumedCap_DefaultAndUnlimited`
  （默认上限与"不限"配置）、`TestConsumedCap_ConcurrentAcks`（高并发反复确认下
  严格封顶、淘汰计数自洽）、`TestConsumedCap_RecoveryStatsAndEviction`
  （恢复时保留多少、挡掉多少在 `RecoverStats` 中可观测，压缩固化后重启不重复计数）。
- **并发与隔离（既有）**：`TestConcurrent_RecoveryAndRotationNoCrossTalk`（多设备并发恢复/轮换/
  确认不串味，-race 口径）、`TestRecovery_CorruptDeviceIsQuarantined`（中部损坏隔离、
  尾部撕裂自愈、换新身份解除隔离）、`TestRecovery_DeviceLimitAndStats`（设备数容量边界与
  恢复统计数字）。
- **原有行为不回归**：会话（`TestSession_*`）、投递（`TestDelivery_*`）、
  离线（`TestOffline_*`，含旧版 `Snapshot/Restore` 兼容用例）全部保留通过。
