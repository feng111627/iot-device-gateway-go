# IoT 设备接入网关（本地模拟）

用本机内存连接模拟大量设备的上报与下发，覆盖设备反复断连重连、消息重复投递、
离线堆积、**网关进程随时被强杀/崩溃后的重启恢复**、以及**凭据平滑轮换与换新身份**
等场景，并保证数据状态可预期。核心代码在 `gateway/` 包：

| 文件 | 职责 |
|---|---|
| `gateway/types.go` | 消息模型与可区分原因的错误码 |
| `gateway/persist.go` | 全量状态的落盘格式（JSON Lines）、原子写盘、逐行恢复与恢复报告 |
| `gateway/session.go` | 会话建立、保持、恢复、凭据轮换、换新身份与清理 |
| `gateway/delivery.go` | 确认窗口、配额、顺序、已消费去重及其快照/恢复 |
| `gateway/offline.go` | 离线暂存、淘汰策略及其快照/恢复 |
| `gateway/gateway.go` | 编排层：连接、下发、拉取、确认、快照与恢复 |

## 持久化与崩溃恢复（重启后会话语义）

网关把**每台设备的完整状态**持久化到 `SnapshotPath`（JSON Lines，一行一台设备）：
会话代次与凭据、订阅、Seq 计数器与配额占用、**未确认在途窗口**、
**已消费（已确认）去重记录**、离线暂存、每条消息的投递次数。

- **每次状态变更成功返回前都同步落盘**（`Connect/Publish/Ack/Subscribe/Disconnect/
  RotateCredential/ResetIdentity` 等）。写盘是「临时文件 + fsync + 原子 rename」，
  所以进程在**任意时刻被强杀或崩溃**都安全：磁盘上要么是上一份完整状态、
  要么是这一份完整状态，不会出现半份文件。
- 恢复出的会话一律先标记为**离线**：进程刚启动时没有真实连接，设备需要重新
  `Connect` 才会上线；上线时自动重传在途、补投暂存。

### 重启后的投递保证

- **已确认的消息跨重启绝不重投**：已消费去重记录落盘，重启后仍在。
- **同一条消息从下发到被消费，跨越任意次重启也只真正消费一次**：设备 `Ack` 成功
  先落盘再返回；崩溃发生在 Ack 落盘前，消息仍在窗口中，重连后会重传（at-least-once
  重传 + 去重 = effectively-once 的消费语义）。
- **已发出未确认的消息跨重启按原先后顺序重传，一条不少**：单设备 `Seq` 单调递增
  并落盘，重传严格按 `Seq` 升序，Seq 本身不随重启改变。
- **离线暂存跨重启不凭空增多、也不整批丢失**：恢复按消息 ID 幂等合并，保持入队顺序。
- **恢复动作可反复执行，也能从「恢复到一半又被中断」接着恢复**：所有合并都按
  设备与消息 ID 幂等，重复恢复或中断后重跑结果一致，不产生重复、不损坏数据。

## 凭据轮换 vs 换新身份（边界清楚区分）

同一台物理设备换密钥有两种明确不同的语义：

| 操作 | API | 代次 Epoch | 订阅 | 未确认在途 | 离线暂存 | 已消费去重 | 重启后 |
|---|---|---|---|---|---|---|---|
| **平滑轮换凭据** | `RotateCredential(dev,newCred)` 后用新凭据 `Connect` | 不变 | 保留 | 保留并重传 | 保留并补投 | 保留（旧消息不重投、不重复消费） | 全部照旧 |
| **换新身份** | `ResetIdentity(dev,cred)` 后重新 `Connect` | 递增 | 清空 | 清空 | 清空 | 清空（旧消息 ID 对新身份可复用） | 旧痕迹不复活 |

- 平滑轮换用于密钥到期/滚动：**还是同一台设备**，在途与积压无缝接续。
- 换新身份用于设备退役、所有权变更等：旧会话的一切痕迹被清除并落盘，
  新身份不会、且重启后也不会收到旧会话里的任何消息。

### 与旧版「一换凭据就当新身份」的兼容性

- 旧行为**仍然保留**：不经过 `RotateCredential`，直接用一个不同的凭据 `Connect`，
  仍按「换新身份」处理（代次递增、清空旧订阅/在途/暂存/去重），并已落盘、跨重启生效。
- **变化点**：需要「换密钥但不换人」时，必须**显式**先调 `RotateCredential`。
  即：默认行为不变，平滑轮换是新增的、需要明确声明的能力。

## 投递保证（原有语义）

- **恰好消费一次**：消息 ID 全局去重（去重记录持久化，跨重启仍生效）。重复确认返回
  `DUPLICATE_ACK` 且无副作用；已确认消息不会因重传/重启再次被消费。
- **顺序稳定**：单设备维度分配单调递增的 `Seq`，投递（含重传、补投、跨重启重传）按
  Seq 顺序进行。
- **离线暂存**：设备离线时消息进入每设备独立队列，容量封顶；满时**确定性地淘汰最旧
  消息**（保护最新数据），淘汰总数可观测（`OfflineEvicted`，含恢复时容量收紧的淘汰）。

### 可区分的拒绝原因（`gateway.Error.Code`）

| 错误码 | 含义 |
|---|---|
| `WINDOW_FULL` | 未确认消息超出确认窗口 |
| `QUOTA_EXCEEDED` | 超出单设备周期配额（`ResetQuota` 模拟周期刷新） |
| `DUPLICATE_ACK` | 重复确认 / 向已消费消息重复下发 |
| `UNKNOWN_MESSAGE` | 确认不存在的消息 |
| `NOT_CONNECTED` | 离线设备拉取消息 |
| `SESSION_EXPIRED` | 会话代次失效（换新身份或被清理） |
| `DEVICE_LIMIT` | 网关注册设备数达到 `MaxDevices` 容量上限 |

## 容量边界与恢复可观测性

- `MaxDevices`：网关注册设备数的明确上限。运行时超容量接入返回 `DEVICE_LIMIT`；
  重启恢复时按设备 ID 排序，超出上限的设备**整台挡下**（不装入其任何状态）。
- `OfflineCap`（每设备暂存）、`AckWindow`（每设备在途窗口）、`DedupCap`
  （每设备已消费去重记录条数，超限淘汰最旧）在恢复时同样生效，容量不足的部分
  确定性挡下，不报错、不拖垮启动。
- `RestoreWithReport()` 返回 `RestoreReport`，恢复了多少、挡下多少、坏了多少一目了然：
  `DevicesRestored / SubscriptionsRestored / InflightRestored / OfflineRestored /
  ConsumedRestored`、`DevicesBlocked / MessagesBlocked`、`CorruptRecords`、
  `FailedDevices`。

## 多设备隔离与损坏容忍

- 窗口、配额、暂存、去重均**按设备隔离**；快照采用 **JSON Lines 一行一设备**，
  单行损坏只跳过该行并计入 `CorruptRecords`，其他设备照常恢复。
- 单台设备恢复发生意外只隔离该设备（计入 `FailedDevices`），不影响其他设备，
  也不会让整个网关起不来。
- 多设备并发恢复、并发轮换/换新身份彼此不串味（测试以 `-race` 覆盖）。

## 验证方法

```bash
go build ./...                       # 编译
go vet ./...                         # 静态检查
go test ./... -v                     # 运行单测（-v 可看到各业务场景的覆盖日志与恢复报告）
go test -race -count=1 ./... -v      # 竞态检测下反复跑并发场景
```

单测按业务分组，日志可直接看出覆盖范围：

- **跨重启不重不丢**：`TestRestart_AckedNeverRedelivered`（已确认跨重启绝不重投）、
  `TestRestart_InflightRetransmitInOrder`（在途按原序重传、二次重启）、
  `TestRestart_OfflineBacklogStable`（离线积压跨重启稳定）、
  `TestRestart_RepeatAndInterruptedRestoreIdempotent`（重复/中断恢复幂等）、
  `TestRestart_EndToEndConsumeOnceAcrossRestarts`（跨多次重启恰好消费一次）。
- **凭据轮换 vs 换新身份**：`TestIdentity_RotateCredentialKeepsState`、
  `TestIdentity_RotateCredentialSurvivesRestart`（轮换保留且跨重启）、
  `TestIdentity_ResetIdentityWipesEverythingAcrossRestart`（新身份清空且重启不复活）、
  `TestIdentity_RotateThenResetAreDistinct`（两步语义清楚可分）、
  `TestIdentity_DirectConnectWithNewCredentialStillResets`（旧行为兼容）。
- **并发恢复互不干扰 / 隔离 / 容量**：
  `TestConcurrent_RestartManyDevicesNoCrossTalk`（24 设备并发恢复确认无串味）、
  `TestConcurrent_RotateAndResetMixed`（并发轮换与换新身份混合）、
  `TestRestore_CorruptRecordIsolation`（坏行只隔离坏数据）、
  `TestRestoreAndRuntime_DeviceCapacityBoundary`（`MaxDevices` 与 `DEVICE_LIMIT`）、
  `TestRestore_PerDeviceDedupAndBacklogCap`（去重/暂存容量挡下可观测）。
- **原有会话/投递/离线用例**：`TestSession_*`、`TestDelivery_*`、`TestOffline_*`
  全部保留且无回归（含原 `TestOffline_RestartNoDupNoLoss`，现基于全量状态快照）。
