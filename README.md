# IoT 设备接入网关（本地模拟）

用本机内存连接模拟大量设备的上报与下发，覆盖设备反复断连重连、消息重复投递、
离线堆积等场景，并保证数据状态可预期。核心代码在 `gateway/` 包：

| 文件 | 职责 |
|---|---|
| `gateway/types.go` | 消息模型与可区分原因的错误码 |
| `gateway/session.go` | 会话建立、保持、恢复与清理 |
| `gateway/delivery.go` | 确认窗口、配额、顺序与去重 |
| `gateway/offline.go` | 离线暂存、淘汰策略与重启快照 |
| `gateway/gateway.go` | 编排层：连接、下发、拉取、确认 |

## 会话语义

- **建立**：`Connect(deviceID, credential)` 首次接入创建会话，代次（Epoch）从 1 开始。
- **保持与恢复**：凭据不变的重连保留全部订阅；重连时自动重传未确认消息、补投离线暂存。
- **正常断开 vs 主动清理**：
  - `Disconnect(deviceID, graceful)` 仅标记离线（`graceful` 区分正常断开/异常掉线），订阅与未确认消息保留；
  - `ClearSession(deviceID)` 主动清理：订阅作废、代次递增。
- **身份变更**：凭据变化视为新身份——代次递增、订阅清空，旧会话下已暂存和未确认的消息
  全部作废，新身份不会收到旧会话的任何消息。
- 网关重启后的首次接入不算代次变化，从快照恢复的暂存可正常补投。

## 投递保证

- **恰好消费一次**：消息 ID 全局去重。重复确认返回 `DUPLICATE_ACK` 且无副作用；
  已确认消息不会因重传再次被消费。
- **顺序稳定**：单设备维度分配单调递增的 `Seq`，投递（含重传、补投）按 Seq 顺序进行。
- **可区分的拒绝原因**（`gateway.Error.Code`）：

  | 错误码 | 含义 |
  |---|---|
  | `WINDOW_FULL` | 未确认消息超出确认窗口 |
  | `QUOTA_EXCEEDED` | 超出单设备周期配额（`ResetQuota` 模拟周期刷新） |
  | `DUPLICATE_ACK` | 重复确认 / 重复下发 |
  | `UNKNOWN_MESSAGE` | 确认不存在的消息 |
  | `NOT_CONNECTED` | 离线设备拉取消息 |
  | `SESSION_EXPIRED` | 会话代次失效（凭据变更或被清理） |

- **离线暂存**：设备离线时消息进入每设备独立队列，容量封顶；
  满时**确定性地淘汰最旧消息**（保护最新数据），淘汰总数可观测（`OfflineEvicted`）。
- **重启安全**：`Snapshot` 原子写盘（临时文件 + rename），`Restore` 按消息 ID 去重合并，
  重复恢复不产生重复、不整批丢失。
- **堵塞隔离**：窗口、配额、暂存均按设备隔离，单个设备堵塞不影响其他设备收发。

## 验证方法

```bash
go build ./...   # 编译
go test ./... -v # 运行单测（-v 可看到各业务场景的覆盖日志）
```

单测按业务分组，日志可直接看出覆盖范围：

- **会话**：`TestSession_ReconnectResume`（重连恢复订阅/重传/补投）、
  `TestSession_GracefulVsCleared`（正常断开 vs 主动清理）、
  `TestSession_CredentialChangeInvalidatesOldMessages`（换凭据后旧消息不可见）。
- **投递**：`TestDelivery_DuplicateAckConsumedOnce`（重复确认恰好消费一次）、
  `TestDelivery_UnknownAck`、`TestDelivery_WindowFullRejected`、
  `TestDelivery_QuotaExceededRejected`、`TestDelivery_PerDeviceOrderStable`。
- **离线**：`TestOffline_BacklogEvictOldest`（堆积淘汰最旧）、
  `TestOffline_RestartNoDupNoLoss`（重启不重复不丢失）、
  `TestOffline_BlockedDeviceIsolation`（堵塞隔离）、
  `TestOffline_ReceiveWhileDisconnected`。
