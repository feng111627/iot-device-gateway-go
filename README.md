# IoT 设备接入网关（本机模拟版）

用 Go 实现的本地设备接入网关：通过本机 TCP 连接模拟大量设备的上报与下发，
在设备反复断连重连、消息重复投递、离线堆积等场景下保证数据状态可预期。

## 构建与运行

```bash
go build ./...

# 启动网关（监听本机 1883，状态落盘）
go run ./cmd/gateway -addr 127.0.0.1:1883 -state gateway-state.jsonl \
  -window 32 -offline-cap 1000 -uplink-quota 100

# 另开终端：模拟 30 台设备，平均每 1s 断连重连一次，持续 10s
go run ./cmd/simulator -addr 127.0.0.1:1883 -devices 30 -duration 10s -reconnect-every 1s
```

## 协议（JSON 行，TCP）

设备→网关：`connect` / `subscribe` / `ack` / `uplink` / `publish` / `disconnect`
网关→设备：`connack` / `message` / `ackresult` / `ok` / `error`

```json
{"type":"connect","device_id":"dev-1","token":"t1","clean":false}
{"type":"subscribe","topics":["cmd/dev-1","broadcast"]}
{"type":"ack","seq":42}
{"type":"uplink","payload":"telemetry"}
{"type":"disconnect","clean":false}
```

## 会话语义

- **建立与恢复**：`connect` 携带 `(device_id, token, clean)`。凭据不变且存在旧会话时
  恢复会话（`connack.resumed=true`）：订阅关系、消息序号、在途未确认消息全部保留，
  重连后无需重新订阅。
- **正常断开 vs 主动清理**：`disconnect(clean=false)` 或连接异常中断只断开连接，
  会话保留可恢复；`disconnect(clean=true)` 或 `connect(clean=true)` 主动清理，
  订阅、在途、离线暂存全部丢弃。
- **凭据变更**：同一 `device_id` 换新凭据接入时，旧会话立即作废（Epoch 递增），
  旧会话的离线暂存被清除且不再下发；凭据指纹随状态落盘，**网关重启后该判定仍然有效**。

## 投递保证

- **恰好一次消费**：网关保证"至少一次送达"——每条下行消息携带会话内单调递增的
  `seq`，未确认的消息在重连后重发；设备端按 `seq` 去重，重复投递只消费一次。
  重复确认幂等：首次确认生效，再次确认返回 `duplicate=true`，不重复计数。
- **顺序稳定**：单设备消息按 `seq` 严格递增到达；重连时先重发在途未确认消息，
  再按序补发离线暂存。
- **可区分的拒绝原因**：
  | 场景 | 错误 |
  |---|---|
  | 在途未确认数超出确认窗口 | `ErrWindowFull` |
  | 上行超出单设备每秒配额 | `ErrQuotaExceeded` |
  | 确认未知序号 | `ErrUnknownSeq` |
  | 会话不存在/凭据失效 | `ErrSessionNotFound` / `ErrSessionInvalid` |
- **离线暂存与过载保护**：设备离线期间消息进入每设备独立队列，容量达到上限时
  **确定性地淘汰最旧消息（FIFO 淘汰）**，淘汰事件通过回调可观测。每设备独立的
  确认窗口、暂存队列与上行配额，单台设备堵塞不影响其他设备。
- **重启不丢不重**：暂存队列、在途消息、设备凭据以 JSONL 事件日志落盘（写后 fsync，
  加载时重放并压缩快照）。重启后：未确认的在途消息按序放回队首重新补发，
  已确认的消息不会重发，暂存消息整批不丢。

## 验证方法

```bash
go test ./... -race -v
```

测试日志按 `业务: <领域> | 用例: <场景>` / `结果: <断言摘要>` 输出，覆盖：

| 业务领域 | 测试用例 |
|---|---|
| 会话恢复 | 断连重连后订阅关系保留 |
| 会话清理 | clean 断开后订阅与暂存被丢弃 |
| 凭据变更 | 新凭据接入后收不到旧会话的离线消息 |
| 可靠确认 | 重复确认幂等；窗口满拒绝(ErrWindowFull)；未知序号拒绝(ErrUnknownSeq) |
| 可靠下发 | 单设备消息顺序稳定（含重连补发） |
| 离线暂存 | 暂存满时 FIFO 淘汰最旧；网关重启后暂存不丢不重 |
| 过载保护 | 单设备堵塞不拖垮其他设备；上行超配额拒绝(ErrQuotaExceeded) |
| 端到端 | 20 设备经 TCP 断连重连风暴 + 重复确认 + 离线堆积，消息恰好消费一次 |

端到端冒烟（真实进程）：

```bash
go run ./cmd/gateway -addr 127.0.0.1:1883 -state /tmp/gw-state.jsonl &
go run ./cmd/simulator -addr 127.0.0.1:1883 -devices 30 -duration 10s
# 输出示例：devices=30 reconnects=128 received=666 dupAcks=49 uplink=713 rejected=0
```

## 代码结构

```
gateway/
  message.go   消息与确认类型
  errors.go    可区分的拒绝原因
  session.go   会话建立/恢复/清理，凭据指纹与 Epoch
  queue.go     离线暂存（FIFO 淘汰）+ JSONL 持久化与重启恢复
  gateway.go   发布/确认/上下线/配额核心逻辑
  server.go    TCP 服务器（JSON 行协议）
  *_test.go    单测与端到端风暴测试
cmd/gateway    网关进程入口
cmd/simulator  设备模拟器（断连重连/重复确认/上行上报/下行发布）
```
