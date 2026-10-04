// Package gateway 实现本地设备接入网关：会话管理、可靠下发、离线暂存。
package gateway

import "fmt"

// ErrCode 用于区分各类被拒绝操作的原因。
type ErrCode string

const (
	ErrCodeUnknownDevice      ErrCode = "UNKNOWN_DEVICE"      // 设备未接入
	ErrCodeSessionExpired     ErrCode = "SESSION_EXPIRED"     // 会话已失效（凭据变更或被清理）
	ErrCodeWindowFull         ErrCode = "WINDOW_FULL"         // 超出确认窗口
	ErrCodeQuotaExceeded      ErrCode = "QUOTA_EXCEEDED"      // 超出配额
	ErrCodeDuplicateAck       ErrCode = "DUPLICATE_ACK"       // 重复确认
	ErrCodeUnknownMessage     ErrCode = "UNKNOWN_MESSAGE"     // 确认不存在的消息
	ErrCodeOfflineQueueFull   ErrCode = "OFFLINE_QUEUE_FULL"  // 离线暂存已满（新消息被淘汰）
	ErrCodeNotConnected       ErrCode = "NOT_CONNECTED"       // 设备不在线
	ErrCodeCredentialMismatch ErrCode = "CREDENTIAL_MISMATCH" // Connect 携带的凭据与记录不符，需显式轮换或换身份
	ErrCodeDeviceLimit        ErrCode = "DEVICE_LIMIT"        // 超出恢复容量边界（设备数上限）
	ErrCodeDeviceQuarantined  ErrCode = "DEVICE_QUARANTINED"  // 设备持久状态损坏被隔离
)

// Error 是可区分原因的业务错误。
type Error struct {
	Code ErrCode
	Msg  string
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Msg) }

func newError(code ErrCode, format string, args ...any) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...)}
}

// Message 是一条下行消息。
type Message struct {
	ID       string // 全局唯一消息 ID，用于去重
	DeviceID string
	Topic    string
	Payload  []byte
	Seq      uint64 // 单设备维度单调递增序号，保证顺序稳定
}

// Delivery 是实际投递给设备的一次下发（可能因重传出现多次，设备只消费一次）。
type Delivery struct {
	Message Message
	Attempt int // 投递次数，从 1 开始
}
