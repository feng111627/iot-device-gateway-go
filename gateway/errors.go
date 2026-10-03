package gateway

import "errors"

// 可区分的拒绝原因。
var (
	ErrWindowFull        = errors.New("ack window full: too many unacked messages")
	ErrQuotaExceeded     = errors.New("device quota exceeded")
	ErrUnknownSeq        = errors.New("ack for unknown sequence")
	ErrSessionNotFound   = errors.New("session not found")
	ErrSessionInvalid    = errors.New("session invalidated by credential change")
	ErrDeviceOffline     = errors.New("device is offline")
)
