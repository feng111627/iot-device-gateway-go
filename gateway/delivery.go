package gateway

import (
	"sort"
	"sync"
)

// deliveryTracker 跟踪单设备的下发窗口、顺序与去重。
type deliveryTracker struct {
	mu        sync.Mutex
	nextSeq   uint64             // 下一个分配的 Seq（从 1 开始）
	window    int                // 确认窗口上限（未确认消息数）
	inflight  map[string]Message // 已投递未确认：msgID -> Message
	consumed  map[string]bool    // 已被设备确认消费的消息 ID，用于重复确认/重传去重
	quota     int                // 单设备周期内允许下发的配额
	used      int
}

func newDeliveryTracker(window, quota int) *deliveryTracker {
	return &deliveryTracker{
		window:   window,
		quota:    quota,
		inflight: make(map[string]Message),
		consumed: make(map[string]bool),
	}
}

// assign 为消息分配顺序号并占用窗口与配额；窗口满或超配额时拒绝并给出可区分原因。
func (t *deliveryTracker) assign(m Message) (Message, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.consumed[m.ID] {
		return m, newError(ErrCodeDuplicateAck, "消息 %s 已被消费，拒绝重复下发", m.ID)
	}
	if _, ok := t.inflight[m.ID]; ok {
		return m, newError(ErrCodeDuplicateAck, "消息 %s 已在窗口中，等待确认", m.ID)
	}
	if len(t.inflight) >= t.window {
		return m, newError(ErrCodeWindowFull, "设备 %s 确认窗口已满（%d/%d）", m.DeviceID, len(t.inflight), t.window)
	}
	if t.used >= t.quota {
		return m, newError(ErrCodeQuotaExceeded, "设备 %s 本周期配额已用尽（%d/%d）", m.DeviceID, t.used, t.quota)
	}
	t.nextSeq++
	m.Seq = t.nextSeq
	t.inflight[m.ID] = m
	t.used++
	return m, nil
}

// ack 确认消息：
//   - 首次确认：移出窗口并标记已消费；
//   - 重复确认：返回 ErrCodeDuplicateAck，但不会产生任何副作用（消息不会被再次消费）；
//   - 未知消息：返回 ErrCodeUnknownMessage。
func (t *deliveryTracker) ack(msgID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.consumed[msgID] {
		return newError(ErrCodeDuplicateAck, "消息 %s 已被确认过", msgID)
	}
	if _, ok := t.inflight[msgID]; !ok {
		return newError(ErrCodeUnknownMessage, "消息 %s 不在确认窗口中", msgID)
	}
	delete(t.inflight, msgID)
	t.consumed[msgID] = true
	return nil
}

// retransmit 返回窗口内需重传的消息（设备重连后未确认的部分），按 Seq 升序保证顺序稳定。
func (t *deliveryTracker) retransmit() []Message {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Message, 0, len(t.inflight))
	for _, m := range t.inflight {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// resetQuota 重置配额计数（模拟周期刷新）。
func (t *deliveryTracker) resetQuota() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.used = 0
}
