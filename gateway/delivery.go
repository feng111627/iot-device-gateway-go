package gateway

import (
	"sort"
	"sync"
)

// trackerSnapshot 是 deliveryTracker 的可持久化全量状态（快照业务负载的一部分）。
type trackerSnapshot struct {
	NextSeq  uint64             `json:"next_seq"`
	Used     int                `json:"used"`
	Inflight map[string]Message `json:"inflight"`           // 在途未确认
	Attempts map[string]int     `json:"attempts,omitempty"` // 在途消息已投递次数
	Consumed map[string]uint64  `json:"consumed"`           // 已消费消息 ID -> Seq（去重集）
}

// deliveryTracker 跟踪单设备的下发窗口、顺序与去重。
type deliveryTracker struct {
	mu              sync.Mutex
	nextSeq         uint64             // 下一个分配的 Seq（从 1 开始）
	window          int                // 确认窗口上限（未确认消息数）
	inflight        map[string]Message // 已投递未确认：msgID -> Message
	attempts        map[string]int     // 在途消息已投递次数（Attempt 从 1 开始）
	consumed        map[string]uint64  // 已被设备确认消费的消息 ID -> Seq，用于去重
	consumedCap     int                // 已消费去重集保留上限，<=0 表示不限（容量边界，可观测）
	consumedEvicted int                // 因超出保留上限被移除的去重记录数（观测用）
	quota           int                // 单设备周期内允许下发的配额
	used            int
}

func newDeliveryTracker(window, quota int) *deliveryTracker {
	return &deliveryTracker{
		window:      window,
		quota:       quota,
		inflight:    make(map[string]Message),
		attempts:    make(map[string]int),
		consumed:    make(map[string]uint64),
		consumedCap: 0,
	}
}

// setConsumedCap 设置已消费去重集保留上限（恢复后/创建时由编排层统一配置）。
func (t *deliveryTracker) setConsumedCap(cap int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.consumedCap = cap
	t.evictConsumedLocked()
}

// precheckAssign 只做占用前的校验，不产生任何状态变更（供"先落盘再改内存"）。
func (t *deliveryTracker) precheckAssign(m Message) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.precheckAssignLocked(m)
}

// precheckAssignLocked 调用方持有 t.mu。
func (t *deliveryTracker) precheckAssignLocked(m Message) error {
	if _, ok := t.consumed[m.ID]; ok {
		return newError(ErrCodeDuplicateAck, "消息 %s 已被消费，拒绝重复下发", m.ID)
	}
	if _, ok := t.inflight[m.ID]; ok {
		return newError(ErrCodeDuplicateAck, "消息 %s 已在窗口中，等待确认", m.ID)
	}
	if t.window > 0 && len(t.inflight) >= t.window {
		return newError(ErrCodeWindowFull, "设备 %s 确认窗口已满（%d/%d）", m.DeviceID, len(t.inflight), t.window)
	}
	if t.quota > 0 && t.used >= t.quota {
		return newError(ErrCodeQuotaExceeded, "设备 %s 本周期配额已用尽（%d/%d）", m.DeviceID, t.used, t.quota)
	}
	return nil
}

// peekNextSeq 返回下一个将分配的 Seq（不消费它）。
func (t *deliveryTracker) peekNextSeq() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.nextSeq + 1
}

// commitAssign 在 assign 事件已落盘后正式占用窗口与配额。
func (t *deliveryTracker) commitAssign(m Message) (Message, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.precheckAssignLocked(m); err != nil {
		return m, err
	}
	t.nextSeq++
	m.Seq = t.nextSeq
	t.inflight[m.ID] = m
	// attempts 是"已实际下发次数"，由 markPushed 在进入待取队列时累加；
	// 这里只占位（0 次），避免同一次下发被计两次。
	t.attempts[m.ID] = 0
	t.used++
	return m, nil
}

// assign 为消息分配顺序号并占用窗口与配额；窗口满或超配额时拒绝并给出可区分原因。
func (t *deliveryTracker) assign(m Message) (Message, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.precheckAssignLocked(m); err != nil {
		return m, err
	}
	t.nextSeq++
	m.Seq = t.nextSeq
	t.inflight[m.ID] = m
	t.attempts[m.ID] = 0
	t.used++
	return m, nil
}

// markPushed 记录一次实际下发（进入待取队列），返回当前投递次数。
func (t *deliveryTracker) markPushed(msgID string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.attempts[msgID]++
	return t.attempts[msgID]
}

// precheckAck 校验确认请求但不产生状态变更（供"先落盘再改内存"）。
func (t *deliveryTracker) precheckAck(msgID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.consumed[msgID]; ok {
		return newError(ErrCodeDuplicateAck, "消息 %s 已被确认过", msgID)
	}
	if _, ok := t.inflight[msgID]; !ok {
		return newError(ErrCodeUnknownMessage, "消息 %s 不在确认窗口中", msgID)
	}
	return nil
}

// commitAck 在 ack 事件已落盘后正式移出窗口并标记已消费。
func (t *deliveryTracker) commitAck(msgID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.consumed[msgID]; ok {
		return newError(ErrCodeDuplicateAck, "消息 %s 已被确认过", msgID)
	}
	m, ok := t.inflight[msgID]
	if !ok {
		return newError(ErrCodeUnknownMessage, "消息 %s 不在确认窗口中", msgID)
	}
	delete(t.inflight, msgID)
	delete(t.attempts, msgID)
	t.consumed[msgID] = m.Seq
	t.evictConsumedLocked()
	return nil
}

// ack 确认消息（内存语义）：
//   - 首次确认：移出窗口并标记已消费；
//   - 重复确认：返回 ErrCodeDuplicateAck，但不会产生任何副作用（消息不会被再次消费）；
//   - 未知消息：返回 ErrCodeUnknownMessage。
func (t *deliveryTracker) ack(msgID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.consumed[msgID]; ok {
		return newError(ErrCodeDuplicateAck, "消息 %s 已被确认过", msgID)
	}
	m, ok := t.inflight[msgID]
	if !ok {
		return newError(ErrCodeUnknownMessage, "消息 %s 不在确认窗口中", msgID)
	}
	delete(t.inflight, msgID)
	delete(t.attempts, msgID)
	t.consumed[msgID] = m.Seq
	t.evictConsumedLocked()
	return nil
}

// evictConsumedLocked 在已消费去重集超限时，确定性地移除 Seq 最小（最旧）的记录。
// 注：系统自身重传的消息一定仍在在途集合中，被移除的只是远超窗口/积压范围的
// 历史确认记录；该上界提供明确容量边界，移除计数可观测。
func (t *deliveryTracker) evictConsumedLocked() {
	if t.consumedCap <= 0 {
		return
	}
	for len(t.consumed) > t.consumedCap {
		var oldestID string
		var oldestSeq uint64
		for id, seq := range t.consumed {
			if oldestID == "" || seq < oldestSeq {
				oldestID, oldestSeq = id, seq
			}
		}
		delete(t.consumed, oldestID)
		t.consumedEvicted++
	}
}

// inflightOrConsumed 判断消息是否已在窗口中或已被消费（补投时做防重判断）。
func (t *deliveryTracker) inflightOrConsumed(msgID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.consumed[msgID]; ok {
		return true
	}
	if _, ok := t.inflight[msgID]; ok {
		return true
	}
	return false
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

// reset 清空全部投递状态（换新身份时使用）：在途、已消费、序号与配额全部归零。
func (t *deliveryTracker) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextSeq = 0
	t.used = 0
	t.inflight = make(map[string]Message)
	t.attempts = make(map[string]int)
	t.consumed = make(map[string]uint64)
}

// inflightCount / consumedCount 供编排层恢复统计使用。
func (t *deliveryTracker) inflightCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.inflight)
}

func (t *deliveryTracker) consumedCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.consumed)
}

// snapshotState 导出全量状态（压缩时调用）。
func (t *deliveryTracker) snapshotState() trackerSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	snap := trackerSnapshot{
		NextSeq:  t.nextSeq,
		Used:     t.used,
		Inflight: make(map[string]Message, len(t.inflight)),
		Attempts: make(map[string]int, len(t.attempts)),
		Consumed: make(map[string]uint64, len(t.consumed)),
	}
	for id, m := range t.inflight {
		snap.Inflight[id] = m
	}
	for id, a := range t.attempts {
		snap.Attempts[id] = a
	}
	for id, seq := range t.consumed {
		snap.Consumed[id] = seq
	}
	return snap
}

// materializeState 从快照装载全量状态（配置中的 window/quota/保留上限以当前进程为准）。
func (t *deliveryTracker) materializeState(snap *trackerSnapshot) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextSeq = snap.NextSeq
	t.used = snap.Used
	t.inflight = make(map[string]Message, len(snap.Inflight))
	for id, m := range snap.Inflight {
		t.inflight[id] = m
	}
	t.attempts = make(map[string]int, len(snap.Attempts))
	for id, a := range snap.Attempts {
		t.attempts[id] = a
	}
	t.consumed = make(map[string]uint64, len(snap.Consumed))
	for id, seq := range snap.Consumed {
		t.consumed[id] = seq
	}
	t.evictConsumedLocked()
}

// applyEvent 重放投递类事件（assign/ack/quota）。
// 所有分支幂等：同一事件因压缩/恢复中断被重放多次不产生重复或双计数。
func (t *deliveryTracker) applyEvent(rec eventRecord) {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch rec.Type {
	case evAssign:
		if rec.Msg == nil {
			return
		}
		if _, ok := t.consumed[rec.Msg.ID]; ok {
			return
		}
		if _, ok := t.inflight[rec.Msg.ID]; ok {
			return // 已重放过，不重复占用配额
		}
		if rec.Msg.Seq > t.nextSeq {
			t.nextSeq = rec.Msg.Seq
		}
		t.inflight[rec.Msg.ID] = *rec.Msg
		att := rec.Attempt
		if att < 1 {
			att = 1
		}
		t.attempts[rec.Msg.ID] = att
		t.used++
	case evAck:
		id := rec.MsgID()
		if _, ok := t.consumed[id]; ok {
			return
		}
		if m, ok := t.inflight[id]; ok {
			delete(t.inflight, id)
			delete(t.attempts, id)
			t.consumed[id] = m.Seq
			t.evictConsumedLocked()
		}
	case evQuota:
		t.used = rec.Used
	}
}

// MsgID 取出 ack 事件的消息 ID（ack 事件不带消息体，仅带 ID，复用 IDs[0]）。
func (rec eventRecord) MsgID() string {
	if len(rec.IDs) > 0 {
		return rec.IDs[0]
	}
	return ""
}
