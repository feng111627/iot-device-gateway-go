package gateway

import (
	"sort"
	"sync"
)

// deliveryTracker 跟踪单设备的下发窗口、顺序与去重。
type deliveryTracker struct {
	mu       sync.Mutex
	nextSeq  uint64                    // 下一个分配的 Seq（从 1 开始）
	window   int                       // 确认窗口上限（未确认消息数）
	inflight map[string]Message        // 已投递未确认：msgID -> Message
	consumed map[string]consumedRecord // 已被设备确认消费的消息，用于重复确认/重传去重
	quota    int                       // 单设备周期内允许下发的配额
	used     int
}

func newDeliveryTracker(window, quota int) *deliveryTracker {
	return &deliveryTracker{
		window:   window,
		quota:    quota,
		inflight: make(map[string]Message),
		consumed: make(map[string]consumedRecord),
	}
}

// assign 为消息分配顺序号并占用窗口与配额；窗口满或超配额时拒绝并给出可区分原因。
func (t *deliveryTracker) assign(m Message) (Message, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.consumed[m.ID]; ok {
		return m, newError(ErrCodeDuplicateAck, "已消费消息 %s 拒绝重复下发", m.ID)
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
	if _, ok := t.consumed[msgID]; ok {
		return newError(ErrCodeDuplicateAck, "消息 %s 已被确认过", msgID)
	}
	if _, ok := t.inflight[msgID]; !ok {
		return newError(ErrCodeUnknownMessage, "消息 %s 不在确认窗口中", msgID)
	}
	m := t.inflight[msgID]
	delete(t.inflight, msgID)
	t.consumed[msgID] = consumedRecord{ID: msgID, Seq: m.Seq}
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

// snapshotDevice 导出投递窗口与去重状态片段（均按 Seq 排序，输出确定）。
func (t *deliveryTracker) snapshotDevice(st *deviceState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st.NextSeq = t.nextSeq
	st.QuotaUsed = t.used
	st.Inflight = make([]Message, 0, len(t.inflight))
	for _, m := range t.inflight {
		st.Inflight = append(st.Inflight, m)
	}
	sort.Slice(st.Inflight, func(i, j int) bool { return st.Inflight[i].Seq < st.Inflight[j].Seq })
	st.Consumed = make([]consumedRecord, 0, len(t.consumed))
	for _, c := range t.consumed {
		st.Consumed = append(st.Consumed, c)
	}
	sort.Slice(st.Consumed, func(i, j int) bool { return st.Consumed[i].Seq < st.Consumed[j].Seq })
}

// trimConsumedLocked 在去重记录超限时按最旧（Seq 最小）淘汰，返回淘汰数。
func (t *deliveryTracker) trimConsumedLocked(dedupCap int) int {
	if dedupCap <= 0 || len(t.consumed) <= dedupCap {
		return 0
	}
	records := make([]consumedRecord, 0, len(t.consumed))
	for _, c := range t.consumed {
		records = append(records, c)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Seq < records[j].Seq })
	drop := len(t.consumed) - dedupCap
	for i := 0; i < drop; i++ {
		delete(t.consumed, records[i].ID)
	}
	return drop
}

// restoreDevice 恢复单台设备的投递状态，幂等可重复执行：
//   - 已消费去重记录与在途窗口按消息 ID 合并，重复恢复不产生重复；
//   - 同一条消息同时出现在窗口与去重中时，以"已消费"为准（不会重投）；
//   - 超出窗口/去重容量的部分确定性丢弃最旧记录并计入 blocked，不报错；
//   - 返回值依次为：新装回的在途条数、新装回的已消费条数、被容量挡下的条数。
func (t *deliveryTracker) restoreDevice(st deviceState, dedupCap int) (inflightN, consumedN, blocked int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if st.NextSeq > t.nextSeq {
		t.nextSeq = st.NextSeq
	}
	t.used = st.QuotaUsed
	for _, c := range st.Consumed {
		if c.ID == "" {
			continue
		}
		if _, ok := t.consumed[c.ID]; !ok {
			t.consumed[c.ID] = c
			consumedN++
		}
	}
	if dropped := t.trimConsumedLocked(dedupCap); dropped > 0 {
		consumedN -= dropped // 被容量淘汰的不算恢复成功
		blocked += dropped
	}

	// 按原先后顺序（Seq 升序）补在途消息；已消费或已在窗口的跳过。
	inflight := make([]Message, len(st.Inflight))
	copy(inflight, st.Inflight)
	sort.Slice(inflight, func(i, j int) bool { return inflight[i].Seq < inflight[j].Seq })
	for _, m := range inflight {
		if m.ID == "" {
			continue
		}
		if _, consumed := t.consumed[m.ID]; consumed {
			continue
		}
		if _, exists := t.inflight[m.ID]; exists {
			continue
		}
		if t.window > 0 && len(t.inflight) >= t.window {
			blocked++ // 恢复同样受窗口容量约束：超出部分确定性挡下
			continue
		}
		t.inflight[m.ID] = m
		inflightN++
	}
	return inflightN, consumedN, blocked
}

// resetQuota 重置配额计数（模拟周期刷新）。
func (t *deliveryTracker) resetQuota() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.used = 0
}
