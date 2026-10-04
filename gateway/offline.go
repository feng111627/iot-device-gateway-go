package gateway

import (
	"encoding/json"
	"os"
	"sync"
)

// OfflineQueue 是单设备的离线暂存队列，容量有限。
// 淘汰策略：队列满时丢弃最旧消息（确定性、可解释：保护最新数据）。
type OfflineQueue struct {
	mu       sync.Mutex
	capacity int
	msgs     []Message
	evicted  int // 被淘汰（丢弃）的消息总数，便于观测
}

func NewOfflineQueue(capacity int) *OfflineQueue {
	return &OfflineQueue{capacity: capacity}
}

// Enqueue 暂存消息；满时淘汰最旧消息并返回被淘汰者（未满时返回 nil）。
// 同一消息 ID 已在队列中时跳过（幂等，保证跨重启/重放不重复）。
func (q *OfflineQueue) Enqueue(m Message) (evicted *Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.enqueueLocked(m)
}

func (q *OfflineQueue) enqueueLocked(m Message) (evicted *Message) {
	for _, ex := range q.msgs {
		if ex.ID == m.ID {
			return nil // 幂等：重复 ID 不再入队、不触发淘汰
		}
	}
	if q.capacity > 0 && len(q.msgs) >= q.capacity {
		old := q.msgs[0]
		q.msgs = q.msgs[1:]
		q.evicted++
		evicted = &old
	}
	q.msgs = append(q.msgs, m)
	return evicted
}

// snapshotLocked 导出队列持久化状态（调用方持锁）。
type queueSnapshot struct {
	Msgs    []Message `json:"messages"`
	Evicted int       `json:"evicted"`
}

func (q *OfflineQueue) snapshotState() queueSnapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := queueSnapshot{Msgs: make([]Message, len(q.msgs)), Evicted: q.evicted}
	copy(out.Msgs, q.msgs)
	return out
}

func (q *OfflineQueue) materializeState(snap queueSnapshot) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.msgs = make([]Message, len(snap.Msgs))
	copy(q.msgs, snap.Msgs)
	q.evicted = snap.Evicted
}

// removeLocked 按消息 ID 移除队列中的消息（drain 事件重放，幂等）。
func (q *OfflineQueue) removeLocked(ids map[string]bool) int {
	if len(ids) == 0 || len(q.msgs) == 0 {
		return 0
	}
	kept := q.msgs[:0]
	removed := 0
	for _, m := range q.msgs {
		if ids[m.ID] {
			removed++
			continue
		}
		kept = append(kept, m)
	}
	q.msgs = kept
	return removed
}

// Drain 取出全部暂存消息（保持入队顺序），用于设备上线后的补投。
func (q *OfflineQueue) Drain() []Message {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := q.msgs
	q.msgs = nil
	return out
}

// Len 返回当前暂存量。
func (q *OfflineQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.msgs)
}

// Evicted 返回被淘汰的消息总数。
func (q *OfflineQueue) Evicted() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.evicted
}

// snapshotRecord 是持久化到磁盘的记录格式。
type snapshotRecord struct {
	DeviceID string    `json:"device_id"`
	Messages []Message `json:"messages"`
}

// OfflineStore 管理所有设备的离线队列，并支持快照持久化以应对网关重启。
// 每个设备独立队列：个别设备堵塞（队列满）不影响其他设备的暂存与收发。
type OfflineStore struct {
	mu     sync.Mutex
	queues map[string]*OfflineQueue
	cap    int
	path   string // 快照文件路径，空表示不持久化
}

func NewOfflineStore(capacity int, snapshotPath string) *OfflineStore {
	return &OfflineStore{queues: make(map[string]*OfflineQueue), cap: capacity, path: snapshotPath}
}

func (s *OfflineStore) queue(deviceID string) *OfflineQueue {
	q, ok := s.queues[deviceID]
	if !ok {
		q = NewOfflineQueue(s.cap)
		s.queues[deviceID] = q
	}
	return q
}

// Enqueue 为设备暂存一条消息（幂等：重复 ID 不重复入队）。
func (s *OfflineStore) Enqueue(deviceID string, m Message) (evicted *Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queue(deviceID).Enqueue(m)
}

// ApplyEnqueue 重放 enqueue 事件：受容量约束，超出部分确定性淘汰最旧，
// 返回该设备累计淘汰数，便于恢复统计。
func (s *OfflineStore) ApplyEnqueue(deviceID string, m Message) (evictedTotal int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queue(deviceID)
	q.mu.Lock()
	defer q.mu.Unlock()
	q.enqueueLocked(m)
	return q.evicted
}

// ApplyDrain 重放 drain 事件：按 ID 幂等移除消息。
func (s *OfflineStore) ApplyDrain(deviceID string, ids []string) int {
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[deviceID]
	if !ok {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.removeLocked(set)
}

// SnapshotDevice 导出单设备队列的持久化状态。
func (s *OfflineStore) SnapshotDevice(deviceID string) *queueSnapshot {
	s.mu.Lock()
	q, ok := s.queues[deviceID]
	s.mu.Unlock()
	if !ok {
		return nil
	}
	snap := q.snapshotState()
	return &snap
}

// MaterializeDevice 从快照装载单设备队列（恢复用）。
func (s *OfflineStore) MaterializeDevice(deviceID string, snap queueSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queue(deviceID).materializeState(snap)
}

// PrependFront 将若干消息按原顺序放回队首（补投中断后把未处理部分还回暂存）。
// 仅放入队列中不存在的 ID（幂等）；容量不足时最旧消息被确定性淘汰。
func (s *OfflineStore) PrependFront(deviceID string, msgs []Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queue(deviceID)
	q.mu.Lock()
	defer q.mu.Unlock()
	have := make(map[string]bool, len(q.msgs))
	for _, m := range q.msgs {
		have[m.ID] = true
	}
	prepend := make([]Message, 0, len(msgs))
	for _, m := range msgs {
		if have[m.ID] {
			continue
		}
		prepend = append(prepend, m)
		have[m.ID] = true
	}
	q.msgs = append(prepend, q.msgs...)
	// 容量约束：超限时淘汰最旧（队列尾部的最旧元素，即 prepend 之前原有的）
	for q.capacity > 0 && len(q.msgs) > q.capacity {
		q.msgs = q.msgs[:len(q.msgs)-1]
		q.evicted++
	}
}

// ResetDevice 清空单设备队列与淘汰计数（换新身份用）。
func (s *OfflineStore) ResetDevice(deviceID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.queues, deviceID)
}

// Drain 取出并清空设备的暂存消息。
func (s *OfflineStore) Drain(deviceID string) []Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[deviceID]
	if !ok {
		return nil
	}
	return q.Drain()
}

// Len 返回设备当前暂存量。
func (s *OfflineStore) Len(deviceID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[deviceID]
	if !ok {
		return 0
	}
	return q.Len()
}

// Evicted 返回设备被淘汰的消息总数。
func (s *OfflineStore) Evicted(deviceID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[deviceID]
	if !ok {
		return 0
	}
	return q.Evicted()
}

// Snapshot 将全部暂存原子写入磁盘（先写临时文件再重命名，避免抖动时写坏）。
func (s *OfflineStore) Snapshot() error {
	if s.path == "" {
		return nil
	}
	s.mu.Lock()
	recs := make([]snapshotRecord, 0, len(s.queues))
	for id, q := range s.queues {
		q.mu.Lock()
		msgs := make([]Message, len(q.msgs))
		copy(msgs, q.msgs)
		q.mu.Unlock()
		recs = append(recs, snapshotRecord{DeviceID: id, Messages: msgs})
	}
	s.mu.Unlock()

	data, err := json.Marshal(recs)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// Restore 从磁盘恢复快照。恢复以"消息 ID 去重"的方式合并：
// 重复恢复或快照与内存重叠时不会产生重复消息，也不会整批丢失。
func (s *OfflineStore) Restore() error {
	if s.path == "" {
		return nil
	}
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var recs []snapshotRecord
	if err := json.Unmarshal(data, &recs); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rec := range recs {
		q := s.queue(rec.DeviceID)
		q.mu.Lock()
		seen := make(map[string]bool, len(q.msgs))
		for _, m := range q.msgs {
			seen[m.ID] = true
		}
		for _, m := range rec.Messages {
			if seen[m.ID] {
				continue // 已在内存中，跳过避免重复下发
			}
			if q.capacity > 0 && len(q.msgs) >= q.capacity {
				continue // 恢复同样受容量约束，超出部分视为已被淘汰
			}
			q.msgs = append(q.msgs, m)
			seen[m.ID] = true
		}
		q.mu.Unlock()
	}
	return nil
}
