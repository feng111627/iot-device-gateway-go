package gateway

import "sync"

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
func (q *OfflineQueue) Enqueue(m Message) (evicted *Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.capacity > 0 && len(q.msgs) >= q.capacity {
		old := q.msgs[0]
		q.msgs = q.msgs[1:]
		q.evicted++
		evicted = &old
	}
	q.msgs = append(q.msgs, m)
	return evicted
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

// OfflineStore 管理所有设备的离线队列，并支持快照持久化以应对网关重启。
// 每个设备独立队列：个别设备堵塞（队列满）不影响其他设备的暂存与收发。
type OfflineStore struct {
	mu     sync.Mutex
	queues map[string]*OfflineQueue
	cap    int
}

func NewOfflineStore(capacity int) *OfflineStore {
	return &OfflineStore{queues: make(map[string]*OfflineQueue), cap: capacity}
}

func (s *OfflineStore) queue(deviceID string) *OfflineQueue {
	q, ok := s.queues[deviceID]
	if !ok {
		q = NewOfflineQueue(s.cap)
		s.queues[deviceID] = q
	}
	return q
}

// Enqueue 为设备暂存一条消息。
func (s *OfflineStore) Enqueue(deviceID string, m Message) (evicted *Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queue(deviceID).Enqueue(m)
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

// snapshotDevice 导出单设备离线暂存片段（保持入队顺序）。
func (s *OfflineStore) snapshotDevice(deviceID string, st *deviceState) {
	s.mu.Lock()
	q, ok := s.queues[deviceID]
	s.mu.Unlock()
	if !ok {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	st.Offline = make([]Message, len(q.msgs))
	copy(st.Offline, q.msgs)
	st.Evicted = q.evicted
}

// restoreDevice 幂等恢复单设备离线暂存：按消息 ID 去重合并，保持原先后顺序；
// 合并后若超出容量，按与运行时一致的"淘汰最旧、保护最新"策略只留最新若干条，
// 被淘汰条数计入 blocked；重复恢复不产生重复。
func (s *OfflineStore) restoreDevice(st deviceState) (restored, blocked int) {
	s.mu.Lock()
	q := s.queue(st.DeviceID)
	s.mu.Unlock()

	q.mu.Lock()
	defer q.mu.Unlock()
	// 恢复会同时重建历史淘汰计数（快照内的淘汰数是权威累计值，取较大值防回退）。
	if st.Evicted > q.evicted {
		q.evicted = st.Evicted
	}
	// 先记录恢复前已在内存中的消息，便于区分"本次新装入"与"幂等命中"。
	before := make(map[string]bool, len(q.msgs))
	merged := make([]Message, 0, len(q.msgs)+len(st.Offline))
	seen := make(map[string]bool, len(q.msgs)+len(st.Offline))
	for _, m := range q.msgs {
		before[m.ID] = true
		seen[m.ID] = true
		merged = append(merged, m)
	}
	newFromSnapshot := make([]Message, 0, len(st.Offline))
	for _, m := range st.Offline {
		if m.ID == "" || seen[m.ID] {
			continue
		}
		seen[m.ID] = true
		merged = append(merged, m)
		newFromSnapshot = append(newFromSnapshot, m)
	}
	// 合并后超容量：按运行时一致的策略淘汰最旧、保留最新。
	if q.capacity > 0 && len(merged) > q.capacity {
		merged = merged[len(merged)-q.capacity:]
	}
	surviving := make(map[string]bool, len(merged))
	for _, m := range merged {
		surviving[m.ID] = true
	}
	for _, m := range newFromSnapshot {
		if surviving[m.ID] {
			restored++
		} else {
			blocked++
			q.evicted++ // 与运行时淘汰共用同一累计观测口径
		}
	}
	q.msgs = merged
	return restored, blocked
}

// wipe 清空指定设备的全部离线暂存与淘汰计数（换新身份时使用）：
// 新身份看到的应是一块完全干净的暂存，旧会话统计也不得残留。
func (s *OfflineStore) wipe(deviceID string) {
	s.mu.Lock()
	q, ok := s.queues[deviceID]
	s.mu.Unlock()
	if !ok {
		return
	}
	q.mu.Lock()
	q.msgs = nil
	q.evicted = 0
	q.mu.Unlock()
}
