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

// snapshotRecord 是持久化到磁盘的记录格式。
type snapshotRecord struct {
	DeviceID string  `json:"device_id"`
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
