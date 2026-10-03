package gateway

import (
	"bufio"
	"encoding/json"
	"os"
	"sort"
	"sync"
)

// offlineQueue 是单设备的离线暂存队列，容量有限，淘汰策略为丢弃最旧（FIFO 淘汰）。
type offlineQueue struct {
	cap  int
	msgs []*Message
}

func newOfflineQueue(cap int) *offlineQueue { return &offlineQueue{cap: cap} }

// push 入队；若已满则淘汰最旧消息并返回它（确定性策略：先进先出淘汰）。
func (q *offlineQueue) push(m *Message) (evicted *Message) {
	if q.cap > 0 && len(q.msgs) >= q.cap {
		evicted = q.msgs[0]
		q.msgs = q.msgs[1:]
	}
	q.msgs = append(q.msgs, m)
	return evicted
}

// pushFront 把消息放回队首（用于重启后恢复在途消息）。
func (q *offlineQueue) pushFront(m *Message) { q.msgs = append([]*Message{m}, q.msgs...) }

// pop 取出队首消息。
func (q *offlineQueue) pop() *Message {
	if len(q.msgs) == 0 {
		return nil
	}
	m := q.msgs[0]
	q.msgs = q.msgs[1:]
	return m
}

// queueEvent 是持久化日志的一条记录。
type queueEvent struct {
	Op     string   `json:"op"` // "push" | "pop" | "clear" | "flight" | "ack" | "token"
	Device string   `json:"device"`
	Seq    uint64   `json:"seq,omitempty"`
	Msg    *Message `json:"msg,omitempty"`
	Token  string   `json:"token,omitempty"` // 凭据指纹（op=token）
}

// QueueStore 管理所有设备的离线队列与在途消息，并负责落盘持久化。
// 每台设备独立队列与配额，单台堵塞不影响其他设备。
type QueueStore struct {
	mu     sync.Mutex
	queues map[string]*offlineQueue
	cap    int
	path   string // 持久化文件路径，空表示不持久化
}

func NewQueueStore(perDeviceCap int, path string) *QueueStore {
	return &QueueStore{queues: make(map[string]*offlineQueue), cap: perDeviceCap, path: path}
}

func (s *QueueStore) queueFor(deviceID string) *offlineQueue {
	q, ok := s.queues[deviceID]
	if !ok {
		q = newOfflineQueue(s.cap)
		s.queues[deviceID] = q
	}
	return q
}

// Enqueue 暂存一条消息；返回被淘汰的最旧消息（可能为 nil）。
func (s *QueueStore) Enqueue(deviceID string, m *Message) (evicted *Message, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	evicted = s.queueFor(deviceID).push(m)
	if err = s.appendLocked(queueEvent{Op: "push", Device: deviceID, Msg: m}); err != nil {
		return nil, err
	}
	return evicted, nil
}

// Dequeue 取出队首消息，无消息时返回 nil。
func (s *QueueStore) Dequeue(deviceID string) (*Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.queueFor(deviceID).pop()
	if m == nil {
		return nil, nil
	}
	if err := s.appendLocked(queueEvent{Op: "pop", Device: deviceID}); err != nil {
		return nil, err
	}
	return m, nil
}

// Clear 清空某设备的暂存（如凭据变更、会话清理时）。
func (s *QueueStore) Clear(deviceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.queues, deviceID)
	return s.appendLocked(queueEvent{Op: "clear", Device: deviceID})
}

// TrackInflight 持久化一条进入在途状态的消息（已下发未确认）。
func (s *QueueStore) TrackInflight(deviceID string, m *Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendLocked(queueEvent{Op: "flight", Device: deviceID, Seq: m.Seq, Msg: m})
}

// UntrackInflight 持久化一条在途消息的确认。
func (s *QueueStore) UntrackInflight(deviceID string, seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendLocked(queueEvent{Op: "ack", Device: deviceID, Seq: seq})
}

// SaveToken 持久化设备凭据指纹，使重启后仍能识别凭据变更。
func (s *QueueStore) SaveToken(deviceID, fingerprint string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendLocked(queueEvent{Op: "token", Device: deviceID, Token: fingerprint})
}

// Len 返回某设备当前暂存条数。
func (s *QueueStore) Len(deviceID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queueFor(deviceID).msgs)
}

// appendLocked 追加一条事件到持久化日志（调用方需已持锁）。
func (s *QueueStore) appendLocked(ev queueEvent) error {
	if s.path == "" {
		return nil
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

// Load 从持久化日志恢复队列状态与设备凭据指纹。
// 崩溃时处于在途（已下发未确认）的消息会被放回对应设备队首，
// 随后压缩日志为当前快照。设备端按 Seq 去重，因此恢复不会造成重复消费。
func (s *QueueStore) Load() (tokens map[string]string, err error) {
	tokens = make(map[string]string)
	if s.path == "" {
		return tokens, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return tokens, nil
		}
		return nil, err
	}
	inflight := make(map[string]map[uint64]*Message) // device -> seq -> msg
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev queueEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			continue // 跳过损坏行（如写入中途掉电）
		}
		switch ev.Op {
		case "push":
			s.queueFor(ev.Device).push(ev.Msg)
		case "pop":
			s.queueFor(ev.Device).pop()
		case "clear":
			delete(s.queues, ev.Device)
		case "flight":
			if inflight[ev.Device] == nil {
				inflight[ev.Device] = make(map[uint64]*Message)
			}
			inflight[ev.Device][ev.Seq] = ev.Msg
		case "ack":
			delete(inflight[ev.Device], ev.Seq)
		case "token":
			tokens[ev.Device] = ev.Token
		}
	}
	f.Close()
	if err := sc.Err(); err != nil {
		return nil, err
	}

	// 未确认的在途消息按序号升序放回队首，保证顺序稳定。
	for dev, m := range inflight {
		if len(m) == 0 {
			continue
		}
		seqs := make([]uint64, 0, len(m))
		for seq := range m {
			seqs = append(seqs, seq)
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		q := s.queueFor(dev)
		for i := len(seqs) - 1; i >= 0; i-- {
			q.pushFront(m[seqs[i]])
		}
	}
	return tokens, s.compactLocked()
}

// compactLocked 将当前内存状态重写为快照，避免日志无限增长。
func (s *QueueStore) compactLocked() error {
	if s.path == "" {
		return nil
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for dev, q := range s.queues {
		for _, m := range q.msgs {
			b, err := json.Marshal(queueEvent{Op: "push", Device: dev, Msg: m})
			if err != nil {
				f.Close()
				return err
			}
			if _, err := w.Write(append(b, '\n')); err != nil {
				f.Close()
				return err
			}
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
