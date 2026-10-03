package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
)

// Session 保存一台设备的会话状态：订阅、序号、在途消息。
type Session struct {
	DeviceID string
	Epoch    uint64 // 凭据变更时递增，旧 Epoch 的消息不再下发

	subs     map[string]struct{}
	nextSeq  uint64
	inflight map[uint64]*Message // 已下发未确认
	acked    map[uint64]struct{} // 已确认（用于识别重复确认）
}

func newSession(deviceID string, epoch uint64) *Session {
	return &Session{
		DeviceID: deviceID,
		Epoch:    epoch,
		subs:     make(map[string]struct{}),
		nextSeq:  1,
		inflight: make(map[uint64]*Message),
		acked:    make(map[uint64]struct{}),
	}
}

// SessionManager 负责会话的建立、保持、恢复与清理。
type SessionManager struct {
	mu       sync.Mutex
	sessions map[string]*Session // key: deviceID
	tokens   map[string]string   // deviceID -> 当前凭据指纹
	epochs   map[string]uint64   // deviceID -> 当前 epoch
}

func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[string]*Session),
		tokens:   make(map[string]string),
		epochs:   make(map[string]uint64),
	}
}

func tokenFingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// Connect 建立或恢复会话。
// 返回 (会话, 是否恢复了旧会话, 是否作废了旧会话, 错误)。
//   - 凭据与上次一致且存在旧会话：恢复原会话（订阅、序号、在途消息保留）。
//   - 凭据变化：作废旧会话（其离线/在途消息不再下发），以新 Epoch 建立全新会话。
//   - clean=true：主动清理旧会话状态后建立新会话（凭据不变也丢弃旧状态）。
func (m *SessionManager) Connect(deviceID, token string, clean bool) (sess *Session, resumed, invalidated bool, err error) {
	if deviceID == "" || token == "" {
		return nil, false, false, ErrSessionNotFound
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	fp := tokenFingerprint(token)
	old, hasOld := m.sessions[deviceID]
	knownFP, hasToken := m.tokens[deviceID]
	// 凭据变更：已知凭据与本次不同（含重启后从持久化恢复的凭据）。
	credChanged := hasToken && knownFP != fp

	// 作废旧会话：凭据变更或主动清理(clean)都不能再收到旧会话的消息。
	invalidated = credChanged || (hasOld && clean)
	if invalidated {
		delete(m.sessions, deviceID)
		hasOld = false
	}
	if credChanged {
		m.epochs[deviceID]++
	}
	if _, ok := m.epochs[deviceID]; !ok {
		m.epochs[deviceID] = 1
	}

	if hasOld {
		return old, true, false, nil
	}
	s := newSession(deviceID, m.epochs[deviceID])
	m.sessions[deviceID] = s
	m.tokens[deviceID] = fp
	return s, false, invalidated, nil
}

// RestoreToken 在网关重启后恢复设备的凭据指纹，
// 使跨重启的凭据变更检测仍然有效。
func (m *SessionManager) RestoreToken(deviceID, fingerprint string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tokens[deviceID] = fingerprint
	if _, ok := m.epochs[deviceID]; !ok {
		m.epochs[deviceID] = 1
	}
}

// Disconnect 区分正常断开与异常断开：两者都保留会话供恢复，
// 真正的状态清理由 Connect(clean=true) 或凭据变更触发。
func (m *SessionManager) Disconnect(deviceID string) {
	// 会话保留在 sessions 中，等待重连恢复。
}

// Get 返回设备当前会话。
func (m *SessionManager) Get(deviceID string) (*Session, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[deviceID]
	return s, ok
}

// Subscribe 记录订阅，重连恢复后继续可用。
func (m *SessionManager) Subscribe(deviceID string, topics ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[deviceID]
	if !ok {
		return ErrSessionNotFound
	}
	for _, t := range topics {
		s.subs[t] = struct{}{}
	}
	return nil
}

// Subscribed 判断设备是否订阅了某主题。
func (m *SessionManager) Subscribed(deviceID, topic string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[deviceID]
	if !ok {
		return false
	}
	_, ok = s.subs[topic]
	return ok
}

// Subscribers 返回订阅了某主题的设备列表。
func (m *SessionManager) Subscribers(topic string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for id, s := range m.sessions {
		if _, ok := s.subs[topic]; ok {
			out = append(out, id)
		}
	}
	return out
}

// Clean 主动清理会话：与正常断开不同，清理后订阅与在途状态全部丢弃。
func (m *SessionManager) Clean(deviceID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, deviceID)
}
