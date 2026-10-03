package gateway

import (
	"sort"
	"sync"
)

// Session 表示一次设备会话。
type Session struct {
	DeviceID   string
	Epoch      uint64 // 会话代次，凭据变更或主动清理后递增，旧代次的消息不再下发
	Credential string // 当前凭据指纹
	Connected  bool
	Graceful   bool // 上一次断开是否为正常断开
	Subs       map[string]bool
}

// SessionManager 管理设备会话的建立、保持、恢复与清理。
type SessionManager struct {
	mu       sync.Mutex
	sessions map[string]*Session
}

func NewSessionManager() *SessionManager {
	return &SessionManager{sessions: make(map[string]*Session)}
}

// Connect 建立或恢复会话：
//   - 首次接入：创建会话，Epoch 从 1 开始；
//   - 凭据不变的重连：保留订阅与代次，仅恢复在线状态；
//   - 凭据变化：视为新身份，旧会话作废，代次递增、订阅清空，
//     旧代次下已暂存/未确认的消息不再下发给新身份。
func (m *SessionManager) Connect(deviceID, credential string) (*Session, error) {
	if deviceID == "" || credential == "" {
		return nil, newError(ErrCodeUnknownDevice, "deviceID 与 credential 不能为空")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[deviceID]
	if !ok {
		s = &Session{DeviceID: deviceID, Epoch: 1, Subs: make(map[string]bool)}
		m.sessions[deviceID] = s
	} else if s.Credential != credential {
		// 身份/凭据变更：旧会话作废
		s.Epoch++
		s.Subs = make(map[string]bool)
	}
	s.Credential = credential
	s.Connected = true
	return s, nil
}

// Disconnect 断开连接。graceful 区分正常断开与异常掉线；
// 无论哪种断开，订阅关系都保留，供重连后继续使用。
func (m *SessionManager) Disconnect(deviceID string, graceful bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[deviceID]; ok {
		s.Connected = false
		s.Graceful = graceful
	}
}

// Clear 主动清理会话：订阅关系作废、代次递增，与正常断开可区分。
func (m *SessionManager) Clear(deviceID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[deviceID]; ok {
		s.Connected = false
		s.Epoch++
		s.Subs = make(map[string]bool)
	}
}

// Get 返回当前会话；不存在时返回 nil。
func (m *SessionManager) Get(deviceID string) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessions[deviceID]
}

// Validate 校验设备当前会话可用：会话存在且代次匹配。
// 代次不匹配说明会话已被清理或凭据已变更。
func (m *SessionManager) Validate(deviceID string, epoch uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[deviceID]
	if !ok {
		return newError(ErrCodeUnknownDevice, "设备 %s 未接入", deviceID)
	}
	if s.Epoch != epoch {
		return newError(ErrCodeSessionExpired, "设备 %s 会话代次 %d 已失效（当前 %d）", deviceID, epoch, s.Epoch)
	}
	return nil
}

// Subscribe 记录订阅，重连后保留。
func (m *SessionManager) Subscribe(deviceID, topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[deviceID]
	if !ok {
		return newError(ErrCodeUnknownDevice, "设备 %s 未接入", deviceID)
	}
	s.Subs[topic] = true
	return nil
}

// Subscriptions 返回设备当前订阅列表（排序保证输出稳定）。
func (m *SessionManager) Subscriptions(deviceID string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[deviceID]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(s.Subs))
	for t := range s.Subs {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}
