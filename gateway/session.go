package gateway

import (
	"sort"
	"sync"
)

// Session 表示一次设备会话。
type Session struct {
	DeviceID   string
	Epoch      uint64 // 会话代次，仅"换新身份/主动清理"时递增；凭据平滑轮换不递增
	Credential string // 当前凭据
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
//   - 首次接入（此前无记录）：创建会话，Epoch 从 1 开始；
//   - 凭据不变的重连：保留订阅与代次，仅恢复在线状态；
//   - 凭据与记录不符：拒绝并返回 ErrCodeCredentialMismatch。
//     平滑换凭据必须显式调用 RotateCredential；
//     以全新身份接入必须显式调用 ResetIdentity（旧会话痕迹全部作废）。
//     这样"换密钥"与"换人"在 API 与持久化语义上都能清楚区分。
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
		return nil, newError(ErrCodeCredentialMismatch,
			"设备 %s 凭据与记录不符：平滑换凭据请调用 RotateCredential，换新身份请调用 ResetIdentity", deviceID)
	}
	s.Credential = credential
	s.Connected = true
	return s, nil
}

// RotateCredential 平滑轮换凭据：同一台设备、同一会话代次，
// 订阅、在途消息、离线暂存与"已消费"记录全部保留继续使用。
// newCredential 不能为空。
func (m *SessionManager) RotateCredential(deviceID, newCredential string) (*Session, error) {
	if newCredential == "" {
		return nil, newError(ErrCodeUnknownDevice, "新凭据不能为空")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[deviceID]
	if !ok {
		return nil, newError(ErrCodeUnknownDevice, "设备 %s 未接入", deviceID)
	}
	s.Credential = newCredential
	return s, nil
}

// ResetIdentity 以全新身份接入：代次递增、订阅清空，调用方随后会清空该设备
// 的在途/已消费/离线暂存等全部会话痕迹。旧身份任何消息都不会再下发。
func (m *SessionManager) ResetIdentity(deviceID, newCredential string) (*Session, error) {
	if newCredential == "" {
		return nil, newError(ErrCodeUnknownDevice, "新凭据不能为空")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[deviceID]
	if !ok {
		s = &Session{DeviceID: deviceID, Subs: make(map[string]bool)}
		m.sessions[deviceID] = s
	}
	s.Epoch++
	s.Credential = newCredential
	s.Connected = true
	s.Graceful = false
	s.Subs = make(map[string]bool)
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
// 代次不匹配说明会话已被清理或已换新身份。
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

// Subscribe 记录订阅，重连与凭据轮换后保留。
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

// ---- 持久化重放支持（仅由恢复路径调用）----

// materializeSession 从快照装载会话；恢复后所有设备一律处于离线状态，
// 必须重新 Connect 才在线（连接本身是瞬时状态，不持久化）。
func (m *SessionManager) materializeSession(deviceID, credential string, epoch uint64, subs []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[deviceID]
	if !ok {
		s = &Session{DeviceID: deviceID, Subs: make(map[string]bool)}
		m.sessions[deviceID] = s
	}
	s.Credential = credential
	s.Epoch = epoch
	s.Connected = false
	s.Subs = make(map[string]bool, len(subs))
	for _, t := range subs {
		s.Subs[t] = true
	}
}

// applyEvent 重放单条会话类事件。
func (m *SessionManager) applyEvent(deviceID string, rec eventRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[deviceID]
	if !ok {
		s = &Session{DeviceID: deviceID, Subs: make(map[string]bool)}
		m.sessions[deviceID] = s
	}
	switch rec.Type {
	case evConnect:
		// connect 是幂等的：会话不存在则建（Epoch=1），存在则只校正凭据
		if s.Epoch == 0 {
			s.Epoch = 1
		}
		s.Credential = rec.Cred
		if s.Subs == nil {
			s.Subs = make(map[string]bool)
		}
	case evSub:
		s.Subs[rec.Topic] = true
	case evRotate:
		s.Credential = rec.Cred
	case evReset:
		s.Epoch++
		s.Credential = rec.Cred
		s.Subs = make(map[string]bool)
		s.Connected = false
	case evClear:
		s.Epoch++
		s.Subs = make(map[string]bool)
		s.Connected = false
	}
}
