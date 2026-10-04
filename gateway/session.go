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

// Connect 建立或恢复会话，返回 (会话, 凭据是否发生变化)：
//   - 首次接入：创建会话，Epoch 从 1 开始，changed=false；
//   - 凭据不变的重连：保留订阅与代次，仅恢复在线状态，changed=false；
//   - 凭据变化：按"换新身份"处理，旧会话作废，代次递增、订阅清空，changed=true，
//     由网关层连带清掉旧会话的在途/暂存/去重记录。
//
// 平滑换凭据请显式走 RotateCredential（不触发代次变化）。
func (m *SessionManager) Connect(deviceID, credential string) (*Session, bool, error) {
	if deviceID == "" || credential == "" {
		return nil, false, newError(ErrCodeUnknownDevice, "deviceID 与 credential 不能为空")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[deviceID]
	if !ok {
		s = &Session{DeviceID: deviceID, Epoch: 1, Credential: credential, Subs: make(map[string]bool)}
		m.sessions[deviceID] = s
	}
	changed := false
	if s.Credential != credential {
		// 身份/凭据变更且未走显式轮换：旧会话作废
		s.Epoch++
		s.Subs = make(map[string]bool)
		s.Credential = credential
		changed = true
	}
	s.Connected = true
	return s, changed, nil
}

// Count 返回当前已注册（见过）的设备数，用于容量边界判断。
func (m *SessionManager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

// IDs 返回当前全部设备 ID（排序，输出稳定），用于快照设备集合。
func (m *SessionManager) IDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.sessions))
	for id := range m.sessions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
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

// RotateCredential 平滑轮换凭据：保持同一设备身份（代次不变），
// 订阅、未确认下发、离线暂存、已消费记录全部保留。
// 设备必须已存在；换成相同凭据视为无变化的幂等操作。
func (m *SessionManager) RotateCredential(deviceID, newCredential string) error {
	if deviceID == "" || newCredential == "" {
		return newError(ErrCodeUnknownDevice, "deviceID 与 credential 不能为空")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[deviceID]
	if !ok {
		return newError(ErrCodeUnknownDevice, "设备 %s 未接入，无法轮换凭据", deviceID)
	}
	s.Credential = newCredential // 代次、订阅、连接状态均不变
	return nil
}

// ResetIdentity 以全新身份接入：代次递增、订阅清空、设备置为离线，
// 等待新身份以新凭据重新 Connect。网关层负责连带清掉在途、暂存与去重记录。
func (m *SessionManager) ResetIdentity(deviceID, credential string) error {
	if deviceID == "" || credential == "" {
		return newError(ErrCodeUnknownDevice, "deviceID 与 credential 不能为空")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[deviceID]
	if !ok {
		s = &Session{DeviceID: deviceID, Epoch: 0, Subs: make(map[string]bool)}
		m.sessions[deviceID] = s
	}
	s.Epoch++
	s.Credential = credential
	s.Subs = make(map[string]bool)
	s.Connected = false
	s.Graceful = false
	return nil
}

// snapshotDevices 导出指定设备（ids 为 nil 时导出全部）的会话状态片段。
func (m *SessionManager) snapshotDevices(ids []string) map[string]deviceState {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]deviceState)
	take := func(id string, s *Session) {
		subs := make([]string, 0, len(s.Subs))
		for t := range s.Subs {
			subs = append(subs, t)
		}
		sort.Strings(subs)
		out[id] = deviceState{DeviceID: id, Epoch: s.Epoch, Credential: s.Credential, Subs: subs}
	}
	if ids == nil {
		for id, s := range m.sessions {
			take(id, s)
		}
		return out
	}
	for _, id := range ids {
		if s, ok := m.sessions[id]; ok {
			take(id, s)
		}
	}
	return out
}

// restoreDevice 恢复单台设备的会话片段（按设备隔离，调用方负责并发与容错）。
// 重启恢复出的会话一律先标记为离线：进程刚起来时没有真实连接，
// 设备必须重新 Connect 才会上线；凭据与代次按快照原样恢复。
// 返回 isNew 表示该设备是否首次装入内存（重复恢复覆盖已有状态时为 false）。
func (m *SessionManager) restoreDevice(st deviceState) (isNew bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[st.DeviceID]; !ok {
		isNew = true
	}
	subs := make(map[string]bool, len(st.Subs))
	for _, t := range st.Subs {
		subs[t] = true
	}
	m.sessions[st.DeviceID] = &Session{
		DeviceID:   st.DeviceID,
		Epoch:      st.Epoch,
		Credential: st.Credential,
		Connected:  false,
		Subs:       subs,
	}
	return isNew
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
