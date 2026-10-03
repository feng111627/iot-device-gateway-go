package gateway

import "sync"

// Config 网关配置。
type Config struct {
	AckWindow    int    // 单设备确认窗口（未确认消息上限）
	Quota        int    // 单设备周期内下发配额
	OfflineCap   int    // 单设备离线暂存上限
	SnapshotPath string // 离线暂存快照路径，空则不持久化
}

// Gateway 是网关主体，组合会话、投递与离线暂存。
// 所有状态按设备隔离：单个设备堵塞（窗口满/暂存满）不影响其他设备。
type Gateway struct {
	cfg      Config
	sessions *SessionManager
	offline  *OfflineStore

	mu       sync.Mutex
	trackers map[string]*deliveryTracker // deviceID -> 投递跟踪器
	inbox    map[string][]Delivery       // deviceID -> 待取投递（模拟本机连接）
	attempts map[string]int              // msgID -> 已投递次数
	epochs   map[string]uint64           // deviceID -> 最近一次接入时的会话代次
}

func New(cfg Config) *Gateway {
	return &Gateway{
		cfg:      cfg,
		sessions: NewSessionManager(),
		offline:  NewOfflineStore(cfg.OfflineCap, cfg.SnapshotPath),
		trackers: make(map[string]*deliveryTracker),
		inbox:    make(map[string][]Delivery),
		attempts: make(map[string]int),
		epochs:   make(map[string]uint64),
	}
}

func (g *Gateway) tracker(deviceID string) *deliveryTracker {
	t, ok := g.trackers[deviceID]
	if !ok {
		t = newDeliveryTracker(g.cfg.AckWindow, g.cfg.Quota)
		g.trackers[deviceID] = t
	}
	return t
}

// push 将消息放入设备待取队列并累计投递次数。
func (g *Gateway) push(m Message) {
	g.attempts[m.ID]++
	g.inbox[m.DeviceID] = append(g.inbox[m.DeviceID], Delivery{Message: m, Attempt: g.attempts[m.ID]})
}

// Connect 设备接入（建立或恢复会话）。
//   - 凭据不变的重连：保留订阅，重传未确认消息，补投离线暂存；
//   - 凭据变更/会话被清理后的接入：旧会话产生的暂存与未确认消息全部作废，
//     新身份不会收到旧会话下的任何消息。
func (g *Gateway) Connect(deviceID, credential string) (*Session, error) {
	sess, err := g.sessions.Connect(deviceID, credential)
	if err != nil {
		return nil, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	// 仅当本网关实例此前见过该设备且代次发生变化时，才判定为
	// "凭据变更/会话被清理"，作废旧会话的投递状态与离线暂存。
	// 网关重启后的首次接入不算代次变化，恢复的暂存可正常补投。
	prevEpoch, seen := g.epochs[deviceID]
	if seen && prevEpoch != sess.Epoch {
		g.epochs[deviceID] = sess.Epoch
		g.trackers[deviceID] = newDeliveryTracker(g.cfg.AckWindow, g.cfg.Quota)
		delete(g.inbox, deviceID)
		g.offline.Drain(deviceID) // 丢弃旧身份下的暂存
		return sess, nil
	}
	g.epochs[deviceID] = sess.Epoch

	t := g.tracker(deviceID)
	// 1. 重传窗口内未确认的消息（顺序稳定）
	for _, m := range t.retransmit() {
		g.push(m)
	}
	// 2. 补投离线暂存；窗口/配额不足时剩余部分回到暂存，等待下次机会
	pending := g.offline.Drain(deviceID)
	for i, m := range pending {
		assigned, err := t.assign(m)
		if err != nil {
			for _, rest := range pending[i:] {
				g.offline.Enqueue(deviceID, rest)
			}
			break
		}
		g.push(assigned)
	}
	return sess, nil
}

// Disconnect 设备断开。graceful=true 为正常断开，false 为异常掉线；
// 两种情况下订阅与未确认消息都保留，供重连恢复。
func (g *Gateway) Disconnect(deviceID string, graceful bool) {
	g.sessions.Disconnect(deviceID, graceful)
}

// ClearSession 主动清理会话：订阅作废、代次递增，与正常断开可区分。
func (g *Gateway) ClearSession(deviceID string) {
	g.sessions.Clear(deviceID)
}

// Subscribe 设备订阅主题，重连后保留。
func (g *Gateway) Subscribe(deviceID, topic string) error {
	return g.sessions.Subscribe(deviceID, topic)
}

// Subscriptions 返回设备当前订阅。
func (g *Gateway) Subscriptions(deviceID string) []string {
	return g.sessions.Subscriptions(deviceID)
}

// Publish 云端向设备下发消息。
//   - 设备在线：占用确认窗口与配额后立即进入待取队列；
//     窗口满或配额用尽时拒绝并返回可区分原因（ErrCodeWindowFull / ErrCodeQuotaExceeded）；
//   - 设备离线：进入离线暂存；暂存满时按"淘汰最旧"策略丢弃最老消息。
func (g *Gateway) Publish(deviceID, msgID, topic string, payload []byte) error {
	sess := g.sessions.Get(deviceID)
	if sess == nil {
		return newError(ErrCodeUnknownDevice, "设备 %s 未接入", deviceID)
	}
	m := Message{ID: msgID, DeviceID: deviceID, Topic: topic, Payload: payload}

	g.mu.Lock()
	defer g.mu.Unlock()
	if !sess.Connected {
		g.offline.Enqueue(deviceID, m)
		return nil
	}
	assigned, err := g.tracker(deviceID).assign(m)
	if err != nil {
		return err
	}
	g.push(assigned)
	return nil
}

// Receive 设备拉取待收投递（含新消息、重传与离线补投）。
// 设备离线时拒绝，返回 ErrCodeNotConnected。
func (g *Gateway) Receive(deviceID string) ([]Delivery, error) {
	sess := g.sessions.Get(deviceID)
	if sess == nil || !sess.Connected {
		return nil, newError(ErrCodeNotConnected, "设备 %s 不在线", deviceID)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	out := g.inbox[deviceID]
	delete(g.inbox, deviceID)
	return out, nil
}

// Ack 设备确认消息。
//   - 首次确认：成功，消息被消费且不会再投递；
//   - 重复确认：返回 ErrCodeDuplicateAck，无副作用（恰好消费一次）；
//   - 未知消息：返回 ErrCodeUnknownMessage。
func (g *Gateway) Ack(deviceID, msgID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	t, ok := g.trackers[deviceID]
	if !ok {
		return newError(ErrCodeUnknownDevice, "设备 %s 未接入", deviceID)
	}
	return t.ack(msgID)
}

// ResetQuota 重置所有设备配额（模拟周期刷新）。
func (g *Gateway) ResetQuota() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, t := range g.trackers {
		t.resetQuota()
	}
}

// OfflineLen 返回设备离线暂存量（观测用）。
func (g *Gateway) OfflineLen(deviceID string) int { return g.offline.Len(deviceID) }

// OfflineEvicted 返回设备被淘汰的暂存消息总数（观测用）。
func (g *Gateway) OfflineEvicted(deviceID string) int { return g.offline.Evicted(deviceID) }

// Snapshot 持久化离线暂存（网关重启前调用）。
func (g *Gateway) Snapshot() error { return g.offline.Snapshot() }

// Restore 恢复离线暂存（网关重启后调用），幂等去重。
func (g *Gateway) Restore() error { return g.offline.Restore() }
