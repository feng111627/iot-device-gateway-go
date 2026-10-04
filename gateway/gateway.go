package gateway

import (
	"sort"
	"sync"
)

// Config 网关配置。
type Config struct {
	AckWindow    int    // 单设备确认窗口（未确认消息上限）
	Quota        int    // 单设备周期内下发配额
	OfflineCap   int    // 单设备离线暂存上限
	SnapshotPath string // 全量状态快照路径，空则不持久化
	MaxDevices   int    // 网关注册设备容量边界，0 表示不限
	DedupCap     int    // 单设备"已消费"去重记录上限，0 表示不限
}

// Gateway 是网关主体，组合会话、投递与离线暂存。
// 所有状态按设备隔离：单个设备堵塞（窗口满/暂存满）不影响其他设备。
type Gateway struct {
	cfg      Config
	sessions *SessionManager
	offline  *OfflineStore

	mu       sync.Mutex
	trackers map[string]*deliveryTracker // deviceID -> 投递跟踪器
	inbox    map[string][]Delivery       // deviceID -> 待取投递（模拟本机连接，仅内存）
	attempts map[string]map[string]int   // deviceID -> msgID -> 已投递次数（持久化）
}

func New(cfg Config) *Gateway {
	return &Gateway{
		cfg:      cfg,
		sessions: NewSessionManager(),
		offline:  NewOfflineStore(cfg.OfflineCap),
		trackers: make(map[string]*deliveryTracker),
		inbox:    make(map[string][]Delivery),
		attempts: make(map[string]map[string]int),
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

// deviceIDsLocked 返回当前已知的全部设备 ID（会话 ∪ 投递跟踪器 ∪ 离线队列，
// 排序后稳定输出）。取并集是为了保证"仅恢复、尚未重连"的设备（此时只有会话
// 和离线数据、还没有 tracker）在任意一次落盘时都不会被漏掉。
func (g *Gateway) deviceIDsLocked() []string {
	seen := make(map[string]bool)
	for _, id := range g.sessions.IDs() {
		seen[id] = true
	}
	for id := range g.trackers {
		seen[id] = true
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// persistLocked 将全量状态写盘。调用方必须持有 g.mu。
// 快照在每次状态变更后同步执行：进程在任意时刻被杀，已返回成功的变更
// 一定已落盘，未返回成功的变更磁盘上不存在（崩溃在写盘前后都安全）。
func (g *Gateway) persistLocked() error {
	if g.cfg.SnapshotPath == "" {
		return nil
	}
	ids := g.deviceIDsLocked()
	sessStates := g.sessions.snapshotDevices(ids)
	states := make([]deviceState, 0, len(ids))
	for _, id := range ids {
		st := sessStates[id] // 会话片段（含 epoch/credential/subs），缺失时只含投递/暂存
		st.DeviceID = id
		g.tracker(id).snapshotDevice(&st)
		g.offline.snapshotDevice(id, &st)
		if am := g.attempts[id]; len(am) > 0 {
			st.Attempts = am
		}
		states = append(states, st)
	}
	return writeStates(g.cfg.SnapshotPath, states)
}

// push 将消息放入设备待取队列并累计投递次数。
func (g *Gateway) pushLocked(m Message) {
	am := g.attempts[m.DeviceID]
	if am == nil {
		am = make(map[string]int)
		g.attempts[m.DeviceID] = am
	}
	am[m.ID]++
	g.inbox[m.DeviceID] = append(g.inbox[m.DeviceID], Delivery{Message: m, Attempt: am[m.ID]})
}

// wipeDeviceLocked 清除单台设备除会话骨架外的一切投递痕迹（换新身份时使用）。
func (g *Gateway) wipeDeviceLocked(deviceID string) {
	g.trackers[deviceID] = newDeliveryTracker(g.cfg.AckWindow, g.cfg.Quota)
	delete(g.inbox, deviceID)
	delete(g.attempts, deviceID)
	g.offline.wipe(deviceID)
}

// Connect 设备接入（建立或恢复会话）。
//   - 凭据不变的重连/重启后重连：保留订阅，按原顺序重传未确认消息、补投离线暂存；
//   - 直接以新凭据 Connect（未走 RotateCredential）：按"换新身份"处理，
//     旧会话产生的暂存与未确认消息全部作废，新身份不会收到旧会话下的任何消息；
//   - 平滑换凭据请先调用 RotateCredential，再用新凭据 Connect。
func (g *Gateway) Connect(deviceID, credential string) (*Session, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.cfg.MaxDevices > 0 {
		if _, exists := g.trackers[deviceID]; !exists && g.sessions.Count() >= g.cfg.MaxDevices {
			return nil, newError(ErrCodeDeviceLimit, "网关注册设备数已达上限 %d", g.cfg.MaxDevices)
		}
	}
	sess, changed, err := g.sessions.Connect(deviceID, credential)
	if err != nil {
		return nil, err
	}
	if changed {
		// 换新身份：清掉旧会话全部投递痕迹，再把新身份的干净状态落盘。
		g.wipeDeviceLocked(deviceID)
		return sess, g.persistLocked()
	}

	t := g.tracker(deviceID)
	// 1. 重传窗口内未确认的消息（按原 Seq 顺序，一条不少）
	for _, m := range t.retransmit() {
		g.pushLocked(m)
	}
	// 2. 补投离线暂存；窗口/配额不足时剩余部分保持原顺序回到暂存，等待下次机会
	pending := g.offline.Drain(deviceID)
	for i, m := range pending {
		assigned, aerr := t.assign(m)
		if aerr != nil {
			for _, rest := range pending[i:] {
				g.offline.Enqueue(deviceID, rest)
			}
			break
		}
		g.pushLocked(assigned)
	}
	return sess, g.persistLocked()
}

// Disconnect 设备断开。graceful=true 为正常断开，false 为异常掉线；
// 两种情况下订阅与未确认消息都保留，供重连恢复。
func (g *Gateway) Disconnect(deviceID string, graceful bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sessions.Disconnect(deviceID, graceful)
	_ = g.persistLocked()
}

// ClearSession 主动清理会话：订阅作废、代次递增，在途与暂存一并清除并落盘，
// 与正常断开可区分。
func (g *Gateway) ClearSession(deviceID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sessions.Clear(deviceID)
	g.wipeDeviceLocked(deviceID)
	_ = g.persistLocked()
}

// Subscribe 设备订阅主题，重连/重启恢复后保留。
func (g *Gateway) Subscribe(deviceID, topic string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.sessions.Subscribe(deviceID, topic); err != nil {
		return err
	}
	return g.persistLocked()
}

// Subscriptions 返回设备当前订阅。
func (g *Gateway) Subscriptions(deviceID string) []string {
	return g.sessions.Subscriptions(deviceID)
}

// RotateCredential 平滑轮换设备凭据：同一设备身份保持不变（Epoch 不变），
// 订阅、未确认下发、离线暂存、已消费去重记录全部保留，并立即持久化；
// 之后用新凭据 Connect 即沿用全部在途与积压。
func (g *Gateway) RotateCredential(deviceID, newCredential string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.sessions.RotateCredential(deviceID, newCredential); err != nil {
		return err
	}
	return g.persistLocked()
}

// ResetIdentity 让设备以全新身份接入：代次递增，订阅、在途、暂存、去重记录
// 全部清除并持久化；重启之后新身份也收不到旧会话里的任何东西。
// 设备随后需用新凭据 Connect 才会上线。
func (g *Gateway) ResetIdentity(deviceID, credential string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.sessions.ResetIdentity(deviceID, credential); err != nil {
		return err
	}
	g.wipeDeviceLocked(deviceID)
	return g.persistLocked()
}

// Publish 云端向设备下发消息。
//   - 设备在线：占用确认窗口与配额后立即进入待取队列，并先落盘再返回成功；
//     窗口满或配额用尽时拒绝并返回可区分原因（码 WINDOW_FULL / QUOTA_EXCEEDED）；
//   - 设备离线：进入离线暂存并落盘；暂存满时按"淘汰最旧"策略丢弃最老消息。
func (g *Gateway) Publish(deviceID, msgID, topic string, payload []byte) error {
	m := Message{ID: msgID, DeviceID: deviceID, Topic: topic, Payload: payload}

	g.mu.Lock()
	defer g.mu.Unlock()
	sess := g.sessions.Get(deviceID) // 持 g.mu 后再读，锁顺序统一为 g.mu -> sessions.mu
	if sess == nil {
		return newError(ErrCodeUnknownDevice, "设备 %s 未接入", deviceID)
	}
	if !sess.Connected {
		g.offline.Enqueue(deviceID, m)
		return g.persistLocked()
	}
	assigned, err := g.tracker(deviceID).assign(m)
	if err != nil {
		return err
	}
	g.pushLocked(assigned)
	return g.persistLocked()
}

// Receive 设备拉取待收投递（含新消息、重传与离线补投）。
// 设备离线时拒绝，返回 ErrCodeNotConnected。拉取不改变已落盘状态：
// 消息仍在确认窗口中，直到 Ack 才标记消费；崩溃后未 Ack 的消息会重传。
func (g *Gateway) Receive(deviceID string) ([]Delivery, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	sess := g.sessions.Get(deviceID)
	if sess == nil || !sess.Connected {
		return nil, newError(ErrCodeNotConnected, "设备 %s 不在线", deviceID)
	}
	out := g.inbox[deviceID]
	delete(g.inbox, deviceID)
	return out, nil
}

// Ack 设备确认消息，确认成功先落盘再返回：
//   - 首次确认：成功，消息被消费且跨重启也不会再投递；
//   - 重复确认：返回 ErrCodeDuplicateAck，无副作用（恰好消费一次）；
//   - 未知消息：返回 ErrCodeUnknownMessage。
func (g *Gateway) Ack(deviceID, msgID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	t, ok := g.trackers[deviceID]
	if !ok {
		return newError(ErrCodeUnknownDevice, "设备 %s 未接入", deviceID)
	}
	if err := t.ack(msgID); err != nil {
		return err
	}
	return g.persistLocked()
}

// ResetQuota 重置所有设备配额（模拟周期刷新）并落盘。
func (g *Gateway) ResetQuota() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, t := range g.trackers {
		t.resetQuota()
	}
	_ = g.persistLocked()
}

// Snapshot 持久化全量状态（会话、窗口、去重、离线暂存），原子写盘。
func (g *Gateway) Snapshot() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.persistLocked()
}

// Restore 恢复全量状态（网关重启后调用），幂等可重复执行，
// 单台设备状态损坏或失败只隔离该设备，不影响其他设备与网关启动。
func (g *Gateway) Restore() error {
	_, err := g.RestoreWithReport()
	return err
}

// RestoreWithReport 与 Restore 相同，但返回可观测的恢复结果
// （恢复多少、挡下多少、损坏多少、哪些设备失败）。可安全反复执行：
// 所有合并均按设备与消息 ID 幂等，恢复到一半再中断后重跑结果一致。
func (g *Gateway) RestoreWithReport() (*RestoreReport, error) {
	report := &RestoreReport{}
	if g.cfg.SnapshotPath == "" {
		return report, nil
	}
	states, corrupt, err := readStates(g.cfg.SnapshotPath)
	if err != nil {
		return report, err
	}
	report.CorruptRecords = corrupt
	sort.Slice(states, func(i, j int) bool { return states[i].DeviceID < states[j].DeviceID })

	g.mu.Lock()
	defer g.mu.Unlock()
	for _, st := range states {
		if g.cfg.MaxDevices > 0 && g.sessions.Count() >= g.cfg.MaxDevices {
			// 容量边界：按设备 ID 排序，超过上限的设备整台挡下，结果可见。
			report.DevicesBlocked++
			report.MessagesBlocked += len(st.Inflight) + len(st.Offline) + len(st.Consumed)
			continue
		}
		// 单设备恢复：任何意外都只隔离该设备，不拖垮其他设备。
		func() {
			defer func() {
				if r := recover(); r != nil {
					report.FailedDevices = append(report.FailedDevices, st.DeviceID)
				}
			}()
			isNew := g.sessions.restoreDevice(st) // 恢复后一律离线，等待设备重新 Connect
			t := g.tracker(st.DeviceID)
			inflightN, consumedN, msgBlocked := t.restoreDevice(st, g.cfg.DedupCap)
			offRestored, offBlocked := g.offline.restoreDevice(st)
			if len(st.Attempts) > 0 {
				am := g.attempts[st.DeviceID]
				if am == nil {
					am = make(map[string]int)
					g.attempts[st.DeviceID] = am
				}
				for id, n := range st.Attempts {
					if n > am[id] {
						am[id] = n
					}
				}
			}
			if isNew {
				report.DevicesRestored++
				report.SubscriptionsRestored += len(st.Subs)
			}
			report.InflightRestored += inflightN
			report.OfflineRestored += offRestored
			report.ConsumedRestored += consumedN
			report.MessagesBlocked += msgBlocked + offBlocked
		}()
	}
	return report, nil
}

// OfflineLen 返回设备离线暂存量（观测用）。
func (g *Gateway) OfflineLen(deviceID string) int { return g.offline.Len(deviceID) }

// OfflineEvicted 返回设备被淘汰的暂存消息总数（观测用）。
func (g *Gateway) OfflineEvicted(deviceID string) int { return g.offline.Evicted(deviceID) }
