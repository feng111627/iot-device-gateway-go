package gateway

import (
	"encoding/json"
	"sync"
)

// Config 网关配置。
type Config struct {
	AckWindow    int    // 单设备确认窗口（未确认消息上限）
	Quota        int    // 单设备周期内下发配额
	OfflineCap   int    // 单设备离线暂存上限
	ConsumedCap  int    // 单设备"已消费"去重记录保留上限，<=0 表示不限；超限确定性淘汰 Seq 最小（最旧）的记录
	SnapshotPath string // （兼容）旧版离线暂存全量快照路径
	StateDir     string // 设备持久状态目录（WAL+快照），空则纯内存
	MaxDevices   int    // 恢复/持久化的设备数容量边界，<=0 表示不限
	CompactEvery int    // 每设备每累计多少条事件做一次快照压缩；<=0 不自动压缩
}

// deviceStateEnvelope 是压缩快照中的设备业务状态（见 durable.snapshotEnvelope.State）。
type deviceStateEnvelope struct {
	Credential string          `json:"credential"`
	Epoch      uint64          `json:"epoch"`
	Subs       []string        `json:"subs"`
	Tracker    trackerSnapshot `json:"tracker"`
	Offline    *queueSnapshot  `json:"offline,omitempty"`
}

// Gateway 是网关主体，组合会话、投递与离线暂存。
// 所有状态按设备隔离：单个设备堵塞（窗口满/暂存满/状态损坏）不影响其他设备。
type Gateway struct {
	cfg      Config
	sessions *SessionManager
	offline  *OfflineStore
	store    *durableStore

	mu           sync.Mutex
	trackers     map[string]*deliveryTracker // deviceID -> 投递跟踪器
	inbox        map[string][]Delivery       // deviceID -> 待取投递（模拟本机连接，瞬时状态）
	quarantined  map[string]bool             // 持久状态损坏、被隔离的设备
	recoverStats RecoverStats
}

func New(cfg Config) *Gateway {
	compactEvery := cfg.CompactEvery
	if compactEvery == 0 {
		compactEvery = 256
	}
	return &Gateway{
		cfg:         cfg,
		sessions:    NewSessionManager(),
		offline:     NewOfflineStore(cfg.OfflineCap, cfg.SnapshotPath),
		store:       newDurableStore(cfg.StateDir, cfg.MaxDevices, compactEvery),
		trackers:    make(map[string]*deliveryTracker),
		inbox:       make(map[string][]Delivery),
		quarantined: make(map[string]bool),
	}
}

// newTracker 创建按当前配置（窗口/配额/去重保留上限）装配好的投递跟踪器。
func (g *Gateway) newTracker() *deliveryTracker {
	t := newDeliveryTracker(g.cfg.AckWindow, g.cfg.Quota)
	t.setConsumedCap(g.cfg.ConsumedCap)
	return t
}

func (g *Gateway) tracker(deviceID string) *deliveryTracker {
	t, ok := g.trackers[deviceID]
	if !ok {
		t = g.newTracker()
		g.trackers[deviceID] = t
	}
	return t
}

// appendEvent 先持久化状态变更，再返回事件序号；dir 为空时不影响内存运行。
func (g *Gateway) appendEvent(deviceID string, rec eventRecord) (uint64, int, error) {
	return g.store.append(deviceID, rec)
}

// maybeCompact 事件落盘后按需做快照压缩；压缩失败不影响已落盘的正确性。
func (g *Gateway) maybeCompact(deviceID string, n, unsnapped uint64) {
	if g.cfg.StateDir == "" {
		return
	}
	at := g.cfg.CompactEvery
	if at <= 0 {
		at = 256
	}
	if unsnapped < uint64(at) {
		return
	}
	state, err := g.buildDeviceState(deviceID)
	if err != nil {
		return
	}
	_ = g.store.compact(deviceID, n, state)
}

// buildDeviceState 在 g.mu 保护下导出设备全量状态（调用方不得已持锁以外的组件锁）。
func (g *Gateway) buildDeviceState(deviceID string) (json.RawMessage, error) {
	t := g.trackers[deviceID]
	sess := g.sessions.Get(deviceID)
	env := deviceStateEnvelope{}
	if sess != nil {
		env.Credential = sess.Credential
		env.Epoch = sess.Epoch
		env.Subs = g.sessions.Subscriptions(deviceID)
	}
	if t != nil {
		env.Tracker = t.snapshotState()
	} else {
		env.Tracker = trackerSnapshot{
			Inflight: map[string]Message{},
			Attempts: map[string]int{},
			Consumed: map[string]uint64{},
		}
	}
	env.Offline = g.offline.SnapshotDevice(deviceID)
	return json.Marshal(&env)
}

// push 将消息放入设备待取队列，投递次数取自持久化跟踪器（Attempt 跨重启连续计数）。
func (g *Gateway) push(deviceID string, m Message) {
	att := g.tracker(deviceID).markPushed(m.ID)
	g.inbox[deviceID] = append(g.inbox[deviceID], Delivery{Message: m, Attempt: att})
}

// resumeLocked 在设备连接（含恢复后的重连）时：重传在途消息、补投离线暂存，
// 每一步都先落盘（assign/drain），保证该过程被强杀后仍可接着恢复、不重不丢。
//
// 新连接建立即作废旧连接遗留的待取投递：那些投递从未被设备取走，对应消息
// 仍在在途窗口中，由下面的重传统一覆盖——否则同一批消息会在 inbox 里出现
// 两份（旧连接残留 + 新连接重传），顺序也被打乱。
func (g *Gateway) resumeLocked(sess *Session) {
	deviceID := sess.DeviceID
	delete(g.inbox, deviceID)
	t := g.tracker(deviceID)
	// 1. 重传窗口内未确认的消息（顺序稳定）。重传不落新事件：assign 事件已在
	// 落盘，Attempt 只是瞬时投递计数，重启后由持久化的 attempts 接续。
	for _, m := range t.retransmit() {
		g.push(deviceID, m)
	}
	// 2. 取出离线暂存逐条补投：只有真正占用窗口成功的消息才从持久队列移除；
	// 窗口/配额/落盘失败时，剩余全部放回队首，保持原顺序等待下次连接。
	pending := g.offline.Drain(deviceID)
	var drainedIDs []string
	var remaining []Message
	for _, m := range pending {
		if t.inflightOrConsumed(m.ID) {
			// 极端抖动下已在窗口/已消费的旧消息，仅从暂存丢弃，不再下发
			drainedIDs = append(drainedIDs, m.ID)
			continue
		}
		// 先预校验窗口/配额（不占状态），通过后先落 assign 事件，再提交内存。
		if err := t.precheckAssign(m); err != nil {
			remaining = append(remaining, m)
			continue
		}
		toAssign := m
		toAssign.Seq = t.peekNextSeq()
		n, unsnapped, werr := g.appendEvent(deviceID, eventRecord{Type: evAssign, Msg: &toAssign, Attempt: 1})
		if werr != nil {
			// 落盘失败：窗口尚未占用，本条与后续全部回到暂存
			remaining = append(remaining, m)
			continue
		}
		assigned, cerr := t.commitAssign(m)
		if cerr != nil {
			remaining = append(remaining, m)
			continue
		}
		drainedIDs = append(drainedIDs, m.ID)
		g.push(deviceID, assigned)
		g.maybeCompact(deviceID, n, uint64(unsnapped))
	}
	g.offline.PrependFront(deviceID, remaining)
	if len(drainedIDs) > 0 {
		n, unsnapped, err := g.appendEvent(deviceID, eventRecord{Type: evDrain, IDs: drainedIDs})
		if err == nil {
			g.maybeCompact(deviceID, n, uint64(unsnapped))
		}
	}
}

// Connect 设备接入（建立或恢复会话）。
//   - 首次接入：创建会话并持久化；
//   - 凭据不变的重连（含网关重启后）：保留订阅，重传未确认消息，补投离线暂存；
//   - 凭据与记录不符：返回 CREDENTIAL_MISMATCH，必须显式选择
//     RotateCredential（同一设备平滑换密钥）或 ResetIdentity（换新身份）。
func (g *Gateway) Connect(deviceID, credential string) (*Session, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.quarantined[deviceID] {
		return nil, newError(ErrCodeDeviceQuarantined,
			"设备 %s 的持久状态损坏已被隔离，请显式 ResetIdentity 后以新身份接入", deviceID)
	}
	existed := g.sessions.Get(deviceID) != nil
	sess, err := g.sessions.Connect(deviceID, credential)
	if err != nil {
		return nil, err
	}
	if !existed {
		n, unsnapped, werr := g.appendEvent(deviceID, eventRecord{Type: evConnect, Cred: credential})
		if werr != nil {
			return nil, werr
		}
		g.maybeCompact(deviceID, n, uint64(unsnapped))
	}
	g.resumeLocked(sess)
	return sess, nil
}

// Disconnect 设备断开。graceful=true 为正常断开，false 为异常掉线；
// 两种情况下订阅与未确认消息都保留，供重连恢复。连接是瞬时状态，不持久化。
//
// 与 Publish/Receive/Connect 同持 g.mu：在线状态的翻转与"按在线下发/拉取"
// 的判定严格串行，并发掉线+下发不会出现"已掉线却按在线投递"的中间态。
// 断开即作废本连接未取走的待取投递（设备不可能再取到它们）；对应消息仍在
// 在途窗口中，重连时由 resumeLocked 统一重传，不重不丢。
func (g *Gateway) Disconnect(deviceID string, graceful bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sessions.Disconnect(deviceID, graceful)
	delete(g.inbox, deviceID)
}

// ClearSession 主动清理会话：订阅作废、代次递增，与正常断开可区分；
// 语义为管理性清理（持久化），不触碰在途消息与离线暂存。
func (g *Gateway) ClearSession(deviceID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sessions.Clear(deviceID)
	n, unsnapped, err := g.appendEvent(deviceID, eventRecord{Type: evClear})
	if err == nil {
		g.maybeCompact(deviceID, n, uint64(unsnapped))
	}
}

// RotateCredential 平滑更换设备凭据：同一台设备、同一会话代次，
// 订阅关系、在途未确认、离线暂存、已消费去重记录全部照旧继续使用。
// 更换后设备可用新凭据重连。
func (g *Gateway) RotateCredential(deviceID, newCredential string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.quarantined[deviceID] {
		return newError(ErrCodeDeviceQuarantined, "设备 %s 已隔离", deviceID)
	}
	if _, err := g.sessions.RotateCredential(deviceID, newCredential); err != nil {
		return err
	}
	n, unsnapped, err := g.appendEvent(deviceID, eventRecord{Type: evRotate, Cred: newCredential})
	if err != nil {
		return err
	}
	g.maybeCompact(deviceID, n, uint64(unsnapped))
	return nil
}

// ResetIdentity 让设备以全新身份接入：旧会话痕迹（订阅、在途、离线暂存、
// 已消费去重记录、投递序号）全部作废；该作废本身持久化并立即压缩快照，
// 即使网关随后重启，旧身份的任何消息也不会再发给新身份。
func (g *Gateway) ResetIdentity(deviceID, newCredential string) (*Session, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	sess, err := g.sessions.ResetIdentity(deviceID, newCredential)
	if err != nil {
		return nil, err
	}
	g.trackers[deviceID] = g.newTracker()
	g.offline.ResetDevice(deviceID)
	delete(g.inbox, deviceID)
	// 先物理清空该设备的历史 WAL/快照（含损坏日志），再以全新日志记录 reset。
	if err := g.store.resetFiles(deviceID); err != nil {
		return nil, err
	}
	n, unsnapped, werr := g.appendEvent(deviceID, eventRecord{Type: evReset, Cred: newCredential})
	if werr != nil {
		return nil, werr
	}
	delete(g.quarantined, deviceID)
	// 立即压缩：快照反映"空的新身份状态"，后续重放只会看到 reset 之后的事件。
	state, err := g.buildDeviceState(deviceID)
	if err == nil {
		_ = g.store.compact(deviceID, n, state)
	}
	g.maybeCompact(deviceID, n, uint64(unsnapped))
	return sess, nil
}

// Subscribe 设备订阅主题，重连与凭据轮换后保留。
func (g *Gateway) Subscribe(deviceID, topic string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.quarantined[deviceID] {
		return newError(ErrCodeDeviceQuarantined, "设备 %s 已隔离", deviceID)
	}
	if err := g.sessions.Subscribe(deviceID, topic); err != nil {
		return err
	}
	n, unsnapped, err := g.appendEvent(deviceID, eventRecord{Type: evSub, Topic: topic})
	if err != nil {
		return err
	}
	g.maybeCompact(deviceID, n, uint64(unsnapped))
	return nil
}

// Subscriptions 返回设备当前订阅。
func (g *Gateway) Subscriptions(deviceID string) []string {
	return g.sessions.Subscriptions(deviceID)
}

// Publish 云端向设备下发消息。
//   - 设备在线：先落 assign 事件再占用确认窗口与配额，随后进入待取队列；
//     窗口满或配额用尽时拒绝（WINDOW_FULL / QUOTA_EXCEEDED），不落盘；
//   - 设备离线：先落 enqueue 事件再进入离线暂存；暂存满按"淘汰最旧"丢弃。
func (g *Gateway) Publish(deviceID, msgID, topic string, payload []byte) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.quarantined[deviceID] {
		return newError(ErrCodeDeviceQuarantined, "设备 %s 已隔离", deviceID)
	}
	sess := g.sessions.Get(deviceID)
	if sess == nil {
		return newError(ErrCodeUnknownDevice, "设备 %s 未接入", deviceID)
	}
	m := Message{ID: msgID, DeviceID: deviceID, Topic: topic, Payload: payload}
	t := g.tracker(deviceID)
	if !sess.Connected {
		n, unsnapped, err := g.appendEvent(deviceID, eventRecord{Type: evEnqueue, Msg: &m})
		if err != nil {
			return err
		}
		g.offline.Enqueue(deviceID, m)
		g.maybeCompact(deviceID, n, uint64(unsnapped))
		return nil
	}
	// 只校验、不生效：确认窗口/配额/去重都通过后先落盘，再正式占用。
	if err := t.precheckAssign(m); err != nil {
		return err
	}
	n, unsnapped, werr := g.appendEvent(deviceID, eventRecord{Type: evAssign, Msg: &Message{
		ID: m.ID, DeviceID: m.DeviceID, Topic: m.Topic, Payload: m.Payload,
		Seq: t.peekNextSeq(),
	}, Attempt: 1})
	if werr != nil {
		return werr
	}
	assigned, err := t.commitAssign(m)
	if err != nil {
		// 理论不可达（precheck 已过），保守返回错误而不占用窗口
		return err
	}
	g.push(deviceID, assigned)
	g.maybeCompact(deviceID, n, uint64(unsnapped))
	return nil
}

// Receive 设备拉取待收投递（含新消息、重传与离线补投）。
// 设备离线时拒绝，返回 ErrCodeNotConnected。
//
// 在线判定与取件在同一把 g.mu 下原子完成：与 Disconnect/Connect 并发时，
// 要么在断开前取走（设备当时确实在线），要么得到 NOT_CONNECTED，
// 不存在"判定在线、取件时已离线"的中间态。
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

// Ack 设备确认消息（先落盘 ack 事件再改内存，崩溃在中间也只会重放出同样结果）。
//   - 首次确认：成功，消息被消费且不会再投递（跨重启同样保证）；
//   - 重复确认：返回 ErrCodeDuplicateAck，无副作用；
//   - 未知消息：返回 ErrCodeUnknownMessage。
func (g *Gateway) Ack(deviceID, msgID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.quarantined[deviceID] {
		return newError(ErrCodeDeviceQuarantined, "设备 %s 已隔离", deviceID)
	}
	t, ok := g.trackers[deviceID]
	if !ok {
		return newError(ErrCodeUnknownDevice, "设备 %s 未接入", deviceID)
	}
	// 先校验（不落状态），ack 事件落盘成功后再把消息移出窗口/标记已消费。
	// 崩溃在"落盘后、提交前"时，重放会得到同一个结果；重复确认的判定不变。
	if err := t.precheckAck(msgID); err != nil {
		return err
	}
	n, unsnapped, err := g.appendEvent(deviceID, eventRecord{Type: evAck, IDs: []string{msgID}})
	if err != nil {
		return err
	}
	if err := t.commitAck(msgID); err != nil {
		return err
	}
	g.maybeCompact(deviceID, n, uint64(unsnapped))
	return nil
}

// ResetQuota 重置所有设备配额（模拟周期刷新），并逐设备持久化。
func (g *Gateway) ResetQuota() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for deviceID, t := range g.trackers {
		t.resetQuota()
		n, unsnapped, err := g.appendEvent(deviceID, eventRecord{Type: evQuota, Used: 0})
		if err == nil {
			g.maybeCompact(deviceID, n, uint64(unsnapped))
		}
	}
}

// OfflineLen 返回设备离线暂存量（观测用）。
func (g *Gateway) OfflineLen(deviceID string) int { return g.offline.Len(deviceID) }

// OfflineEvicted 返回设备被淘汰的暂存消息总数（观测用）。
func (g *Gateway) OfflineEvicted(deviceID string) int { return g.offline.Evicted(deviceID) }

// ConsumedRetained 返回设备当前保留的"已消费"去重记录数（观测用）。
// 该集合按 ConsumedCap 封顶，不会随运行时间无限增长。
func (g *Gateway) ConsumedRetained(deviceID string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	t, ok := g.trackers[deviceID]
	if !ok {
		return 0
	}
	return t.consumedCount()
}

// ConsumedEvicted 返回设备因超出 ConsumedCap 被淘汰的去重记录总数（观测用）。
func (g *Gateway) ConsumedEvicted(deviceID string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	t, ok := g.trackers[deviceID]
	if !ok {
		return 0
	}
	return t.consumedEvictedCount()
}

// Snapshot （兼容旧版）持久化离线暂存全量快照；使用 StateDir 时无需手工调用。
func (g *Gateway) Snapshot() error { return g.offline.Snapshot() }

// Restore （兼容旧版）从 SnapshotPath 恢复离线暂存，幂等去重。
func (g *Gateway) Restore() error { return g.offline.Restore() }

// RecoveryStats 返回最近一次 Recover 的结果统计（恢复了多少、丢了多少、挡掉了多少）。
func (g *Gateway) RecoveryStats() RecoverStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.recoverStats
}

// Recover 在网关启动时从 StateDir 恢复全部设备状态。
// 语义：
//   - 幂等且可中断：每台设备"快照装载 + 序号之后的事件重放"，事件全部按类型幂等；
//     Recover 可以反复调用，恢复到一半被中断后再次调用结果一致；
//   - 隔离：单个设备状态损坏只隔离该设备（DEVICE_QUARANTINED），
//     不影响其他设备恢复，也不会让整个网关起不来；
//   - 容量边界：超过 MaxDevices 的设备不会恢复，计入 DevicesSkippedLimit；
//   - 恢复后所有设备处于离线状态，需重新 Connect 才会重传/补投。
func (g *Gateway) Recover() (RecoverStats, error) {
	stats := RecoverStats{}
	if g.cfg.StateDir == "" {
		g.mu.Lock()
		g.recoverStats = stats
		g.mu.Unlock()
		return stats, nil
	}
	recovered, skipped, err := g.store.scan()
	if err != nil {
		return stats, err
	}
	stats.SkippedDevices = skipped
	stats.DevicesSkippedLimit = len(skipped)

	g.mu.Lock()
	defer g.mu.Unlock()
	for _, rd := range recovered {
		g.recoverOneLocked(rd, &stats)
	}
	g.recoverStats = stats
	return stats, nil
}

func (g *Gateway) recoverOneLocked(rd recoveredDevice, stats *RecoverStats) {
	deviceID := rd.deviceID
	fail := func(reason string) {
		g.quarantined[deviceID] = true
		stats.DevicesFailed++
		stats.CorruptRecords += rd.corrupt
		stats.FailedDevices = append(stats.FailedDevices, deviceID)
	}

	// 1. 快照损坏 / WAL 中部损坏缺号：无法保证完整重建，隔离该设备。
	//    仅尾部"写一半"的撕裂记录会在扫描时安全截断，不算失败。
	if rd.snapCorrupt || rd.walCorrupt {
		fail("persistent state corrupt")
		return
	}

	t := g.newTracker()
	g.trackers[deviceID] = t

	if rd.state != nil {
		var env deviceStateEnvelope
		if err := json.Unmarshal(rd.state, &env); err != nil {
			fail("snapshot unmarshal")
			return
		}
		g.sessions.materializeSession(deviceID, env.Credential, env.Epoch, env.Subs)
		if env.Tracker.Inflight == nil {
			env.Tracker.Inflight = map[string]Message{}
		}
		if env.Tracker.Consumed == nil {
			env.Tracker.Consumed = map[string]uint64{}
		}
		t.materializeState(&env.Tracker)
		if env.Offline != nil {
			g.offline.MaterializeDevice(deviceID, *env.Offline)
		}
	}

	// 2. 重放快照点之后的事件；全部幂等。
	for _, rec := range rd.events {
		switch rec.Type {
		case evConnect, evSub, evRotate, evReset, evClear:
			if rec.Type == evReset {
				t.reset()
				g.offline.ResetDevice(deviceID)
			}
			g.sessions.applyEvent(deviceID, rec)
		case evAssign, evAck, evQuota:
			t.applyEvent(rec)
		case evEnqueue:
			if rec.Msg != nil {
				g.offline.ApplyEnqueue(deviceID, *rec.Msg)
			}
		case evDrain:
			g.offline.ApplyDrain(deviceID, rec.IDs)
		}
		stats.EventsReplayed++
	}

	stats.DevicesRecovered++
	stats.InflightRecovered += t.inflightCount()
	stats.ConsumedRetained += t.consumedCount()
	stats.ConsumedEvicted += t.consumedEvictedCount()
	stats.OfflineRecovered += g.offline.Len(deviceID)
	stats.CorruptRecords += rd.corrupt
}
