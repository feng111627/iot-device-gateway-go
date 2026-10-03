package gateway

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Config 网关配置。
type Config struct {
	AckWindow       int    // 单设备最大在途未确认消息数
	OfflineCapacity int    // 单设备离线暂存上限
	UplinkQuota     int    // 单设备每秒上行配额（0 表示不限）
	StatePath       string // 状态持久化路径，空为纯内存
}

// Gateway 是设备接入网关核心。
type Gateway struct {
	cfg      Config
	sessions *SessionManager
	queues   *QueueStore

	mu      sync.Mutex
	online  map[string]func(*Message) error // deviceID -> 下发函数
	onEvict func(deviceID string, m *Message)
	onUp    func(deviceID string, payload []byte) // 上行消息回调

	msgID atomic.Uint64
	now   func() time.Time // 可注入时钟，便于测试配额

	upMu   sync.Mutex
	upWin  map[string]time.Time // deviceID -> 当前配额窗口起点
	upCnt  map[string]int       // deviceID -> 窗口内已用配额
}

func New(cfg Config) *Gateway {
	g := &Gateway{
		cfg:      cfg,
		sessions: NewSessionManager(),
		queues:   NewQueueStore(cfg.OfflineCapacity, cfg.StatePath),
		online:   make(map[string]func(*Message) error),
		now:      time.Now,
		upWin:    make(map[string]time.Time),
		upCnt:    make(map[string]int),
	}
	// 恢复上次运行持久化的暂存、在途消息与设备凭据。
	tokens, _ := g.queues.Load()
	for dev, fp := range tokens {
		g.sessions.RestoreToken(dev, fp)
	}
	return g
}

// SetEvictHandler 设置离线暂存淘汰回调（可观测淘汰行为）。
func (g *Gateway) SetEvictHandler(fn func(deviceID string, m *Message)) { g.onEvict = fn }

// SetUplinkHandler 设置上行消息处理回调。
func (g *Gateway) SetUplinkHandler(fn func(deviceID string, payload []byte)) { g.onUp = fn }

// Connect 设备接入：建立或恢复会话，注册下发通道，并重发未确认消息、补发离线暂存。
// 返回 resumed 表示是否恢复了旧会话（订阅关系继续可用）。
func (g *Gateway) Connect(deviceID, token string, clean bool, deliver func(*Message) error) (resumed bool, err error) {
	sess, resumed, invalidated, err := g.sessions.Connect(deviceID, token, clean)
	if err != nil {
		return false, err
	}
	if invalidated {
		// 凭据变更或主动清理：旧会话的暂存消息不得再下发。
		_ = g.queues.Clear(deviceID)
	}
	if !resumed {
		// 持久化凭据指纹，重启后仍能识别凭据变更。
		_ = g.queues.SaveToken(deviceID, tokenFingerprint(token))
	}
	g.mu.Lock()
	g.online[deviceID] = deliver
	g.mu.Unlock()

	// 先重发本会话在途未确认消息（序号较小），再补发离线暂存，保证按 Seq 顺序到达。
	// 设备端按 Seq 去重，重复投递安全。
	g.sessions.mu.Lock()
	var pending []*Message
	for _, m := range sess.inflight {
		pending = append(pending, m)
	}
	g.sessions.mu.Unlock()
	sort.Slice(pending, func(i, j int) bool { return pending[i].Seq < pending[j].Seq })
	for _, m := range pending {
		_ = g.deliver(deviceID, m)
	}
	g.drain(deviceID)
	return resumed, nil
}

// Disconnect 设备断开：clean=false 为正常断开（会话保留可恢复），
// clean=true 为主动清理（订阅、在途、暂存全部丢弃）。
func (g *Gateway) Disconnect(deviceID string, clean bool) {
	g.mu.Lock()
	delete(g.online, deviceID)
	g.mu.Unlock()
	if clean {
		g.sessions.Clean(deviceID)
		_ = g.queues.Clear(deviceID)
	}
}

// Subscribe 为设备添加订阅，重连恢复后继续有效。
func (g *Gateway) Subscribe(deviceID string, topics ...string) error {
	return g.sessions.Subscribe(deviceID, topics...)
}

// Publish 向所有订阅了 topic 的设备下发消息。
// 在线且在确认窗口内：直接下发并计入在途；
// 在线但窗口已满：返回 ErrWindowFull（可区分原因）；
// 离线：进入暂存队列，满则按 FIFO 淘汰最旧（确定性策略）。
func (g *Gateway) Publish(topic string, payload []byte) error {
	var firstErr error
	for _, dev := range g.sessions.Subscribers(topic) {
		if err := g.publishTo(dev, topic, payload); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (g *Gateway) publishTo(deviceID, topic string, payload []byte) error {
	sess, ok := g.sessions.Get(deviceID)
	if !ok {
		return ErrSessionNotFound
	}
	m := &Message{
		ID:      fmt.Sprintf("m-%d", g.msgID.Add(1)),
		Topic:   topic,
		Payload: payload,
	}

	g.sessions.mu.Lock()
	m.Seq = sess.nextSeq
	sess.nextSeq++
	windowFull := g.cfg.AckWindow > 0 && len(sess.inflight) >= g.cfg.AckWindow
	g.sessions.mu.Unlock()

	g.mu.Lock()
	_, online := g.online[deviceID]
	g.mu.Unlock()

	if online && windowFull {
		return fmt.Errorf("device %s: %w", deviceID, ErrWindowFull)
	}
	if !online {
		evicted, err := g.queues.Enqueue(deviceID, m)
		if evicted != nil && g.onEvict != nil {
			g.onEvict(deviceID, evicted)
		}
		return err
	}
	return g.deliverTracked(deviceID, m)
}

// drain 在确认窗口允许时持续补发离线暂存消息。
func (g *Gateway) drain(deviceID string) {
	for {
		if g.cfg.AckWindow > 0 && g.InflightLen(deviceID) >= g.cfg.AckWindow {
			return // 确认窗口已满，剩余暂存留待确认后继续
		}
		m, err := g.queues.Dequeue(deviceID)
		if err != nil || m == nil {
			return
		}
		if err := g.deliverTracked(deviceID, m); err != nil {
			// 下发失败则放回暂存，等待下次重连。
			_, _ = g.queues.Enqueue(deviceID, m)
			return
		}
	}
}

// deliverTracked 下发并计入在途（用于新消息）。
func (g *Gateway) deliverTracked(deviceID string, m *Message) error {
	if err := g.deliver(deviceID, m); err != nil {
		_, _ = g.queues.Enqueue(deviceID, m)
		return err
	}
	g.sessions.mu.Lock()
	if sess, ok := g.sessions.sessions[deviceID]; ok {
		sess.inflight[m.Seq] = m
	}
	g.sessions.mu.Unlock()
	return g.queues.TrackInflight(deviceID, m)
}

// deliver 实际下发一条消息。
func (g *Gateway) deliver(deviceID string, m *Message) error {
	g.mu.Lock()
	fn, ok := g.online[deviceID]
	g.mu.Unlock()
	if !ok {
		return ErrDeviceOffline
	}
	return fn(m)
}

// Ack 处理设备确认。
// 返回 duplicate=true 表示重复确认（幂等忽略，消息只被消费一次）；
// 确认未知序号返回 ErrUnknownSeq（可区分原因）。
func (g *Gateway) Ack(deviceID string, seq uint64) (duplicate bool, err error) {
	g.sessions.mu.Lock()
	sess, ok := g.sessions.sessions[deviceID]
	if !ok {
		g.sessions.mu.Unlock()
		return false, ErrSessionNotFound
	}
	if _, ok := sess.inflight[seq]; ok {
		delete(sess.inflight, seq)
		sess.acked[seq] = struct{}{}
		g.sessions.mu.Unlock()
		if err := g.queues.UntrackInflight(deviceID, seq); err != nil {
			return false, err
		}
		g.drain(deviceID) // 窗口腾出空间，继续补发暂存
		return false, nil
	}
	if _, ok := sess.acked[seq]; ok {
		g.sessions.mu.Unlock()
		return true, nil // 重复确认：幂等忽略
	}
	g.sessions.mu.Unlock()
	return false, fmt.Errorf("device %s seq %d: %w", deviceID, seq, ErrUnknownSeq)
}

// HandleUplink 处理设备上行消息，受单设备每秒配额限制。
// 超出配额返回 ErrQuotaExceeded（可区分原因），不影响其他设备。
func (g *Gateway) HandleUplink(deviceID string, payload []byte) error {
	if g.cfg.UplinkQuota > 0 {
		g.upMu.Lock()
		now := g.now()
		win, ok := g.upWin[deviceID]
		if !ok || now.Sub(win) >= time.Second {
			g.upWin[deviceID] = now
			g.upCnt[deviceID] = 0
		}
		if g.upCnt[deviceID] >= g.cfg.UplinkQuota {
			g.upMu.Unlock()
			return fmt.Errorf("device %s: %w", deviceID, ErrQuotaExceeded)
		}
		g.upCnt[deviceID]++
		g.upMu.Unlock()
	}
	if g.onUp != nil {
		g.onUp(deviceID, payload)
	}
	return nil
}

// OfflineLen 返回某设备当前离线暂存条数（用于测试与监控）。
func (g *Gateway) OfflineLen(deviceID string) int { return g.queues.Len(deviceID) }

// InflightLen 返回某设备当前在途未确认条数（用于测试与监控）。
func (g *Gateway) InflightLen(deviceID string) int {
	g.sessions.mu.Lock()
	defer g.sessions.mu.Unlock()
	if sess, ok := g.sessions.sessions[deviceID]; ok {
		return len(sess.inflight)
	}
	return 0
}
