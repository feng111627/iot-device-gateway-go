package gateway

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// 业务场景【离线堆积】：暂存满时确定性淘汰最旧，淘汰可观测。
func TestOffline_EvictsOldestDeterministically(t *testing.T) {
	t.Log("业务: 离线暂存 | 用例: 暂存满时按 FIFO 淘汰最旧(确定性策略)")
	var evicted []uint64
	g := New(Config{AckWindow: 8, OfflineCapacity: 3})
	g.SetEvictHandler(func(dev string, m *Message) { evicted = append(evicted, m.Seq) })

	c := &collector{}
	_, _ = g.Connect("dev1", "t", false, c.deliver)
	_ = g.Subscribe("dev1", "cmd/dev1")
	g.Disconnect("dev1", false)

	for _, p := range []string{"p1", "p2", "p3", "p4", "p5"} {
		_ = g.Publish("cmd/dev1", []byte(p))
	}
	if got := g.OfflineLen("dev1"); got != 3 {
		t.Fatalf("offline queue should be capped at 3, got %d", got)
	}
	if len(evicted) != 2 || evicted[0] != 1 || evicted[1] != 2 {
		t.Fatalf("expected seq 1,2 evicted in order, got %v", evicted)
	}

	// 重连后只能收到存活的 3、4、5。
	c2 := &collector{}
	_, _ = g.Connect("dev1", "t", false, c2.deliver)
	if got := len(c2.all()); got != 3 {
		t.Fatalf("expected 3 surviving messages, got %d", got)
	}
	for i, m := range c2.all() {
		if m.Seq != uint64(3+i) {
			t.Fatalf("expected seq %d, got %d", 3+i, m.Seq)
		}
	}
	t.Logf("结果: 容量3, 写入5条, 淘汰最旧 seq=%v, 存活 3,4,5 按序补发 ✓", evicted)
}

// 业务场景【重启恢复】：网关重启后暂存不丢不重，在途消息安全重发。
func TestOffline_SurvivesRestart(t *testing.T) {
	t.Log("业务: 离线暂存 | 用例: 网关重启后暂存不丢失不重复")
	dir := t.TempDir()
	state := filepath.Join(dir, "state.jsonl")

	g1 := New(Config{AckWindow: 8, OfflineCapacity: 16, StatePath: state})
	c1 := &collector{}
	_, _ = g1.Connect("dev1", "t", false, c1.deliver)
	_ = g1.Subscribe("dev1", "cmd/dev1")
	// 在线收 1 条不确认（在途），再离线积 2 条。
	_ = g1.Publish("cmd/dev1", []byte("inflight-1"))
	g1.Disconnect("dev1", false)
	_ = g1.Publish("cmd/dev1", []byte("offline-2"))
	_ = g1.Publish("cmd/dev1", []byte("offline-3"))
	if got := g1.OfflineLen("dev1"); got != 2 {
		t.Fatalf("expected 2 offline before restart, got %d", got)
	}

	// 模拟重启：同一状态文件新建网关。
	g2 := New(Config{AckWindow: 8, OfflineCapacity: 16, StatePath: state})
	if got := g2.OfflineLen("dev1"); got != 3 {
		t.Fatalf("expected 3 messages recovered (1 inflight + 2 offline), got %d", got)
	}
	// 设备重连：3 条全部补发，序号 1,2,3 各一次。
	c2 := &collector{}
	_, _ = g2.Connect("dev1", "t", false, c2.deliver)
	msgs := c2.all()
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages after restart, got %d", len(msgs))
	}
	for i, m := range msgs {
		if m.Seq != uint64(i+1) {
			t.Fatalf("expected seq %d, got %d", i+1, m.Seq)
		}
	}
	// 全部确认后队列清空，再次重启不会重复下发。
	for _, m := range msgs {
		if _, err := g2.Ack("dev1", m.Seq); err != nil {
			t.Fatalf("ack %d: %v", m.Seq, err)
		}
	}
	g3 := New(Config{AckWindow: 8, OfflineCapacity: 16, StatePath: state})
	c3 := &collector{}
	_, _ = g3.Connect("dev1", "t", false, c3.deliver)
	if got := len(c3.all()); got != 0 {
		t.Fatalf("acked messages must not be redelivered after restart, got %d", got)
	}
	t.Log("结果: 重启恢复 3 条(在途1+暂存2), 确认后再次重启零重复 ✓")
}

// 业务场景【堵塞隔离】：单个设备堵塞不影响其他设备收发。
func TestOffline_BlockedDeviceIsolation(t *testing.T) {
	t.Log("业务: 过载保护 | 用例: 单设备堵塞不拖垮其他设备")
	g := New(Config{AckWindow: 2, OfflineCapacity: 4})
	blocked := &collector{}
	_, _ = g.Connect("blocked", "t", false, blocked.deliver)
	_ = g.Subscribe("blocked", "alerts")
	// 打满 blocked 的确认窗口（不确认）。
	_ = g.Publish("alerts", []byte("x"))
	_ = g.Publish("alerts", []byte("x"))
	if err := g.Publish("alerts", []byte("x")); !errors.Is(err, ErrWindowFull) {
		t.Fatalf("blocked device should hit ErrWindowFull, got %v", err)
	}

	// 另一台正常设备不受任何影响。
	normal := &collector{}
	_, _ = g.Connect("normal", "t", false, normal.deliver)
	_ = g.Subscribe("normal", "alerts")
	if err := g.Publish("alerts", []byte("for-normal")); err != nil {
		// Publish 返回首个错误(blocked 的窗口满), 但 normal 必须已收到。
		if !errors.Is(err, ErrWindowFull) {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if got := len(normal.all()); got != 1 {
		t.Fatalf("normal device should receive message despite blocked peer, got %d", got)
	}
	t.Log("结果: blocked 窗口满被拒绝, normal 正常收到消息 ✓")
}

// 业务场景【配额拒绝】：上行超出配额被拒绝且原因可区分。
func TestUplink_QuotaExceeded(t *testing.T) {
	t.Log("业务: 过载保护 | 用例: 上行超配额被拒绝(ErrQuotaExceeded)")
	g := New(Config{AckWindow: 8, OfflineCapacity: 16, UplinkQuota: 3})
	c := &collector{}
	_, _ = g.Connect("dev1", "t", false, c.deliver)

	var accepted, rejected int
	for i := 0; i < 5; i++ {
		err := g.HandleUplink("dev1", []byte("telemetry"))
		if errors.Is(err, ErrQuotaExceeded) {
			rejected++
		} else if err == nil {
			accepted++
		} else {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if accepted != 3 || rejected != 2 {
		t.Fatalf("expected 3 accepted + 2 rejected, got %d/%d", accepted, rejected)
	}
	// 其他设备配额独立。
	if err := g.HandleUplink("dev2", []byte("telemetry")); err != nil {
		t.Fatalf("other device should not be affected: %v", err)
	}
	// 窗口滚动后恢复。
	g.now = func() time.Time { return time.Now().Add(2 * time.Second) }
	if err := g.HandleUplink("dev1", []byte("telemetry")); err != nil {
		t.Fatalf("quota should recover after window: %v", err)
	}
	t.Log("结果: 配额3, 5次上行接受3拒绝2(ErrQuotaExceeded), 他设备不受影响, 窗口滚动后恢复 ✓")
}
