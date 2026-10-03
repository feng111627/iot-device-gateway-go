package gateway

import (
	"sync"
	"testing"
)

// collector 收集下发到某设备的消息，模拟设备端。
type collector struct {
	mu   sync.Mutex
	msgs []*Message
}

func (c *collector) deliver(m *Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, m)
	return nil
}

func (c *collector) all() []*Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*Message, len(c.msgs))
	copy(out, c.msgs)
	return out
}

// 业务场景【会话恢复】：设备断连后重新接入，订阅关系继续可用。
func TestSessionResume_KeepsSubscriptions(t *testing.T) {
	t.Log("业务: 会话恢复 | 用例: 断连重连后订阅关系保留")
	g := New(Config{AckWindow: 8, OfflineCapacity: 16})
	c1 := &collector{}
	if _, err := g.Connect("dev1", "token-a", false, c1.deliver); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := g.Subscribe("dev1", "cmd/dev1"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	g.Disconnect("dev1", false) // 正常断开，会话保留

	c2 := &collector{}
	resumed, err := g.Connect("dev1", "token-a", false, c2.deliver)
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if !resumed {
		t.Fatal("expected resumed=true on reconnect with same token")
	}
	if err := g.Publish("cmd/dev1", []byte("reboot")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := len(c2.all()); got != 1 {
		t.Fatalf("expected 1 message delivered to resumed session, got %d", got)
	}
	t.Log("结果: 重连后无需重新订阅即收到消息, resumed=true ✓")
}

// 业务场景【会话清理】：正常断开保留会话，主动清理丢弃状态。
func TestSessionClean_DiscardsState(t *testing.T) {
	t.Log("业务: 会话清理 | 用例: clean 断开后订阅与暂存被丢弃")
	g := New(Config{AckWindow: 8, OfflineCapacity: 16})
	c := &collector{}
	_, _ = g.Connect("dev1", "token-a", false, c.deliver)
	_ = g.Subscribe("dev1", "cmd/dev1")
	g.Disconnect("dev1", true) // 主动清理

	if g.sessions.Subscribed("dev1", "cmd/dev1") {
		t.Fatal("subscription should be discarded after clean disconnect")
	}
	// 清理后重新接入应视为全新会话。
	resumed, _ := g.Connect("dev1", "token-a", false, c.deliver)
	if resumed {
		t.Fatal("expected resumed=false after clean disconnect")
	}
	t.Log("结果: clean 断开后订阅被清除, 重连 resumed=false ✓")
}

// 业务场景【凭据变更】：换新凭据后旧会话消息不再下发。
func TestSessionCredentialChange_OldMessagesNotDelivered(t *testing.T) {
	t.Log("业务: 凭据变更 | 用例: 新凭据接入后收不到旧会话的离线消息")
	g := New(Config{AckWindow: 8, OfflineCapacity: 16})
	c1 := &collector{}
	_, _ = g.Connect("dev1", "token-a", false, c1.deliver)
	_ = g.Subscribe("dev1", "cmd/dev1")
	g.Disconnect("dev1", false)

	// 离线期间堆积 3 条消息。
	for _, p := range []string{"m1", "m2", "m3"} {
		if err := g.Publish("cmd/dev1", []byte(p)); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if got := g.OfflineLen("dev1"); got != 3 {
		t.Fatalf("expected 3 offline messages, got %d", got)
	}

	// 设备换新凭据接入：旧会话作废，暂存消息不得下发。
	c2 := &collector{}
	resumed, err := g.Connect("dev1", "token-b", false, c2.deliver)
	if err != nil {
		t.Fatalf("reconnect with new token: %v", err)
	}
	if resumed {
		t.Fatal("expected resumed=false after credential change")
	}
	if got := len(c2.all()); got != 0 {
		t.Fatalf("old session messages must not be delivered after credential change, got %d", got)
	}
	if got := g.OfflineLen("dev1"); got != 0 {
		t.Fatalf("offline queue should be cleared after credential change, got %d", got)
	}
	t.Log("结果: 新凭据接入后旧会话 3 条暂存消息被清除且未下发 ✓")
}
