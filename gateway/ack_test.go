package gateway

import (
	"errors"
	"testing"
)

// 业务场景【重复确认】：同一条消息重复确认只生效一次，重传也只消费一次。
func TestAck_DuplicateIsIdempotent(t *testing.T) {
	t.Log("业务: 可靠确认 | 用例: 重复确认幂等, 消息只被消费一次")
	g := New(Config{AckWindow: 8, OfflineCapacity: 16})
	c := &collector{}
	_, _ = g.Connect("dev1", "t", false, c.deliver)
	_ = g.Subscribe("dev1", "cmd/dev1")
	_ = g.Publish("cmd/dev1", []byte("hello"))

	if got := g.InflightLen("dev1"); got != 1 {
		t.Fatalf("expected 1 inflight, got %d", got)
	}
	dup, err := g.Ack("dev1", 1)
	if err != nil || dup {
		t.Fatalf("first ack should succeed, dup=%v err=%v", dup, err)
	}
	if got := g.InflightLen("dev1"); got != 0 {
		t.Fatalf("inflight should be empty after ack, got %d", got)
	}
	// 重复确认：幂等忽略，不报错、不重复计数。
	dup, err = g.Ack("dev1", 1)
	if err != nil || !dup {
		t.Fatalf("duplicate ack should be reported as duplicate, dup=%v err=%v", dup, err)
	}
	t.Log("结果: 首次确认生效, 重复确认识别为 duplicate 且幂等 ✓")
}

// 业务场景【确认窗口】：超出窗口的发布被拒绝，原因可区分。
func TestAck_WindowFullRejected(t *testing.T) {
	t.Log("业务: 可靠确认 | 用例: 超出确认窗口的发布被拒绝(ErrWindowFull)")
	g := New(Config{AckWindow: 2, OfflineCapacity: 16})
	c := &collector{}
	_, _ = g.Connect("dev1", "t", false, c.deliver)
	_ = g.Subscribe("dev1", "cmd/dev1")

	if err := g.Publish("cmd/dev1", []byte("a")); err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	if err := g.Publish("cmd/dev1", []byte("b")); err != nil {
		t.Fatalf("publish 2: %v", err)
	}
	err := g.Publish("cmd/dev1", []byte("c"))
	if !errors.Is(err, ErrWindowFull) {
		t.Fatalf("expected ErrWindowFull, got %v", err)
	}
	// 确认一条后窗口腾出空间，可以继续发布。
	if _, err := g.Ack("dev1", 1); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := g.Publish("cmd/dev1", []byte("c")); err != nil {
		t.Fatalf("publish after ack should succeed: %v", err)
	}
	t.Log("结果: 窗口满时发布被拒绝(ErrWindowFull), 确认后恢复 ✓")
}

// 业务场景【未知序号】：确认不存在的序号被拒绝，原因可区分。
func TestAck_UnknownSeqRejected(t *testing.T) {
	t.Log("业务: 可靠确认 | 用例: 未知序号确认被拒绝(ErrUnknownSeq)")
	g := New(Config{AckWindow: 8, OfflineCapacity: 16})
	c := &collector{}
	_, _ = g.Connect("dev1", "t", false, c.deliver)
	_ = g.Subscribe("dev1", "cmd/dev1")

	_, err := g.Ack("dev1", 99)
	if !errors.Is(err, ErrUnknownSeq) {
		t.Fatalf("expected ErrUnknownSeq, got %v", err)
	}
	t.Log("结果: 未知序号确认被拒绝(ErrUnknownSeq) ✓")
}

// 业务场景【顺序稳定】：单设备消息按序号单调递增到达，重连补发不乱序。
func TestDelivery_OrderStable(t *testing.T) {
	t.Log("业务: 可靠下发 | 用例: 单设备消息顺序稳定(含重连补发)")
	g := New(Config{AckWindow: 8, OfflineCapacity: 16})
	c1 := &collector{}
	_, _ = g.Connect("dev1", "t", false, c1.deliver)
	_ = g.Subscribe("dev1", "cmd/dev1")

	// 在线发 2 条（不确认，留在在途），然后断开。
	_ = g.Publish("cmd/dev1", []byte("online-1"))
	_ = g.Publish("cmd/dev1", []byte("online-2"))
	g.Disconnect("dev1", false)
	// 离线再积 3 条。
	_ = g.Publish("cmd/dev1", []byte("offline-3"))
	_ = g.Publish("cmd/dev1", []byte("offline-4"))
	_ = g.Publish("cmd/dev1", []byte("offline-5"))

	// 重连：应先重发在途 1、2，再补发离线 3、4、5，序号严格递增。
	c2 := &collector{}
	resumed, _ := g.Connect("dev1", "t", false, c2.deliver)
	if !resumed {
		t.Fatal("expected resumed session")
	}
	var last uint64
	for _, m := range c2.all() {
		if m.Seq <= last {
			t.Fatalf("sequence not increasing: got %d after %d", m.Seq, last)
		}
		last = m.Seq
	}
	if last != 5 {
		t.Fatalf("expected to receive up to seq 5, got %d", last)
	}
	t.Logf("结果: 重连后按序收到 1..%d 共 %d 条, 顺序稳定 ✓", last, len(c2.all()))
}
