package gateway

import (
	"fmt"
	"testing"
)

// ========== 凭据平滑轮换 vs 换新身份 ==========

func TestCredential_RotateKeepsEverythingAcrossRestart(t *testing.T) {
	t.Log("场景: 平滑换凭据后订阅/在途/离线积压/已消费记录全部照旧，跨重启也一样")
	cfg, _ := durableConfig(t, 4)
	g := New(cfg)
	mustConnect(t, g, "dev1", "key-A")
	if err := g.Subscribe("dev1", "t/telemetry"); err != nil {
		t.Fatal(err)
	}
	// m1 在线已消费；m2 在线未确认（在途）
	mustPublish(t, g, "dev1", "m1")
	mustPublish(t, g, "dev1", "m2")
	mustReceive(t, g, "dev1")
	if err := g.Ack("dev1", "m1"); err != nil {
		t.Fatal(err)
	}
	// 离线期间积压 m3
	g.Disconnect("dev1", false)
	mustPublish(t, g, "dev1", "m3")

	// 平滑轮换凭据（设备可在离线时由管理面发起）
	if err := g.RotateCredential("dev1", "key-B"); err != nil {
		t.Fatalf("RotateCredential 失败: %v", err)
	}

	// 用旧凭据接入必须被拒绝
	_, err := g.Connect("dev1", "key-A")
	assertErrCode(t, err, ErrCodeCredentialMismatch)

	// 重启后用新凭据接入：状态原样接上
	g = restart(t, cfg)
	if got := g.Subscriptions("dev1"); fmt.Sprint(got) != "[t/telemetry]" {
		t.Fatalf("轮换后订阅应保留，实际 %v", got)
	}
	mustConnect(t, g, "dev1", "key-B")
	got := msgIDs(mustReceive(t, g, "dev1"))
	want := []string{"m2", "m3"} // 在途重传 + 离线补投；m1 已消费绝不重投
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("轮换后期望 %v，实际 %v", want, got)
	}
	// m1 的已消费记录仍在：重复确认仍被识别
	assertErrCode(t, g.Ack("dev1", "m1"), ErrCodeDuplicateAck)
	// m2/m3 可以正常确认
	for _, id := range []string{"m2", "m3"} {
		if err := g.Ack("dev1", id); err != nil {
			t.Fatalf("确认 %s 失败: %v", id, err)
		}
	}
	// 再重启一次：没有任何残留投递
	g = restart(t, cfg)
	mustConnect(t, g, "dev1", "key-B")
	if ds := mustReceive(t, g, "dev1"); len(ds) != 0 {
		t.Fatalf("轮换并全部确认后不应有残留投递: %v", msgIDs(ds))
	}
	if n := g.OfflineLen("dev1"); n != 0 {
		t.Fatalf("暂存应为 0，实际 %d", n)
	}
	t.Log("平滑轮换：订阅/在途/积压/已消费记录全部接续 ✓")
}

func TestCredential_ResetIdentityWipesEverythingAcrossRestart(t *testing.T) {
	t.Log("场景: 换新身份后旧会话痕迹清干净，跨重启新身份也收不到任何旧消息")
	cfg, _ := durableConfig(t, 3)
	g := New(cfg)
	mustConnect(t, g, "dev1", "key-A")
	if err := g.Subscribe("dev1", "t/old"); err != nil {
		t.Fatal(err)
	}
	mustPublish(t, g, "dev1", "m1")
	mustPublish(t, g, "dev1", "m2")
	mustReceive(t, g, "dev1")
	g.Ack("dev1", "m1")
	g.Disconnect("dev1", false)
	mustPublish(t, g, "dev1", "m3")

	// 显式换新身份
	sess, err := g.ResetIdentity("dev1", "key-B")
	if err != nil {
		t.Fatalf("ResetIdentity 失败: %v", err)
	}
	if sess.Epoch != 2 {
		t.Fatalf("换新身份后 Epoch 应为 2，实际 %d", sess.Epoch)
	}
	if got := g.Subscriptions("dev1"); len(got) != 0 {
		t.Fatalf("旧订阅应清空，实际 %v", got)
	}
	mustConnect(t, g, "dev1", "key-B")
	if ds := mustReceive(t, g, "dev1"); len(ds) != 0 {
		t.Fatalf("新身份不应收到旧消息: %v", msgIDs(ds))
	}
	// 旧的已消费记录也清掉了：m1 对新身份是"未知消息"而不是"重复确认"
	assertErrCode(t, g.Ack("dev1", "m1"), ErrCodeUnknownMessage)

	// 新身份产生自己的消息
	if err := g.Subscribe("dev1", "t/new"); err != nil {
		t.Fatal(err)
	}
	mustPublish(t, g, "dev1", "n1")
	if got := msgIDs(mustReceive(t, g, "dev1")); fmt.Sprint(got) != "[n1]" {
		t.Fatalf("新身份应收 n1，实际 %v", got)
	}

	// 重启：旧消息依旧不可见，新身份状态接续
	g = restart(t, cfg)
	if got := g.Subscriptions("dev1"); fmt.Sprint(got) != "[t/new]" {
		t.Fatalf("重启后新身份订阅应为 [t/new]，实际 %v", got)
	}
	mustConnect(t, g, "dev1", "key-B")
	// n1 在途：重启后 Connect 即按重传补回（Attempt 接续），但绝不会出现 m1/m2/m3
	if ds := mustReceive(t, g, "dev1"); fmt.Sprint(msgIDs(ds)) != "[n1]" {
		t.Fatalf("新身份的在途 n1 应照常重传，旧消息不得出现，实际 %v", msgIDs(ds))
	}
	if n := g.OfflineLen("dev1"); n != 0 {
		t.Fatalf("旧积压不应残留，实际 %d", n)
	}
	t.Log("换新身份：旧痕迹跨重启彻底不可见，新身份状态独立接续 ✓")
}

func TestCredential_RotateThenResetBoundaries(t *testing.T) {
	t.Log("场景: 先轮换再换新身份，两种语义边界清楚且都持久化")
	cfg, _ := durableConfig(t, 2)
	g := New(cfg)
	mustConnect(t, g, "dev1", "k1")
	mustPublish(t, g, "dev1", "old-1")
	mustReceive(t, g, "dev1")

	// 轮换 k1->k2：消息仍在途
	if err := g.RotateCredential("dev1", "k2"); err != nil {
		t.Fatal(err)
	}
	// 再换新身份 k2->k3：旧在途作废
	if _, err := g.ResetIdentity("dev1", "k3"); err != nil {
		t.Fatal(err)
	}
	// k1/k2 都不能连
	for _, old := range []string{"k1", "k2"} {
		_, err := g.Connect("dev1", old)
		assertErrCode(t, err, ErrCodeCredentialMismatch)
	}
	mustConnect(t, g, "dev1", "k3")
	if ds := mustReceive(t, g, "dev1"); len(ds) != 0 {
		t.Fatalf("换新身份后旧在途不得下发: %v", msgIDs(ds))
	}
	// 重启后边界依旧
	g = restart(t, cfg)
	for _, old := range []string{"k1", "k2"} {
		_, err := g.Connect("dev1", old)
		assertErrCode(t, err, ErrCodeCredentialMismatch)
	}
	if s := g.sessions.Get("dev1"); s == nil || s.Epoch != 2 || s.Credential != "k3" {
		t.Fatalf("重启后期望 Epoch=2/k3，实际 %+v", s)
	}
	t.Log("轮换与换身份顺序发生，凭据/代次边界跨重启保持 ✓")
}
