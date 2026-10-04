package gateway

import (
	"path/filepath"
	"testing"
)

func durConfig(path string) Config {
	return Config{AckWindow: 8, Quota: 64, OfflineCap: 32, SnapshotPath: path}
}

func restartGateway(t *testing.T, path string, cfg Config) *Gateway {
	t.Helper()
	g := New(cfg)
	rep, err := g.RestoreWithReport()
	if err != nil {
		t.Fatalf("Restore 失败: %v", err)
	}
	t.Logf("重启恢复: %s", rep)
	return g
}

// ========== 跨重启：不重不丢 ==========

// 已确认（已消费）的消息，即使跨越网关重启，也绝不能再次下发给设备。
func TestRestart_AckedNeverRedelivered(t *testing.T) {
	t.Log("场景: 消息确认后网关崩溃重启，新进程不得再次投递该消息")
	path := filepath.Join(t.TempDir(), "state.jsonl")
	cfg := durConfig(path)

	g1 := New(cfg)
	mustConnect(t, g1, "dev1", "cred")
	mustPublish(t, g1, "dev1", "m1")
	mustReceive(t, g1, "dev1")
	if err := g1.Ack("dev1", "m1"); err != nil {
		t.Fatalf("Ack 失败: %v", err)
	}
	// 再来一条未确认的，作为对照
	mustPublish(t, g1, "dev1", "m2")
	mustReceive(t, g1, "dev1")
	// 不调用 Snapshot：模拟强杀，状态依赖每次变更后的同步落盘

	g2 := restartGateway(t, path, cfg)
	mustConnect(t, g2, "dev1", "cred")
	got := msgIDs(mustReceive(t, g2, "dev1"))
	if len(got) != 1 || got[0] != "m2" {
		t.Fatalf("重启后只能重传未确认的 m2，实际: %v", got)
	}
	t.Logf("已确认的 m1 跨重启未再投递，未确认的 m2 重传: %v ✓", got)

	// 对 m1 的迟到 Ack 在新进程仍被识别为重复/未知，不会误判
	if err := g2.Ack("dev1", "m1"); err == nil {
		t.Fatal("对已消费的 m1 再次确认不应成功")
	} else {
		t.Logf("对已消费 m1 的确认被挡下: %v ✓", err)
	}
}

// 已发出但未确认的消息，跨重启后必须按原先后顺序补回，一条不少。
func TestRestart_InflightRetransmitInOrder(t *testing.T) {
	t.Log("场景: 多条在途未确认消息随崩溃恢复，按原 Seq 顺序重传且无丢失")
	path := filepath.Join(t.TempDir(), "state.jsonl")
	cfg := durConfig(path)

	g1 := New(cfg)
	mustConnect(t, g1, "dev1", "cred")
	for _, id := range []string{"a", "b", "c"} {
		mustPublish(t, g1, "dev1", id)
	}
	before := mustReceive(t, g1, "dev1")
	if len(before) != 3 {
		t.Fatalf("期望首次收到 3 条，实际 %d", len(before))
	}
	seqs := map[string]uint64{}
	for _, d := range before {
		seqs[d.Message.ID] = d.Message.Seq
	}
	// 一条都不确认，网关崩溃

	g2 := restartGateway(t, path, cfg)
	mustConnect(t, g2, "dev1", "cred")
	after := mustReceive(t, g2, "dev1")
	got := msgIDs(after)
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("重启后应按序重传 a,b,c，实际: %v", got)
	}
	for i := 1; i < len(after); i++ {
		if after[i].Message.Seq < after[i-1].Message.Seq {
			t.Fatalf("重传 Seq 未保持原顺序: %v", got)
		}
		if after[i].Message.Seq != seqs[got[i]] {
			t.Fatalf("%s 的 Seq 跨重启发生变化", got[i])
		}
		if after[i].Attempt <= 1 {
			t.Fatalf("%s 应为再次投递（Attempt>1）", got[i])
		}
	}
	t.Logf("在途 a,b,c 跨重启按原序重传，Attempt: a=%d b=%d c=%d ✓",
		after[0].Attempt, after[1].Attempt, after[2].Attempt)

	// 重启后确认，再重启一次，仍不得重投
	if err := g2.Ack("dev1", "b"); err != nil {
		t.Fatalf("Ack(b) 失败: %v", err)
	}
	g3 := restartGateway(t, path, cfg)
	mustConnect(t, g3, "dev1", "cred")
	got3 := msgIDs(mustReceive(t, g3, "dev1"))
	if len(got3) != 2 || got3[0] != "a" || got3[1] != "c" {
		t.Fatalf("第二次重启后应只剩 a,c，实际: %v", got3)
	}
	t.Logf("二次重启：已确认的 b 消失，a,c 继续重传: %v ✓", got3)
}

// 离线期间攒下的消息，跨重启后不能多也不能少，补投后暂存清空。
func TestRestart_OfflineBacklogStable(t *testing.T) {
	t.Log("场景: 离线积压跨重启不凭空增加也不整批丢失，顺序保持")
	path := filepath.Join(t.TempDir(), "state.jsonl")
	cfg := durConfig(path)

	g1 := New(cfg)
	mustConnect(t, g1, "dev1", "cred")
	g1.Disconnect("dev1", false)
	for _, id := range []string{"o1", "o2", "o3", "o4"} {
		mustPublish(t, g1, "dev1", id)
	}
	// 崩溃，不做额外 Snapshot；再连续两个进程周期都不接入，积压应原样存活
	g2 := restartGateway(t, path, cfg)
	if n := g2.OfflineLen("dev1"); n != 4 {
		t.Fatalf("第一次重启后积压应为 4，实际 %d", n)
	}
	g3 := restartGateway(t, path, cfg)
	mustConnect(t, g3, "dev1", "cred")
	got := msgIDs(mustReceive(t, g3, "dev1"))
	want := []string{"o1", "o2", "o3", "o4"}
	if len(got) != 4 {
		t.Fatalf("应补投 4 条，实际 %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("顺序错乱，期望 %v 实际 %v", want, got)
		}
	}
	if n := g3.OfflineLen("dev1"); n != 0 {
		t.Fatalf("补投后暂存应清空，实际 %d", n)
	}
	t.Logf("离线积压跨多个重启周期完整按序补投: %v ✓", got)
}

// 恢复动作本身可反复执行：正常重复恢复、以及"恢复到一半又被中断"后接着恢复，
// 都不得产生重复或损坏数据。
func TestRestart_RepeatAndInterruptedRestoreIdempotent(t *testing.T) {
	t.Log("场景: 对同一快照多次恢复、模拟恢复中断后再恢复，结果幂等")
	path := filepath.Join(t.TempDir(), "state.jsonl")
	cfg := durConfig(path)

	g1 := New(cfg)
	mustConnect(t, g1, "dev1", "cred")
	if err := g1.Subscribe("dev1", "t/a"); err != nil {
		t.Fatal(err)
	}
	mustPublish(t, g1, "dev1", "x1")
	mustReceive(t, g1, "dev1")
	if err := g1.Ack("dev1", "x1"); err != nil {
		t.Fatal(err)
	}
	g1.Disconnect("dev1", false)
	mustPublish(t, g1, "dev1", "x2") // 离线 1 条

	// 进程 2：连续恢复 3 次（模拟抖动重试），再"恢复到一半被杀"：只恢复不接入即退出
	g2 := New(cfg)
	for i := 0; i < 3; i++ {
		rep, err := g2.RestoreWithReport()
		if err != nil {
			t.Fatalf("第 %d 次恢复失败: %v", i+1, err)
		}
		if i == 0 {
			t.Logf("首次恢复: %s", rep)
			if rep.DevicesRestored != 1 || rep.MessagesRestored() != 1 ||
				rep.InflightRestored != 0 || rep.OfflineRestored != 1 || rep.ConsumedRestored != 1 {
				t.Fatalf("首次恢复计量不符: %+v", rep)
			}
		} else if rep.MessagesRestored() != 0 || rep.DevicesRestored != 0 || rep.ConsumedRestored != 0 {
			t.Fatalf("重复恢复应全部幂等命中，实际新增恢复: %+v", rep)
		}
	}
	if subs := g2.Subscriptions("dev1"); len(subs) != 1 || subs[0] != "t/a" {
		t.Fatalf("订阅应跨重启保留: %v", subs)
	}

	// 进程 3：从中断点继续恢复，数据仍然只有一份
	g3 := restartGateway(t, path, cfg)
	mustConnect(t, g3, "dev1", "cred")
	got := msgIDs(mustReceive(t, g3, "dev1"))
	if len(got) != 1 || got[0] != "x2" {
		t.Fatalf("多次+中断恢复后只应补投 x2，实际: %v", got)
	}
	// x1 已消费，任何时刻都不得重投
	if err := g3.Ack("dev1", "x1"); err == nil {
		t.Fatal("已消费的 x1 不应再次确认成功")
	}
	t.Logf("重复/中断恢复幂等：只补投 x2，x1 永不重投 ✓")
}

// 同一条消息从下发到消费跨越多次重启，只被真正消费一次。
func TestRestart_EndToEndConsumeOnceAcrossRestarts(t *testing.T) {
	t.Log("场景: 下发->崩溃->重传->部分确认->再崩溃，全程恰好消费一次")
	path := filepath.Join(t.TempDir(), "state.jsonl")
	cfg := durConfig(path)

	g1 := New(cfg)
	mustConnect(t, g1, "dev1", "cred")
	mustPublish(t, g1, "dev1", "k1")
	mustReceive(t, g1, "dev1") // 设备收到但没确认，网关崩溃

	g2 := restartGateway(t, path, cfg)
	mustConnect(t, g2, "dev1", "cred")
	if got := msgIDs(mustReceive(t, g2, "dev1")); len(got) != 1 || got[0] != "k1" {
		t.Fatalf("首次崩溃后应重传 k1，实际 %v", got)
	}
	if err := g2.Ack("dev1", "k1"); err != nil {
		t.Fatalf("确认 k1 失败: %v", err)
	} // 确认落盘后再次崩溃

	g3 := restartGateway(t, path, cfg)
	mustConnect(t, g3, "dev1", "cred")
	if got := msgIDs(mustReceive(t, g3, "dev1")); len(got) != 0 {
		t.Fatalf("确认后再次崩溃也不得重传，实际 %v", got)
	}
	// 同 ID 重新下发应被拒绝（已消费去重跨重启生效）
	err := g3.Publish("dev1", "k1", "t", nil)
	assertErrCode(t, err, ErrCodeDuplicateAck)
	t.Log("k1 跨两次重启、一次重传，最终恰好消费一次 ✓")
}

// 已恢复但尚未重连的设备（只有会话+离线数据、还没有在途窗口），
// 不能因为其他设备的操作触发落盘而从快照里丢失。
func TestRestart_RestoredButNotReconnectedDeviceSurvivesResnapshot(t *testing.T) {
	t.Log("场景: A 恢复后一直不重连，B 的操作触发重写快照，再次重启 A 的积压仍在")
	path := filepath.Join(t.TempDir(), "state.jsonl")
	cfg := durConfig(path)

	g1 := New(cfg)
	mustConnect(t, g1, "A", "cred")
	if err := g1.Subscribe("A", "t/a"); err != nil {
		t.Fatal(err)
	}
	g1.Disconnect("A", false)
	mustPublish(t, g1, "A", "a-only") // A 的离线积压

	// g2 恢复 A，但 A 始终不重连；B 接入并产生若干次落盘
	g2 := New(cfg)
	if _, err := g2.RestoreWithReport(); err != nil {
		t.Fatal(err)
	}
	mustConnect(t, g2, "B", "cred")
	for _, id := range []string{"b1", "b2", "b3"} {
		mustPublish(t, g2, "B", id)
	}
	if err := g2.Ack("B", "b1"); err != nil {
		t.Fatal(err)
	}
	if n := g2.OfflineLen("A"); n != 1 {
		t.Fatalf("A 的积压在 g2 内应仍为 1，实际 %d", n)
	}

	// g3 再重启：A 的会话、订阅、积压必须都还在
	g3 := New(cfg)
	rep, err := g3.RestoreWithReport()
	if err != nil {
		t.Fatal(err)
	}
	if rep.OfflineRestored != 1 {
		t.Fatalf("A 的积压应跨第二次重启保留，报告: %s", rep)
	}
	if subs := g3.Subscriptions("A"); len(subs) != 1 {
		t.Fatalf("A 的订阅应保留，实际 %v", subs)
	}
	mustConnect(t, g3, "A", "cred")
	if got := msgIDs(mustReceive(t, g3, "A")); len(got) != 1 || got[0] != "a-only" {
		t.Fatalf("A 重连后应补投 a-only，实际 %v", got)
	}
	t.Log("仅恢复未重连的设备在他人触发的重写快照后仍完整 ✓")
}
