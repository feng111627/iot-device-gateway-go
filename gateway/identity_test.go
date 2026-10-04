package gateway

import (
	"path/filepath"
	"testing"
)

// 平滑轮换凭据：同一台设备身份不变（Epoch 不变），订阅、未确认下发、
// 离线暂存全部沿用；用新凭据接入即可继续。
func TestIdentity_RotateCredentialKeepsState(t *testing.T) {
	t.Log("场景: 设备平滑换密钥后，订阅/在途/离线积压照旧接着用")
	g := New(testConfig())
	mustConnect(t, g, "dev1", "key-A")
	if err := g.Subscribe("dev1", "t/a"); err != nil {
		t.Fatal(err)
	}
	epochBefore := g.sessions.Get("dev1").Epoch

	mustPublish(t, g, "dev1", "m1") // 在途未确认
	mustReceive(t, g, "dev1")
	g.Disconnect("dev1", false)
	mustPublish(t, g, "dev1", "m2") // 离线积压

	if err := g.RotateCredential("dev1", "key-B"); err != nil {
		t.Fatalf("轮换凭据失败: %v", err)
	}
	if e := g.sessions.Get("dev1").Epoch; e != epochBefore {
		t.Fatalf("轮换凭据不得改变代次，轮换前 %d 轮换后 %d", epochBefore, e)
	}
	if subs := g.Subscriptions("dev1"); len(subs) != 1 || subs[0] != "t/a" {
		t.Fatalf("轮换后订阅丢失: %v", subs)
	}

	// 用新凭据接入，重传 m1 + 补投 m2，一条不少
	mustConnect(t, g, "dev1", "key-B")
	got := msgIDs(mustReceive(t, g, "dev1"))
	if len(got) != 2 || got[0] != "m1" || got[1] != "m2" {
		t.Fatalf("轮换后应继续收到 m1,m2，实际: %v", got)
	}
	t.Logf("平滑轮换 key-A->key-B：代次不变，m1 重传、m2 补投: %v ✓", got)

	// 旧凭据再连不影响身份（业务上应在鉴权层挡掉，这里仅验证网关不崩）
	// 显式再轮换回同一凭据是幂等无副作用的
	if err := g.RotateCredential("dev1", "key-B"); err != nil {
		t.Fatalf("重复轮换为相同凭据应幂等成功: %v", err)
	}
}

// 平滑轮换凭据后跨越重启：身份、在途、积压、已消费记录全部照旧。
func TestIdentity_RotateCredentialSurvivesRestart(t *testing.T) {
	t.Log("场景: 换密钥后网关崩溃，新进程用新凭据仍能接着之前的状态走")
	path := filepath.Join(t.TempDir(), "state.jsonl")
	cfg := durConfig(path)

	g1 := New(cfg)
	mustConnect(t, g1, "dev1", "key-A")
	if err := g1.Subscribe("dev1", "t/a"); err != nil {
		t.Fatal(err)
	}
	mustPublish(t, g1, "dev1", "done")
	mustReceive(t, g1, "dev1")
	if err := g1.Ack("dev1", "done"); err != nil { // 已消费
		t.Fatal(err)
	}
	mustPublish(t, g1, "dev1", "pending") // 在途未确认
	mustReceive(t, g1, "dev1")
	g1.Disconnect("dev1", false)
	mustPublish(t, g1, "dev1", "queued") // 离线
	if err := g1.RotateCredential("dev1", "key-B"); err != nil {
		t.Fatal(err)
	}

	g2 := restartGateway(t, path, cfg)
	mustConnect(t, g2, "dev1", "key-B") // 用新凭据
	if subs := g2.Subscriptions("dev1"); len(subs) != 1 {
		t.Fatalf("重启后订阅应保留: %v", subs)
	}
	got := msgIDs(mustReceive(t, g2, "dev1"))
	if len(got) != 2 || got[0] != "pending" || got[1] != "queued" {
		t.Fatalf("重启后应重传 pending、补投 queued，done 绝不重投；实际: %v", got)
	}
	// done 已消费：重复下发被挡
	assertErrCode(t, g2.Publish("dev1", "done", "t", nil), ErrCodeDuplicateAck)
	t.Logf("换密钥+重启：done 不重投，pending/queued 接续: %v ✓", got)
}

// 全新身份：旧会话痕迹（订阅、在途、积压、已消费）全部清干净，跨重启也不留。
func TestIdentity_ResetIdentityWipesEverythingAcrossRestart(t *testing.T) {
	t.Log("场景: 设备以全新身份接入，旧会话一切作废且重启后仍不可见")
	path := filepath.Join(t.TempDir(), "state.jsonl")
	cfg := durConfig(path)

	g1 := New(cfg)
	mustConnect(t, g1, "dev1", "key-A")
	if err := g1.Subscribe("dev1", "t/old"); err != nil {
		t.Fatal(err)
	}
	mustPublish(t, g1, "dev1", "old-done")
	mustReceive(t, g1, "dev1")
	if err := g1.Ack("dev1", "old-done"); err != nil {
		t.Fatal(err)
	}
	mustPublish(t, g1, "dev1", "old-flight")
	mustReceive(t, g1, "dev1")
	g1.Disconnect("dev1", false)
	mustPublish(t, g1, "dev1", "old-queued")
	epochBefore := g1.sessions.Get("dev1").Epoch

	if err := g1.ResetIdentity("dev1", "key-NEW"); err != nil {
		t.Fatalf("ResetIdentity 失败: %v", err)
	}
	if e := g1.sessions.Get("dev1").Epoch; e != epochBefore+1 {
		t.Fatalf("换新身份代次应递增，期望 %d 实际 %d", epochBefore+1, e)
	}
	if subs := g1.Subscriptions("dev1"); len(subs) != 0 {
		t.Fatalf("换新身份后订阅应清空，实际 %v", subs)
	}
	if n := g1.OfflineLen("dev1"); n != 0 {
		t.Fatalf("换新身份后旧积压应为 0，实际 %d", n)
	}

	// 新身份接入：旧消息一条都收不到
	mustConnect(t, g1, "dev1", "key-NEW")
	if got := msgIDs(mustReceive(t, g1, "dev1")); len(got) != 0 {
		t.Fatalf("新身份不应收到旧会话消息，实际 %v", got)
	}
	// 旧消息 ID 可被新身份重新使用（旧去重记录已清）
	mustPublish(t, g1, "dev1", "old-done")
	if got := msgIDs(mustReceive(t, g1, "dev1")); len(got) != 1 || got[0] != "old-done" {
		t.Fatalf("旧消息 ID 对新身份应是全新消息，实际 %v", got)
	}
	if err := g1.Ack("dev1", "old-done"); err != nil {
		t.Fatalf("新身份确认消息失败: %v", err)
	}
	t.Log("同进程内：旧订阅/在途/积压/去重全部清除，旧消息 ID 可复用 ✓")

	// 重启后：旧痕迹仍然不存在
	g2 := restartGateway(t, path, cfg)
	mustConnect(t, g2, "dev1", "key-NEW")
	if got := msgIDs(mustReceive(t, g2, "dev1")); len(got) != 0 {
		t.Fatalf("重启后新身份仍不应收到旧会话消息，实际 %v", got)
	}
	if n := g2.OfflineLen("dev1"); n != 0 {
		t.Fatalf("重启后旧积压不应复活，实际 %d", n)
	}
	if subs := g2.Subscriptions("dev1"); len(subs) != 0 {
		t.Fatalf("重启后旧订阅不应复活，实际 %v", subs)
	}
	t.Log("重启后旧会话痕迹依旧清空 ✓")
}

// 同一条物理设备：先平滑轮换（保留），再换新身份（清空），两种语义清楚可分。
func TestIdentity_RotateThenResetAreDistinct(t *testing.T) {
	t.Log("场景: 轮换保留状态，随后换新身份彻底切断，两步语义互不混淆")
	g := New(testConfig())
	mustConnect(t, g, "dev1", "k1")
	mustPublish(t, g, "dev1", "keep")
	mustReceive(t, g, "dev1")
	g.Disconnect("dev1", false)

	if err := g.RotateCredential("dev1", "k2"); err != nil {
		t.Fatal(err)
	}
	mustConnect(t, g, "dev1", "k2")
	if got := msgIDs(mustReceive(t, g, "dev1")); len(got) != 1 || got[0] != "keep" {
		t.Fatalf("轮换后 keep 应保留，实际 %v", got)
	}
	t.Log("轮换阶段：keep 保留 ✓")

	if err := g.ResetIdentity("dev1", "k3"); err != nil {
		t.Fatal(err)
	}
	mustConnect(t, g, "dev1", "k3")
	if got := msgIDs(mustReceive(t, g, "dev1")); len(got) != 0 {
		t.Fatalf("换新身份后 keep 必须消失，实际 %v", got)
	}
	t.Log("换新身份阶段：keep 被彻底清除 ✓")
}

// 兼容旧行为：不经过 RotateCredential，直接用新凭据 Connect 仍按"换新身份"处理。
func TestIdentity_DirectConnectWithNewCredentialStillResets(t *testing.T) {
	t.Log("场景: 直接换凭据接入（未显式轮换）仍等同换新身份，且跨重启生效")
	path := filepath.Join(t.TempDir(), "state.jsonl")
	cfg := durConfig(path)

	g1 := New(cfg)
	mustConnect(t, g1, "dev1", "key-A")
	if err := g1.Subscribe("dev1", "t/a"); err != nil {
		t.Fatal(err)
	}
	mustPublish(t, g1, "dev1", "secret")
	mustReceive(t, g1, "dev1")
	g1.Disconnect("dev1", false)
	mustPublish(t, g1, "dev1", "secret-offline")

	mustConnect(t, g1, "dev1", "key-B") // 直接换凭据 = 新身份
	if got := msgIDs(mustReceive(t, g1, "dev1")); len(got) != 0 {
		t.Fatalf("直接换新凭据应清空旧消息，实际 %v", got)
	}
	if subs := g1.Subscriptions("dev1"); len(subs) != 0 {
		t.Fatalf("直接换新凭据应清空旧订阅，实际 %v", subs)
	}

	g2 := restartGateway(t, path, cfg)
	mustConnect(t, g2, "dev1", "key-B")
	if got := msgIDs(mustReceive(t, g2, "dev1")); len(got) != 0 {
		t.Fatalf("重启后旧会话消息仍不应复活，实际 %v", got)
	}
	t.Log("直接换新凭据=新身份的旧行为保持兼容，且重启后一致 ✓")
}
