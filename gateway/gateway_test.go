package gateway

import (
	"path/filepath"
	"testing"
)

func testConfig() Config {
	return Config{AckWindow: 4, Quota: 8, OfflineCap: 3}
}

func mustConnect(t *testing.T, g *Gateway, dev, cred string) {
	t.Helper()
	if _, err := g.Connect(dev, cred); err != nil {
		t.Fatalf("Connect(%s) 失败: %v", dev, err)
	}
}

func mustPublish(t *testing.T, g *Gateway, dev, msgID string) {
	t.Helper()
	if err := g.Publish(dev, msgID, "t/telemetry", []byte(msgID)); err != nil {
		t.Fatalf("Publish(%s,%s) 失败: %v", dev, msgID, err)
	}
}

func mustReceive(t *testing.T, g *Gateway, dev string) []Delivery {
	t.Helper()
	ds, err := g.Receive(dev)
	if err != nil {
		t.Fatalf("Receive(%s) 失败: %v", dev, err)
	}
	return ds
}

func msgIDs(ds []Delivery) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.Message.ID
	}
	return out
}

func assertErrCode(t *testing.T, err error, want ErrCode) {
	t.Helper()
	ge, ok := err.(*Error)
	if !ok {
		t.Fatalf("期望 *Error(%s)，实际: %v", want, err)
	}
	if ge.Code != want {
		t.Fatalf("期望错误码 %s，实际 %s（%v）", want, ge.Code, ge)
	}
	t.Logf("拒绝原因可区分: %s", ge)
}

// ========== 业务一：会话建立、保持与恢复 ==========

func TestSession_ReconnectResume(t *testing.T) {
	t.Log("场景: 设备重连后订阅保留、未确认消息重传、离线暂存补投")
	g := New(testConfig())
	mustConnect(t, g, "dev1", "cred-A")
	if err := g.Subscribe("dev1", "t/telemetry"); err != nil {
		t.Fatal(err)
	}

	// 在线收一条但不确认，随后掉线
	mustPublish(t, g, "dev1", "m1")
	if ds := mustReceive(t, g, "dev1"); len(ds) != 1 {
		t.Fatalf("期望收到 1 条，实际 %d", len(ds))
	}
	g.Disconnect("dev1", false) // 异常掉线

	// 离线期间再堆积两条
	mustPublish(t, g, "dev1", "m2")
	mustPublish(t, g, "dev1", "m3")

	// 重连：订阅保留
	mustConnect(t, g, "dev1", "cred-A")
	subs := g.Subscriptions("dev1")
	if len(subs) != 1 || subs[0] != "t/telemetry" {
		t.Fatalf("重连后订阅丢失: %v", subs)
	}
	t.Logf("重连后订阅保留: %v", subs)

	// 重连后先收到重传的 m1，再收到补投的 m2、m3，顺序稳定
	ds := mustReceive(t, g, "dev1")
	want := []string{"m1", "m2", "m3"}
	got := msgIDs(ds)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("第 %d 条期望 %s，实际 %s（全部: %v）", i, want[i], got[i], got)
		}
	}
	if ds[0].Attempt != 2 {
		t.Fatalf("m1 应为第 2 次投递（重传），实际 Attempt=%d", ds[0].Attempt)
	}
	for i := 1; i < len(ds); i++ {
		if ds[i].Message.Seq <= ds[i-1].Message.Seq {
			t.Fatalf("Seq 未单调递增: %v", got)
		}
	}
	t.Logf("重连恢复投递: %v（m1 为重传 Attempt=%d）", got, ds[0].Attempt)
}

func TestSession_GracefulVsCleared(t *testing.T) {
	t.Log("场景: 正常断开保留订阅，主动清理会话则订阅作废")
	g := New(testConfig())

	// 正常断开：订阅保留
	mustConnect(t, g, "devA", "cred")
	if err := g.Subscribe("devA", "t/a"); err != nil {
		t.Fatal(err)
	}
	g.Disconnect("devA", true)
	mustConnect(t, g, "devA", "cred")
	if got := g.Subscriptions("devA"); len(got) != 1 {
		t.Fatalf("正常断开重连后订阅应保留，实际: %v", got)
	}
	t.Log("正常断开 -> 订阅保留 ✓")

	// 主动清理：订阅作废
	mustConnect(t, g, "devB", "cred")
	if err := g.Subscribe("devB", "t/b"); err != nil {
		t.Fatal(err)
	}
	g.ClearSession("devB")
	if got := g.Subscriptions("devB"); len(got) != 0 {
		t.Fatalf("清理会话后订阅应作废，实际: %v", got)
	}
	t.Log("主动清理 -> 订阅作废 ✓")
}

func TestSession_CredentialChangeInvalidatesOldMessages(t *testing.T) {
	t.Log("场景: 设备显式换新身份（ResetIdentity）后，旧会话下的暂存与未确认消息不再下发")
	g := New(testConfig())
	mustConnect(t, g, "dev1", "cred-OLD")
	if err := g.Subscribe("dev1", "t/old"); err != nil {
		t.Fatal(err)
	}
	mustPublish(t, g, "dev1", "m-old-online") // 在线未确认
	mustReceive(t, g, "dev1")
	g.Disconnect("dev1", false)
	mustPublish(t, g, "dev1", "m-old-offline") // 离线暂存

	// 直接用新凭据 Connect 必须被拒绝，逼调用方显式选择语义
	_, err := g.Connect("dev1", "cred-NEW")
	assertErrCode(t, err, ErrCodeCredentialMismatch)

	// 显式换新身份
	if _, err := g.ResetIdentity("dev1", "cred-NEW"); err != nil {
		t.Fatalf("ResetIdentity 失败: %v", err)
	}
	if got := g.Subscriptions("dev1"); len(got) != 0 {
		t.Fatalf("换新身份后旧订阅应失效，实际: %v", got)
	}
	mustConnect(t, g, "dev1", "cred-NEW")
	if ds := mustReceive(t, g, "dev1"); len(ds) != 0 {
		t.Fatalf("换新身份后不应收到旧会话消息，实际: %v", msgIDs(ds))
	}
	if n := g.OfflineLen("dev1"); n != 0 {
		t.Fatalf("换新身份后旧暂存应作废，实际剩余 %d", n)
	}
	t.Log("旧会话消息与订阅对新身份完全不可见 ✓")
}

// ========== 业务二：消息可靠下发与确认 ==========

func TestDelivery_DuplicateAckConsumedOnce(t *testing.T) {
	t.Log("场景: 重复确认与重传下，同一条消息只被真正消费一次")
	g := New(testConfig())
	mustConnect(t, g, "dev1", "cred")
	mustPublish(t, g, "dev1", "m1")
	mustReceive(t, g, "dev1")

	// 首次确认成功
	if err := g.Ack("dev1", "m1"); err != nil {
		t.Fatalf("首次确认应成功: %v", err)
	}
	// 重复确认：可区分错误，无副作用
	assertErrCode(t, g.Ack("dev1", "m1"), ErrCodeDuplicateAck)
	assertErrCode(t, g.Ack("dev1", "m1"), ErrCodeDuplicateAck)

	// 确认后重连不应再重传
	g.Disconnect("dev1", false)
	mustConnect(t, g, "dev1", "cred")
	if ds := mustReceive(t, g, "dev1"); len(ds) != 0 {
		t.Fatalf("已确认消息不应重传，实际: %v", msgIDs(ds))
	}
	t.Log("已确认消息不重传，恰好消费一次 ✓")
}

func TestDelivery_UnknownAck(t *testing.T) {
	t.Log("场景: 确认不存在的消息返回可区分原因")
	g := New(testConfig())
	mustConnect(t, g, "dev1", "cred")
	assertErrCode(t, g.Ack("dev1", "no-such-msg"), ErrCodeUnknownMessage)
}

func TestDelivery_WindowFullRejected(t *testing.T) {
	t.Log("场景: 超出确认窗口的下发被拒绝，确认后恢复")
	g := New(Config{AckWindow: 2, Quota: 10, OfflineCap: 3})
	mustConnect(t, g, "dev1", "cred")
	mustPublish(t, g, "dev1", "m1")
	mustPublish(t, g, "dev1", "m2")
	assertErrCode(t, g.Publish("dev1", "m3", "t", nil), ErrCodeWindowFull)

	if err := g.Ack("dev1", "m1"); err != nil {
		t.Fatal(err)
	}
	mustPublish(t, g, "dev1", "m3") // 窗口释放后成功
	t.Log("窗口满拒绝 -> 确认释放后恢复 ✓")
}

func TestDelivery_QuotaExceededRejected(t *testing.T) {
	t.Log("场景: 超出配额的下发被拒绝，周期刷新后恢复")
	g := New(Config{AckWindow: 10, Quota: 2, OfflineCap: 3})
	mustConnect(t, g, "dev1", "cred")
	mustPublish(t, g, "dev1", "m1")
	mustPublish(t, g, "dev1", "m2")
	assertErrCode(t, g.Publish("dev1", "m3", "t", nil), ErrCodeQuotaExceeded)

	g.ResetQuota()
	mustPublish(t, g, "dev1", "m3")
	t.Log("配额用尽拒绝 -> 周期刷新后恢复 ✓")
}

func TestDelivery_PerDeviceOrderStable(t *testing.T) {
	t.Log("场景: 单设备消息 Seq 单调递增、投递顺序与下发顺序一致")
	g := New(testConfig())
	mustConnect(t, g, "dev1", "cred")
	for _, id := range []string{"a", "b", "c", "d"} {
		mustPublish(t, g, "dev1", id)
	}
	ds := mustReceive(t, g, "dev1")
	for i := 1; i < len(ds); i++ {
		if ds[i].Message.Seq != ds[i-1].Message.Seq+1 {
			t.Fatalf("Seq 不连续: %+v", msgIDs(ds))
		}
	}
	t.Logf("投递顺序: %v, Seq 连续递增 ✓", msgIDs(ds))
}

// ========== 业务三：离线暂存与过载保护 ==========

func TestOffline_BacklogEvictOldest(t *testing.T) {
	t.Log("场景: 离线暂存达上限时确定性地淘汰最旧消息")
	g := New(Config{AckWindow: 4, Quota: 8, OfflineCap: 2})
	mustConnect(t, g, "dev1", "cred")
	g.Disconnect("dev1", false)

	mustPublish(t, g, "dev1", "m1")
	mustPublish(t, g, "dev1", "m2")
	mustPublish(t, g, "dev1", "m3") // 触发淘汰 m1

	if n := g.OfflineLen("dev1"); n != 2 {
		t.Fatalf("暂存应封顶为 2，实际 %d", n)
	}
	if n := g.OfflineEvicted("dev1"); n != 1 {
		t.Fatalf("应淘汰 1 条，实际 %d", n)
	}
	mustConnect(t, g, "dev1", "cred")
	got := msgIDs(mustReceive(t, g, "dev1"))
	if len(got) != 2 || got[0] != "m2" || got[1] != "m3" {
		t.Fatalf("应补投 m2,m3（最旧的 m1 被淘汰），实际: %v", got)
	}
	t.Logf("淘汰最旧: m1 被丢弃，补投 %v ✓", got)
}

func TestOffline_RestartNoDupNoLoss(t *testing.T) {
	t.Log("场景: 网关重启后暂存不重复下发也不整批丢失")
	dir := t.TempDir()
	path := filepath.Join(dir, "offline.json")

	g1 := New(Config{AckWindow: 4, Quota: 8, OfflineCap: 10, SnapshotPath: path})
	mustConnect(t, g1, "dev1", "cred")
	g1.Disconnect("dev1", false)
	for _, id := range []string{"m1", "m2", "m3"} {
		mustPublish(t, g1, "dev1", id)
	}
	if err := g1.Snapshot(); err != nil {
		t.Fatalf("Snapshot 失败: %v", err)
	}

	// 模拟重启：全新网关实例从快照恢复，重复恢复也不产生重复
	g2 := New(Config{AckWindow: 4, Quota: 8, OfflineCap: 10, SnapshotPath: path})
	if err := g2.Restore(); err != nil {
		t.Fatalf("Restore 失败: %v", err)
	}
	if err := g2.Restore(); err != nil { // 抖动导致重复恢复
		t.Fatalf("重复 Restore 失败: %v", err)
	}
	if n := g2.OfflineLen("dev1"); n != 3 {
		t.Fatalf("恢复后应为 3 条（不重复不丢失），实际 %d", n)
	}

	mustConnect(t, g2, "dev1", "cred")
	got := msgIDs(mustReceive(t, g2, "dev1"))
	if len(got) != 3 || got[0] != "m1" || got[1] != "m2" || got[2] != "m3" {
		t.Fatalf("重启后应按序补投 m1,m2,m3，实际: %v", got)
	}
	if n := g2.OfflineLen("dev1"); n != 0 {
		t.Fatalf("补投后暂存应清空，实际 %d", n)
	}
	t.Logf("重启恢复: %v 完整补投一次 ✓", got)
}

func TestOffline_BlockedDeviceIsolation(t *testing.T) {
	t.Log("场景: 个别设备堵塞（窗口满/暂存满）不影响其他设备收发")
	g := New(Config{AckWindow: 2, Quota: 10, OfflineCap: 1})
	mustConnect(t, g, "blocked", "cred")
	mustConnect(t, g, "normal", "cred")

	// blocked 设备窗口打满
	mustPublish(t, g, "blocked", "b1")
	mustPublish(t, g, "blocked", "b2")
	assertErrCode(t, g.Publish("blocked", "b3", "t", nil), ErrCodeWindowFull)

	// normal 设备收发不受影响
	mustPublish(t, g, "normal", "n1")
	mustPublish(t, g, "normal", "n2")
	got := msgIDs(mustReceive(t, g, "normal"))
	if len(got) != 2 || got[0] != "n1" || got[1] != "n2" {
		t.Fatalf("normal 设备应正常收到 n1,n2，实际: %v", got)
	}
	if err := g.Ack("normal", "n1"); err != nil {
		t.Fatal(err)
	}
	t.Log("堵塞设备与正常设备互不影响 ✓")
}

func TestOffline_ReceiveWhileDisconnected(t *testing.T) {
	t.Log("场景: 离线设备拉取消息被拒绝并给出可区分原因")
	g := New(testConfig())
	mustConnect(t, g, "dev1", "cred")
	g.Disconnect("dev1", true)
	_, err := g.Receive("dev1")
	assertErrCode(t, err, ErrCodeNotConnected)
}
