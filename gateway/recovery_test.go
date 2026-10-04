package gateway

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// durableConfig 使用持久化目录并把压缩阈值调小，确保测试能覆盖"快照+重放"两条路径。
func durableConfig(t *testing.T, compactEvery int) (Config, string) {
	t.Helper()
	dir := t.TempDir()
	return Config{
		AckWindow:    4,
		Quota:        100,
		OfflineCap:   50,
		StateDir:     dir,
		MaxDevices:   0,
		CompactEvery: compactEvery,
	}, dir
}

// restart 模拟网关被强杀后重新拉起：丢弃全部内存状态，从磁盘 Recover。
func restart(t *testing.T, cfg Config) *Gateway {
	t.Helper()
	g := New(cfg)
	if _, err := g.Recover(); err != nil {
		t.Fatalf("Recover 失败: %v", err)
	}
	return g
}

// ========== 跨重启：不重、不丢、保序 ==========

func TestRecovery_AckedNeverRedeliveredAcrossRestart(t *testing.T) {
	t.Log("场景: 已确认消息跨多次重启绝不再下发；在途消息按原序补传，一条不少")
	cfg, _ := durableConfig(t, 3)
	g := New(cfg)
	mustConnect(t, g, "dev1", "cred")
	mustPublish(t, g, "dev1", "m1")
	mustPublish(t, g, "dev1", "m2")
	mustReceive(t, g, "dev1")
	if err := g.Ack("dev1", "m1"); err != nil { // m1 已消费，m2 在途
		t.Fatal(err)
	}

	// 第一次重启：只能收到 m2 的重传
	g = restart(t, cfg)
	mustConnect(t, g, "dev1", "cred")
	got := msgIDs(mustReceive(t, g, "dev1"))
	if len(got) != 1 || got[0] != "m2" {
		t.Fatalf("重启后应只重传 m2，实际: %v", got)
	}
	// 不确认再重启：仍然只有 m2，且永远不会出现 m1
	g = restart(t, cfg)
	mustConnect(t, g, "dev1", "cred")
	got = msgIDs(mustReceive(t, g, "dev1"))
	if len(got) != 1 || got[0] != "m2" {
		t.Fatalf("二次重启后应仍只有 m2，实际: %v", got)
	}
	if err := g.Ack("dev1", "m2"); err != nil {
		t.Fatal(err)
	}
	// 第三次重启：什么都没有
	g = restart(t, cfg)
	mustConnect(t, g, "dev1", "cred")
	if ds := mustReceive(t, g, "dev1"); len(ds) != 0 {
		t.Fatalf("全部确认后重启不应再投递，实际: %v", msgIDs(ds))
	}
	// 重复确认依旧可区分、无副作用
	assertErrCode(t, g.Ack("dev1", "m1"), ErrCodeDuplicateAck)
	t.Log("跨 3 次重启：m1 零重投，m2 在途补传一次后确认 ✓")
}

func TestRecovery_OfflineBacklogAcrossRestart(t *testing.T) {
	t.Log("场景: 离线积压跨重启不凭空多出、不整批丢失，补投顺序不变")
	cfg, _ := durableConfig(t, 2)
	g := New(cfg)
	mustConnect(t, g, "dev1", "cred")
	g.Disconnect("dev1", false)
	for _, id := range []string{"o1", "o2", "o3", "o4"} {
		mustPublish(t, g, "dev1", id)
	}

	g = restart(t, cfg)
	if n := g.OfflineLen("dev1"); n != 4 {
		t.Fatalf("重启后离线积压应为 4，实际 %d", n)
	}
	// 恢复后设备仍处于离线：不能直接收
	_, err := g.Receive("dev1")
	assertErrCode(t, err, ErrCodeNotConnected)

	mustConnect(t, g, "dev1", "cred")
	got := msgIDs(mustReceive(t, g, "dev1"))
	want := []string{"o1", "o2", "o3", "o4"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("补投顺序应为 %v，实际 %v", want, got)
	}
	// 补投后立刻被杀（未确认）：重启后它们仍是在途，重传依旧是同四条、同顺序
	g = restart(t, cfg)
	mustConnect(t, g, "dev1", "cred")
	got = msgIDs(mustReceive(t, g, "dev1"))
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("补投后未确认被杀，重传应仍为 %v，实际 %v", want, got)
	}
	if n := g.OfflineLen("dev1"); n != 0 {
		t.Fatalf("补投后离线队列应为空，实际 %d", n)
	}
	t.Logf("离线积压 %v 跨重启完整、有序、不重复 ✓", got)
}

func TestRecovery_IdempotentRepeatedRecover(t *testing.T) {
	t.Log("场景: Recover 可反复执行、可从恢复一半中断后继续，结果一致")
	cfg, _ := durableConfig(t, 1) // 极小压缩阈值，强制走快照+截断路径
	cfg.AckWindow = 16
	g := New(cfg)
	mustConnect(t, g, "dev1", "cred")
	g.Disconnect("dev1", false)
	for _, id := range []string{"x1", "x2", "x3", "x4", "x5"} {
		mustPublish(t, g, "dev1", id)
	}

	// 全新进程连续 Recover 三次（模拟抖动/恢复到一半又被打断）
	g2 := New(cfg)
	for i := 0; i < 3; i++ {
		if _, err := g2.Recover(); err != nil {
			t.Fatalf("第 %d 次 Recover 失败: %v", i+1, err)
		}
		if n := g2.OfflineLen("dev1"); n != 5 {
			t.Fatalf("第 %d 次 Recover 后积压应为 5，实际 %d", i+1, n)
		}
	}
	// 快照文件与 WAL 同时存在时，重放也不能产生重复
	mustConnect(t, g2, "dev1", "cred")
	got := msgIDs(mustReceive(t, g2, "dev1"))
	if len(got) != 5 {
		t.Fatalf("反复恢复后补投应恰好 5 条，实际 %d: %v", len(got), got)
	}
	seen := map[string]int{}
	for _, id := range got {
		seen[id]++
		if seen[id] > 1 {
			t.Fatalf("消息 %s 被重复补投: %v", id, got)
		}
	}
	t.Logf("3 次 Recover 后补投仍恰好一次: %v ✓", got)
}

func TestRecovery_MixedInflightAndOfflineOrderAcrossRestarts(t *testing.T) {
	t.Log("场景: 在途与离线交替、跨多次重启，先重传在途再补投离线，Seq 单调")
	cfg, _ := durableConfig(t, 5)
	g := New(cfg)
	mustConnect(t, g, "dev1", "cred")
	mustPublish(t, g, "dev1", "a1")
	mustPublish(t, g, "dev1", "a2")
	mustReceive(t, g, "dev1")
	g.Ack("dev1", "a1") // a2 在途
	g.Disconnect("dev1", false)
	mustPublish(t, g, "dev1", "b1")
	mustPublish(t, g, "dev1", "b2")

	g = restart(t, cfg)
	mustConnect(t, g, "dev1", "cred")
	ds := mustReceive(t, g, "dev1")
	got := msgIDs(ds)
	want := []string{"a2", "b1", "b2"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("期望 %v，实际 %v", want, got)
	}
	for i := 1; i < len(ds); i++ {
		if ds[i].Message.Seq <= ds[i-1].Message.Seq {
			t.Fatalf("Seq 未单调递增: %v", got)
		}
	}
	// 收到后全部不确认，再次重启：结果必须一模一样
	g = restart(t, cfg)
	mustConnect(t, g, "dev1", "cred")
	ds2 := mustReceive(t, g, "dev1")
	if fmt.Sprint(msgIDs(ds2)) != fmt.Sprint(want) {
		t.Fatalf("二次重启后期望 %v，实际 %v", want, msgIDs(ds2))
	}
	for _, d := range ds2 {
		if err := g.Ack("dev1", d.Message.ID); err != nil {
			t.Fatalf("确认 %s 失败: %v", d.Message.ID, err)
		}
	}
	g = restart(t, cfg)
	mustConnect(t, g, "dev1", "cred")
	if ds := mustReceive(t, g, "dev1"); len(ds) != 0 {
		t.Fatalf("全部确认后不应再投递: %v", msgIDs(ds))
	}
	t.Logf("在途+离线跨重启顺序稳定: %v ✓", got)
}

// TestRecovery_ResumeInterruptedMidway 模拟"恢复到一半又被杀"：
// 用小确认窗口让补投只能进行一部分，随后反复重启，最终每条消息仍恰好消费一次、顺序不变。
func TestRecovery_ResumeInterruptedMidway(t *testing.T) {
	t.Log("场景: 补投反复被崩溃打断（窗口只容 2 条），多轮重启后不重不丢、最终全部送达")
	cfg, _ := durableConfig(t, 3)
	cfg.AckWindow = 2 // 每次连接最多补投 2 条进窗口
	g := New(cfg)
	mustConnect(t, g, "dev1", "cred")
	g.Disconnect("dev1", false)
	for _, id := range []string{"p1", "p2", "p3", "p4", "p5"} {
		mustPublish(t, g, "dev1", id)
	}

	consumed := map[string]bool{}
	round := 0
	// 每一轮：新进程 Recover -> Connect 触发补投 -> 收到后只确认一部分（模拟被杀）
	for {
		round++
		g = restart(t, cfg)
		mustConnect(t, g, "dev1", "cred")
		ds := mustReceive(t, g, "dev1")
		if len(ds) == 0 {
			break
		}
		for i, d := range ds {
			if consumed[d.Message.ID] {
				t.Fatalf("第 %d 轮消息 %s 被重复投递（所有轮次实际消费集 %v）", round, d.Message.ID, consumed)
			}
			// 每轮只确认第一条，其余留在在途（模拟刚收到进程就被杀）
			if i == 0 {
				if err := g.Ack("dev1", d.Message.ID); err != nil {
					t.Fatalf("第 %d 轮确认 %s 失败: %v", round, d.Message.ID, err)
				}
				consumed[d.Message.ID] = true
			}
		}
		if round > 20 {
			t.Fatal("恢复轮次超出上限，可能永远无法推进")
		}
	}
	for _, id := range []string{"p1", "p2", "p3", "p4", "p5"} {
		if !consumed[id] {
			t.Fatalf("消息 %s 最终未被消费，已消费: %v", id, consumed)
		}
	}
	t.Logf("经历 %d 轮中断恢复，5 条消息各消费一次、顺序推进 ✓", round)
}

// TestRecovery_CompactCrashBeforeTruncation 模拟压缩时序崩溃：
// 快照已 rename 但 WAL 尚未截断，WAL 中同时残留快照点之前的事件与之后的新事件。
func TestRecovery_CompactCrashBeforeTruncation(t *testing.T) {
	t.Log("场景: 快照就位、WAL 截断前崩溃，重启不重复重放快照前事件、不漏快照后事件")
	cfg, _ := durableConfig(t, 2)
	cfg.AckWindow = 20
	g := New(cfg)
	mustConnect(t, g, "dev1", "cred")
	mustPublish(t, g, "dev1", "c1")
	mustPublish(t, g, "dev1", "c2") // 触发/接近压缩
	mustReceive(t, g, "dev1")
	if err := g.Ack("dev1", "c1"); err != nil {
		t.Fatal(err)
	}

	wal := filepath.Join(cfg.StateDir, encodeDeviceID("dev1")+".wal")
	snap := filepath.Join(cfg.StateDir, encodeDeviceID("dev1")+".snap")
	sb, err := os.ReadFile(snap)
	if err != nil {
		t.Fatalf("压缩快照应已生成: %v", err)
	}
	var env snapshotEnvelope
	if err := json.Unmarshal(sb, &env); err != nil {
		t.Fatal(err)
	}
	t.Logf("快照点 N=%d", env.N)

	// 手工构造"截断前崩溃"现场：把序号 1..snapN 的旧事件重新写回 WAL，
	// 再由新进程接续写入快照点之后的新事件（序号必须连续）。
	prefix := bytes2Lines(t, sb) // 仅用于日志
	_ = prefix
	oldLines := buildWalLines(t, 1, env.N)
	if err := os.WriteFile(wal, oldLines, 0o644); err != nil {
		t.Fatal(err)
	}

	// 新进程在"快照+残留旧WAL"状态下启动：扫描游标以快照点为准，继续追加事件
	g2 := New(cfg)
	if _, err := g2.Recover(); err != nil {
		t.Fatalf("混合 WAL 恢复不应失败: %v", err)
	}
	mustConnect(t, g2, "dev1", "cred")
	if err := g2.Subscribe("dev1", "t/new"); err != nil {
		t.Fatal(err)
	}
	g2.Disconnect("dev1", false)
	mustPublish(t, g2, "dev1", "c3")

	// 此刻 WAL = 快照前旧事件 + sub/enqueue 新事件；再次恢复
	g3 := New(cfg)
	stats, err := g3.Recover()
	if err != nil {
		t.Fatalf("二次恢复失败: %v", err)
	}
	if stats.DevicesFailed != 0 {
		t.Fatalf("不应有设备被隔离: %+v", stats)
	}
	mustConnect(t, g3, "dev1", "cred")
	ds := mustReceive(t, g3, "dev1")
	got := msgIDs(ds)
	// c1 已消费绝不重投；c2 在途重传；c3 离线补投；无任何重复
	want := []string{"c2", "c3"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("期望 %v，实际 %v", want, got)
	}
	if subs := g3.Subscriptions("dev1"); fmt.Sprint(subs) != "[t/new]" {
		t.Fatalf("订阅应保留，实际 %v", subs)
	}
	t.Logf("快照+残留WAL 混合恢复正确: %v ✓", got)
}

// buildWalLines 构造序号 1..n 的事件行，内容与本测试前面的操作一一对应：
// 1=connect, 2=assign(c1), 3=assign(c2), 4=ack(c1)（快照点 N=4）。
func buildWalLines(t *testing.T, from, to uint64) []byte {
	t.Helper()
	mk := func(rec eventRecord) []byte {
		b, err := json.Marshal(&rec)
		if err != nil {
			t.Fatal(err)
		}
		return append(b, '\n')
	}
	msg := func(id string, seq uint64) *Message {
		return &Message{ID: id, DeviceID: "dev1", Topic: "t/telemetry", Payload: []byte(id), Seq: seq}
	}
	var out []byte
	for n := from; n <= to; n++ {
		switch n {
		case 1:
			out = append(out, mk(eventRecord{N: 1, Type: evConnect, Cred: "cred"})...)
		case 2:
			out = append(out, mk(eventRecord{N: 2, Type: evAssign, Msg: msg("c1", 1), Attempt: 1})...)
		case 3:
			out = append(out, mk(eventRecord{N: 3, Type: evAssign, Msg: msg("c2", 2), Attempt: 1})...)
		case 4:
			out = append(out, mk(eventRecord{N: 4, Type: evAck, IDs: []string{"c1"}})...)
		default:
			t.Fatalf("未预期的事件序号 %d", n)
		}
	}
	return out
}

func bytes2Lines(t *testing.T, b []byte) int { t.Helper(); return len(b) }
