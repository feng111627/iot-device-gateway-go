package gateway

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// ========== 多设备并发恢复 / 并发换凭据：互不串味 ==========

func TestConcurrent_RecoveryAndRotationNoCrossTalk(t *testing.T) {
	t.Log("场景: 多设备同时恢复、同时轮换/换身份，消息不串设备、不重不丢（-race 下运行）")
	const n = 12
	cfg, _ := durableConfig(t, 7)
	cfg.AckWindow = 20
	cfg.Quota = 200

	g := New(cfg)
	// 预置：每台设备 1 条已消费 + 2 条在途 + 3 条离线积压
	for i := 0; i < n; i++ {
		dev := fmt.Sprintf("dev%02d", i)
		cred := fmt.Sprintf("key-%d-a", i)
		mustConnect(t, g, dev, cred)
		mustPublish(t, g, dev, dev+"-done")
		mustReceive(t, g, dev)
		if err := g.Ack(dev, dev+"-done"); err != nil {
			t.Fatal(err)
		}
		mustPublish(t, g, dev, dev+"-in1")
		mustPublish(t, g, dev, dev+"-in2")
		mustReceive(t, g, dev) // 取走但不确认 => 在途
		g.Disconnect(dev, false)
		for _, k := range []string{"off1", "off2", "off3"} {
			mustPublish(t, g, dev, dev+"-"+k)
		}
	}

	// 重启：多个 goroutine 同时 Recover（反复、并发执行，必须幂等且安全）
	g2 := New(cfg)
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := g2.Recover(); err != nil {
				t.Errorf("并发 Recover 返回错误: %v", err)
			}
		}()
	}
	wg.Wait()

	// 并发：每台设备 Connect、轮换凭据、收消息、确认，彼此互不干扰
	var wg2 sync.WaitGroup
	for i := 0; i < n; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()
			dev := fmt.Sprintf("dev%02d", i)
			newCred := fmt.Sprintf("key-%d-b", i)
			if err := g2.RotateCredential(dev, newCred); err != nil {
				t.Errorf("RotateCredential(%s) 失败: %v", dev, err)
				return
			}
			if _, err := g2.Connect(dev, newCred); err != nil {
				t.Errorf("Connect(%s) 失败: %v", dev, err)
				return
			}
			ds, err := g2.Receive(dev)
			if err != nil {
				t.Errorf("Receive(%s) 失败: %v", dev, err)
				return
			}
			// 期望恰好 2 条在途重传 + 3 条离线补投，且全部属于本设备、无重复
			want := []string{dev + "-in1", dev + "-in2", dev + "-off1", dev + "-off2", dev + "-off3"}
			got := msgIDs(ds)
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("设备 %s 期望 %v，实际 %v（串味/丢失/重复）", dev, want, got)
				return
			}
			for _, d := range ds {
				if d.Message.DeviceID != dev {
					t.Errorf("串味：设备 %s 收到属于 %s 的消息 %s", dev, d.Message.DeviceID, d.Message.ID)
					return
				}
				if !strings.HasPrefix(d.Message.ID, dev+"-") {
					t.Errorf("串味：设备 %s 收到外设备消息 %s", dev, d.Message.ID)
					return
				}
			}
			for _, d := range ds {
				if err := g2.Ack(dev, d.Message.ID); err != nil {
					t.Errorf("Ack(%s,%s) 失败: %v", dev, d.Message.ID, err)
				}
			}
			// 已消费的旧消息绝不能重新出现
			if err := g2.Ack(dev, dev+"-done"); err == nil || err.(*Error).Code != ErrCodeDuplicateAck {
				t.Errorf("设备 %s 已消费记录丢失，期望 DUPLICATE_ACK，实际 %v", dev, err)
			}
		}(i)
	}
	wg2.Wait()

	// 再重启一轮：全部确认后不应有任何残留投递，再并发换身份也不串
	g3 := restart(t, cfg)
	var wg3 sync.WaitGroup
	for i := 0; i < n; i++ {
		wg3.Add(1)
		go func(i int) {
			defer wg3.Done()
			dev := fmt.Sprintf("dev%02d", i)
			cred := fmt.Sprintf("key-%d-b", i)
			if _, err := g3.Connect(dev, cred); err != nil {
				t.Errorf("二次重启后 Connect(%s) 失败: %v", dev, err)
				return
			}
			ds, err := g3.Receive(dev)
			if err != nil {
				t.Errorf("Receive(%s) 失败: %v", dev, err)
				return
			}
			if len(ds) != 0 {
				t.Errorf("设备 %s 全部确认后仍有投递: %v", dev, msgIDs(ds))
			}
		}(i)
	}
	wg3.Wait()
	t.Logf("%d 台设备并发恢复/轮换/确认，无串味、无重复、无丢失 ✓", n)
}

// ========== 单设备状态损坏：隔离而不拖垮全局 ==========

func TestRecovery_CorruptDeviceIsQuarantined(t *testing.T) {
	t.Log("场景: 一台设备 WAL 中部损坏只隔离它自己；尾部写一半的撕裂记录可自愈；换新身份解除隔离")
	cfg, _ := durableConfig(t, 5)
	cfg.AckWindow = 10

	g := New(cfg)
	mustConnect(t, g, "good", "k")
	mustPublish(t, g, "good", "g1")
	mustConnect(t, g, "bad", "k")
	mustPublish(t, g, "bad", "b1")
	mustReceive(t, g, "bad")
	mustConnect(t, g, "torn", "k")
	mustPublish(t, g, "torn", "t1")

	// bad：在 WAL 中部插入一条无法解析的记录（后面还跟着好记录），属于真正损坏
	badWAL := filepath.Join(cfg.StateDir, encodeDeviceID("bad")+".wal")
	orig, err := os.ReadFile(badWAL)
	if err != nil {
		t.Fatal(err)
	}
	middle := append(append([]byte{}, orig...), []byte("{\"n\":99,\"t\":???\n")...)
	middle = append(middle, []byte("{\"n\":100,\"t\":\"sub\",\"topic\":\"x\"}\n")...)
	if err := os.WriteFile(badWAL, middle, 0o644); err != nil {
		t.Fatal(err)
	}

	// torn：只在文件尾部追加无换行结尾的半截记录（模拟写一半被强杀）
	tornWAL := filepath.Join(cfg.StateDir, encodeDeviceID("torn")+".wal")
	tf, err := os.OpenFile(tornWAL, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tf.WriteString("{\"n\":77,\"t\":\"ack\",\"ids\":[\"partial\""); err != nil {
		t.Fatal(err)
	}
	tf.Close()

	g2 := New(cfg)
	stats, err := g2.Recover()
	if err != nil {
		t.Fatalf("单设备损坏不应让 Recover 整体失败: %v", err)
	}
	if stats.DevicesFailed != 1 || len(stats.FailedDevices) != 1 || stats.FailedDevices[0] != "bad" {
		t.Fatalf("期望仅 bad 被隔离，实际 %+v", stats)
	}
	if stats.DevicesRecovered != 2 {
		t.Fatalf("good/torn 应恢复成功，实际 recovered=%d", stats.DevicesRecovered)
	}
	if stats.CorruptRecords < 2 {
		t.Fatalf("CorruptRecords 应计入坏记录与撕裂记录，实际 %d", stats.CorruptRecords)
	}

	// good 完全正常
	mustConnect(t, g2, "good", "k")
	if got := msgIDs(mustReceive(t, g2, "good")); fmt.Sprint(got) != "[g1]" {
		t.Fatalf("good 设备应正常恢复 g1，实际 %v", got)
	}
	// torn：撕裂尾记录被丢弃截断，之前的好状态完整恢复，之后还能继续追加事件
	mustConnect(t, g2, "torn", "k")
	if got := msgIDs(mustReceive(t, g2, "torn")); fmt.Sprint(got) != "[t1]" {
		t.Fatalf("torn 设备应自愈并恢复 t1，实际 %v", got)
	}
	mustPublish(t, g2, "torn", "t2") // 残片已截断，新事件可正常落盘
	if got := msgIDs(mustReceive(t, g2, "torn")); fmt.Sprint(got) != "[t2]" {
		t.Fatalf("torn 自愈后应能继续收发，实际 %v", got)
	}

	// bad 被隔离：任何正常操作被挡回 DEVICE_QUARANTINED
	if _, err := g2.Connect("bad", "k"); err == nil || err.(*Error).Code != ErrCodeDeviceQuarantined {
		t.Fatalf("bad 应被隔离，实际 %v", err)
	}
	if err := g2.Publish("bad", "x", "t", nil); err == nil || err.(*Error).Code != ErrCodeDeviceQuarantined {
		t.Fatalf("隔离设备下发应被拒绝，实际 %v", err)
	}

	// 显式换新身份后解除隔离，新身份干净可用
	if _, err := g2.ResetIdentity("bad", "k2"); err != nil {
		t.Fatalf("ResetIdentity 解除隔离失败: %v", err)
	}
	mustConnect(t, g2, "bad", "k2")
	if ds := mustReceive(t, g2, "bad"); len(ds) != 0 {
		t.Fatalf("新身份不应收到旧消息 b1: %v", msgIDs(ds))
	}
	mustPublish(t, g2, "bad", "fresh")
	if got := msgIDs(mustReceive(t, g2, "bad")); fmt.Sprint(got) != "[fresh]" {
		t.Fatalf("新身份应收到 fresh，实际 %v", got)
	}

	// 重启后：bad 以新身份正常存在，不再是隔离态；good/torn 不受影响
	g3 := restart(t, cfg)
	st3 := g3.RecoveryStats()
	if st3.DevicesFailed != 0 {
		t.Fatalf("换新身份并压缩后重启不应再有损坏隔离，实际 %+v", st3)
	}
	mustConnect(t, g3, "bad", "k2")
	if got := msgIDs(mustReceive(t, g3, "bad")); fmt.Sprint(got) != "[fresh]" {
		t.Fatalf("重启后新身份的在途 fresh 应重传，实际 %v", got)
	}
	mustConnect(t, g3, "good", "k")
	if got := msgIDs(mustReceive(t, g3, "good")); fmt.Sprint(got) != "[g1]" {
		t.Fatalf("good 重启后仍应正常，实际 %v", got)
	}
	t.Logf("损坏隔离生效（恢复 %d/隔离 %d/坏记录 %d），撕裂自愈，换新身份可恢复 ✓",
		stats.DevicesRecovered, stats.DevicesFailed, stats.CorruptRecords)
}

// ========== 容量边界与恢复统计 ==========

func TestRecovery_DeviceLimitAndStats(t *testing.T) {
	t.Log("场景: 恢复设备数有明确容量边界，被挡住的设备与恢复数量都可观测")
	dir := t.TempDir()
	base := Config{AckWindow: 10, Quota: 100, OfflineCap: 50, StateDir: dir, CompactEvery: 100}

	// 先用无容量限制的实例预置 3 台设备与不同形态的状态
	g := New(base)
	mustConnect(t, g, "dev1", "k")
	mustPublish(t, g, "dev1", "d1-in")
	mustReceive(t, g, "dev1") // 在途 1
	g.Disconnect("dev1", false)
	mustPublish(t, g, "dev1", "d1-off")

	mustConnect(t, g, "dev2", "k")
	mustPublish(t, g, "dev2", "d2-done")
	mustReceive(t, g, "dev2")
	if err := g.Ack("dev2", "d2-done"); err != nil {
		t.Fatal(err)
	}

	mustConnect(t, g, "dev3", "k")
	mustPublish(t, g, "dev3", "d3")

	// 用 MaxDevices=2 的新实例恢复：按设备 ID 排序，dev3 被挡
	limited := base
	limited.MaxDevices = 2
	g2 := New(limited)
	stats, err := g2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if stats.DevicesRecovered != 2 {
		t.Fatalf("应恢复 2 台，实际 %d", stats.DevicesRecovered)
	}
	if stats.DevicesSkippedLimit != 1 || fmt.Sprint(stats.SkippedDevices) != "[dev3]" {
		t.Fatalf("dev3 应被容量挡住，实际 %+v", stats)
	}
	if stats.InflightRecovered != 1 || stats.OfflineRecovered != 1 || stats.ConsumedRetained != 1 {
		t.Fatalf("恢复计数不符（期望 在途1/离线1/已消费1），实际 %+v", stats)
	}
	if stats.EventsReplayed == 0 {
		t.Fatalf("应有事件被重放，实际 %d", stats.EventsReplayed)
	}

	// 运行期容量同样封顶：恢复了 2 台后，新设备落盘被拒绝
	err = g2.Publish("dev3", "x", "t", nil) // dev3 没有会话
	if err == nil || err.(*Error).Code != ErrCodeUnknownDevice {
		t.Fatalf("未接入设备下发期望 UNKNOWN_DEVICE，实际 %v", err)
	}
	_, err = g2.Connect("dev3", "k")
	if err == nil || err.(*Error).Code != ErrCodeDeviceLimit {
		t.Fatalf("达到容量边界的新设备 Connect 应返回 DEVICE_LIMIT，实际 %v", err)
	}
	// 已恢复的设备不受影响
	mustConnect(t, g2, "dev1", "k")
	got := msgIDs(mustReceive(t, g2, "dev1"))
	if fmt.Sprint(got) != "[d1-in d1-off]" {
		t.Fatalf("dev1 应收到在途重传 d1-in 与离线补投 d1-off，实际 %v", got)
	}
	if n := g2.OfflineLen("dev1"); n != 0 {
		t.Fatalf("dev1 补投后暂存应为 0，实际 %d", n)
	}
	t.Logf("容量边界生效：恢复 %d、挡住 %v、在途 %d、离线 %d、已消费 %d、重放事件 %d ✓",
		stats.DevicesRecovered, stats.SkippedDevices,
		stats.InflightRecovered, stats.OfflineRecovered, stats.ConsumedRetained, stats.EventsReplayed)
}
