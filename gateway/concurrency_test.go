package gateway

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

// ========== 单设备：掉线/重连与下发/拉取并发进行 ==========

// TestConcurrent_ChurnSingleDeviceEventuallyConsistent 在同一台设备上并发跑
// 下发、拉取、确认、掉线、重连，验证最终一致性：
//   - 每条成功下发的消息恰好被消费一次（不重、不丢、不卡住）；
//   - 每次 Receive 拿到的批次内 Seq 严格递增（投递顺序不乱）；
//   - 在线判断与实际一致：离线时 Receive 只能得到 NOT_CONNECTED，
//     已掉线设备不会收到"按在线路径"的投递。
func TestConcurrent_ChurnSingleDeviceEventuallyConsistent(t *testing.T) {
	t.Log("场景: 单设备并发 掉线/重连 × 下发/拉取/确认，最终恰好消费一次、顺序不乱（-race 下运行）")
	// 窗口/配额/暂存不设限，聚焦并发正确性本身（容量拒绝由其他用例覆盖）
	g := New(Config{AckWindow: 0, Quota: 0, OfflineCap: 100000})
	const dev = "dev-churn"
	const total = 400
	mustConnect(t, g, dev, "cred")

	var stop atomic.Bool
	var wg sync.WaitGroup

	// 设备侧语义：同一消息 ID 只消费一次（重传的重复投递被去重）
	var seenMu sync.Mutex
	seen := make(map[string]bool)
	var consumed atomic.Int64

	// 2 个拉取 goroutine：模拟设备侧并发取件
	for c := 0; c < 2; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for !stop.Load() {
				ds, err := g.Receive(dev)
				if err != nil {
					if err.(*Error).Code != ErrCodeNotConnected {
						t.Errorf("Receive 返回意外错误: %v", err)
					}
					time.Sleep(time.Millisecond)
					continue
				}
				for i, d := range ds {
					if i > 0 && ds[i].Message.Seq <= ds[i-1].Message.Seq {
						t.Errorf("批次内 Seq 未严格递增: %v", msgIDs(ds))
					}
					if d.Message.DeviceID != dev {
						t.Errorf("串味：%s 收到属于 %s 的消息", dev, d.Message.DeviceID)
					}
					seenMu.Lock()
					first := !seen[d.Message.ID]
					if first {
						seen[d.Message.ID] = true
					}
					seenMu.Unlock()
					if !first {
						continue // 重传的重复投递：设备侧去重，不重复消费
					}
					if err := g.Ack(dev, d.Message.ID); err != nil {
						t.Errorf("Ack(%s) 失败: %v", d.Message.ID, err)
						continue
					}
					consumed.Add(1)
				}
			}
		}(c)
	}

	// 1 个掉线/重连 goroutine：高频异常掉线 + 重连
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			g.Disconnect(dev, false)
			if _, err := g.Connect(dev, "cred"); err != nil {
				t.Errorf("重连失败: %v", err)
			}
		}
	}()

	// 云端下发 goroutine：顺序发 total 条；窗口/配额不限，Publish 不应失败从队列积压。
	var published []string
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			id := fmt.Sprintf("m-%04d", i)
			if err := g.Publish(dev, id, "t/churn", []byte(id)); err != nil {
				t.Errorf("Publish(%s) 失败: %v", id, err)
				continue
			}
			published = append(published, id)
		}
	}()

	// 等待全部消息被消费（带超时，卡死即失败）
	deadline := time.Now().Add(60 * time.Second)
	for consumed.Load() < int64(total) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()

	if len(published) != total {
		t.Fatalf("下发成功数应为 %d，实际 %d", total, len(published))
	}
	if got := consumed.Load(); got != int64(total) {
		t.Fatalf("消费数应为 %d（每条恰好一次），实际 %d", total, got)
	}

	// 收敛检查：重连后把残留取净，不应再有新消息，离线暂存也应清空
	mustConnect(t, g, dev, "cred")
	for round := 0; round < 10; round++ {
		ds := mustReceive(t, g, dev)
		if len(ds) == 0 {
			break
		}
		for _, d := range ds {
			seenMu.Lock()
			first := !seen[d.Message.ID]
			seenMu.Unlock()
			if !first {
				continue
			}
			t.Errorf("收敛阶段出现未被消费过的消息 %s（漏消费）", d.Message.ID)
		}
	}
	if n := g.OfflineLen(dev); n != 0 {
		t.Fatalf("收敛后离线暂存应为 0，实际 %d（消息被卡住）", n)
	}
	t.Logf("%d 条消息在并发掉线/重连/下发/拉取下各恰好消费一次，无丢失无卡住 ✓", total)
}

// ========== 多设备并发 churn：互不串味；单设备被隔离不拖垮全局 ==========

// TestConcurrent_MultiDeviceChurnIsolation 多台设备同时做
// 掉线/重连/下发/拉取/确认的完整循环，另有一台设备因持久状态损坏被隔离：
//   - 每台设备的连接状态、在途、离线积压、已消费记录互不串味；
//   - 被隔离设备的所有操作返回 DEVICE_QUARANTINED，且不影响其他设备收敛。
func TestConcurrent_MultiDeviceChurnIsolation(t *testing.T) {
	t.Log("场景: 多设备并发掉线重连+下发确认互不干扰；一台设备被隔离，其余照常收敛（-race 下运行）")
	cfg, _ := durableConfig(t, 40)
	cfg.AckWindow = 0
	cfg.Quota = 0
	cfg.OfflineCap = 100000

	const healthy = 6
	const perDevice = 40

	// 预置：健康设备各留一些在途与离线积压；bad 设备随后被损坏 WAL 隔离
	g := New(cfg)
	for i := 0; i < healthy; i++ {
		dev := fmt.Sprintf("ok-%d", i)
		mustConnect(t, g, dev, "k")
		mustPublish(t, g, dev, dev+"-pre")
		mustReceive(t, g, dev) // 在途 1 条
		g.Disconnect(dev, false)
	}
	mustConnect(t, g, "bad", "k")
	mustPublish(t, g, "bad", "b1")

	// 损坏 bad 的 WAL 中部（后面还跟一条好记录），恢复时必须被隔离
	badWAL := filepath.Join(cfg.StateDir, encodeDeviceID("bad")+".wal")
	orig, err := os.ReadFile(badWAL)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append(append([]byte{}, orig...), []byte("{\"n\":90,\"t\":???\n")...)
	corrupt = append(corrupt, []byte("{\"n\":91,\"t\":\"sub\",\"topic\":\"x\"}\n")...)
	if err := os.WriteFile(badWAL, corrupt, 0o644); err != nil {
		t.Fatal(err)
	}

	// 重启恢复：bad 被隔离，健康设备全部恢复
	g2 := New(cfg)
	stats, err := g2.Recover()
	if err != nil {
		t.Fatalf("Recover 失败: %v", err)
	}
	if stats.DevicesFailed != 1 || stats.FailedDevices[0] != "bad" {
		t.Fatalf("应仅隔离 bad，实际 %+v", stats)
	}
	if stats.DevicesRecovered != healthy {
		t.Fatalf("应恢复 %d 台健康设备，实际 %d", healthy, stats.DevicesRecovered)
	}

	var wg sync.WaitGroup
	// 健康设备：每台独立 goroutine 做完整 churn，最终各自收敛
	for i := 0; i < healthy; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			dev := fmt.Sprintf("ok-%d", i)
			cred := "k"
			seen := map[string]bool{}
			// 多轮：重连 -> 收 -> 确认 -> 下发新消息 -> 掉线
			published := []string{}
			for round := 0; round < 4; round++ {
				if _, err := g2.Connect(dev, cred); err != nil {
					t.Errorf("Connect(%s) 失败: %v", dev, err)
					return
				}
				ds, err := g2.Receive(dev)
				if err != nil {
					t.Errorf("Receive(%s) 失败: %v", dev, err)
					return
				}
				for _, d := range ds {
					if d.Message.DeviceID != dev || !strings.HasPrefix(d.Message.ID, dev+"-") {
						t.Errorf("串味：%s 收到外设备消息 %s", dev, d.Message.ID)
						return
					}
					if seen[d.Message.ID] {
						continue
					}
					seen[d.Message.ID] = true
					if err := g2.Ack(dev, d.Message.ID); err != nil {
						t.Errorf("Ack(%s,%s) 失败: %v", dev, d.Message.ID, err)
						return
					}
				}
				for j := 0; j < perDevice/4; j++ {
					id := fmt.Sprintf("%s-r%dm%d", dev, round, j)
					if err := g2.Publish(dev, id, "t/x", []byte(id)); err != nil {
						t.Errorf("Publish(%s,%s) 失败: %v", dev, id, err)
						return
					}
					published = append(published, id)
				}
				g2.Disconnect(dev, false)
			}
			// 收敛：重连后把剩余全部取净并确认
			if _, err := g2.Connect(dev, cred); err != nil {
				t.Errorf("收敛 Connect(%s) 失败: %v", dev, err)
				return
			}
			for {
				ds, err := g2.Receive(dev)
				if err != nil || len(ds) == 0 {
					break
				}
				for _, d := range ds {
					if seen[d.Message.ID] {
						continue
					}
					seen[d.Message.ID] = true
					if err := g2.Ack(dev, d.Message.ID); err != nil {
						t.Errorf("收敛 Ack(%s,%s) 失败: %v", dev, d.Message.ID, err)
						return
					}
				}
			}
			// 每台设备：预置 1 条 + 自发 perDevice 条，全部恰好消费一次
			want := 1 + perDevice
			if len(seen) != want {
				t.Errorf("设备 %s 应消费 %d 条，实际 %d（漏消费或串味）", dev, want, len(seen))
			}
			if n := g2.OfflineLen(dev); n != 0 {
				t.Errorf("设备 %s 收敛后离线暂存应为 0，实际 %d", dev, n)
			}
		}(i)
	}

	// 隔离设备：并发地尝试各种操作，全部应被 DEVICE_QUARANTINED 挡回
	for k := 0; k < 3; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if _, err := g2.Connect("bad", "k"); err == nil || err.(*Error).Code != ErrCodeDeviceQuarantined {
					t.Errorf("隔离设备 Connect 期望 DEVICE_QUARANTINED，实际 %v", err)
					return
				}
				if err := g2.Publish("bad", "x", "t", nil); err == nil || err.(*Error).Code != ErrCodeDeviceQuarantined {
					t.Errorf("隔离设备 Publish 期望 DEVICE_QUARANTINED，实际 %v", err)
					return
				}
				if err := g2.Ack("bad", "b1"); err == nil || err.(*Error).Code != ErrCodeDeviceQuarantined {
					t.Errorf("隔离设备 Ack 期望 DEVICE_QUARANTINED，实际 %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// 全局收敛后再重启一轮：健康设备状态完整（无残留投递），bad 仍被隔离
	g3 := restart(t, cfg)
	st3 := g3.RecoveryStats()
	if st3.DevicesFailed != 1 || st3.DevicesRecovered != healthy {
		t.Fatalf("二次恢复应为 恢复%d/隔离%d，实际 %+v", healthy, 1, st3)
	}
	for i := 0; i < healthy; i++ {
		dev := fmt.Sprintf("ok-%d", i)
		mustConnect(t, g3, dev, "k")
		if ds := mustReceive(t, g3, dev); len(ds) != 0 {
			t.Fatalf("设备 %s 全部确认后重启不应再有投递: %v", dev, msgIDs(ds))
		}
	}
	t.Logf("%d 台健康设备并发 churn 全部收敛（各消费 %d 条），隔离设备未拖垮全局 ✓", healthy, 1+perDevice)
}

// ========== 去重记录容量上限：确定性淘汰最旧，运行期与恢复统计均可观测 ==========

func TestConsumedCap_BoundedEvictOldest(t *testing.T) {
	t.Log("场景: 已消费去重记录按 ConsumedCap 封顶，超限确定性淘汰最旧；运行期与恢复统计都能看出保留/淘汰数")
	cfg, _ := durableConfig(t, 1000)
	cfg.AckWindow = 0
	cfg.Quota = 0
	cfg.ConsumedCap = 10

	g := New(cfg)
	mustConnect(t, g, "dev1", "k")
	// 连续确认 25 条：去重记录最多保留 10 条，最旧的 15 条被淘汰
	for i := 0; i < 25; i++ {
		id := fmt.Sprintf("m-%02d", i)
		mustPublish(t, g, "dev1", id)
		mustReceive(t, g, "dev1")
		if err := g.Ack("dev1", id); err != nil {
			t.Fatal(err)
		}
	}
	if got := g.ConsumedRetained("dev1"); got != 10 {
		t.Fatalf("去重记录应封顶为 10，实际保留 %d", got)
	}
	if got := g.ConsumedEvicted("dev1"); got != 15 {
		t.Fatalf("应淘汰最旧的 15 条，实际淘汰 %d", got)
	}
	// 被淘汰的最旧记录：重复 Ack 不再识别为"已消费"，而是 UNKNOWN_MESSAGE
	assertErrCode(t, g.Ack("dev1", "m-00"), ErrCodeUnknownMessage)
	assertErrCode(t, g.Ack("dev1", "m-14"), ErrCodeUnknownMessage)
	// 保留的最近 10 条：重复 Ack 仍返回 DUPLICATE_ACK（去重仍然生效）
	assertErrCode(t, g.Ack("dev1", "m-15"), ErrCodeDuplicateAck)
	assertErrCode(t, g.Ack("dev1", "m-24"), ErrCodeDuplicateAck)

	// 重启恢复：保留数仍封顶为 10，保留的去重记录跨重启继续生效。
	// 本例事件未达压缩阈值，恢复靠 WAL 重放：25 条 ack 逐条重放、逐条触发淘汰，
	// 因此恢复统计里的淘汰数 = 重放期间淘汰的 15 条。
	g2 := restart(t, cfg)
	st := g2.RecoveryStats()
	if st.ConsumedRetained != 10 {
		t.Fatalf("恢复后应保留 10 条去重记录，实际 %d", st.ConsumedRetained)
	}
	if st.ConsumedEvicted != 15 {
		t.Fatalf("重放过程应淘汰最旧的 15 条，实际 %d", st.ConsumedEvicted)
	}
	assertErrCode(t, g2.Ack("dev1", "m-15"), ErrCodeDuplicateAck)
	assertErrCode(t, g2.Ack("dev1", "m-24"), ErrCodeDuplicateAck)

	// 恢复后继续确认，集合依然封顶：再确认 5 条，retained 仍为 10；
	// 本进程累计淘汰 = 重放淘汰 15 + 恢复后运行淘汰 5。
	mustConnect(t, g2, "dev1", "k")
	for i := 25; i < 30; i++ {
		id := fmt.Sprintf("m-%02d", i)
		mustPublish(t, g2, "dev1", id)
		mustReceive(t, g2, "dev1")
		if err := g2.Ack("dev1", id); err != nil {
			t.Fatal(err)
		}
	}
	if got := g2.ConsumedRetained("dev1"); got != 10 {
		t.Fatalf("恢复后继续运行仍应封顶为 10，实际 %d", got)
	}
	if got := g2.ConsumedEvicted("dev1"); got != 20 {
		t.Fatalf("本进程累计应淘汰 15（重放）+5（运行）= 20 条，实际 %d", got)
	}
	t.Logf("去重记录封顶 10：运行期淘汰 15 条，恢复重放再淘汰 15 条、保留 %d 条，最旧记录确定性出局 ✓", st.ConsumedRetained)
}

// TestConsumedCap_RecoveryStatsShowEvicted 验证"恢复时"的淘汰同样可观测：
// 旧实例不限容量攒下去重记录，换用带 ConsumedCap 的配置恢复，
// RecoverStats 能直接看出保留了多少、淘汰了多少。
func TestConsumedCap_RecoveryStatsShowEvicted(t *testing.T) {
	t.Log("场景: 无上限实例积累的去重记录，在带 ConsumedCap 的实例恢复时按上限淘汰，恢复统计可见")
	cfg, _ := durableConfig(t, 1000)
	cfg.AckWindow = 0
	cfg.Quota = 0
	// 第一期：不限容量，确认 25 条，去重记录全部落盘
	g := New(cfg)
	mustConnect(t, g, "dev1", "k")
	for i := 0; i < 25; i++ {
		id := fmt.Sprintf("m-%02d", i)
		mustPublish(t, g, "dev1", id)
		mustReceive(t, g, "dev1")
		if err := g.Ack("dev1", id); err != nil {
			t.Fatal(err)
		}
	}

	// 第二期：同一 StateDir，ConsumedCap=10 恢复
	limited := cfg
	limited.ConsumedCap = 10
	g2 := New(limited)
	stats, err := g2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if stats.ConsumedRetained != 10 {
		t.Fatalf("恢复后应保留 10 条，实际 %d", stats.ConsumedRetained)
	}
	if stats.ConsumedEvicted != 15 {
		t.Fatalf("恢复过程应淘汰最旧的 15 条，实际 %d", stats.ConsumedEvicted)
	}
	// 淘汰的是最旧记录：m-00..m-14 不再被识别为已消费，m-15..m-24 仍然去重
	assertErrCode(t, g2.Ack("dev1", "m-00"), ErrCodeUnknownMessage)
	assertErrCode(t, g2.Ack("dev1", "m-24"), ErrCodeDuplicateAck)
	t.Logf("恢复统计：保留 %d、淘汰 %d（最旧优先），去重对保留记录跨重启生效 ✓",
		stats.ConsumedRetained, stats.ConsumedEvicted)
}

// TestConcurrent_ConsumedCapUnderChurn 并发确认压力下，去重集合始终不越上限。
func TestConcurrent_ConsumedCapUnderChurn(t *testing.T) {
	t.Log("场景: 并发下发/确认持续进行，去重记录实时淘汰、任何时刻都不超过 ConsumedCap（-race 下运行）")
	g := New(Config{AckWindow: 0, Quota: 0, OfflineCap: 1000, ConsumedCap: 32})
	const dev = "dev-cap"
	mustConnect(t, g, dev, "k")

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				id := fmt.Sprintf("w%d-m%03d", w, i)
				if err := g.Publish(dev, id, "t/cap", []byte(id)); err != nil {
					t.Errorf("Publish(%s) 失败: %v", id, err)
					return
				}
				if _, err := g.Receive(dev); err != nil {
					t.Errorf("Receive 失败: %v", err)
					return
				}
				if err := g.Ack(dev, id); err != nil {
					t.Errorf("Ack(%s) 失败: %v", id, err)
					return
				}
				if got := g.ConsumedRetained(dev); got > 32 {
					t.Errorf("去重记录数 %d 超过上限 32", got)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	if got := g.ConsumedRetained(dev); got != 32 {
		t.Fatalf("200 条确认后应恰好保留上限 32 条，实际 %d", got)
	}
	if got := g.ConsumedEvicted(dev); got != 200-32 {
		t.Fatalf("应淘汰 %d 条，实际 %d", 200-32, got)
	}
	t.Logf("200 条并发确认：去重记录稳定在 32 条上限，累计淘汰 %d 条 ✓", g.ConsumedEvicted(dev))
}
