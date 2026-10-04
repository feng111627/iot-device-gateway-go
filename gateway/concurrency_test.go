package gateway

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// 多台设备同时经历"崩溃->恢复->重连确认"，彼此状态不得串味；用 -race 运行。
func TestConcurrent_RestartManyDevicesNoCrossTalk(t *testing.T) {
	t.Log("场景: 多设备同时恢复+并发收确认，设备间状态互不串扰")
	path := filepath.Join(t.TempDir(), "state.jsonl")
	const n = 24
	cfg := Config{AckWindow: 32, Quota: 128, OfflineCap: 64, SnapshotPath: path}

	g1 := New(cfg)
	for i := 0; i < n; i++ {
		dev := fmt.Sprintf("dev-%02d", i)
		cred := fmt.Sprintf("cred-%02d", i)
		mustConnect(t, g1, dev, cred)
		// 每台：1 条已确认 + 2 条在途未确认，再离线积压 2 条
		mustPublish(t, g1, dev, dev+"-done")
		mustReceive(t, g1, dev)
		if err := g1.Ack(dev, dev+"-done"); err != nil {
			t.Fatal(err)
		}
		mustPublish(t, g1, dev, dev+"-f1")
		mustPublish(t, g1, dev, dev+"-f2")
		mustReceive(t, g1, dev)
		g1.Disconnect(dev, false)
		mustPublish(t, g1, dev, dev+"-q1")
		mustPublish(t, g1, dev, dev+"-q2")
	}

	// 新进程：并发恢复（同一实例重复恢复也必须安全）
	g2 := New(cfg)
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := g2.RestoreWithReport(); err != nil {
				t.Errorf("并发恢复出错: %v", err)
			}
		}()
	}
	wg.Wait()

	// 并发重连、拉取并确认；每台设备应恰好收到 f1,f2,q1,q2 四条，收不到 done
	errs := make(chan error, n)
	wg = sync.WaitGroup{}
	for i := 0; i < n; i++ {
		dev := fmt.Sprintf("dev-%02d", i)
		cred := fmt.Sprintf("cred-%02d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := g2.Connect(dev, cred); err != nil {
				errs <- fmt.Errorf("%s connect: %w", dev, err)
				return
			}
			ds, err := g2.Receive(dev)
			if err != nil {
				errs <- fmt.Errorf("%s receive: %w", dev, err)
				return
			}
			want := map[string]bool{dev + "-f1": true, dev + "-f2": true, dev + "-q1": true, dev + "-q2": true}
			if len(ds) != 4 {
				errs <- fmt.Errorf("%s 应收 4 条，实际 %d: %v", dev, len(ds), msgIDs(ds))
				return
			}
			for _, d := range ds {
				if d.Message.DeviceID != dev {
					errs <- fmt.Errorf("%s 串味收到其他设备消息 %s", dev, d.Message.ID)
					return
				}
				if !want[d.Message.ID] {
					errs <- fmt.Errorf("%s 收到意外/重复消息 %s", dev, d.Message.ID)
					return
				}
				want[d.Message.ID] = false
				if err := g2.Ack(dev, d.Message.ID); err != nil {
					errs <- fmt.Errorf("%s ack %s: %w", dev, d.Message.ID, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// 再重启一次：全部已确认，不得有任何重传/积压
	g3 := New(cfg)
	if _, err := g3.RestoreWithReport(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		dev := fmt.Sprintf("dev-%02d", i)
		mustConnect(t, g3, dev, fmt.Sprintf("cred-%02d", i))
		if got := msgIDs(mustReceive(t, g3, dev)); len(got) != 0 {
			t.Fatalf("%s 全部确认后重启不应有重传，实际 %v", dev, got)
		}
	}
	t.Logf("%d 台设备并发恢复+确认无串味、无重复，二次重启全部干净 ✓", n)
}

// 多台设备同时平滑换凭据、同时换新身份，彼此隔离，状态按各自语义变化。
func TestConcurrent_RotateAndResetMixed(t *testing.T) {
	t.Log("场景: 部分设备并发轮换凭据、部分并发换新身份，结果互不影响")
	path := filepath.Join(t.TempDir(), "state.jsonl")
	cfg := Config{AckWindow: 16, Quota: 64, OfflineCap: 16, SnapshotPath: path}
	const n = 16
	g := New(cfg)
	for i := 0; i < n; i++ {
		dev := fmt.Sprintf("d-%02d", i)
		mustConnect(t, g, dev, "k-old")
		mustPublish(t, g, dev, dev+"-m")
		mustReceive(t, g, dev)
		g.Disconnect(dev, false)
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		dev := fmt.Sprintf("d-%02d", i)
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if idx%2 == 0 {
				if err := g.RotateCredential(dev, "k-rotated"); err != nil {
					t.Errorf("%s rotate: %v", dev, err)
					return
				}
				if _, err := g.Connect(dev, "k-rotated"); err != nil {
					t.Errorf("%s connect after rotate: %v", dev, err)
				}
			} else {
				if err := g.ResetIdentity(dev, "k-fresh"); err != nil {
					t.Errorf("%s reset: %v", dev, err)
					return
				}
				if _, err := g.Connect(dev, "k-fresh"); err != nil {
					t.Errorf("%s connect after reset: %v", dev, err)
				}
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		dev := fmt.Sprintf("d-%02d", i)
		got := msgIDs(mustReceive(t, g, dev))
		if i%2 == 0 {
			if len(got) != 1 || got[0] != dev+"-m" {
				t.Fatalf("轮换设备 %s 应保留旧消息，实际 %v", dev, got)
			}
		} else {
			if len(got) != 0 {
				t.Fatalf("换新身份设备 %s 不应有旧消息，实际 %v", dev, got)
			}
		}
	}

	// 重启后语义保持
	g2 := New(cfg)
	if _, err := g2.RestoreWithReport(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		dev := fmt.Sprintf("d-%02d", i)
		cred := "k-rotated"
		if i%2 != 0 {
			cred = "k-fresh"
		}
		mustConnect(t, g2, dev, cred)
		got := msgIDs(mustReceive(t, g2, dev))
		if i%2 == 0 {
			if len(got) != 1 || got[0] != dev+"-m" {
				t.Fatalf("重启后轮换设备 %s 仍应保留消息，实际 %v", dev, got)
			}
		} else if len(got) != 0 {
			t.Fatalf("重启后换新身份设备 %s 不应有旧消息，实际 %v", dev, got)
		}
	}
	t.Log("并发轮换/换新身份各按语义生效，跨重启保持，互不串扰 ✓")
}

// 个别设备的持久化记录损坏，只跳过该设备；其他设备照常恢复，网关仍能启动。
func TestRestore_CorruptRecordIsolation(t *testing.T) {
	t.Log("场景: 快照中混入坏行与坏设备记录，只隔离坏数据，好设备完整恢复")
	path := filepath.Join(t.TempDir(), "state.jsonl")
	cfg := durConfig(path)

	g1 := New(cfg)
	mustConnect(t, g1, "good", "cred")
	g1.Disconnect("good", false)
	mustPublish(t, g1, "good", "g1")
	mustConnect(t, g1, "bad-soon", "cred")
	g1.Disconnect("bad-soon", false)
	mustPublish(t, g1, "bad-soon", "b1")

	// 直接在快照文件里做破坏：插入两行坏数据
	if err := g1.Snapshot(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	poison := append([]byte("{这不是合法JSON\n"), raw...)
	poison = append(poison, []byte("{\"version\":1}\n")...) // 缺 device_id
	if err := os.WriteFile(path, poison, 0o644); err != nil {
		t.Fatal(err)
	}

	g2 := New(cfg)
	rep, err := g2.RestoreWithReport()
	if err != nil {
		t.Fatalf("存在坏记录时 Restore 不应整体失败: %v", err)
	}
	if rep.CorruptRecords != 2 {
		t.Fatalf("应识别 2 行坏记录，实际 %d（%s）", rep.CorruptRecords, rep)
	}
	if rep.DevicesRestored < 2 {
		t.Fatalf("好设备仍应恢复，实际 %s", rep)
	}
	t.Logf("坏行隔离: %s ✓", rep)

	mustConnect(t, g2, "good", "cred")
	if got := msgIDs(mustReceive(t, g2, "good")); len(got) != 1 || got[0] != "g1" {
		t.Fatalf("好设备消息应完整恢复，实际 %v", got)
	}
	mustConnect(t, g2, "bad-soon", "cred")
	if got := msgIDs(mustReceive(t, g2, "bad-soon")); len(got) != 1 || got[0] != "b1" {
		t.Fatalf("其余设备不受坏行影响，实际 %v", got)
	}
	t.Log("损坏记录只影响坏行本身，好设备与其他设备均正常 ✓")
}

// 明确的容量边界：设备数超 MaxDevices 时整台挡下，结果在报告中可见；
// 在线侧超限接入返回可区分错误码。
func TestRestoreAndRuntime_DeviceCapacityBoundary(t *testing.T) {
	t.Log("场景: MaxDevices 限制在线接入与重启恢复，挡下数量可观测")
	path := filepath.Join(t.TempDir(), "state.jsonl")
	full := Config{AckWindow: 8, Quota: 32, OfflineCap: 8, SnapshotPath: path}

	g1 := New(full)
	for _, dev := range []string{"a", "b", "c"} {
		mustConnect(t, g1, dev, "cred")
		g1.Disconnect(dev, false)
		mustPublish(t, g1, dev, dev+"-msg")
	}

	// 新进程容量只允许 2 台：按 device_id 顺序，c 被整台挡下
	cfg2 := full
	cfg2.MaxDevices = 2
	g2 := New(cfg2)
	rep, err := g2.RestoreWithReport()
	if err != nil {
		t.Fatal(err)
	}
	if rep.DevicesBlocked != 1 {
		t.Fatalf("应挡下 1 台设备，实际 %s", rep)
	}
	if rep.MessagesBlocked < 1 {
		t.Fatalf("被挡设备的消息也应计入挡下，实际 %s", rep)
	}
	if rep.DevicesRestored != 2 {
		t.Fatalf("应恢复 2 台，实际 %s", rep)
	}
	t.Logf("容量边界: %s ✓", rep)

	for _, dev := range []string{"a", "b"} {
		mustConnect(t, g2, dev, "cred")
		if got := msgIDs(mustReceive(t, g2, dev)); len(got) != 1 {
			t.Fatalf("%s 应正常恢复消息，实际 %v", dev, got)
		}
	}
	// c 在容量外：既没恢复积压，也无法新接入
	if n := g2.OfflineLen("c"); n != 0 {
		t.Fatalf("被挡设备不应装入积压，实际 %d", n)
	}
	_, err = g2.Connect("c", "cred")
	assertErrCode(t, err, ErrCodeDeviceLimit)
	t.Log("超容量设备在线接入被 DEVICE_LIMIT 挡下 ✓")
}

// 单设备维度容量边界：去重记录与离线暂存超限时确定性挡下最旧部分，数量可观测。
func TestRestore_PerDeviceDedupAndBacklogCap(t *testing.T) {
	t.Log("场景: DedupCap/OfflineCap 限制恢复装入量，超出部分计入挡下")
	path := filepath.Join(t.TempDir(), "state.jsonl")
	full := Config{AckWindow: 8, Quota: 64, OfflineCap: 8, SnapshotPath: path}

	g1 := New(full)
	mustConnect(t, g1, "dev1", "cred")
	// 4 条已消费（去重记录）
	for _, id := range []string{"c1", "c2", "c3", "c4"} {
		mustPublish(t, g1, "dev1", id)
		mustReceive(t, g1, "dev1")
		if err := g1.Ack("dev1", id); err != nil {
			t.Fatal(err)
		}
	}
	// 3 条离线积压
	g1.Disconnect("dev1", false)
	for _, id := range []string{"o1", "o2", "o3"} {
		mustPublish(t, g1, "dev1", id)
	}

	// 新进程收紧到 DedupCap=2、OfflineCap=1
	cfg2 := full
	cfg2.DedupCap = 2
	cfg2.OfflineCap = 1
	g2 := New(cfg2)
	rep, err := g2.RestoreWithReport()
	if err != nil {
		t.Fatal(err)
	}
	if rep.ConsumedRestored != 2 {
		t.Fatalf("去重记录只应装入 2 条，实际 %d（%s）", rep.ConsumedRestored, rep)
	}
	if rep.OfflineRestored != 1 || rep.MessagesBlocked != 4 {
		// 2 条最旧去重 + 2 条最旧积压被挡
		t.Fatalf("容量边界计量不符: %s", rep)
	}
	t.Logf("单设备容量边界生效: %s ✓", rep)

	// 只保留最新 1 条积压补投（o3）
	mustConnect(t, g2, "dev1", "cred")
	got := msgIDs(mustReceive(t, g2, "dev1"))
	if len(got) != 1 || got[0] != "o3" {
		t.Fatalf("只应补投未超容量的最新一条 o3，实际 %v", got)
	}
	t.Logf("超容量积压确定性挡下最旧，仅补投 %v ✓", got)
}
