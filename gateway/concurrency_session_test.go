package gateway

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrent_SessionFlapWithPublishAndReceive 覆盖核心并发场景：
// 同一台设备在掉线/重连的同时，云端下发与设备拉取持续并发进行。
// 要求：在线状态判断与实际一致；该重传/补投的一条不少、顺序不乱；
// 不出现消息卡住或该下发没下发；最终所有消息恰好消费一次（-race 下反复运行）。
func TestConcurrent_SessionFlapWithPublishAndReceive(t *testing.T) {
	t.Log("场景: 单设备反复掉线/重连，与云端下发、设备拉取/确认同时并发，消息不丢不卡不乱序")

	dir := t.TempDir()
	// 窗口/配额/离线暂存均不设上限，使本用例只压"状态判断与并发时序"，
	// 不受窗口满/暂存满等背压分支干扰；StateDir 打开，顺带压并发落盘与压缩。
	cfg := Config{
		AckWindow:    0, // <=0 表示不限制
		Quota:        0,
		OfflineCap:   0,
		StateDir:     dir,
		CompactEvery: 128,
	}
	g := New(cfg)
	mustConnect(t, g, "dev1", "cred")

	const (
		publishers = 6
		perP       = 120
		total      = publishers * perP
	)
	published := make([]string, 0, total)
	pubMu := sync.Mutex{}
	for i := 0; i < total; i++ {
		published = append(published, fmt.Sprintf("m-%05d", i))
	}

	var (
		acked     = map[string]bool{}
		recvCount = map[string]int{}
		stateMu   sync.Mutex
		ackedN    int
		badOrder  int
		badDevice int
		stopFlap  = make(chan struct{})
		stopRecv  = make(chan struct{})
		wgPub     sync.WaitGroup
		wgFlap    sync.WaitGroup
		wgRecv    sync.WaitGroup
	)

	// 1) 掉线/重连 goroutine：两种断开方式都覆盖
	wgFlap.Add(1)
	go func() {
		defer wgFlap.Done()
		i := 0
		for {
			select {
			case <-stopFlap:
				return
			default:
			}
			g.Disconnect("dev1", i%2 == 0)
			time.Sleep(time.Microsecond)
			if _, err := g.Connect("dev1", "cred"); err != nil {
				t.Errorf("并发重连失败: %v", err)
				return
			}
			i++
		}
	}()

	// 2) 下发 goroutine：任意时刻下发都必须成功（在线走窗口、离线进暂存）
	for p := 0; p < publishers; p++ {
		wgPub.Add(1)
		go func(p int) {
			defer wgPub.Done()
			for j := 0; j < perP; j++ {
				id := fmt.Sprintf("m-%05d", p*perP+j)
				if err := g.Publish("dev1", id, "t/x", []byte(id)); err != nil {
					t.Errorf("并发 Publish(%s) 失败: %v", id, err)
					return
				}
				pubMu.Lock()
				pubMu.Unlock()
			}
		}(p)
	}

	// 3) 拉取/确认 goroutine：离线时收到 NOT_CONNECTED 重试；在线时取走并确认
	recvOne := func() {
		ds, err := g.Receive("dev1")
		if err != nil {
			if ge, ok := err.(*Error); !ok || ge.Code != ErrCodeNotConnected {
				t.Errorf("并发 Receive 出现非预期错误: %v", err)
			}
			return
		}
		var lastSeq uint64
		for _, d := range ds {
			if d.Message.DeviceID != "dev1" {
				badDevice++
			}
			if lastSeq != 0 && d.Message.Seq <= lastSeq {
				badOrder++
			}
			lastSeq = d.Message.Seq
			stateMu.Lock()
			recvCount[d.Message.ID]++
			first := !acked[d.Message.ID]
			if first {
				acked[d.Message.ID] = true
				ackedN++
			}
			stateMu.Unlock()
			if first {
				if err := g.Ack("dev1", d.Message.ID); err != nil {
					t.Errorf("并发 Ack(%s) 失败: %v", d.Message.ID, err)
				}
			}
		}
	}
	for r := 0; r < 2; r++ {
		wgRecv.Add(1)
		go func() {
			defer wgRecv.Done()
			for {
				select {
				case <-stopRecv:
					return
				default:
					stateMu.Lock()
					before := ackedN
					stateMu.Unlock()
					recvOne()
					stateMu.Lock()
					after := ackedN
					stateMu.Unlock()
					// 没有进展（离线或暂无投递）时退避，模拟设备周期轮询，避免打爆锁
					if after == before {
						time.Sleep(2 * time.Microsecond)
					}
				}
			}
		}()
	}

	wgPub.Wait()
	// 下发完成后让抖动与拉取再跑一小段，覆盖"尾部消息"的竞态
	time.Sleep(50 * time.Millisecond)
	close(stopFlap)
	wgFlap.Wait()

	// 4) 静止收敛：确保在线后反复拉取确认，直到在途/暂存/待取全部清空（带超时防卡死）
	if _, err := g.Connect("dev1", "cred"); err != nil {
		t.Fatalf("收敛阶段 Connect 失败: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		recvOne()
		if g.OfflineLen("dev1") == 0 && g.InflightLen("dev1") == 0 {
			if ds, err := g.Receive("dev1"); err == nil && len(ds) == 0 {
				break
			}
		}
		if g.InflightLen("dev1") == 0 && g.OfflineLen("dev1") == 0 {
			break
		}
	}
	close(stopRecv)
	wgRecv.Wait()

	// ---- 断言 ----
	if badDevice > 0 {
		t.Fatalf("存在属于其他设备的投递: %d 条", badDevice)
	}
	if badOrder > 0 {
		t.Fatalf("存在 %d 个批次内 Seq 非递增（顺序乱）", badOrder)
	}
	if len(acked) != total {
		missing := []string{}
		for _, id := range published {
			if !acked[id] {
				missing = append(missing, id)
				if len(missing) >= 10 {
					break
				}
			}
		}
		t.Fatalf("恰好消费一次不成立：已确认 %d/%d，缺失示例 %v（在途=%d 暂存=%d）",
			len(acked), total, missing, g.InflightLen("dev1"), g.OfflineLen("dev1"))
	}
	if g.InflightLen("dev1") != 0 || g.OfflineLen("dev1") != 0 {
		t.Fatalf("仍有消息卡住：在途=%d 暂存=%d", g.InflightLen("dev1"), g.OfflineLen("dev1"))
	}
	// 每条消息至少被投递一次；允许掉线竞态下重传（at-least-once），由去重保证只消费一次
	never := 0
	for id := range acked {
		if recvCount[id] == 0 {
			never++
		}
	}
	if never > 0 {
		t.Fatalf("%d 条消息从未被投递过", never)
	}
	t.Logf("并发抖动下 %d 条消息全部恰好消费一次（在途0/暂存0），重复投递由去重吸收 ✓", total)
}

// TestConcurrent_MultiDeviceFlapNoCrossTalk 多台设备同时掉线重连、同时下发确认，
// 验证：连接状态、待确认消息、离线积压、已消费记录全部按设备隔离，不串味；
// 每台设备最终恰好收到属于自己的全部消息（-race 下运行）。
func TestConcurrent_MultiDeviceFlapNoCrossTalk(t *testing.T) {
	t.Log("场景: 多设备并发掉线/重连 + 下发 + 拉取确认，每台设备的状态互不串味、不丢不重")

	const devices = 10
	cfg := Config{AckWindow: 0, Quota: 0, OfflineCap: 0, CompactEvery: 256}
	g := New(cfg)
	for d := 0; d < devices; d++ {
		mustConnect(t, g, fmt.Sprintf("dev%02d", d), "k")
	}

	const perDev = 80
	var (
		stateMu sync.Mutex
		// deviceID -> msgID -> 投递次数；同时用于"属于谁"的串味检查
		seen  = map[string]map[string]int{}
		acked = map[string]map[string]bool{}
	)
	for d := 0; d < devices; d++ {
		dev := fmt.Sprintf("dev%02d", d)
		seen[dev] = map[string]int{}
		acked[dev] = map[string]bool{}
	}
	var crossTalk int

	var wg sync.WaitGroup
	for d := 0; d < devices; d++ {
		dev := fmt.Sprintf("dev%02d", d)

		// 每台设备一个抖动 goroutine
		wg.Add(1)
		go func(dev string) {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				g.Disconnect(dev, i%2 == 0)
				time.Sleep(time.Microsecond)
				if _, err := g.Connect(dev, "k"); err != nil {
					t.Errorf("设备 %s 并发重连失败: %v", dev, err)
					return
				}
			}
		}(dev)

		// 每台设备一个下发 goroutine
		wg.Add(1)
		go func(dev string) {
			defer wg.Done()
			for i := 0; i < perDev; i++ {
				id := dev + fmt.Sprintf("-m%03d", i)
				if err := g.Publish(dev, id, "t/x", []byte(id)); err != nil {
					t.Errorf("设备 %s 并发下发失败: %v", dev, err)
					return
				}
			}
		}(dev)

		// 每台设备一个拉取确认 goroutine
		wg.Add(1)
		go func(dev string) {
			defer wg.Done()
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				ds, err := g.Receive(dev)
				if err != nil {
					time.Sleep(2 * time.Microsecond)
					continue
				}
				for _, dl := range ds {
					if dl.Message.DeviceID != dev {
						crossTalk++
						continue
					}
					id := dl.Message.ID
					stateMu.Lock()
					first := !acked[dev][id]
					seen[dev][id]++
					if first {
						acked[dev][id] = true
					}
					stateMu.Unlock()
					if first {
						if err := g.Ack(dev, id); err != nil {
							t.Errorf("设备 %s 并发确认 %s 失败: %v", dev, id, err)
						}
					}
				}
				if len(ds) == 0 {
					time.Sleep(2 * time.Microsecond)
				}
			}
		}(dev)
	}
	wg.Wait()

	// 静止收敛：所有设备确保在线，把尾部投递取尽确认完
	for d := 0; d < devices; d++ {
		dev := fmt.Sprintf("dev%02d", d)
		if _, err := g.Connect(dev, "k"); err != nil {
			t.Fatalf("收敛 Connect(%s) 失败: %v", dev, err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			ds, err := g.Receive(dev)
			if err != nil {
				t.Fatalf("收敛阶段 %s 应在线: %v", dev, err)
			}
			for _, dl := range ds {
				if dl.Message.DeviceID != dev {
					crossTalk++
				}
				stateMu.Lock()
				first := !acked[dev][dl.Message.ID]
				seen[dev][dl.Message.ID]++
				if first {
					acked[dev][dl.Message.ID] = true
				}
				stateMu.Unlock()
				if first {
					if err := g.Ack(dev, dl.Message.ID); err != nil {
						t.Fatalf("收敛 Ack(%s,%s): %v", dev, dl.Message.ID, err)
					}
				}
			}
			if g.InflightLen(dev) == 0 && g.OfflineLen(dev) == 0 {
				break
			}
		}
	}

	if crossTalk > 0 {
		t.Fatalf("发现 %d 条跨设备串味投递", crossTalk)
	}
	for d := 0; d < devices; d++ {
		dev := fmt.Sprintf("dev%02d", d)
		if len(acked[dev]) != perDev {
			t.Fatalf("设备 %s 恰好消费一次不成立：%d/%d（在途=%d 暂存=%d）",
				dev, len(acked[dev]), perDev, g.InflightLen(dev), g.OfflineLen(dev))
		}
		if g.InflightLen(dev) != 0 || g.OfflineLen(dev) != 0 {
			t.Fatalf("设备 %s 仍有卡住消息：在途=%d 暂存=%d", dev, g.InflightLen(dev), g.OfflineLen(dev))
		}
		// 串味的另一角度：本设备记录里只能出现自己前缀的 ID
		for id := range acked[dev] {
			if len(id) < len(dev) || id[:len(dev)] != dev {
				t.Fatalf("设备 %s 收到外设备消息 %s", dev, id)
			}
		}
	}
	t.Logf("%d 台设备并发抖动/下发/确认，各 %d 条消息互不串味、恰好一次、无积压 ✓", devices, perDev)
}

// TestConcurrent_QuarantinedDeviceDoesNotSinkGateway 单设备持久状态损坏被隔离后，
// 对它并发发起连接/下发/拉取/确认，全部以 DEVICE_QUARANTINED 挡回；
// 与此同时其他设备的并发抖动、收发确认完全不受影响，网关不被拖垮（-race 下运行）。
func TestConcurrent_QuarantinedDeviceDoesNotSinkGateway(t *testing.T) {
	t.Log("场景: 被隔离设备的并发操作全部挡回自身，其他设备并发抖动/收发照常可用")
	cfg, _ := durableConfig(t, 20)
	cfg.AckWindow = 0
	cfg.Quota = 0
	cfg.OfflineCap = 0

	g := New(cfg)
	mustConnect(t, g, "bad", "k")
	mustPublish(t, g, "bad", "b1")
	mustConnect(t, g, "good", "k")

	// 直接破坏 bad 的快照：写入无法解析的快照，并制造"快照点之后无可用 WAL"的局面
	if err := writeJunkSnapshot(cfg.StateDir, "bad"); err != nil {
		t.Fatal(err)
	}

	g2 := New(cfg)
	stats, err := g2.Recover()
	if err != nil {
		t.Fatalf("单设备损坏不应让 Recover 失败: %v", err)
	}
	if stats.DevicesFailed != 1 || len(stats.FailedDevices) != 1 || stats.FailedDevices[0] != "bad" {
		t.Fatalf("期望仅 bad 被隔离，实际 %+v", stats)
	}
	mustConnect(t, g2, "good", "k")

	const goodN = 60
	var wg sync.WaitGroup

	// 对 bad 的并发操作：全部只能得到 DEVICE_QUARANTINED（或 unknown 的合理错误），
	// 绝不能 panic、绝不能成功、绝不能影响其他设备。
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if _, err := g2.Connect("bad", "k"); !isQuarantine(err) {
					t.Errorf("隔离设备 Connect 应被挡回: %v", err)
				}
				if err := g2.Publish("bad", fmt.Sprintf("x%d", j), "t", nil); !isQuarantine(err) {
					t.Errorf("隔离设备 Publish 应被挡回: %v", err)
				}
				if _, err := g2.Receive("bad"); !isQuarantine(err) {
					// 隔离状态下 Receive 也必须安全：要么隔离挡回，要么 NOT_CONNECTED，绝不 panic/串数据
					if ge, ok := err.(*Error); !ok || (ge.Code != ErrCodeDeviceQuarantined && ge.Code != ErrCodeNotConnected) {
						t.Errorf("隔离设备 Receive 返回非预期错误: %v", err)
					}
				}
				if err := g2.Ack("bad", "b1"); !isQuarantine(err) {
					if ge, ok := err.(*Error); !ok || ge.Code != ErrCodeDeviceQuarantined {
						t.Errorf("隔离设备 Ack 应被挡回: %v", err)
					}
				}
			}
		}(i)
	}

	// good 同时并发抖动与收发确认
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			g2.Disconnect("good", false)
			time.Sleep(time.Microsecond)
			if _, err := g2.Connect("good", "k"); err != nil {
				t.Errorf("good 重连失败: %v", err)
				return
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < goodN; i++ {
			if err := g2.Publish("good", fmt.Sprintf("g%03d", i), "t", nil); err != nil {
				t.Errorf("good 下发失败: %v", err)
				return
			}
		}
	}()

	goodAcked := map[string]bool{}
	var mu sync.Mutex
	wg.Add(1)
	go func() {
		defer wg.Done()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			ds, err := g2.Receive("good")
			if err != nil {
				time.Sleep(2 * time.Microsecond)
				continue
			}
			for _, d := range ds {
				if d.Message.DeviceID != "good" {
					t.Errorf("串味：good 收到 %s 的消息", d.Message.DeviceID)
				}
				mu.Lock()
				first := !goodAcked[d.Message.ID]
				if first {
					goodAcked[d.Message.ID] = true
				}
				mu.Unlock()
				if first {
					if err := g2.Ack("good", d.Message.ID); err != nil {
						t.Errorf("good Ack 失败: %v", err)
					}
				}
			}
		}
	}()
	wg.Wait()

	if _, err := g2.Connect("good", "k"); err != nil {
		t.Fatalf("收敛 Connect good: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ds, _ := g2.Receive("good")
		for _, d := range ds {
			mu.Lock()
			first := !goodAcked[d.Message.ID]
			if first {
				goodAcked[d.Message.ID] = true
			}
			mu.Unlock()
			if first {
				if err := g2.Ack("good", d.Message.ID); err != nil {
					t.Fatal(err)
				}
			}
		}
		if g2.InflightLen("good") == 0 && g2.OfflineLen("good") == 0 {
			break
		}
	}
	if len(goodAcked) != goodN {
		t.Fatalf("good 应收发确认 %d 条，实际 %d（在途=%d 暂存=%d）",
			goodN, len(goodAcked), g2.InflightLen("good"), g2.OfflineLen("good"))
	}
	t.Logf("隔离设备的并发操作全部挡回自身；good 并发 %d 条全部恰好一次 ✓", goodN)
}

func isQuarantine(err error) bool {
	ge, ok := err.(*Error)
	return ok && ge.Code == ErrCodeDeviceQuarantined
}
