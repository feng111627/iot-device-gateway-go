package gateway

import (
	"fmt"
	"sync"
	"testing"
)

// 发布->拉取->确认一条消息的小助手。
func roundTrip(t *testing.T, g *Gateway, dev, id string) {
	t.Helper()
	if err := g.Publish(dev, id, "t/x", []byte(id)); err != nil {
		t.Fatalf("Publish(%s): %v", id, err)
	}
	ds, err := g.Receive(dev)
	if err != nil {
		t.Fatalf("Receive(%s): %v", id, err)
	}
	if len(ds) != 1 || ds[0].Message.ID != id {
		t.Fatalf("期望仅取到 %s，实际 %v", id, msgIDs(ds))
	}
	if err := g.Ack(dev, id); err != nil {
		t.Fatalf("Ack(%s): %v", id, err)
	}
}

// TestConsumedCap_RuntimeEvictOldest 运行期去重集达到上限后：
// 容量被严格封顶，超出按 Seq 最旧优先确定性淘汰，淘汰计数可观测；
// 被淘汰记录的重复确认不再被识别（语义说明：容量边界外的历史去重记录已让位）；
// 但仍在窗口内的在途消息不受影响，绝不被错误淘汰。
func TestConsumedCap_RuntimeEvictOldest(t *testing.T) {
	t.Log("场景: 已消费去重集达到 ConsumedCap 后确定性淘汰最旧，计数可观测，在途消息不受影响")
	cfg := Config{AckWindow: 10, Quota: 100, OfflineCap: 10, ConsumedCap: 5}
	g := New(cfg)
	mustConnect(t, g, "dev1", "k")

	for i := 1; i <= 8; i++ {
		roundTrip(t, g, "dev1", fmt.Sprintf("m%d", i))
		if n := g.ConsumedLen("dev1"); n > 5 {
			t.Fatalf("去重集应封顶为 5，实际 %d", n)
		}
	}
	if n := g.ConsumedLen("dev1"); n != 5 {
		t.Fatalf("最终应保留最旧淘汰后的 5 条，实际 %d", n)
	}
	if n := g.ConsumedEvicted("dev1"); n != 3 {
		t.Fatalf("应累计淘汰 3 条，实际 %d", n)
	}

	// 保留下来的是 Seq 最大的 5 条（m4..m8）；最旧的 m1 已被淘汰
	for _, id := range []string{"m4", "m5", "m6", "m7", "m8"} {
		if err := g.Ack("dev1", id); err == nil || err.(*Error).Code != ErrCodeDuplicateAck {
			t.Fatalf("保留期内的 %s 重复确认应为 DUPLICATE_ACK，实际 %v", id, err)
		}
	}
	// 对已被淘汰去重记录的重复 Ack：不再识别为重复，按"不在窗口"返回 UNKNOWN_MESSAGE
	// （该消息早已确认消费、不会重投；丢失的只是容量边界外的历史幂等标记）
	if err := g.Ack("dev1", "m1"); err == nil || err.(*Error).Code != ErrCodeUnknownMessage {
		t.Fatalf("被淘汰去重记录 m1 的重复确认应为 UNKNOWN_MESSAGE，实际 %v", err)
	}
	t.Logf("去重集封顶 5、淘汰 3；保留记录仍可去重，在途消息不受影响 ✓")
}

// TestConsumedCap_DefaultAndUnlimited 容量配置的边界：0 取默认值，负数不限。
func TestConsumedCap_DefaultAndUnlimited(t *testing.T) {
	t.Log("场景: ConsumedCap=0 采用默认上限；负数表示不设上限")
	if c := New(Config{}).consumedCap; c != defaultConsumedCap {
		t.Fatalf("默认上限应为 %d，实际 %d", defaultConsumedCap, c)
	}
	g := New(Config{AckWindow: 0, Quota: 0, ConsumedCap: -1})
	mustConnect(t, g, "dev1", "k")
	for i := 0; i < 3000; i++ {
		roundTrip(t, g, "dev1", fmt.Sprintf("u%04d", i))
	}
	if n := g.ConsumedLen("dev1"); n != 3000 {
		t.Fatalf("不限模式应保留全部 3000 条，实际 %d", n)
	}
	if n := g.ConsumedEvicted("dev1"); n != 0 {
		t.Fatalf("不限模式不应淘汰，实际 %d", n)
	}
	t.Logf("默认 %d / 不限模式 3000 条无淘汰 ✓", defaultConsumedCap)
}

// TestConsumedCap_ConcurrentAcks 高并发反复下发确认下，去重集始终不超过上限、
// 淘汰计数与实际移除数量一致，且每次 Ack 仍然语义正确（-race 下运行）。
func TestConsumedCap_ConcurrentAcks(t *testing.T) {
	t.Log("场景: 多 goroutine 并发下发-确认远超过容量，去重集严格封顶、计数自洽")
	const cap = 50
	g := New(Config{AckWindow: 0, Quota: 0, OfflineCap: 0, ConsumedCap: cap})
	mustConnect(t, g, "dev1", "k")

	const workers = 8
	const each = 200
	for w := 0; w < workers; w++ {
		mustConnect(t, g, fmt.Sprintf("dev%02d", w), "k")
	}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			dev := fmt.Sprintf("dev%02d", w)
			for i := 0; i < each; i++ {
				id := fmt.Sprintf("m%04d", i)
				if err := g.Publish(dev, id, "t/x", nil); err != nil {
					t.Errorf("Publish %s/%s: %v", dev, id, err)
					return
				}
				ds, err := g.Receive(dev)
				if err != nil || len(ds) != 1 || ds[0].Message.ID != id {
					t.Errorf("Receive %s/%s: err=%v n=%d", dev, id, err, len(ds))
					return
				}
				if err := g.Ack(dev, id); err != nil {
					t.Errorf("Ack %s/%s: %v", dev, id, err)
					return
				}
				if g.ConsumedLen(dev) > cap {
					t.Errorf("设备 %s 去重集超过上限: %d > %d", dev, g.ConsumedLen(dev), cap)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	total := workers * each
	evicted := 0
	for w := 0; w < workers; w++ {
		dev := fmt.Sprintf("dev%02d", w)
		if n := g.ConsumedLen(dev); n != cap {
			t.Fatalf("设备 %s 应收敛为 %d 条，实际 %d", dev, cap, n)
		}
		evicted += g.ConsumedEvicted(dev)
	}
	if evicted != total-workers*cap {
		t.Fatalf("应累计淘汰 %d 条，实际 %d", total-workers*cap, evicted)
	}
	t.Logf("%d 台设备并发确认共 %d 条：每台去重集封顶 %d、合计淘汰 %d，全程不越界 ✓",
		workers, total, cap, evicted)
}

// TestConsumedCap_RecoveryStatsAndEviction 恢复结果能看出保留了多少、淘汰/挡掉了多少：
// 先用大容量产生一批已消费记录并压缩，再用小容量实例恢复，超出部分被确定性淘汰并计入统计。
func TestConsumedCap_RecoveryStatsAndEviction(t *testing.T) {
	t.Log("场景: 重启恢复时去重集受当前 ConsumedCap 约束，保留数与淘汰数在 RecoverStats 中可观测")
	cfg, _ := durableConfig(t, 3)
	cfg.AckWindow = 20
	cfg.Quota = 200
	cfg.ConsumedCap = 100 // 产生阶段容量充足
	g := New(cfg)
	mustConnect(t, g, "dev1", "k")
	for i := 0; i < 12; i++ {
		roundTrip(t, g, "dev1", fmt.Sprintf("r%02d", i))
	}
	// 显式触发持久化（后续事件+重启即可覆盖，这里主要确保快照路径被走到）
	if _, err := g.Connect("dev1", "k"); err != nil {
		t.Fatal(err)
	}

	small := cfg
	small.ConsumedCap = 5 // 恢复进程容量更小
	g2 := New(small)
	stats, err := g2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if stats.ConsumedRetained != 5 {
		t.Fatalf("恢复后应保留 5 条去重记录，实际 %d（%+v）", stats.ConsumedRetained, stats)
	}
	if stats.ConsumedEvicted != 7 {
		t.Fatalf("恢复时应淘汰/挡掉 7 条，实际 %d（%+v）", stats.ConsumedEvicted, stats)
	}
	if n := g2.ConsumedLen("dev1"); n != 5 {
		t.Fatalf("内存中去重集应为 5，实际 %d", n)
	}
	// 恢复后在线确认语义不回归：淘汰掉的 r00 不再识别为重复，保留的 r11 仍是重复
	mustConnect(t, g2, "dev1", "k")
	if err := g2.Ack("dev1", "r11"); err == nil || err.(*Error).Code != ErrCodeDuplicateAck {
		t.Fatalf("保留记录 r11 应为 DUPLICATE_ACK，实际 %v", err)
	}
	// 恢复后产生少量新事件并触发压缩：缩小后的去重集（5 条）与累计淘汰数（7）
	// 被固化进新快照、旧 WAL 被截断，淘汰决定就此持久化。
	if err := g2.Subscribe("dev1", "t/new"); err != nil {
		t.Fatal(err)
	}
	roundTrip(t, g2, "dev1", "post1")

	// 再次以小容量重启：之前被挡的 7 条已随压缩固化、不再重复计数；
	// 唯一新增淘汰来自恢复后的新消费 post1（容量为 5，进 1 条必淘汰 1 条最旧）。
	g3 := New(small)
	st, err := g3.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if st.ConsumedRetained != 5 {
		t.Fatalf("二次重启应保留 5，实际 retained=%d", st.ConsumedRetained)
	}
	if st.ConsumedEvicted != 1 {
		t.Fatalf("仅 post1 这一条新消费应触发 1 次淘汰（旧淘汰已固化不重复），实际 evicted=%d", st.ConsumedEvicted)
	}
	if n := g3.ConsumedEvicted("dev1"); n != 8 {
		t.Fatalf("累计淘汰应为 8（恢复时 7 + post1 1），实际 %d", n)
	}
	t.Logf("恢复统计：保留 %d、淘汰 %d；二次重启状态一致 ✓", stats.ConsumedRetained, stats.ConsumedEvicted)
}
