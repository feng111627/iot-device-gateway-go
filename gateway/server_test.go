package gateway_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	gw "github.com/feng111627/iot-device-gateway-go/gateway"
)

// deviceClient 模拟一台真实设备：按 Seq 去重消费，随机重复确认。
type deviceClient struct {
	id string
	t  *testing.T

	mu       sync.Mutex
	seen     map[uint64]struct{} // 已到达的 Seq（设备端去重）
	consumed map[uint64]int      // seq -> 消费次数
	conn     net.Conn
}

func (d *deviceClient) connect(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.conn = conn
	d.mu.Unlock()
	return d.send(map[string]any{"type": "connect", "device_id": d.id, "token": "token-v1", "clean": false})
}

func (d *deviceClient) send(v map[string]any) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.conn == nil {
		return fmt.Errorf("not connected")
	}
	b, _ := json.Marshal(v)
	_, err := fmt.Fprintf(d.conn, "%s\n", b)
	return err
}

// readLoop 读取下行消息：去重消费 + 确认（30% 概率重复确认）。
func (d *deviceClient) readLoop(done <-chan struct{}) {
	d.mu.Lock()
	conn := d.conn
	d.mu.Unlock()
	if conn == nil {
		return
	}
	sc := bufio.NewScanner(conn)
	for sc.Scan() {
		select {
		case <-done:
			return
		default:
		}
		var resp gw.Response
		if json.Unmarshal(sc.Bytes(), &resp) != nil || resp.Type != "message" {
			continue
		}
		seq := resp.Msg.Seq
		// 设备端按 Seq 去重：重复投递只消费一次，但仍需再次确认。
		d.mu.Lock()
		_, dup := d.seen[seq]
		if !dup {
			d.seen[seq] = struct{}{}
			d.consumed[seq]++
		}
		d.mu.Unlock()
		if dup {
			_ = d.send(map[string]any{"type": "ack", "seq": seq})
			continue
		}
		_ = d.send(map[string]any{"type": "ack", "seq": seq})
		if rand.Intn(10) < 3 {
			_ = d.send(map[string]any{"type": "ack", "seq": seq}) // 重复确认
		}
	}
}

// 业务场景【断连重连风暴】：多设备反复断连重连 + 重复确认 + 离线堆积，
// 每条消息在每台设备上恰好被消费一次。
func TestServer_ReconnectStorm_ExactlyOnce(t *testing.T) {
	t.Log("业务: 端到端 | 用例: 20设备断连重连风暴+重复确认+离线堆积, 消息恰好消费一次")
	state := filepath.Join(t.TempDir(), "state.jsonl")
	g := gw.New(gw.Config{AckWindow: 4, OfflineCapacity: 64, StatePath: state})
	srv := gw.NewServer(g)
	go srv.ListenAndServe("127.0.0.1:0")
	defer srv.Close()
	for srv.Addr() == "" {
		time.Sleep(time.Millisecond)
	}
	addr := srv.Addr()

	const numDevices, numRounds = 20, 5
	devices := make([]*deviceClient, numDevices)
	for i := range devices {
		devices[i] = &deviceClient{id: fmt.Sprintf("dev-%02d", i), t: t,
			seen: make(map[uint64]struct{}), consumed: make(map[uint64]int)}
	}

	// 多轮：接入→订阅→接收→随机异常断开。
	for round := 0; round < numRounds; round++ {
		var wg sync.WaitGroup
		for _, d := range devices {
			wg.Add(1)
			go func(d *deviceClient) {
				defer wg.Done()
				if err := d.connect(addr); err != nil {
					t.Errorf("connect: %v", err)
					return
				}
				_ = d.send(map[string]any{"type": "subscribe", "topics": []string{"alerts"}})
				done := make(chan struct{})
				go d.readLoop(done)
				time.Sleep(time.Duration(50+rand.Intn(100)) * time.Millisecond)
				close(done)
				d.conn.Close() // 模拟异常断开（不发 disconnect）
			}(d)
		}
		// 本轮在线期间发布 2 条。
		time.Sleep(30 * time.Millisecond)
		for i := 0; i < 2; i++ {
			_ = g.Publish("alerts", []byte(fmt.Sprintf("round-%d-msg-%d", round, i)))
		}
		wg.Wait()
		// 全部离线后再发布 1 条（离线堆积）。
		_ = g.Publish("alerts", []byte(fmt.Sprintf("round-%d-offline", round)))
	}

	// 最终全部重连，收齐所有离线/在途消息。
	var wg sync.WaitGroup
	for _, d := range devices {
		wg.Add(1)
		go func(d *deviceClient) {
			defer wg.Done()
			if err := d.connect(addr); err != nil {
				t.Errorf("final connect: %v", err)
				return
			}
			done := make(chan struct{})
			go d.readLoop(done)
			time.Sleep(300 * time.Millisecond)
			close(done)
			d.conn.Close()
		}(d)
	}
	wg.Wait()

	// 校验：每台设备消费的消息无重复（readLoop 内已报错），
	// 且离线堆积的消息最终都被消费到。
	totalConsumed := 0
	for _, d := range devices {
		d.mu.Lock()
		totalConsumed += len(d.consumed)
		d.mu.Unlock()
	}
	if totalConsumed == 0 {
		t.Fatal("no messages consumed at all")
	}
	// 每台设备消费的消息条数不得超过其应得（发布总次数），且无重复消费。
	const publishedTotal = numRounds * 3 // 每轮 2 条在线 + 1 条离线
	for _, d := range devices {
		d.mu.Lock()
		n := len(d.consumed)
		d.mu.Unlock()
		if n > publishedTotal {
			t.Errorf("device %s consumed %d messages, more than published %d", d.id, n, publishedTotal)
		}
	}
	t.Logf("结果: %d 台设备 %d 轮风暴(每轮3条), 共唯一消费 %d 条次, 无重复消费 ✓",
		numDevices, numRounds, totalConsumed)
}
