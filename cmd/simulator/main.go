// 设备模拟器：在本机连接网关，模拟大量设备的上报、断连重连、
// 重复确认与离线堆积，验证网关的会话语义与投递保证。
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feng111627/iot-device-gateway-go/gateway"
)

type stats struct {
	connected   atomic.Int64
	reconnects  atomic.Int64
	received    atomic.Int64
	dupFiltered atomic.Int64
	acked       atomic.Int64
	dupAcks     atomic.Int64
	uplinkOK    atomic.Int64
	uplinkRej   atomic.Int64
}

// device 模拟一台设备：按 Seq 去重消费，随机断连重连，随机重复确认。
type device struct {
	id    string
	token string
	st    *stats

	mu      sync.Mutex
	seenSeq map[uint64]struct{} // 已消费序号，保证重复投递只消费一次
	lastAck uint64
	writeMu sync.Mutex // 保护同一连接上的并发写
}

func (d *device) run(conn net.Conn, offlineFor time.Duration) error {
	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)

	send := func(v any) error {
		d.writeMu.Lock()
		defer d.writeMu.Unlock()
		return enc.Encode(v)
	}
	if err := send(map[string]any{"type": "connect", "device_id": d.id, "token": d.token, "clean": false}); err != nil {
		return err
	}
	if err := send(map[string]any{"type": "subscribe", "topics": []string{"cmd/" + d.id, "broadcast"}}); err != nil {
		return err
	}

	done := make(chan struct{})
	defer close(done)
	// 读循环：消费下行消息并确认。
	go func() {
		for {
			var resp gateway.Response
			if err := dec.Decode(&resp); err != nil {
				return
			}
			switch resp.Type {
			case "message":
				m := resp.Msg
				d.mu.Lock()
				if _, dup := d.seenSeq[m.Seq]; dup {
					d.st.dupFiltered.Add(1) // 重复投递：去重，不重复消费
					d.mu.Unlock()
				} else {
					d.seenSeq[m.Seq] = struct{}{}
					d.st.received.Add(1)
					d.mu.Unlock()
				}
				// 确认；小概率立刻重复确认一次，验证幂等。
				_ = send(map[string]any{"type": "ack", "seq": m.Seq})
				d.st.acked.Add(1)
				if rand.Intn(20) == 0 {
					_ = send(map[string]any{"type": "ack", "seq": m.Seq})
					d.st.dupAcks.Add(1)
				}
			case "error":
				d.st.uplinkRej.Add(1)
			}
		}
	}()

	// 上行上报。
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	deadline := time.Now().Add(offlineFor)
	for time.Now().Before(deadline) {
		select {
		case <-done:
			return nil
		case <-tick.C:
			if err := send(map[string]any{"type": "uplink", "payload": fmt.Sprintf("telemetry from %s", d.id)}); err != nil {
				return err
			}
			d.st.uplinkOK.Add(1)
		}
	}
	return nil
}

func main() {
	addr := flag.String("addr", "127.0.0.1:1883", "gateway address")
	devices := flag.Int("devices", 50, "number of simulated devices")
	duration := flag.Duration("duration", 10*time.Second, "simulation duration")
	reconnectEvery := flag.Duration("reconnect-every", 2*time.Second, "average reconnect interval")
	flag.Parse()

	var st stats
	var wg sync.WaitGroup
	// 注意：不能用 time.After 的通道做广播——它只发送一次值，只有一个
	// goroutine 能收到。close 才能让所有设备 goroutine 同时收到停止信号。
	stop := make(chan struct{})
	time.AfterFunc(*duration, func() { close(stop) })

	// 发布者：周期性向广播主题与随机设备主题发下行消息，
		// 设备频繁离线，消息会进入暂存并在重连后补发。
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := net.Dial("tcp", *addr)
		if err != nil {
			log.Printf("publisher dial: %v", err)
			return
		}
		defer conn.Close()
		enc := json.NewEncoder(conn)
		n := 0
		for {
			select {
			case <-stop:
				return
			case <-time.After(150 * time.Millisecond):
			}
			n++
			topic := "broadcast"
			if n%3 == 0 {
				topic = fmt.Sprintf("cmd/dev-%04d", rand.Intn(*devices))
			}
			if err := enc.Encode(map[string]any{"type": "publish", "topic": topic,
				"payload": fmt.Sprintf("downlink-%d", n)}); err != nil {
				return
			}
		}
	}()

	for i := 0; i < *devices; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d := &device{
				id:      fmt.Sprintf("dev-%04d", i),
				token:   "token-v1",
				st:      &st,
				seenSeq: make(map[uint64]struct{}),
			}
			for {
				select {
				case <-stop:
					return
				default:
				}
				conn, err := net.Dial("tcp", *addr)
				if err != nil {
					time.Sleep(200 * time.Millisecond)
					continue
				}
				st.connected.Add(1)
				// 在线一段时间后异常断开（直接关连接，不发 disconnect）。
				_ = d.run(conn, *reconnectEvery/2+time.Duration(rand.Int63n(int64(*reconnectEvery))))
				conn.Close()
				st.reconnects.Add(1)
				time.Sleep(time.Duration(rand.Int63n(int64(500 * time.Millisecond))))
			}
		}(i)
	}
	wg.Wait()
	log.Printf("simulation done: devices=%d reconnects=%d received=%d dupFiltered=%d acked=%d dupAcks=%d uplink=%d rejected=%d",
		*devices, st.reconnects.Load(), st.received.Load(), st.dupFiltered.Load(),
		st.acked.Load(), st.dupAcks.Load(), st.uplinkOK.Load(), st.uplinkRej.Load())
}
