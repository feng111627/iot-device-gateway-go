package main

import (
	"flag"
	"log"

	"github.com/feng111627/iot-device-gateway-go/gateway"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:1883", "listen address")
	window := flag.Int("window", 32, "per-device ack window")
	offlineCap := flag.Int("offline-cap", 1000, "per-device offline queue capacity")
	uplinkQuota := flag.Int("uplink-quota", 100, "per-device uplink quota per second")
	state := flag.String("state", "gateway-state.jsonl", "state persistence path (empty to disable)")
	flag.Parse()

	gw := gateway.New(gateway.Config{
		AckWindow:       *window,
		OfflineCapacity: *offlineCap,
		UplinkQuota:     *uplinkQuota,
		StatePath:       *state,
	})
	gw.SetEvictHandler(func(dev string, m *gateway.Message) {
		log.Printf("[evict] device=%s seq=%d topic=%s (offline queue full, oldest dropped)", dev, m.Seq, m.Topic)
	})
	srv := gateway.NewServer(gw)
	log.Printf("gateway listening on %s", *addr)
	if err := srv.ListenAndServe(*addr); err != nil {
		log.Fatal(err)
	}
}
