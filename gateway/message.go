package gateway

// Message 是一条下行（网关→设备）消息。
type Message struct {
	ID      string `json:"id"`
	Topic   string `json:"topic"`
	Payload []byte `json:"payload"`
	Seq     uint64 `json:"seq"` // 设备会话内单调递增序号
}

// Ack 是设备对某条下行消息的确认。
type Ack struct {
	Seq uint64 `json:"seq"`
}
