package gateway

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
)

// Request 是设备/客户端发往网关的协议帧（JSON 行）。
type Request struct {
	Type     string   `json:"type"` // connect|subscribe|ack|uplink|publish|disconnect
	DeviceID string   `json:"device_id,omitempty"`
	Token    string   `json:"token,omitempty"`
	Clean    bool     `json:"clean,omitempty"`
	Topics   []string `json:"topics,omitempty"`
	Seq      uint64   `json:"seq,omitempty"`
	Topic    string   `json:"topic,omitempty"`
	Payload  string   `json:"payload,omitempty"`
}

// Response 是网关回给客户端的协议帧（JSON 行）。
type Response struct {
	Type      string   `json:"type"` // connack|message|ackresult|ok|error
	Resumed   bool     `json:"resumed,omitempty"`
	Msg       *Message `json:"msg,omitempty"`
	Seq       uint64   `json:"seq,omitempty"`
	Duplicate bool     `json:"duplicate,omitempty"`
	Err       string   `json:"err,omitempty"`
}

// Server 在本机 TCP 端口上接受设备连接。
type Server struct {
	gw  *Gateway
	ln  net.Listener
	mu  sync.Mutex
	wg  sync.WaitGroup
}

func NewServer(gw *Gateway) *Server { return &Server{gw: gw} }

// ListenAndServe 监听地址并接受连接，直到 Close。
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.serveConn(conn)
		}()
	}
}

// Addr 返回监听地址（用于测试取随机端口）。
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// Close 关闭监听器并等待连接处理协程退出。
func (s *Server) Close() {
	s.mu.Lock()
	if s.ln != nil {
		s.ln.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()
	var writeMu sync.Mutex
	send := func(resp *Response) error {
		b, err := json.Marshal(resp)
		if err != nil {
			return err
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		_, err = fmt.Fprintf(conn, "%s\n", b)
		return err
	}

	var deviceID string
	connected := false
	defer func() {
		if connected {
			s.gw.Disconnect(deviceID, false) // 连接中断视为异常断开，会话保留
		}
	}()

	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		var req Request
		if err := json.Unmarshal(sc.Bytes(), &req); err != nil {
			_ = send(&Response{Type: "error", Err: "bad frame: " + err.Error()})
			continue
		}
		switch req.Type {
		case "connect":
			deviceID = req.DeviceID
			resumed, err := s.gw.Connect(req.DeviceID, req.Token, req.Clean, func(m *Message) error {
				return send(&Response{Type: "message", Msg: m})
			})
			if err != nil {
				_ = send(&Response{Type: "error", Err: err.Error()})
				continue
			}
			connected = true
			_ = send(&Response{Type: "connack", Resumed: resumed})
		case "subscribe":
			if err := s.gw.Subscribe(deviceID, req.Topics...); err != nil {
				_ = send(&Response{Type: "error", Err: err.Error()})
			} else {
				_ = send(&Response{Type: "ok"})
			}
		case "ack":
			dup, err := s.gw.Ack(deviceID, req.Seq)
			resp := &Response{Type: "ackresult", Seq: req.Seq, Duplicate: dup}
			if err != nil {
				resp.Err = classifyErr(err)
			}
			_ = send(resp)
		case "uplink":
			if err := s.gw.HandleUplink(deviceID, []byte(req.Payload)); err != nil {
				_ = send(&Response{Type: "error", Err: classifyErr(err)})
			} else {
				_ = send(&Response{Type: "ok"})
			}
		case "publish":
			if err := s.gw.Publish(req.Topic, []byte(req.Payload)); err != nil {
				_ = send(&Response{Type: "error", Err: classifyErr(err)})
			} else {
				_ = send(&Response{Type: "ok"})
			}
		case "disconnect":
			s.gw.Disconnect(deviceID, req.Clean)
			connected = false // 已显式断开，defer 不再重复处理
			_ = send(&Response{Type: "ok"})
		default:
			_ = send(&Response{Type: "error", Err: "unknown type: " + req.Type})
		}
	}
	if err := sc.Err(); err != nil {
		log.Printf("conn %s read error: %v", conn.RemoteAddr(), err)
	}
}

// classifyErr 把内部错误映射为可区分的原因字符串。
func classifyErr(err error) string {
	switch {
	case errors.Is(err, ErrWindowFull):
		return "window_full"
	case errors.Is(err, ErrQuotaExceeded):
		return "quota_exceeded"
	case errors.Is(err, ErrUnknownSeq):
		return "unknown_seq"
	case errors.Is(err, ErrSessionNotFound):
		return "session_not_found"
	case errors.Is(err, ErrSessionInvalid):
		return "session_invalid"
	default:
		return err.Error()
	}
}
