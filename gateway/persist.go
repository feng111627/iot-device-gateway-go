package gateway

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// snapshotVersion 是持久化状态文件的格式版本。
const snapshotVersion = 1

// deviceState 是一台设备的完整持久化状态：会话 + 投递窗口 + 已消费去重 + 离线暂存。
// 文件中每行一条 JSON（JSON Lines），单行损坏只影响该设备，不牵连其他设备。
type deviceState struct {
	Version    int              `json:"version"`
	DeviceID   string           `json:"device_id"`
	Epoch      uint64           `json:"epoch"`
	Credential string           `json:"credential"`
	Subs       []string         `json:"subs"`
	NextSeq    uint64           `json:"next_seq"`
	QuotaUsed  int              `json:"quota_used"`
	Inflight   []Message        `json:"inflight"`
	Consumed   []consumedRecord `json:"consumed"`
	Offline    []Message        `json:"offline"`
	Evicted    int              `json:"evicted"`
	Attempts   map[string]int   `json:"attempts"`
}

// consumedRecord 是一条"已消费"去重记录，Seq 用于容量超限时按最旧顺序淘汰。
type consumedRecord struct {
	ID  string `json:"id"`
	Seq uint64 `json:"seq"`
}

// RestoreReport 描述一次重启恢复的可观测结果：恢复了多少、挡掉了多少、坏了多少。
type RestoreReport struct {
	DevicesRestored       int      // 成功恢复的设备数
	SubscriptionsRestored int      // 恢复的订阅条数
	InflightRestored      int      // 恢复的在途（未确认）消息条数
	OfflineRestored       int      // 恢复的离线暂存消息条数
	ConsumedRestored      int      // 恢复的"已消费"去重记录条数
	DevicesBlocked        int      // 因设备容量上限被挡下的设备数
	MessagesBlocked       int      // 因窗口/暂存/去重容量被挡下的消息条数
	CorruptRecords        int      // 损坏被跳过的记录行数
	FailedDevices         []string // 恢复过程中发生异常被隔离的设备
}

// MessagesRestored 返回恢复的在途 + 离线消息总数（便捷汇总）。
func (r *RestoreReport) MessagesRestored() int { return r.InflightRestored + r.OfflineRestored }

func (r *RestoreReport) String() string {
	return fmt.Sprintf(
		"恢复结果: 设备 %d 台 / 订阅 %d 条 / 在途 %d 条 + 离线 %d 条 / 已消费去重 %d 条；"+
			"容量挡下 设备 %d 台、消息 %d 条；损坏记录 %d 行；失败设备 %v",
		r.DevicesRestored, r.SubscriptionsRestored, r.InflightRestored, r.OfflineRestored, r.ConsumedRestored,
		r.DevicesBlocked, r.MessagesBlocked, r.CorruptRecords, r.FailedDevices,
	)
}

// writeStates 把全部设备状态原子写盘（临时文件 + fsync + rename）。
// 设备按 DeviceID 排序输出，使快照确定性可复现；进程在任何时刻被杀，
// 磁盘上要么是上一份完整快照、要么是这一份完整快照，不会出现半份文件。
func writeStates(path string, states []deviceState) error {
	var buf bytes.Buffer
	for i := range states {
		states[i].Version = snapshotVersion
		data, err := json.Marshal(&states[i])
		if err != nil {
			return err
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil { // 强杀/掉电也能保住已落盘的内容
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// readStates 逐行读取设备状态（JSON Lines）。
//   - 文件不存在：返回空结果、无错误（全新启动）；
//   - 单行损坏或字段缺失：跳过该行并计入 corrupt，其他行照常恢复，
//     个别设备的坏数据不会拖垮整批恢复。
func readStates(path string) (states []deviceState, corrupt int, err error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	states = make([]deviceState, 0)
	scanner := bufio.NewScanner(f)
	// 单行可能较大（离线积压），放宽扫描器上限。
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var st deviceState
		if jerr := json.Unmarshal(line, &st); jerr != nil || st.DeviceID == "" {
			corrupt++
			continue
		}
		states = append(states, st)
	}
	if serr := scanner.Err(); serr != nil && serr != io.EOF {
		return states, corrupt, serr
	}
	return states, corrupt, nil
}
