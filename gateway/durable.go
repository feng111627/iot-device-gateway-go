// 持久化层：每设备一条只追加事件日志（WAL）+ 周期性原子快照压缩。
// 所有状态变更先落盘再改内存（event sourcing），重启时通过"快照 + 事件重放"重建。
//
// 崩溃与可重入性：
//   - 每条事件写盘后 fsync，写完才改内存；崩溃在写盘前/中 => 事件不存在，内存也未生效；
//     崩溃在写盘后、内存修改前 => 重放时事件再生效一次，而所有事件的重放都是幂等的。
//   - 快照压缩 = 原子写快照（tmp+rename+fsync）后截断 WAL；
//     崩溃在截断前 => 重放时序号 <= 快照点的事件被跳过；崩溃在截断后 => 快照已就位。
//     因此压缩可反复、可中断，不会重复或损坏。
//   - 单个设备日志损坏只影响该设备：扫描到损坏记录即停止该设备的后续读取并计入统计，
//     其他设备照常恢复。
package gateway

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// 事件类型：一次状态变更对应一条只追加记录。
const (
	evConnect = "connect" // 会话建立（携带当前凭据）
	evSub     = "sub"     // 订阅
	evRotate  = "rotate"  // 平滑轮换凭据（同一设备、状态保留）
	evReset   = "reset"   // 换新身份（旧会话痕迹全部作废）
	evClear   = "clear"   // 主动清理会话（代次递增、订阅作废，但不碰在途/暂存）
	evAssign  = "assign"  // 消息进入确认窗口（在途）
	evAck     = "ack"     // 消息被确认消费
	evEnqueue = "enqueue" // 离线暂存入队
	evDrain   = "drain"   // 离线消息在上线补投时移出队列（按消息 ID）
	evQuota   = "quota"   // 周期配额重置
)

// eventRecord 是 WAL 中的一条记录（单行 JSON）。
type eventRecord struct {
	N       uint64   `json:"n"`              // 本设备日志内单调递增的事件序号
	Type    string   `json:"t"`              // 事件类型
	Cred    string   `json:"cred,omitempty"` // connect/rotate/reset 的凭据
	Topic   string   `json:"topic,omitempty"`
	Msg     *Message `json:"m,omitempty"`
	IDs     []string `json:"ids,omitempty"`  // drain 移出的消息 ID
	Attempt int      `json:"att,omitempty"`  // assign 时的已投递次数
	Used    int      `json:"used,omitempty"` // quota 事件的已用配额
}

// snapshotEnvelope 是快照文件的磁盘格式：截至 N 的全量状态 + 不透明业务负载。
type snapshotEnvelope struct {
	N     uint64          `json:"n"`
	State json.RawMessage `json:"state"`
}

// recoveredDevice 是单台设备从磁盘读出的原始恢复素材（解释权在编排层）。
type recoveredDevice struct {
	deviceID    string
	snapN       uint64
	state       json.RawMessage // 快照中的业务状态；nil 表示无快照
	events      []eventRecord   // 序号大于 snapN 的 WAL 事件（已排序）
	corrupt     int             // 放弃的损坏/撕裂记录数
	torn        bool            // 仅尾部一条撕裂记录（写一半崩溃），已安全丢弃，设备可恢复
	snapCorrupt bool            // 快照损坏（WAL 已截断前缀丢失，只能隔离）
	walCorrupt  bool            // WAL 中部损坏或存在序号缺口（无法保证不丢），必须隔离
}

// RecoverStats 是一次恢复的可观测结果（恢复了多少、丢了多少、挡掉了多少）。
type RecoverStats struct {
	DevicesRecovered    int      // 成功恢复的设备数
	DevicesSkippedLimit int      // 因达到容量边界未恢复的设备数
	DevicesFailed       int      // 状态损坏、恢复失败被隔离的设备数
	EventsReplayed      int      // 重放的事件总数
	OfflineRecovered    int      // 恢复的离线暂存消息数
	InflightRecovered   int      // 恢复的在途未确认消息数
	ConsumedRetained    int      // 恢复后保留的"已消费"去重记录数
	CorruptRecords      int      // 损坏记录条数
	FailedDevices       []string // 恢复失败被隔离的设备
	SkippedDevices      []string // 因容量被挡住的设备
}

// deviceLog 是单设备日志在内存中的写入游标。
type deviceLog struct {
	mu        sync.Mutex
	n         uint64 // 最近事件序号
	snapN     uint64 // 快照点序号
	unsnapped int    // 快照之后写入的事件数（用于触发压缩）
}

// durableStore 按设备维护 WAL 与快照文件。dir=="" 表示纯内存、不持久化。
type durableStore struct {
	dir        string
	maxDevices int
	compactAt  uint64

	mu    sync.Mutex
	logs  map[string]*deviceLog
	known int // 磁盘上存在日志的设备数（含扫描结果），用于运行期容量边界
}

func newDurableStore(dir string, maxDevices, compactEvery int) *durableStore {
	return &durableStore{
		dir:        dir,
		maxDevices: maxDevices,
		compactAt:  uint64(compactEvery),
		logs:       make(map[string]*deviceLog),
	}
}

func encodeDeviceID(deviceID string) string {
	return hex.EncodeToString([]byte(deviceID))
}

func (s *durableStore) walPath(deviceID string) string {
	return filepath.Join(s.dir, encodeDeviceID(deviceID)+".wal")
}

func (s *durableStore) snapPath(deviceID string) string {
	return filepath.Join(s.dir, encodeDeviceID(deviceID)+".snap")
}

// scan 读取目录下所有设备的快照与 WAL（按设备 ID 排序，结果确定）。
// 超出 maxDevices 的设备进入 skipped；单设备损坏只标记该设备，不影响其他设备。
func (s *durableStore) scan() ([]recoveredDevice, []string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	stemSet := make(map[string]bool)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		var stem string
		switch filepath.Ext(name) {
		case ".wal":
			stem = name[:len(name)-len(".wal")]
		case ".snap":
			stem = name[:len(name)-len(".snap")]
		default:
			continue
		}
		stemSet[stem] = true
	}
	stems := make([]string, 0, len(stemSet))
	for stem := range stemSet {
		stems = append(stems, stem)
	}
	sort.Strings(stems)

	var out []recoveredDevice
	var skipped []string
	s.mu.Lock()
	defer s.mu.Unlock()
	for idx, stem := range stems {
		raw, derr := hex.DecodeString(stem)
		if derr != nil {
			continue // 非本系统文件，忽略
		}
		deviceID := string(raw)
		if s.maxDevices > 0 && idx >= s.maxDevices {
			skipped = append(skipped, deviceID)
			continue
		}
		rd := recoveredDevice{deviceID: deviceID}

		if data, rerr := os.ReadFile(s.snapPath(deviceID)); rerr == nil {
			var env snapshotEnvelope
			if jerr := json.Unmarshal(data, &env); jerr != nil {
				// 快照损坏且压缩时 WAL 前缀已被截断，无法完整恢复，隔离该设备
				rd.snapCorrupt = true
			} else {
				rd.snapN = env.N
				rd.state = env.State
			}
		} else if !os.IsNotExist(rerr) {
			return nil, nil, rerr
		}

		if data, rerr := os.ReadFile(s.walPath(deviceID)); rerr == nil {
			log := &deviceLog{snapN: rd.snapN, n: rd.snapN}
			// 按换行符分割：最后一段若非空且文件不以 '\n' 结尾，是崩溃时写了一半的
			// "撕裂记录"——该写从未提交，可安全丢弃；若问题出现在记录中部或序号不连续，
			// 则属于真正的日志损坏，无法保证"不丢"，标记该设备隔离。
			tailIncomplete := len(data) > 0 && data[len(data)-1] != '\n'
			lines := bytes.Split(data, []byte{'\n'})
			if len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
				lines = lines[:len(lines)-1] // 末尾换行产生的空段
			}
			expectedN := rd.snapN
			validPrefix := 0 // 已确认完好的记录累计字节数（含换行）
			for idx, rawLine := range lines {
				line := bytes.TrimSpace(rawLine)
				isTail := idx == len(lines)-1
				if len(line) == 0 {
					if !isTail {
						validPrefix += len(rawLine) + 1
					}
					continue
				}
				var rec eventRecord
				jerr := json.Unmarshal(line, &rec)
				// 序号连续性只对"快照点之后"的事件强制；<= snapN 的事件已包含在快照中
				// （压缩在 WAL 截断前崩溃会留下它们），跳过即可。
				bad := jerr != nil || rec.N == 0 ||
					(rec.N > rd.snapN && (rec.N != expectedN+1 || rec.N <= log.n))
				if bad {
					if isTail && tailIncomplete {
						rd.torn = true // 写一半崩溃：最后一条未提交，丢弃即可，设备可恢复
						rd.corrupt++
						break
					}
					rd.walCorrupt = true // 中部损坏/缺号：完整性无法保证，隔离
					rd.corrupt++
					break
				}
				validPrefix += len(rawLine) + 1
				if rec.N > rd.snapN {
					expectedN = rec.N
					log.n = rec.N
					rd.events = append(rd.events, rec)
				}
			}
			if rd.torn && validPrefix >= 0 {
				// 就地截掉未提交的残片，保证后续 append 不会粘在残片之后；
				// 截断本身幂等，截断后再次崩溃也只会重放出同样的好记录。
				if terr := os.Truncate(s.walPath(deviceID), int64(validPrefix)); terr != nil {
					rd.walCorrupt = true
				}
			}
			log.unsnapped = len(rd.events)
			s.logs[deviceID] = log
			out = append(out, rd)
		} else if os.IsNotExist(rerr) {
			// 只有快照（压缩后 WAL 为空后被删除的极端情形不会发生，此处保持容错）
			s.logs[deviceID] = &deviceLog{n: rd.snapN, snapN: rd.snapN}
			out = append(out, rd)
		} else {
			return nil, nil, rerr
		}
		s.known = len(s.logs) + len(skipped)
	}
	return out, skipped, nil
}

func (s *durableStore) cursor(deviceID string) *deviceLog {
	log := s.logs[deviceID]
	if log == nil {
		log = &deviceLog{}
		s.logs[deviceID] = log
	}
	return log
}

// resetFiles 删除设备的历史 WAL 与快照并把序号游标归零（换新身份/解除隔离用）。
// 调用方随后追加的事件从 n=1 开始，等于一条全新的设备日志；旧数据不可达。
func (s *durableStore) resetFiles(deviceID string) error {
	s.mu.Lock()
	s.logs[deviceID] = &deviceLog{}
	s.mu.Unlock()
	if s.dir == "" {
		return nil
	}
	if err := os.Remove(s.walPath(deviceID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(s.snapPath(deviceID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// append 向设备日志追加一条事件（fsync 落盘），返回事件序号与快照后事件数。
// 运行期新建设备日志同样受 maxDevices 容量边界约束。
func (s *durableStore) append(deviceID string, rec eventRecord) (uint64, int, error) {
	s.mu.Lock()
	log, exists := s.logs[deviceID]
	if !exists {
		if s.maxDevices > 0 && s.known >= s.maxDevices {
			s.mu.Unlock()
			return 0, 0, newError(ErrCodeDeviceLimit, "设备数已达恢复容量边界 %d，拒绝为 %s 新建日志", s.maxDevices, deviceID)
		}
		log = &deviceLog{}
		s.logs[deviceID] = log
		s.known++
	}
	s.mu.Unlock()

	log.mu.Lock()
	defer log.mu.Unlock()
	log.n++
	rec.N = log.n
	data, err := json.Marshal(rec)
	if err != nil {
		log.n--
		return 0, 0, err
	}
	data = append(data, '\n')

	if s.dir != "" {
		f, err := os.OpenFile(s.walPath(deviceID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			log.n--
			return 0, 0, err
		}
		if _, err := f.Write(data); err != nil {
			f.Close()
			log.n--
			return 0, 0, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			log.n--
			return 0, 0, err
		}
		if err := f.Close(); err != nil {
			log.n--
			return 0, 0, err
		}
	}
	log.unsnapped++
	return log.n, log.unsnapped, nil
}

// compact 用全量状态快照压缩设备日志：原子写快照后截断 WAL。
// 任何一步崩溃都安全：重放对"快照点之前的事件"按序号跳过，WAL 截断幂等。
func (s *durableStore) compact(deviceID string, n uint64, state json.RawMessage) error {
	if s.dir == "" {
		return nil
	}
	log, ok := s.logs[deviceID]
	if !ok {
		return fmt.Errorf("compact 未知设备 %s", deviceID)
	}
	log.mu.Lock()
	defer log.mu.Unlock()

	env := snapshotEnvelope{N: n, State: state}
	data, err := json.Marshal(&env)
	if err != nil {
		return err
	}
	snap := s.snapPath(deviceID)
	tmp := snap + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := os.Rename(tmp, snap); err != nil {
		return err
	}
	if df, err := os.Open(s.dir); err == nil {
		_ = df.Sync()
		_ = df.Close()
	}

	// 快照就位后截断 WAL；即便没截成，重放也会按序号去重。
	wf, err := os.OpenFile(s.walPath(deviceID), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := wf.Sync(); err != nil {
		wf.Close()
		return err
	}
	if err := wf.Close(); err != nil {
		return err
	}
	log.snapN = n
	log.unsnapped = 0
	return nil
}
