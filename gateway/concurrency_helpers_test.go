package gateway

import (
	"os"
	"path/filepath"
)

// writeJunkSnapshot 在设备快照文件写入损坏内容（隔离恢复路径用）。
func writeJunkSnapshot(stateDir, deviceID string) error {
	p := filepath.Join(stateDir, encodeDeviceID(deviceID)+".snap")
	return os.WriteFile(p, []byte("{this-is-not-json"), 0o644)
}
